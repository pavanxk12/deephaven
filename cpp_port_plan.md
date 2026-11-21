# Port `algo` to C++: one native host + strategies as `.so` plugins

## Context

Today everything runs as Python inside one Deephaven session. That includes master, candles, straddle, history (`absolute`/`relative`), EOD, the registry/book/bridge, the NATS service, limits/guard, the day schedule and the console, with the strategy scripts in `examples/` pasted into that same session. `tech.md` §8 names the ceiling: **all of it shares one Python thread**. On the India host, candle batches take 0.4–0.9 s against a 250 ms budget, and every strategy listener and console callback queues behind them.

Goal: rewrite all of `algo/` in C++ as one standalone process (`algod`). Strategies become `.so` files built against a small SDK and loaded at runtime, the same pattern as the engine's `StrategySDK.h` / [examples/CrossOrder_plugin.cpp](examples/CrossOrder_plugin.cpp).

Decisions taken:
- **Feed input: switchable `FeedSource`** (config `feed.source = kafka | deephaven`):
  - `KafkaFeed` (librdkafka) consumes `hft.marketdata.fo` directly. It is the low-latency path: every trade, exact OHLC, Open/High/Low not sampled.
  - `DhBarrageFeed` subscribes to `fo_market_data` through the DH C++ client: today's behaviour, about 1 s cycles, last value per token.
  - Both decode into the same 64 B (one cache line) `Tick` and push into the same shard rings, so nothing downstream knows which source is in use. Volume comes from cumulative `vol` in both, so bars agree on Close and Volume, while Kafka gives the true High and Low. Kafka needs a deduplicating offset: on restart, replay from the day's first offset to rebuild today's bars (the cutoff is the mmap day file's last minute).
- **Data path:** `algod` also uses the **Deephaven C++ client** for `orders_trade` / `orders_ack` / `strategy_events` (limits). It computes natively and **writes `candles`, `straddle_series`, `nats_events`, `portfolio_state_log`, `portfolio_registry`, `strategy_limits` back** as Deephaven tables. Deephaven stays the feed, the store and the place you look at data.
- **No UI in this project.** The backend exposes a complete, versioned **API** (every operator action plus live state streams). The frontend team builds the UI on it later. `algoctl` (CLI) is just the first client of that same API.

Constraint that shapes everything: the Deephaven C++ client is a *client* (Arrow Flight / Barrage). It cannot run inside the session like Python does, so `algod` is its own process/container beside Deephaven.

---

## Architecture

```
            Deephaven server (unchanged: Kafka -> fo_market_data, orders_trade, orders_ack ...)
                 ▲  publish (Arrow DoPut / input tables)        │  Barrage subscribe (ticking)
                 │                                              ▼
┌──────────────────────────────── algod (C++20, one process) ──────────────────────────────────┐
│ core:   App / Plugin lifecycle · Config(Market, Paths) · EventBus · log · statedb (SQLite/PG) │
│                                                                                                │
│ FeedSource(Kafka|DH) ─► Master (.db3/PG) ─► CandleAggregator (sharded, SPSC rings, mmap day file)│
│                                                       │ CandleBatch (closed bars)              │
│                       ┌───────────────┬──────────────┼──────────────┬──────────────────┐      │
│                       ▼               ▼              ▼              ▼                  ▼      │
│                 StraddleEngine   TablePublisher   EodStore      HistoryService    StrategyHost │
│                 (+ sampler)      (-> DH tables)   (parquet)     absolute/relative  dlopen *.so │
│                                                                  (.hday mmap)      1 ring/strat │
│                                                                                       │        │
│  Registry ◄── Book (per strategy) ◄── Bridge (swap/retire/exit sequences) ◄───────────┘        │
│                                          │                                                     │
│  PfStates ◄──ok── NatsService (cnats: lane per (user,pf), max_in_flight, switches, blocks)     │
│  Limits/Guard ◄── trades subscribe (turnover, M2M net of expense)                              │
│  DaySchedule (master 09:00, candles 09:14:30, close 15:30)    Api: NATS + HTTP/WS + StateHub   │
└──────────────────────────────────────────────┬────────────────────────────────────┬────────────┘
                                               ▼ cmd.hft.<pf> (request/reply, same JSON) │
                                      engine / order bridge (unchanged)   algoctl / frontend
```

**Threads:** this is where the GIL problem goes away.
- `feed`: Barrage callback that only decodes the Arrow batch and pushes it to shard queues.
- `candle-k` (N shards, `token % N`): each shard owns its tokens' open bars, so there are no locks.
- `publisher`: batches rows to DH every flush.
- `straddle`: sampler.
- `nats`: one event loop, same lane design as today.
- One worker per strategy: a slow strategy can never delay candles or another strategy.
- `schedule`, `api` (NATS + HTTP/WS gateway, StateHub).

**Rules kept from the Python design:**
- Services are independent: no service includes another, and each is found by name through `App`. Enforced by CMake target deps: `core` ← `data` ← `series`; `comms` and `registry` link only `core`.
- A command is never resent, and a refusal is a status, not an exception.
- Atomic file writes (temp + rename).
- One place for each fact.

### Low-latency design (hot path: feed → candle → strategy → NATS)

The standard HFT recipe: no locks, no allocation, no syscalls on the hot path, and memory sized up front.

**Transport: SPSC rings everywhere, SHM where it crosses a process.**
- **In-process:** single-producer/single-consumer lock-free rings (power-of-two capacity, cache-line-padded head/tail on separate 64 B lines, `acquire`/`release` atomics only, batch pop). Topology: `feed → shard[k]` (one ring per candle shard), `shard[k] → strategy[j]` (one ring per pair, so every ring stays SPSC and needs no CAS), `strategy[j] → nats_inbox[j]` (one SPSC ring per strategy instance, polled by the nats thread). **Every fan-in is a set of SPSC rings, one per producer, never an MPSC queue**, so the whole path from tick to order is wait-free (see Threads and synchronisation).
- **Cross-process (SHM):** the same ring layout over `shm_open` + `mmap` (`/dev/shm/algo.<topic>`, `MAP_POPULATE`, optional `MAP_HUGETLB`), seqlock-versioned slots, one writer and many readers that each keep their own cursor (broadcast ring, Aeron/Chronicle-style). Used for (a) **candles and ticks out of algod**, so any number of readers (a Python research process, a recorder, the DH publisher) attach without touching the hot path, and (b) **optional out-of-process strategies**. A `.so` can run inside `algod` (fastest) or inside `algo-runner` attached to the SHM rings (crash isolation, at a cost of ~100–300 ns). Both use the same SDK and the same binary `.so`.
- Wait strategy is configurable per consumer: busy-spin on a pinned core (lowest latency), or spin N iterations then futex wait (saves a core).

**Memory: bounded, preallocated, nothing on the heap per tick.**
- **POD messages, fixed size:** `Tick` 64 B (token, ltp, bid, ask, bid/ask qty, ltq, cum vol, ltt, tsc) and `Bar` 64 B (`int32 token`, `int32 minute`, int64 o/h/l/c in minor units, `int64 vol`, `int64 cvd`, tsc). Both are exactly one cache line. Prices are int64 minor units on the hot path, never `double`+string. Symbol text is never carried: it's a token-indexed lookup.
- **Token-indexed flat arrays** instead of hash maps: `open_bar[token]`, `scale[token]`, `allowed[token]`, sized `max_token+1` once from the master. About 85k × 64 B ≈ 5.5 MB of open-bar state, which fits in L2/L3 per shard.
- **Today's bars live in an mmap'd columnar day file** (`/data/algo/day/<date>.bars`, one fixed slot per (token, minute)), not in RSS-owned vectors. 85k tokens × 375 min × 24 B ≈ 770 MB *on disk / page cache*. The kernel pages it in and out, so resident memory is only what is read. `last_candles`, `absolute`/`relative` "today" and EOD read straight from it with no copy. EOD parquet is a conversion of this file. This replaces today's 1.8 GB Deephaven candle table as the source of truth. DH only gets a published copy for viewing, and that copy can be filtered (`expiries=1`) without affecting strategies.
- Arenas/pools for everything else. `std::pmr` monotonic per cycle for decode scratch. The history LRU keeps its byte budget (200 MB default).
- No `std::string`, `shared_ptr` or `std::function` on the hot path, and no logging on the hot path either: an SPSC log ring feeds a background logger thread (binary log records, formatted off-thread).

**CPU:**
- Thread-per-core with `pthread_setaffinity_np`. Isolate cores (`isolcpus`/`nohz_full` on the host) for `feed`, `candle-k` and the latency-critical strategies; everything else (publisher, history, schedule, control) goes on housekeeping cores.
- NUMA-local allocation (`numa_alloc_onnode`) for the rings of a pinned thread.
- Build with `-O3 -march=native`, LTO and PGO from a recorded day, `-fno-exceptions` in hot TUs (errors are status codes, which matches the Python "nothing raises" rule).

**Latency budget (target, measured with `rdtsc` stamps carried in each message):** feed decode → bar close → strategy callback < 5 µs p99 in-process (< 10 µs via SHM). Strategy → NATS publish < 20 µs. The network round trip to the engine is then the dominant cost.

**Instrumentation:** each message carries `t_feed` / `t_bar` / `t_strategy` / `t_send` TSC stamps. HDR histograms per stage are exposed through `algoctl stats`, with no locks (per-thread histograms merged on read).

### Shared memory, the double-mapped ring, zero copy

**One SHM region per day**, `/dev/shm/algo.<date>`: huge pages, `MAP_POPULATE`, `mlock`, laid out once at startup from the master.
- Contents: header (version, offsets, checkpoint) · `Contract[token]` · tick ring · bar ring · `quote[token]` · `open_bar[token]` · `analytics[series][symbol]` · per-strategy command rings.
- Every part is a fixed POD array. **Nothing is allocated after startup.**

**The double-mapped ring:**
- **Layout:** the ring's physical pages are mapped twice, back to back, so any message or batch is one contiguous span, even across the wrap.
- **Writer:** each ring has one writer. It `claim()`s a slot, constructs the message in place, then `publish()`es with one release-store of the head.
- **Readers:** each has its own cursor and reads in place. 64-bit ever-growing indexes are masked into the ring, so there's no ABA problem.
- **Lapped readers:** a reader that falls a whole ring behind gets `lapped` and resyncs from `quote[]` / the day file. It never reads a slot that is being overwritten.

**Pointers:** raw pointers are fine within a process. **Across processes, SHM stores offsets, never raw pointers** (`shm_ptr<T>` = base + offset). Long-lived data (master, today's bars, books) lives in fixed arrays that never move, so pointers into them stay valid all day.

**Copies only at the edges:**
- Kafka → `Tick`: one decode, written directly into the slot.
- Command → NATS: JSON into a preallocated buffer.
- algod → DH / API: serialized off the hot path.

The rest of the pipeline makes no copies and no allocations.

### Many readers of the same data

- **`quote[token]`**: the latest bid/ask/qtys/ltp/ltt/vol, exactly one 64 B line per token, guarded by a seqlock. A read is ~10–20 ns, never torn, and needs no lock. `Tick` is 64 B so it can carry the quote fields; spike 0 confirms which fields the feed has.
- **Tick ring and bar ring**: broadcast, any number of readers. The writer never waits.
- **Analytics computed once:**
  - Strategies register what they need (`host.want(series, level, ref, basket, n)`), and the Analytics service merges the requests.
  - Each minute, as the bars close, it computes now, the absolute rank/percentile and the relative rank/percentile for pct (open / vwap / prev_close / Nmin / fixed time), turnover, volume (rvolume) and IV.
  - It writes them to `analytics[...]` and publishes **one version per minute**, so every strategy sees the same consistent snapshot of a basket.
  - Strategies are then notified and read the results in place. A one-off `absolute()` / `relative()` with other parameters is cached per (series, level, ref, n, minute).

### Cache and memory layout rules

**Cache lines:**
- Every hot struct is `alignas(64)` and sized to 64 or 32 B (two per line), checked with `static_assert(sizeof(T) == 64)` and `std::is_trivially_copyable`.
- Ring head and tail sit on separate lines (no false sharing), with the cached copy of the other side's index on the reader's/writer's own line.
- Per-thread counters and histograms are padded to their own lines.

**Hot/cold split and SoA:**
- Fields touched per tick (`open_bar`: o/h/l/c, cum vol, cvd, last ltt) are kept apart from cold contract metadata (symbol text, expiry, lot), which is read only on lookups.
- Analytics and history arrays are structure-of-arrays (`close[slot]`, `cum_pv[slot]`, `cum_v[slot]`) so scans are sequential and SIMD-friendly (AVX2 for rank/percentile).

**Indexing:**
- Token → dense index once (from the master), then flat arrays everywhere.
- No hash maps or pointer chasing on the hot path. Open-addressing tables only where the keys are sparse (symbol → index, off the hot path).

**Allocation:**
- Zero allocations after startup on hot threads. A test-build hook on `operator new` fails the test if a hot thread allocates.
- Per-cycle scratch is a monotonic arena reset each batch. Everything else is preallocated pools.

**Pages:**
- 2 MB huge pages for the SHM region and the rings, to cut TLB misses.
- `MAP_POPULATE` + `mlock` so there are no page faults during the session.
- The day file and `.hday` history are paged by the OS with `madvise` (`WILLNEED` for warm sets, `SEQUENTIAL` for EOD scans).

**NUMA:** the rings and arrays of a pinned thread are allocated on its node. Cross-node traffic is only for broadcast readers.

**Prefetch and branches:**
- `__builtin_prefetch` on the next batch's `open_bar[token]` lines while the current tick is processed.
- `[[likely]]` / `[[unlikely]]` on validity checks, and branch-free min/max for high/low.

**Number widths:**
- Prices are int64 minor units (paise/cents) on the hot path. They are converted to double only at the edges, which also removes the float-noise rounding Python needed.
- History is float32 (as today), volume int64.

**Verification:**
- `perf stat` (cache misses, TLB misses, IPC) on a replayed day in CI, compared against a baseline.
- `pahole` checks of struct layout.
- The allocation-fail hook described under Allocation.

**Extra layout rules:**
- **`HotTokenMeta[dense_id]`** holds only what the tick loop touches: `scale_mul`/`scale_div` (fixed-point), `flags` (allowed, kind, has_cvd) and `shard_id`, two entries per cache line. Symbol text, expiry, strike and lot stay in the cold `Contract[]` table, so a tick never pulls a cold line into L1/L2.
- **Ring sizing:** capacity is a power of two, and the byte size is a **multiple of the page size** (2 MB when on huge pages), so the double mapping lines up. The ring is sized to absorb the worst burst seen in a recorded day × 4 between two consumer polls (measured in spike 0), so a spin-then-futex consumer does not lap.
- **mlock scope:** only the live SHM region (quotes, open bars, rings, analytics) is locked. The day file and `.hday` history are never `mlock`ed in bulk: the page cache manages them, with `MADV_WILLNEED` for the active look-back window only.

### Arithmetic and lookup rules (hot path)

**No division and no modulo on the hot path.**
- **Ring index:** `seq & (cap - 1)` (capacity is a power of two).
- **Shard:** `dense_id & (N - 1)` with N a power of two, never `token % N`.
- **Minute bucket:**
  - The LTT parser produces `minute_of_day` directly from the digits (`HH*60 + MM`), so `epoch / 60` is never computed.
  - Bar sizes of 30 s / 1 / 5 / 15 min become a precomputed `minute_to_bucket[1440]` table (one load).
- **Session slot:** `slot = minute_of_day - session_start`, which is a subtraction plus a bounds check, never `Clock` arithmetic.
- **Price scale:**
  - Prices stay int64 minor units (paise/cents) all the way to strategies, so **there is no scaling on the hot path at all**.
  - A contract whose divisor differs (US options 10000) carries a `shift`/`mul` in `HotTokenMeta`. Conversion to rupees/dollars happens only at the edges (API, DH, JSON).
- **Percent, VWAP and ranks** (analytics, once per minute, not per tick):
  - Percent change uses multiply-by-reciprocal: `(px - ref) * inv_ref`, with `inv_ref` computed once when the reference is set.
  - VWAP keeps running `cum_pv` / `cum_v`, and the single divide happens at read time.
  - Percentile rank is a SIMD compare-and-count (`_mm256_cmp_ps` + `popcnt`) over the n history values, with no sort.
- **Constants** that still divide are compile-time, so the compiler turns them into multiply-shift.

**Bit packing:**
- Per-token flags (`allowed`, `kind`, `has_cvd`, `is_option`, `is_future`, `stale`) live in one `uint8_t`/`uint16_t`, tested with masks.
- `Leg`, `Side`, `OptType` and `Status` are `uint8_t` enums.
- Universe membership and "which tokens changed this cycle" are bitsets (`uint64_t` words): iterate with `std::countr_zero` and clear with `w &= w - 1`.
- Doorbells are bitmasks, as described under Threads and synchronisation.

**No hashing on the hot path:**
- **token → dense index:** done **once, by the feed thread at decode**. Every later stage uses `dense_id` as a plain array index. `TokenIndex` picks a direct array `dense_of[token]` (one load) when tokens are dense, or a perfect hash when they are sparse or huge (US vendor ids). See the edge cases below.
- **symbol → index** (API, config, baskets, which are off the hot path): a **perfect hash built once from the master** (CHD/BDZ, or a sorted array + binary search), with no collisions and no probing. Strategies resolve symbols to dense ids in `onStart`, and from then on they use ids only.
- **(user, pf) → lane:** pf numbers are dense per user (from 1001), so `lane[user_idx][pf - pf_base]` is a direct index. A small open-addressed table is a fallback for sparse imported numbers.

**Edge cases these rules must handle:**
- **`TokenIndex` (token → dense id), chosen once per day from the master:**
  - Direct array if `max_token ≤ 4 × count`, otherwise a perfect hash (CHD/PTHash) over the master's tokens.
  - Every lookup is bounds-checked (`token > max_token` counts as a miss) before any array index.
  - **Tokens not in the master** keep Python's behaviour: they still get candles, with a null symbol and the default scale, and are dropped only when a `symbols=`/`kind=` filter is on. The feed thread (their only writer) gives them dense ids from a **preallocated overflow range** (e.g. 4,096 ids, through a small open-addressed table), so there is no allocation. When the range is full, they are dropped and counted.
  - Spike 0 records `max_token`, the count and the overflow hits for NSE and US.
- **Minute validation before any table index:**
  - The parser yields `minute_of_day` in the **exchange's local time**. NSE `ltt` is already local. **US `ltt` is UTC text**, so the day's UTC→New York offset (DST-aware) is computed once per day and applied before `HH*60+MM`.
  - The date part must equal the session day. A wrong date, an unparsable string or a minute outside `[0, 1440)` means the tick is dropped and counted, and no index is ever formed from an unvalidated value.
  - A tick for an earlier bar than the token's open one is ignored, as in Python.
- **Master swap and dense-id stability:**
  - MasterData, `TokenIndex`, the symbol perfect hash and the option chains are **one immutable snapshot behind one pointer**, swapped by RCU. A reader can never see a half-built index.
  - **Dense ids are stable for the whole day:** a mid-day reload may only *append* new tokens (new ids at the end), never renumber, so ids that strategies resolved in `onStart` stay valid.
  - A new day means a new SHM region and fresh ids.
- **SIMD rank with missing values (same semantics as Python `absolute`/`relative`):**
  - A missing value is **always NaN, never 0.0**: a holiday or missing day file, a minute beyond `max_age` carry (5; IV 3), or a stale symbol. A real 0 is a valid value.
  - `n_valid = popcnt(cmp(x, x, _CMP_ORD_Q))`, and `idx = popcnt(cmp(x, now, _CMP_LT_OQ))`. Ordered-quiet compares make NaN count as neither valid nor below. Strict `<` means ties are not counted, as today.
  - `idx = -1` (and the percentile is NaN) when `now` is NaN or `n_valid < min_valid`.
  - Tail lanes past n are filled with NaN, so they mask out the same way.
  - A golden test checks the SIMD result against Python on the saved days, including holidays, an illiquid symbol and ties.

**Parsing and formatting (the edges, made cheap):**
- **LTT text:** fixed-format, digit arithmetic (`c - '0'`), SWAR/SIMD for the whole timestamp. The date part is cached per day, so only `HH:MM:SS` is parsed per tick. No `strptime`.
- **Kafka JSON** (if the topic is JSON): `simdjson` on-demand with only the needed fields. If the producer can switch to a binary schema, decode becomes a fixed-offset load.
- **NATS JSON out:** a precompiled template per command type with fixed offsets. Numbers are written with a branchless `itoa`/fixed-point formatter (prices are ints, so there is no float formatting), with no `snprintf` and no allocation.

**Branches and calls:**
- High/low are `std::min`/`std::max` (cmov), and validity checks are branch-free masks where the batch is vectorised.
- The host classes are `final`, so their calls are devirtualised. A strategy is called once per **batch**, not per tick, so the cost of the one virtual call per callback is negligible.
- Clock reads use `rdtsc` (calibrated once), never `clock_gettime` per message.

**Network:** `SO_BUSY_POLL` / `SO_PREFER_BUSY_POLL` on the Kafka and NATS sockets on the pinned cores, and NIC IRQs steered to housekeeping cores.

**Rule: measure before micro-optimising.** Every one of these must show up in the per-stage HDR histograms or in `perf` (cycles, IPC, cache misses) on a replayed day. One that doesn't is dropped rather than kept for show.

### Threads and synchronisation

**Rule: every piece of state has exactly one owner thread.** Others send it a message (a ring) or read a published snapshot (a seqlock or epoch). Nothing is written by two threads, so the hot path has no locks. This replaces today's `RLock` in `Portfolios`, which strategy threads, the bridge and the UI all wrote to.

| thread | owns | core |
|---|---|---|
| `feed` | decode, `quote[]`, tick ring | isolated, spin |
| `candle-k` (`token % N`) | `open_bar[]`, its day-file slots, bar ring | isolated, spin |
| `analytics` | `analytics[]`, published per minute | isolated |
| `strategy-j` (one per instance) | its own book, its indicators | isolated or housekeeping |
| `nats` | lanes, cap, switches, blocks, `PfStates` | housekeeping |
| `registry`, `persist`, `pnl`, `straddle`, `publisher`, `api`, `timer`, `schedule`, `log` | their own state | housekeeping |

**The primitives:**
- **SPSC / broadcast ring:** one writer, one or many readers.
- **Fan-in = one SPSC ring per producer, polled by the owner.** This applies to strategies → nats, API/Guard/schedule → nats, API → each strategy's inbox, every thread → persist and every thread → StateHub. There is no MPSC queue anywhere. A bounded MPSC queue has a preemption gap: a producer preempted between claiming and publishing a slot stalls the consumer at that slot. Per-producer SPSC rings remove that gap, and every handoff is wait-free.
  - **Doorbell:** a producer sets its bit in the owner's `std::atomic<uint64_t> ready` with `fetch_or` (one wait-free instruction, `lock or` on x86) after publishing. The owner exchanges the mask to 0 and drains only the rings whose bits are set, so an idle poll is one load and not a scan of 32 rings. Owners with more than 64 producers use a two-level mask.
  - **Command records are fixed-size POD**, written straight into the ring slot: `{seq, user, pf, cmd, option, params_index, client_id, tsc}`. `params` sit in a per-strategy preallocated slot pool and are referenced by index. The strategy thread does no JSON, strings or `std::function`. The nats thread stamps `USERIDENTIFIER`/`user_id`/`SENDER`, serializes JSON into a preallocated per-flight buffer, and does cnats, `PfStates` and audit, all off the strategy thread.
  - **A full ring returns `full` at once**: no spin, no wait. It counts as a refusal in `nats_events`, as today.
  - **Lane state stays on the nats thread alone:** the per-(user, pf) FIFO lanes, `max_in_flight`, `lane_depth`, `max_queued`, the master and per-client switches, blocks and the "never resend" rule. Draining rings in a fixed order keeps each producer's order. The (user, pf) order holds across producers because a portfolio belongs to one book, and an operator command for it enters the lane in arrival order on the nats thread.
- **Seqlock:** latest value (quotes, analytics, `open_bar` read by the API).
- **Epoch/RCU pointer swap:** immutable tables (master, live-changed config). A new snapshot is published by atomic release-store. The old one is freed only after every reader thread has passed a quiescent point (an end of callback).
- **Spin-then-futex/eventfd** when a consumer is not on a dedicated core.

**Progress guarantees, component by component:**

| piece | guarantee |
|---|---|
| SPSC ring, per-producer fan-in, doorbell `fetch_or` | wait-free (producer and consumer) |
| Broadcast SHM ring | writer and readers wait-free; a slow reader is `lapped` and never blocks anyone |
| Seqlock | writer wait-free; readers lock-free (they retry only if a write overlapped a ~20 ns read) |
| Epoch/RCU swap | readers wait-free; only the reclaimer (a housekeeping thread) waits |
| Spin-then-futex | blocking **only while idle**; a writer's wake is a non-blocking `futex_wake` |
| `persist`, HTTP/WS, startup/shutdown, `dlopen` | locks / blocking I/O, deliberately, on housekeeping threads only |

**Specifics:**
- **Operator commands** (API `pf_pause`, `pf_allow` ...) go into the strategy's **inbox**, and the strategy thread applies them between callbacks. The book keeps one writer, and the operator and strategy share one state.
- **No I/O on a strategy thread.**
  - The registry gives each strategy a **pre-reserved, durably committed block of pf numbers** (refilled in the background), so `pfAssign` is local.
  - Book changes go to an **SHM journal** first (µs, survives a crash) and `persist` writes Postgres after. Startup replays the journal, so nothing is lost and no number is handed out twice.
- **Timers:** the timer wheel posts `onTimer` into the strategy's inbox, so it runs on the strategy's own thread.
- **Memory ordering:** release-store / acquire-load on ring heads and seqlocks. `relaxed` is used only on counters.
- **No deadlock by construction:** nobody holds a lock while waiting on another thread, and a full ring returns `full`.
- **Locks that remain:** DB connections (owned by `persist`), the HTTP server internals and startup/shutdown only. A test-build lock hook plus `-Wthread-safety` fail CI if a hot thread takes one.
- **Shutdown** is a `stop` message. Owners drain and flush, then join in dependency order: strategies → bridge → nats → persist.
- **Tests:**
  - TSan over a replayed day.
  - Ring stress with N producers/readers checking sequences and torn reads.
  - A kill -9 + journal/checkpoint restart test.

### Configuration: everything in `.ini` files, nothing hardcoded

No exchange, time, path, address or number is compiled in. `algo/core/config.py` hardcodes the `NSE` and `US` `Market` objects. In C++ every field of them, and every constant scattered in the services, comes from `.ini` files. **A new exchange is a new `.ini`, not a code change.**

```
config/
  algod.ini                 # the process: which market, paths, cores, feed, DH, NATS, API, state store
  markets/NSE.ini  US.ini   # one file per exchange (add MCX.ini, CME.ini ... the same way)
  markets/NSE.holidays      # one date per line; trading days = weekdays in the .ini minus these
  strategies/<name>.ini     # each strategy's SETTINGS block (user, universe, thresholds, sizing, window, exits)
```

**`markets/<X>.ini`** holds everything that differs per exchange:

| section | keys |
|---|---|
| `[market]` | `code`, `currency`, `tz` (exchange), `weekend = Sat,Sun` |
| `[feed]` | `ltt_tz` (NSE local, US `UTC`), `ltt_format`, `kafka_topic`, `dh_table`, column names, `price_div` (fallback when a contract has no `Divisor`) |
| `[session]` | `regular = 09:15-15:30`, plus optional extra windows (`evening = 17:00-23:55` for MCX, special sessions such as Muhurat on a date), `trade_window`, `bar_grace_s = 10`. **Slots per day (375 / 390) are computed from these, never hardcoded.** |
| `[schedule]` | `master_at`, `candles_at`, `services_at`, `close_at`, `eod_at`, `straddle_at_close`, `retry_s`, `retries` |
| `[contracts]` | source (`file` / `postgres` / `auto`), file pattern (`<yyyymmdd>DB.db3`), Postgres table pattern (`resultset_<yyyymmdd>`), the column mapping (`Token, Symbol, ExpiryDate ...`), `expiry_epoch`, `strike_div`, `tick_div`, the instrument-type and option-type codes |
| `[expense]` | per kind, per side, per crore (`FUT = 600,5400` ...); empty = none |
| `[history]` | `max_age`, `iv_max_age`, `min_valid`, cache budget, folders |
| `[limits]` | the turnover display unit (lakh for INR) |

**`algod.ini`** holds the process settings:
- `market = NSE`
- paths (`market_data`, `work`, `eod_dir`, `day_dir`, `shm`)
- `feed.source = kafka | deephaven`, Kafka brokers and group, Deephaven host/port
- NATS (`url`, `users`, `sender`, `timeout_s`, `max_in_flight`, `lane_depth`, `max_queued`, `subject = cmd.hft.{pf}`, `dry_run`) and the default subscribe payload
- `first_pf = 1001`
- the state store (`postgres | files`)
- API ports, CPU pinning per thread (`feed.core = 2`, `candle.cores = 3,4`, ...), ring sizes, huge pages on/off, log level

**Rules:**
- **Secrets are never in an `.ini`:** Postgres password, NATS credentials, API tokens. An `.ini` names an environment variable or a secret file outside the repo (`password_env = ALGO_PG_PASSWORD`), the same way the Python code reads `AUTOMATION_DATABASE_URL` today.
- **Strict loading:** an unknown key, a missing required key, a bad time or timezone, or overlapping sessions stop startup with `file:line` and the reason, so a typo is never silently ignored.
- **Command-line overrides** use `--set section.key=value` (for tests and one-off runs). An environment override (`ALGO_MARKET`, `ALGO_NATS_IP`) is kept for compatibility with today's images.
- **Live changes:** keys marked `live` (strategy thresholds, limits, schedule times, `dry_run`) can be changed through the API (`config_set`). They are validated, published as a new immutable snapshot by RCU swap and written back to the `.ini` atomically. Other keys need a restart, and the API says which.
- **`config_get` / `config_set` / `config_reload`** are in the API, so the frontend can show and edit settings. Every change is audited (who, when, old → new).
- **Library:** `inih` (a tiny C INI parser), wrapped in a typed loader. It runs only at startup and reload, never on the hot path. The values become plain structs with precomputed forms (minute offsets, slot counts, UTC↔local offsets per day, bit flags), so the hot path reads numbers, never strings.
- The Python `NSE` / `US` values are the first two `.ini` files, and a test checks they load to exactly the Python `Market` fields.

### History: parquet loading, fast reads, bounded RAM

Today `absolute()`/`relative()`, RSI seeding (`HIST_DAYS` in 08/11), z-score look-backs (90 days in 10/12) and `eod_load` all read `candles_<day>.parquet` / `straddle_<day>.parquet` through pyarrow with a 200 MB LRU. The C++ version keeps parquet as the **archive format**, but strategies never parse it on the hot path.

**Two tiers:**
1. **Parquet archive (unchanged format, interchangeable with the Python era):**
   - Same names, columns, ZSTD and `.meta.json`. `eod_save` writes them from the mmap day file. The converters in `tools/` keep producing them, and the GDFL/Databento/US vendor history already on the servers is read as-is.
   - Read with Arrow C++ `parquet::arrow::FileReader`, with **column projection** (only `Minute, Symbol, O,H,L,C,Volume` / `Time, Symbol, Expiry, IV`) and **row-group pruning** on `Symbol` min/max statistics. The files are sorted by Symbol, so a one-symbol read touches one or two row groups.
   - Batched reads (`bars_many` equivalent) read a file once for all symbols. `arrow::MemoryPool` gets `ReleaseUnused()` after bulk reads.
   - Edge cases ported as-is: old straddle files without `IV` give "no data", rows with a null `Symbol` are skipped, a missing day counts as a holiday, and two folders (EOD dir + `/data/fo_market_data`) are searched with the first one winning.
2. **Compiled day cache (`.hday`, mmap'd, zero-copy):**
   - Each parquet day is compiled once into a fixed-layout binary next to it (or in `/data/algo/hcache/`). The header holds the symbol → offset index, the futures-per-underlying index (replacing `_index_of` / `month_contract` lookups) and the schema version plus the source file's mtime/size/SHA. The body has per-symbol `Day` blocks: 375 (NSE) / 390 (US) slots × float32 `close_carried`, `cum_pv`, `cum_v`, plus `open`, `last`, `pv`, `v`. That is exactly today's ~4.7 KB `Day`, plus `IvDay` blocks for straddle files.
   - Built by a background compiler thread: at startup for missing or stale days, and at EOD for today. It never runs on a strategy thread.
   - A read is a pointer plus an offset, with no allocation and no decode, in the nanosecond range. That is better than today's 0.1 ms per cached call and far better than the 0.5 s first call.
   - **RAM stays bounded by the OS page cache**, not by the process. A strategy's `absoluteWarm(...)` becomes `madvise(MADV_WILLNEED)` prefetch of exactly the (symbol, day) blocks it will need. Cold days are evicted by the kernel. An optional `mlock` budget pins the hot set (e.g. 22 days × the strategy's universe ≈ tens of MB).
   - Size: about 4.7 KB × symbols × days. All 1,900 cash symbols × 250 days is about 2.2 GB on disk, of which only the touched pages are ever resident.
- **Raw bar access for indicator seeding:** `host.historyBars(sym, days, tf)` returns spans over the `.hday` blocks, or a parquet read for OHLC detail the `.hday` doesn't carry. This covers RSI/EMA seeding from earlier days, including the rule of picking each day's nearest future (`future_names` in 11).
- **Today + history unified:** today comes from the live mmap day file (`/data/algo/day/<date>.bars`) through the same `Day` view, so `absolute`/`relative` treat today and history identically. That removes the "read today's table on every call" cost (255 ms/stock → 0.35 s per pass in Python; here sub-µs). A mid-day restart of algod re-maps today's file and loses nothing. In Python, a restart lost the day's candles.
- **`eod_load` (viewing history in Deephaven):** the parquet files are already on the Deephaven server's volume, so `algoctl eod-load <day|days=N>` asks Deephaven to read the parquet itself rather than streaming rows through algod. The C++ client runs the `read` server-side, so algod uses no RAM for it. The fallback is an Arrow Flight DoPut that streams record batches without materialising them. The schema-mismatch refusal across days is kept.

### Backend API (for the frontend team; no UI built here)

One `Api` class inside algod is the single implementation of every operator function. Three thin transports sit over it, and none of them is on the hot path: they run on housekeeping cores and read state through seqlock snapshots, so a busy UI cannot slow candles or strategies.

**Transports:**
1. **NATS request/reply**: `algo.api.v1.<function>`, JSON in/out. This is what `algoctl` and other services use.
2. **HTTP/JSON + WebSocket gateway** (`api.http_port`, e.g. Boost.Beast or uWebSockets, optional at build time). REST for commands and queries, WebSocket for live streams. This is what a browser frontend uses, with an OpenAPI spec generated in `cpp/api/openapi.yaml` for the frontend team.
3. **In-process C++** (`Api&`): what strategies and tests use.

**Response shape:**
- Every call returns `{ok, status, data, detail}`, using the same status vocabulary as today's `Reply` (`ok, off, blocked, timeout, disconnected, service_down, full, no_user, user_not_allowed, ...`).
- Commands return as soon as they are queued, with a `job_id`. The outcome arrives on the `pf_state` / `events` stream: the same "never block, never raise, never resend" rule as now.
- Auth is a shared token or mTLS on the HTTP gateway. Every command is audited with `by` = the API client name (today's `operator`).

**Functions (v1):** this covers everything in `api.py` and the operator side of `automation.py`.

| group | functions |
|---|---|
| portfolio | `pf_pause`, `pf_unpause`, `pf_squareup` (keeps today's rule: only while paused), `pf_subscribe`, `pf_unsubscribe` (refused while the net position is open), `pf_apply`, `pf_create`, `pf_register`, `pf_list` |
| strategy | `algo_start` (load a `.so` + config), `algo_stop` (pause → squareup → settle → unsubscribe → unload), `algo_exit`, `algo_pause`, `algo_unpause`, `algo_unsubscribe`, `algo_status`, `algo_list`, `algo_portfolios`, `algo_reload` (swap `.so` version), `pf_allow` (the old console untick, per slot) |
| sending | `nats_send` (master or per strategy), `nats_block` / `nats_unblock`, `nats_add_user`, `nats_status`/`stats`, `nats_restart` |
| limits | `limit_set` (turnover / M2M per strategy, latched), `limits_get` |
| data | `master_build`, `master_status`, `candles_start/stop`, `straddle_start/stop`, `contract`, `contracts`, `expiries`, `strikes`, `nearest_strikes`, `last_price`, `last_candles` |
| history | `absolute`, `relative`, `absolute_warm`, `absolute_settings`, `basket` (list/get/set/add/remove/delete), `eod_save`, `eod_load`, `eod_auto` |
| system | `status` (every service, one line each, like `algo_status()`), `day_auto` (schedule on/off/times), `latency` (HDR percentiles per stage), `health` |

**Rule: every piece of state algod holds is readable through the API.** There is no hidden state: if the engine knows it, the frontend can show it.

**Books and portfolio views (read):**

One **`PortfolioView`** row joins everything known about a portfolio:

| source | fields |
|---|---|
| registry | `user, pf, strategy (algo), owner (book), name, created, note` |
| book | `instrument` (signal token + symbol), `slot`, `option` (traded token + symbol, kind, expiry, strike, CE/PE), `leg` (CALL/PUT/BUY/SELL/none), `cap` (cumulative maxqty), `paused`, `allowed` (console tick), `cooling_until`, `last_fail_at` + reason, `current` (vs retired this session), `tracked` |
| pf state (last confirmed command) | `subscribed, paused, squared, by, cmd, at` |
| live | `net_qty, avg_price, ltp, realised, unrealised, gross_m2m, expense, net_m2m, turnover, orders_open, last_fill_at` |
| derived | `status`: RUNNING / PAUSED / SQUARED / UNSUBSCRIBED / COOLING / BLOCKED / RETIRED, plus `in_flight` (commands queued on its NATS lane), `blocked` (user or pf) |

Functions:

| function | answers |
|---|---|
| `book_get(strategy)` | the whole book: settings (user, pf_start, add_qty, unsubscribe_on_retire, squareup_on_repoint, subscribe payload), counts, and every `PortfolioView` of it |
| `book_list()` | every strategy's book: user, #portfolios current/retired, #paused, #open positions, sending on/off, limit status |
| `pf_get(user, pf)` | one `PortfolioView` plus its command history (from `portfolio_state_log` + `nats_events` + `cmd_queue_automated`) and its orders/fills |
| `pf_query(filter)` | `PortfolioView`s filtered by `strategy, user, symbol/underlying, token, status, leg, open_only, since` |
| `symbol_view(symbol \| token)` | **which strategies trade this symbol**: every portfolio (any strategy, any user) whose instrument or option is that contract or underlying, with its status and position |
| `strategy_symbols(strategy)` | the strategy's universe (tracked instruments, slots, baskets) and for each: has a PF?, leg, status |
| `registry_get(user?)`, `registry_log(user?, pf?)` | `portfolio_registry` and its change log |
| `pf_history(user, pf)` | the timeline of every confirmed command, failure and fill |

Writes on books: `pf_allow(strategy, instrument, slot, on)`, `pf_mark(strategy, instrument, leg, slot)`, `pf_cooling_clear(strategy, instrument, slot)`, `book_adopt(strategy, map_file, sub_file)` (today's `map_file=`/`sub_file=` import, reporting clashes). They go through the same rules and the same state store as the strategy's own calls.

**Strategy views:**
- `algo_status(strategy?)`: loaded `.so` (path, name, version, ABI), config, user, state (starting/running/stopping/stopped/crashed), sending (own switch + master), limit status (latched?), worker-thread queue depth, callback latency p50/p99, last error, counters (signals, assigns, activations ok/failed).
- `algo_config(strategy)` (get/set the `.ini` settings that are allowed to change live).

**Streams (WebSocket topics / NATS `algo.stream.v1.*`):** the data the old console drew, now as feeds:
- `book.<strategy>` and `book.*`: a `PortfolioView` delta on every change to a book, pf state, position or status (the old `on_change` / redraw, now pushed).
- `symbol.<token>`: the `symbol_view` deltas, for a symbol page.
- `algo_status`: on every state change of a strategy.
- **How the views stay cheap:** the book, registry, pf states and Pnl each publish changes to an internal `StateHub` (one SPSC ring per publisher, doorbell-polled by the API thread). The hub keeps the joined `PortfolioView` table in memory, indexed by (user, pf), strategy and token, so a query is a lookup, never a scan of the engine's live state, and the hot path only pushes a small delta.
- `pf_state`: every confirmed command per (user, pf): option, subscribed, paused, squared, by, at.
- `events`: today's `nats_events` (failures, connection up/down, limit breaches, swap failed).
- `positions`: per strategy and contract: net qty, avg, LTP, realised, unrealised, gross M2M, expense, net M2M (today's `net_positions` logic, including `dedup_fills` and per-contract divisor).
- `summary`: per strategy plus all strategies: realised, unrealised, gross, expense, net, turnover.
- `limits`: today's `strategy_limits` rows.
- `orders`: the order book from `orders_ack` / `orders_trade`.
- `registry`, `status`.

Each stream sends a snapshot on subscribe followed by deltas, with server-side throttling per client (e.g. at most 4 updates/s for `positions`), so a frontend never sees price-tick floods.

**Where the numbers come from:** algod computes positions/M2M/expense natively from the subscribed `orders_trade` + the live LTP. That logic moves out of `ui/tables.py` into `strategy/Pnl`, and the Guard uses the same object. The frontend therefore needs no Deephaven queries. The same tables are still published to Deephaven for anyone who wants them in the IDE.

### Python → C++ module map

| Python | C++ (under `cpp/`) | Libraries |
|---|---|---|
| `core/app.py`, `plugin.py` | `core/App`, `core/Plugin` (start/stop, `requires`, `follow` → EventBus topic, owned threads joined on stop) | — |
| `core/config.py`, `timeframe.py`, `nulls.py`, `files.py`, `log.py` | `core/Config`, `Timeframe`, `Nulls`, `AtomicFile`, `Log` | spdlog, inih |
| `core/statedb.py`, `pg.py` | `core/StateDB` (SQLite or Postgres, same 4 tables `algo_portfolios`, `algo_pf_map`, `algo_pf_sub`, `algo_limits`, JSON backup copy) | sqlite3, libpq |
| `core/pfstate.py`, `limits.py` | `core/PfStates`, `core/Limits` | — |
| `data/master.py`, `symbols.py` | `data/MasterData` (columnar arrays + hash maps, cache file = flatbuffer/binary instead of pickle) | sqlite3, libpq |
| `data/candles.py` | `data/LttParser`, `data/CandleAggregator` (OHLC, Volume from cum vol, day CVD tick-rule, 10 s grace, epoch buckets), `data/CandlesPlugin` | DH C++ client |
| `data/store.py` | `data/EodStore` (parquet ZSTD + `.meta.json`, same names `candles_<day>.parquet`) | Arrow/Parquet |
| `series/straddle.py` | `series/StraddleMath` (implied_vol, natural cubic spline, repair_quotes, synthetic_forward, vix_iv), `StraddleEngine`, `replay` | — |
| `series/absolute.py` | `series/Clock`, `Day`/`IvDay`, `Cache` (byte-budget LRU, per-key load gate), `Baskets` (same `baskets.txt` grammar and block-preserving edits), `HistoryService` (`absolute`, `relative`, `warm`) | Arrow/Parquet |
| `comms/nats_service.py` | `comms/NatsService` (lane per (user,pf), `lane_depth`, `max_in_flight`, `max_queued`, master + per-client switches, blocks, `Reply` statuses, events table) | nats.c |
| `strategy/registry.py`, `book.py`, `bridge.py`, `guard.py`, `config.py` | `strategy/Registry`, `Book`, `Bridge`, `Guard`, `BridgeConfig` | — |
| `automation.py`, `lookup.py` | `host/StrategyHost` + the **SDK** functions (below) | dlfcn |
| `api.py` + operator side of `automation.py` | `api/Api` (one implementation) + `api/NatsApi`, `api/HttpWsGateway`, `api/Streams` + `tools/algoctl` CLI | nats.c, Boost.Beast, CLI11 |
| `schedule.py` | `control/DaySchedule` | — |
| `ui/tables.py` (net positions, totals, expense, order book, pf_by_option) | `strategy/Pnl` + `api/Streams` (feeds Guard and the `positions` / `summary` / `orders` streams) | — |
| `ui/console.py`, `filters.py`, `redraw.py`, `controls.py` | **not ported**: the frontend team builds the UI on the API; the rules in them (squareup only while paused, no unsubscribe with an open position, master vs strategy switch) move into `api/Api` | — |
| `tools/*.py` converters | `tools/` C++ binaries, last (or keep Python: they are offline) | Arrow |

### Strategy SDK (`cpp/sdk/AlgoSDK.h`): what a `.so` sees

**Written:** [cpp/sdk/AlgoSDK.h](cpp/sdk/AlgoSDK.h), [cpp/sdk/AlgoUtil.h](cpp/sdk/AlgoUtil.h), [cpp/sdk/DEVELOPER_GUIDE.md](cpp/sdk/DEVELOPER_GUIDE.md).
- **Same conventions as the engine's `StrategySDK.h`:** C ABI factory (`algoGetInfo` / `algoCreate(class, instance)` / `algoDestroy`), every service a function pointer in one `AlgoHostAPI` table, `ALGO_SDK_API_VERSION` checked at load, the `.so` links only libc/libstdc++/libm.
- **Only PODs, C strings and function pointers cross the boundary**, with no `std::` types, so a strategy can be built by another compiler. Prices are int64 minor units everywhere.
- **Handlers:** `onStart`, `onBars`, plus optional `onQuotes`, `onAnalytics`, `onPfState`, `onCommandResult`, `onTimer`, `onSendingChanged`, `onSettingsUpdate`, `onStop`.
- **Services:** contracts, quotes/bars (zero copy), `want()`/`analytics()` (computed once per minute for all strategies), the book (`track`, `assign`, `activate`, `leg`, `allowed`, `cooling`, `mark`, `newPf`), direct commands, timer, log, events, status vars.
- **Examples (verified):**
  - `NiftySmaCross` (port of 05) passes the mock host (`make -C cpp sdk-test`, 18 checks), and its apply prices equal the Python `order_params`.
  - `BasketRsiSingle` (idea of 07) builds.
  - `tools/check_plugin.sh` rejects a `.so` with anything other than the three exports, or with writable globals.

- **Callbacks run on the strategy's own worker thread**, one at a time, so a strategy needs no locks (same guarantee the Python listener had). The host never calls a strategy from the feed thread.
- **Shared helpers for strategies:** `sdk/util/` (header-only): Wilder RSI, EMA/SMA, z-score, 5-min roll-up of 1-min bars, `floor_tick`/`ceil_tick`/`order_params`. Today every example re-implements these.
- **Load/unload:** `algoctl start <so> --config config/strategies/nifty_sma.ini --user 3 --name nifty_sma` → dlopen, ABI check, `algoStart` = today's `algo_start` (book + bridge + switch OFF), then the strategy calls `sending` after its own checks. `algoctl stop` = `algo_stop` (pause → squareup → wait `squareup_settle_s` → unsubscribe), then `onStop`, destroy, dlclose. Strategies run out-of-process in `algo-runner` by default, and only promoted, proven ones run in-process (see Risks).
- **Several strategies in one `.so`** is allowed. `algoInfo()` lists the classes, and `algoCreate(className, instanceName)` picks one. The same class can run as many instances as needed, e.g. one per user or universe.
- **Instance isolation:** every piece of mutable strategy state lives in the instance object. **No mutable globals or `static` locals in a strategy `.so`**: the build runs a check (`nm` for writable `.data`/`.bss` symbols plus a clang-tidy rule) and fails if a strategy has one. Each instance has its own inbox, worker thread, book and `(user, strategy_name)` lanes, so instances never contend with each other.
- **Safe unload:** the host reference-counts instances per loaded `.so`. `dlclose` runs only after **every** instance from it has stopped, drained its inbox and been destroyed. `algo_reload` loads the new version under a new handle first, and the old handle is closed when its last instance is gone.

### Repo layout (new top-level `cpp/`, Python stays as the reference until cutover)

```
cpp/CMakeLists.txt  vcpkg.json (librdkafka, arrow, parquet, sqlite3, libpq, cnats, hdrhistogram-c, spdlog, nlohmann-json, inih, cli11, gtest)
cpp/core/ data/ series/ comms/ strategy/ host/ control/ sdk/ app/algod.cpp tools/algoctl.cpp
cpp/strategies/05_nifty_sma_cross/ ... 22_india_directional_1min/   (a target per .so; one .so may hold several strategy classes)
cpp/tests/  (gtest)   cpp/docker/Dockerfile (algod image beside the deephaven service)
```
The Deephaven C++ client is not on vcpkg: build it from `deephaven-core/cpp-client` in the Docker image and point CMake at it.

---

## Risks and how they are contained

| risk | containment |
|---|---|
| **Deephaven append from outside the session** (Phase 0 spike 2) | Deephaven is **not on the critical path**. Strategies read bars from the mmap day file and the SHM ring, never from DH. If appending is slow or needs the blink-table fallback (`add_only_to_append` / `update_by`), only viewing in the IDE is affected. Go/no-go for the spike: 85k-token minute flush visible in the IDE in < 2 s, publisher thread < 1 core. If it fails, publish a filtered set (futures + `expiries=1`) and keep the full day in the day file. |
| **Kafka replay / dedup on restart** | Exactly-once into the aggregator by **checkpoint, not re-derivation**. Every few seconds (and at every bar flush) the candle thread writes a checkpoint into the day file header: per-partition last applied offset, the `open_bar[]` array, last `(ltt, vol)` per token and CVD state, under a seqlock with offsets written last. Restart = load the checkpoint and seek each partition to offset + 1. A second guard stays (Python's `is_new(ltt, vol)`): a tick whose `(ltt, vol)` equals the token's last one is dropped. That covers a duplicate across the checkpoint boundary. OHLC/Close/Volume are idempotent under duplicates (volume is a cumulative difference), and CVD is the only field a duplicate would corrupt, which this guard prevents. Spike 0 must confirm the topic's partitioning: per-token ordering holds only if the producer keys by token. If it doesn't, per-token reordering by `ltt` is needed. Test: kill -9 algod at random points over a recorded day; the bars must stay byte-identical to an uninterrupted run. |
| **A `.so` crash takes down algod** | **Out-of-process by default.** New or changed strategies run in `algo-runner` (SHM rings, ~100–300 ns extra). A strategy is promoted to in-process only after N clean live days, and only by an explicit config flag. In-process plugins are built with the same flags and checked with ASan/UBSan in CI on a replayed day. If algod itself dies, nothing new is sent, but the engine still holds the portfolios and their positions. That is the same as today's "Deephaven restarted" case (`tech.md` §9): restart, re-adopt from Postgres state, square up from the API if needed. Hot `algo_reload` is done stop → unload → load, never `dlclose` under a running callback. |
| **Scope vs payoff** | Do **not** cut over all at once (see "Strangler order" below). The measured bottleneck is the candle builder on the one Python thread (0.4–0.9 s batches vs a 250 ms budget, `tech.md` §8), which Python-side caching cannot fix. Phase 1 alone removes it while the Python strategies keep running unchanged. Every later phase is taken only if the one before paid off. |

### Strangler order (each step runs alongside Python and can be switched back)
1. **algod = feed + master + candles only.** It publishes `candles` to Deephaven under the same name and columns. Python stops building candles (`candles_start` not called) and everything else in Python (straddle, `watch("candles")`, strategies, console) keeps working on the C++-built table. This is the biggest win, with the least change.
2. **+ NATS service, registry, book, bridge, API.** Python strategies still run. algod exposes `pf_*` / `algo_*` / `book_*` for the frontend team.
3. **+ SDK and the first `.so` strategy** (05) in algo-runner, dry-run beside its Python twin, then live small.
4. **+ straddle, history (`.hday`), the rest of the strategies.** Python is retired only when the last strategy has moved.

## How I would go about it (phases, each one shippable)

**Phase 0: spikes (prove the risky integrations first).**
0. Kafka: read the message schema of `hft.marketdata.fo` (JSON, protobuf or binary) from the producer or a sample, and measure consume → decode → ring latency with librdkafka (`fetch.wait.max.ms=0`, `enable.auto.commit=false`).
1. DH C++ client: subscribe to `fo_market_data`, get added and modified rows per cycle (the Python candles read both), and measure throughput for 85k tokens.
2. DH C++ client: create a table that **grows all day** (an input table / repeated DoPut appends bound to a session variable named `candles`) and confirm a Python or IDE viewer can read it ticking.
3. nats.c: request to `cmd.hft.<pf>` with `{"Cmd","Payload","ReplyId"}` against the real engine on one paused test PF.

If (2) cannot produce an appending table, fall back to Barrage/Flight streaming into a blink table plus a server-side `.add_only_to_append()` / `update_by`. Decide before Phase 1.

**Phase 1: core + master + candles.**
- `App`/`Plugin`/`EventBus`/`Config`, then `MasterData` from `.db3` and Postgres (`resultset_<day>`), then `CandleAggregator` + `LttParser` + publish `candles`.
- **Parity harness:** a Python tool records a day's `fo_market_data` update cycles to parquet. Python `CandleAggregator` and C++ both replay it, and the closed bars must be byte-identical (OHLC rounded to 6 dp, Volume, CVD).

**Phase 2: commands.**
- `NatsService` (lanes, caps, statuses, switches, blocks, reconnect), `PfStates`, `Registry`, `Book`, `Bridge` (swap/retire/exit + rollback + cooldown), `StateDB` (the same Postgres tables, so Python and C++ can hand over state) and `Limits`/`Guard` fed from a subscription to `orders_trade`.
- Port `tests/test_nats_service.py`, `test_registry.py`, `test_book.py`, `test_bridge.py` and `test_pf_state.py` to gtest against a fake transport, then integration tests against a real `nats-server` in Docker.

**Phase 3: SDK + host + API + first strategy.**
- `AlgoSDK.h`, `StrategyHost`, then `api/Api` + NATS transport + `algoctl`, then the HTTP/WS gateway + `openapi.yaml` + streams, handed to the frontend team as soon as `pf_*`, `algo_*` and `status` work.
- Port `05_nifty_sma_cross` first (it is the smallest real one), run it in dry-run (`NatsConfig.dry_run`) side by side with the Python version on the same day, and compare the command sequence.

**Phase 4: series and history.**
- `StraddleEngine` + publish `straddle_series`, `EodStore`, then `HistoryService` (`absolute`/`relative`/`warm`, `Baskets`, LRU cache, today's rows from the C++ in-memory candles instead of DH reads, so the 255 ms/stock problem disappears).
- History is built in this order: parquet reader (projection + row-group pruning), then the `.hday` compiler + catalog (days list, futures index, staleness by mtime/size), then mmap reader + `madvise` warm, then `HistoryService` on top. Compile the servers' existing history (GDFL futures 2026-01→10, US 2026 set, straddle history) and check that the `.hday` values equal Python's `Day` arrays exactly (float32 bit-equal).
- Golden tests: Python `StraddleEngine.replay` and `absolute()`/`relative()` on the saved parquet days → JSON → gtest compares within 1e-6.

**Phase 5: all strategies + schedule.**
- Port the remaining examples to `.so` in order of how they are used live: 07, 11, 13, 21, 22, then the rest. The test scripts (`stress_tset.py`) and `CrossOrder_plugin.cpp` (engine side) are out of scope.
- Add `DaySchedule`. The converters in `tools/` stay Python unless wanted.

**Phase 6: cutover.** Run C++ for one day with Python candles still on (separate table name `candles_cpp`) and compare. Then switch the `init.py` start-up to only the Deephaven feed, start `algod` from compose, and retire `algo/`.

---

## Verification

- `cmake --build cpp/build && ctest`: the gtest ports of the pure-logic pytest files (candles, timeframe, master, straddle, absolute/relative/baskets, nats lanes, registry, book, bridge, pfstate, limits).
- **Parity tests (Python as oracle):** recorded feed → candles identical; saved days → straddle/IV/absolute/relative within 1e-6; the same strategy in Python and `.so` in dry-run on one replayed day → the same `(user, pf, cmd, payload)` sequence.
- **Live (in order):** `algod --dry-run` against the India Deephaven and look at `candles`, `straddle_series` and `nats_events` in the IDE. Then one paused test portfolio with a real `cmd.hft` subscribe → pause → apply → unpause. Then one strategy live with small size.
- **Books:** a dry-run of a replayed day with two strategies on overlapping symbols. `symbol_view` must list both. `book_get` must equal Python's `algo_portfolios()` + `pf_states` + `net_positions` for the same replay. An operator pause/squareup/unsubscribe must show up in `book.<strategy>` within one stream tick.
- **API:** a contract test suite drives every v1 function over both NATS and HTTP against algod in dry-run, checks the responses against `openapi.yaml`, and checks that each command appears on the `pf_state` / `events` streams. A load test (many WS clients) must show no change in the candle/strategy latency histograms.
- **Load:** all 85,957 instruments on 1-min bars. Target: batch-to-closed-bar latency under 50 ms p99 (today 0.4–0.9 s median), with CPU and memory recorded.
