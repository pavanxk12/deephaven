# algod: Strategy Plugin Developer Guide

**SDK version:** 1 (`ALGO_SDK_API_VERSION`)
**Audience:** quant developers writing signal strategies for algod, the C++ port of the Deephaven `algo` stack
**Headers:** `sdk/AlgoSDK.h` (the interface, the only one you need) and `sdk/AlgoUtil.h` (optional helpers)

> **Status.** The SDK, the two example strategies and the mock host in this folder build and pass their tests. algod
> itself, which implements the services behind the SDK, is still being built (see `cpp_port_plan.md`). Until it
> ships, build and test your strategy against `sdk/tests/mock_host.cpp` (section 15).

---

## Contents

1. [Architecture: who does what](#1-architecture-who-does-what)
2. [Build](#2-build)
3. [Strategy skeleton](#3-strategy-skeleton)
4. [Event lifecycle](#4-event-lifecycle)
5. [Settings: the `.ini` file](#5-settings-the-ini-file)
6. [Contracts](#6-contracts)
7. [Market data: bars and quotes](#7-market-data-bars-and-quotes)
8. [Analytics: absolute and relative, computed once](#8-analytics-absolute-and-relative-computed-once)
9. [Portfolios: the book and `activate`](#9-portfolios-the-book-and-activate)
10. [Direct portfolio commands (single-order style)](#10-direct-portfolio-commands-single-order-style)
11. [Order parameters: `AlgoParams` and prices](#11-order-parameters-algoparams-and-prices)
12. [Operator actions, state and results](#12-operator-actions-state-and-results)
13. [Timer, logging, status variables](#13-timer-logging-status-variables)
14. [Threading and performance rules](#14-threading-and-performance-rules)
15. [Testing with the mock host](#15-testing-with-the-mock-host)
16. [Deployment](#16-deployment)
17. [Porting a Python script](#17-porting-a-python-script)
18. [Quick reference](#18-quick-reference)

---

## 1. Architecture: who does what

```
 Kafka / Deephaven feed ──► algod ──────────────────────────────────────────────────────────────┐
                            │ master · candles · quotes · analytics (absolute / relative)        │
                            │ book · registry · NATS service · limits · API                      │
                            │                                                                    │
                            │   AlgoHostAPI (function pointers)                                  │
                            ▼                                                                    │
              ┌──────── YOUR .so (links only libc / libstdc++ / libm) ────────┐                  │
              │ class MyStrategy : public AlgoStrategy {                      │                  │
              │   onStart(settings)   read config, resolve tokens, seed       │                  │
              │   onBars(batch)       closed candles: your signal             │                  │
              │   onAnalytics(min)    this minute's absolute/relative ranks   │                  │
              │   ...                 decide WHAT and WHEN                    │                  │
              │ };                                                            │                  │
              └───────────────┬───────────────────────────────────────────────┘                  │
                              │ assign() / activate() / subscribe() / squareup() ...             │
                              ▼                                                                  │
                    algod's NATS service ── cmd.hft.<pf> ──► trading engine ──► its plugin (StrategySDK.h:
                                                                                CrossOrder, Churning ...) works the orders
```

There are **two kinds of plugin, at two different layers**:

| | **this SDK (`AlgoSDK.h`)** | **the engine's SDK (`StrategySDK.h`)** |
|---|---|---|
| runs in | algod (beside Deephaven) | the trading engine |
| decides | *what* to trade and *when*: signals from candles, quotes, analytics | *how* to trade one portfolio: order slots, place / modify / cancel |
| outputs | portfolio commands: subscribe, pause, apply, unpause, squareup, unsubscribe | exchange orders |
| one instance per | strategy (with many portfolios) | portfolio |

An algod strategy never places an exchange order. It takes a portfolio from its **book** and sends portfolio commands,
and the engine's plugin, named by the `strategyname` field of the subscribe, works the orders. This is exactly the
split of the Python scripts in `examples/`.

The `.so` has **zero link-time dependency** on algod. Every service arrives as a function pointer in one
`AlgoHostAPI` table, wired before `onStart` is called.

---

## 2. Build

### Layout

```
MyStrategy/
├── CMakeLists.txt      ← copy sdk/examples/NiftySmaCross/CMakeLists.txt, change the two names
├── MyStrategy.cpp
└── my_strategy.ini     ← its settings (section 5)
```

### Commands

```bash
cmake -B build -DALGO_SDK_DIR=/path/to/deephaven/cpp/sdk
cmake --build build
nm -D build/libMyStrategy.so | grep -E "algoGetInfo|algoCreate|algoDestroy"   # exactly three T lines, unmangled
```

Without cmake: `c++ -std=c++17 -O3 -fPIC -fvisibility=hidden -fvisibility-inlines-hidden -shared -I<sdk> MyStrategy.cpp -o libMyStrategy.so`

### Rules

| rule | why |
|---|---|
| C++17 or newer | the SDK headers are C++17 |
| `-fvisibility=hidden` | only the three factory symbols are exported (`ALGO_EXPORT`) |
| no `-flto` | keep the exported symbols plain for `dlsym` (same rule as the engine SDK) |
| link no algod library | every service is a function pointer; `nm -u` must show only libc / libstdc++ / libm |
| **no mutable globals or `static` locals** | several instances of your class can run at once; all state goes in members. The build check fails a `.so` that has writable global data |
| any compiler | only PODs, C strings and function pointers cross the boundary (no `std::string` / `std::vector` / exceptions), so you need not match algod's compiler. Build with the same one anyway when you can |

---

## 3. Strategy skeleton

```cpp
#include "AlgoSDK.h"
#include "AlgoUtil.h"      // optional: Window, sma, zscore, Ema, WilderRsi, floorTick / ceilTick, inWindow

class MyStrategy final : public AlgoStrategy {
public:
    void onStart(const AlgoSettings& s) override { /* read settings, resolve tokens, seed indicators */ }
    void onBars(const AlgoBarBatch& batch) override { /* the signal */ }
private:
    // ALL state here, never in globals
};

// ============================================================
// Required C-linkage factory symbols. DO NOT rename.
// ============================================================
static const char* const kClasses[] = {"MyStrategy"};
static const AlgoPluginInfo kInfo = {ALGO_SDK_API_VERSION, 1, "my_strategy", "1.0.0", kClasses};

extern "C" ALGO_EXPORT const AlgoPluginInfo* algoGetInfo() { return &kInfo; }
extern "C" ALGO_EXPORT AlgoStrategy* algoCreate(const char* cls, const char* /*instance*/) {
    return (cls && std::strcmp(cls, "MyStrategy") == 0) ? new MyStrategy() : nullptr;
}
extern "C" ALGO_EXPORT void algoDestroy(AlgoStrategy* s) { delete s; }
```

**Several strategies in one `.so`:** list every class in `kClasses` (set `class_count`) and return the right one from
`algoCreate`. The same class may run as many instances as you like (one per user, per universe ...), each with its own
`.ini`, book, worker thread and switch.

---

## 4. Event lifecycle

```
dlopen(lib.so) ─► algoGetInfo()             version check (ALGO_SDK_API_VERSION) and class list
      ▼
algoCreate(class, instance) ─► constructor
      ▼
_algo_init(&api, ctx)                       services wired
      ▼
onStart(settings)                           read config, find() tokens, want() analytics, track(), seed from bars()
      ▼                                     sending is OFF here; [host] autostart, or the operator, turns it on
onSendingChanged(true)
      ▼
onBars(batch)          every flush of closed candles (one call for all tokens of the universe)
onQuotes(q, n)         ticks of the tokens given to watchQuotes() (optional)
onAnalytics(minute)    the wanted analytics for this minute are published
onPfState(state)       a confirmed change of one of its portfolios (its own, or the operator's)
onCommandResult(r)     the engine's answer to a queued command
onTimer()              after setTimerMs(ms)
onSettingsUpdate(s)    live settings changed through the API
      ▼ (algo_stop from the API, stopSelf(), or algod shutting down)
onStop()               last call; the host has already paused / squared up / unsubscribed if asked
      ▼
algoDestroy(s) ─► destructor ─► dlclose once every instance from this .so is gone
```

**Sending.** Every strategy starts OFF. Commands from it go out only while its own switch AND the master switch are on
(`sending()`). While OFF, `activate()` and the other commands return `-OFF` at once; check `sending()` first to skip the
work. A breached turnover or M2M limit switches it OFF and keeps it off until the limit is changed (as in Python).

---

## 5. Settings: the `.ini` file

Each instance has its own `config/strategies/<name>.ini`. The **SETTINGS block at the top of a Python script becomes
this file**, so nothing is hardcoded in the `.so`.

```ini
[strategy]                 # bare keys: s.get("underlying")
underlying   = NIFTY
n_strikes    = 5
trade_from   = 09:20

[signal]                   # section.key: s.getInt("signal.fast")
fast = 5
slow = 20

[apply]                    # copied whole into an AlgoParams: s.toParams("apply", p)
target   = 1000
stoploss = -400

[host]                     # read by algod, not by the strategy
user      = 3
pf_start  = 1001
add_qty   = 3
autostart = false

[subscribe]                # added to every subscribe the book sends
strategyname = Churning
```

| `AlgoSettings` method | returns |
|---|---|
| `get(key)` | `const char*`, or `nullptr` when missing |
| `getInt(key, dflt)` / `getDouble(key, dflt)` / `getBool(key, dflt)` | the value, or `dflt` when missing or unreadable |
| `getMinute(key, dflt)` | `"HH:MM"` as the minute of the day (09:20 = 560) |
| `item(key, i, buf, len)` | the i-th item of a comma-separated value, with no allocation |
| `toParams(section, params)` | every key of `[section]` into an `AlgoParams`, typed by how it reads (whole number, number, text) |
| `has(key)` | true when set |

**Section names.** `[host]` and `[subscribe]` are algod's; every other section is yours. Keys that may change while
running are listed in `[live]` (`keys = signal.fast, signal.slow`). The API's `config_set` changes them, and you get
`onSettingsUpdate`.

---

## 6. Contracts

From today's master, an immutable snapshot. Lookups are lock-free, but they are searches, so **do them in `onStart`
and keep the tokens**.

```cpp
int32_t fut = find("NIFTY");                 // a bare name = its nearest future ("RELIANCE" = cash; a full symbol; "48704")
AlgoContract c;
info(fut, c);                                // token, future/spot/alt tokens, lot, expiry yyyymmdd, strike, tick, divisor,
                                             // kind (FUT/OPT/EQ/INDEX), opt_type (CE/PE), symbol, underlying
int32_t exp[8];
uint32_t ne = expiries("NIFTY", AlgoKind::OPT, exp, 8);                  // dates that have not passed, nearest first
int32_t tok[64]; int64_t k[64];
uint32_t ns = strikes("NIFTY", 0, AlgoOptType::CE, tok, k, 64);           // the chain of the nearest expiry
int32_t near5[5];
uint32_t nn = nearest("NIFTY", c_ltp, AlgoOptType::PE, 0, 5, near5);      // the 5 strikes nearest a price (minor units)
```

Prices, strikes and ticks are **int64 minor units** (paise for NSE; for other contracts the units of its own `divisor`).

---

## 7. Market data: bars and quotes

### Bars (`onBars`)

```cpp
void onBars(const AlgoBarBatch& batch) override {
    for (uint32_t i = 0; i < batch.count; ++i) {
        const AlgoBar& b = batch.bars[i];      // token, idx, start_minute (epoch minutes), minute_of_day,
                                               // open, high, low, close (minor units), volume, cvd
    }
}
```

* Bars are **closed** bars only: a bar closes 10 s (the market's `bar_grace_s`) after it ends, by the feed's own clock.
  They are built by the same rules as the Python `candles` table, checked bar for bar against it.
* A token with no trade in a bar has no bar (no forward fill).
* `batch.bars` points into shared memory and is valid until `onBars` returns. Copy what you keep.

| call | gives |
|---|---|
| `bars(token, n, out)` | today's last n closed bars, oldest first (zero copy) |
| `dayBars(token, days_back, out)` | a saved day (1 = the previous trading day); seeds multi-day indicators |
| `lastPrice(token)` | the quote's LTP, else the last bar's close, else 0 |

### Quotes

```cpp
AlgoQuote q;
quote(token, q);       // ltp, bid, ask, bid_qty, ask_qty, ltq, cum_volume, ltt_ns: one seqlock read, ~20 ns, never torn
```

`quote()` can be called from any handler. For every tick of some tokens, call `watchQuotes(tokens, n)` in `onStart`
and implement `onQuotes`.

---

## 8. Analytics: absolute and relative, computed once

These are the Python `absolute()` (now against the same session minute of the last n days, weeks or months) and
`relative()` (now against the other symbols of a basket). With several strategies on the same basket, Python computed the
same numbers once per strategy per call. In algod you **declare** what you need once, algod computes it **once per minute
for everyone**, and you read the result.

```cpp
// onStart: declare
hOpen_ = want(AlgoSeries::PCT, AlgoLevel::INTRADAY, AlgoRef::open(), "banks", 22);   // % from the open: relative in "banks", absolute over 22 days
hTurn_ = want(AlgoSeries::TURNOVER, AlgoLevel::INTRADAY, AlgoRef::none(), "banks", 22);

// onAnalytics(minute): read
AlgoAnalytics a;
if (analytics(hOpen_, token, a) && a.rel_idx >= 0 && a.rel_pctile > 75) { /* strong in its basket */ }
```

| `AlgoAnalytics` field | meaning |
|---|---|
| `now` | the value now (NaN when none) |
| `abs_idx`, `abs_n_valid`, `abs_pctile` | how many of the n past values are below now; -1 = cannot tell |
| `rel_idx`, `rel_n_valid`, `rel_pctile` | how many basket symbols are below this one; -1 = cannot tell |
| `minute_of_day` | the session minute the values are for |

* Series: `PCT` (with a reference), `IV` (nearest expiry's straddle IV), `VOLUME` and `TURNOVER` (running totals, no reference).
* References: `AlgoRef::open()`, `vwap()`, `prevClose()`, `minutesAgo(10)`, `atMinute(9, 30)`.
* Levels: `INTRADAY` (previous n trading days), `INTRAWEEK` (same weekday), `INTRAMONTH` (same trading day of the month).
* One minute's values for a whole basket are published together, so every strategy sees the same snapshot.
* A one-off question with other parameters: `absolute(...)` / `relative(...)` compute it now (cached for the minute).
  They are slower than `want()`, so don't call them for every symbol on every bar.
* Basket members: `basket("banks", tokens, max)`.

---

## 9. Portfolios: the book and `activate`

The churning style (`05`, `06`, `11` ...): a signal instrument (a future) has one portfolio per **slot** (strike
1, 2 ...), and the portfolio follows the signal.

```cpp
track(&future_, 1, nStrikes_);                       // onStart: re-adopt the portfolios held before a restart

AlgoAssignment a;
if (assign(future_, option, "NIFTY", slot, a) == AlgoStatus::OK)   // the BOOK picks the number: same option = same pf,
    activate(a, AlgoLeg::CALL, params);                             // another option = a new pf and a.old_pf to retire
```

`activate` queues the whole sequence on that portfolio's lane and returns at once:

```
[retire a.old_pf: pause -> squareup (if configured) -> unsubscribe (if configured)]
subscribe (if new, with the [subscribe] fields) -> pause -> apply (params, maxqty = cap + add_qty) -> unpause
```

If any step is not `ok`, the sequence stops, the portfolio stays paused, the book is rolled back, the slot cools down
(`fail_cooldown_s`) and `onCommandResult` reports it. You never pick a portfolio number.

| call | meaning |
|---|---|
| `leg(instr, slot)` | the leg this slot runs now (`NONE` after an operator squareup) |
| `allowed(instr, slot)` | false while the operator has unticked it (`assign` then returns `NOT_ALLOWED`) |
| `cooling(instr, seconds, slot)` | true for `seconds` after a failed activation of that slot |
| `mark(instr, leg, option, slot)` | set the leg by hand (e.g. reset after your own exit) |

---

## 10. Direct portfolio commands (single-order style)

The single-order style (`07`, `08`, `11` ...): a **new portfolio per trade**, started by one subscribe that carries the
engine plugin's own fields, and closed by a squareup.

```cpp
const int32_t pf = newPf("SBIN FUT");                      // a fresh, never reused number
AlgoParams p;  settings.toParams("engine", p);             // strategyname=single, SERVERCODE, ACCOUNT, ALGOID
p.setInt("TOKEN", token); p.setInt("SIDE", 1); p.setInt("QTY", lots);
int64_t r = subscribe(pf, token, p);                       // > 0: queued (job id); <= 0: -AlgoStatus
...
squareup(pf);                                              // exit: the plugin's SquareUpEvent closes the position
unsubscribe(pf, token);                                    // later: frees the engine's plugin slot
```

Also: `apply(pf, p)`, `pause(pf)`, `unpause(pf)`, `pfState(pf, out)`. `statusOf(r)` turns a return value into an
`AlgoStatus`. **The engine has a limited number of plugin slots**, so unsubscribe portfolios you have closed and cap how
many you hold (`max_portfolios` in the examples).

---

## 11. Order parameters: `AlgoParams` and prices

`AlgoParams` is a fixed-size key/value list built on the stack, with no allocation (40 entries).

```cpp
AlgoParams p;
p.setPrice("ival", 120);        // MINOR units: 120 paise
p.setInt("maxqty", 3);
p.setDouble("ratio", 0.5);
p.setStr("side", "BUY");
```

**Prices go in with `setPrice`, in minor units.** algod writes them in the engine's currency units with the small
upward nudge the engine needs (it truncates ×100 to an integer), so 230 paise arrives as `2.300001` and never as
2.29. Do not add the nudge yourself, and don't pass prices as doubles.

Round to the tick with `algo::floorTick` / `algo::ceilTick` (`AlgoUtil.h`). `NiftySmaCross::orderParams` gives exactly
the values of the Python `order_params` (checked: 120.00 premium on a 0.05 tick → ival 120, jval 90, sval 360, trp 12005,
range 11400 to 12600 paise).

---

## 12. Operator actions, state and results

The operator (the API, and later the frontend) can act on your portfolios at any time. You are told in `onPfState`:

| operator does | you see | what the book does |
|---|---|---|
| Pause / Resume | `paused` flips, `by_operator = 1` | your next activation applies but does **not** unpause (their pause stands) |
| Square up | `squared = 1` | `leg()` becomes `NONE`: the slot may start again on your next signal |
| Unsubscribe | `subscribed = 0` | the slot is blocked: `allowed()` false, `assign()` `NOT_ALLOWED`, until subscribed again |
| Untick a slot | — | `allowed()` false |

`onCommandResult` carries the engine's answer to every command you queued (`job_id` matches the return value).
`TIMEOUT` means **unknown whether the engine acted**. algod never re-sends, so check `pfState` before you decide.

---

## 13. Timer, logging, status variables

```cpp
setTimerMs(1000);               // one-shot: onTimer() in about a second; call again inside onTimer to repeat
log("text", AlgoLogLevel::WARN);// to the log (formatted off your thread; cheap)
event("SBIN: BUY 1 lot on PF 1042");   // to the API `events` stream: the operator sees it
setVar("positions", "3");       // shown in algo_status for this strategy (like the engine's update_ui_vars)
clockNs();  minuteOfDay();  today();  user();  name(buf, len);
```

Nothing that takes a lock or does I/O runs on your thread: `log` and `event` only drop a record in a ring.

---

## 14. Threading and performance rules

**Guarantee:** every handler of one instance runs on that instance's own worker thread, one at a time, never
concurrently. You need no locks. Two instances never share a thread or state. A slow strategy only delays itself:
candles, analytics and the other strategies never wait for it.

**In `onBars`, `onQuotes` and `onAnalytics`:**

| don't | do |
|---|---|
| `new`, `std::vector::push_back` that grows, `std::string`, `std::function`, `std::map` | fixed arrays and `algo::Window<T, N>` (`AlgoUtil.h`), sized in `onStart` |
| `printf`, files, sockets, sleeping, mutexes | `log()` / `event()`, which only drop a record in a ring |
| `find()` / `nearest()` / `strikes()` per bar for the whole universe | resolve tokens once in `onStart`; call `nearest()` only when a signal fires |
| `absolute()` / `relative()` per symbol per bar | `want()` in `onStart`, `analytics()` in `onAnalytics` |
| doubles for prices | int64 minor units; doubles only for indicators |
| let an exception escape a handler | handle it; algod marks the strategy `error` after repeated escapes |

**Crash isolation.** By default a strategy runs out of process in `algo-runner`, on the same shared-memory data with
the same SDK. A crash then kills only your strategy, never algod. Proven strategies can be moved into algod for the
last ~100–300 ns. The `.so` is the same.

---

## 15. Testing with the mock host

`sdk/tests/mock_host.cpp` loads a `.so` the way algod does (`dlopen`, version check, `algoCreate`, `_algo_init`,
`onStart`) with a fake `AlgoHostAPI`: a small NIFTY chain, a book and a command recorder. It then drives
`NiftySmaCross` through a cross up and a cross down:

```bash
make -C cpp sdk-test
# 18 passed, 0 failed
```

Copy it for your strategy: change the market it fakes, the bars it feeds and the commands it expects.

---

## 16. Deployment

```bash
cp build/libMyStrategy.so          /opt/algod/strategies/
cp my_strategy.ini                 /opt/algod/config/strategies/
algoctl start /opt/algod/strategies/libMyStrategy.so --class MyStrategy --config config/strategies/my_strategy.ini
algoctl send on my_strategy        # its sending switch (or [host] autostart = true)
algoctl status my_strategy
algoctl stop my_strategy           # pause, square up, wait, unsubscribe every portfolio, onStop, unload
algoctl reload my_strategy libMyStrategy.so   # new version: stop -> load the new .so -> start; the old one is unloaded once unused
```

The same actions are API functions (`algo_start`, `algo_stop`, `algo_status`, `nats_send` ...) for the frontend.

**ABI mismatch:** `algoGetInfo()->api_version` must equal algod's. If it doesn't, the load is refused with
`API version mismatch (plugin=1, host=2)`: rebuild against the new header.

---

## 17. Porting a Python script

| Python (`examples/*.py`) | AlgoSDK |
|---|---|
| the `SETTINGS` block | `config/strategies/<name>.ini`, read in `onStart` |
| `algo_start(USER, STRATEGY, add_qty=, subscribe={...})` | `[host]` and `[subscribe]` sections; algod does it before `onStart` |
| `nats_connect`, `registry_connect` | nothing: algod checks before it starts you |
| `contract(x)["token"]`, `nearest_strikes(...)`, `strikes(...)` | `find(x)`, `nearest(...)`, `strikes(...)`, `info(token, c)` |
| `last_candles(x, n)` | `bars(token, n, out)` |
| `watch("candles", on_candle)` | `onBars(batch)` |
| `update.added(["token", "Close"])` | `batch.bars[i].token`, `.close` |
| `absolute(...)` / `relative(...)` in a loop | `want(...)` once + `analytics(h, token, out)` in `onAnalytics` |
| `pf_track`, `pf_assign`, `pf_activate`, `pf_leg`, `pf_allowed`, `pf_cooling`, `pf_mark` | `track`, `assign`, `activate`, `leg`, `allowed`, `cooling`, `mark` |
| `svc.send(pf, "subscribe", {...})`, `svc.squareup(pf)`, `pf_unsubscribe` | `subscribe(pf, opt, p)`, `squareup(pf)`, `unsubscribe(pf, opt)` |
| `algo_sending(STRATEGY)` | `sending()` |
| `algo_stop(STRATEGY)` (time exit) | `stopSelf(true)` |
| `threading.Timer(...)` | `setTimerMs(ms)` + `onTimer()` |
| `print(...)` | `log(...)` / `event(...)` |
| `order_params`, `floor_tick`, `ceil_tick`, RSI / SMA / EMA / z-score | `algo::floorTick`, `ceilTick`, `WilderRsi`, `sma`, `Ema`, `zscore` (`AlgoUtil.h`) |
| prices in rupees (floats) | int64 paise; `setPrice` for apply fields |

Worked ports in `sdk/examples/`: `NiftySmaCross` (from `05_nifty_sma_cross.py`, churning) and `BasketRsiSingle` (the
idea of `07_basket_rsi_crossorder.py`, single order, with a relative filter).

---

## 18. Quick reference

### Handlers

| handler | when | required |
|---|---|---|
| `onStart(settings)` | once, after wiring | yes |
| `onBars(batch)` | each flush of closed bars | yes |
| `onQuotes(q, n)` | ticks of watched tokens | no |
| `onAnalytics(minute)` | wanted analytics published | no |
| `onPfState(state)` | a confirmed portfolio change | no |
| `onCommandResult(result)` | the engine's answer | no |
| `onTimer()` | after `setTimerMs` | no |
| `onSendingChanged(on)` | switch flipped | no |
| `onSettingsUpdate(settings)` | live settings changed | no |
| `onStop()` | last call | no |

### `AlgoStatus`

| value | meaning | sent? |
|---|---|---|
| `OK` | the engine answered ok (or dry run) | yes |
| `QUEUED` | accepted; the answer comes in `onCommandResult` | — |
| `OFF` | its switch or the master switch is off | no |
| `BLOCKED` | user or portfolio blocked by the operator | no |
| `FULL` | that portfolio's lane or this strategy's ring is full | no |
| `NOT_ALLOWED` | slot unticked, or a limit is breached | no |
| `COOLING` | the slot failed a moment ago | no |
| `INVALID` / `NOT_FOUND` | bad argument / no such thing | no |
| `NO_SERVICE` | the NATS service is not running | no |
| `TIMEOUT` | no answer in time: **unknown whether it acted**; never re-sent | yes |
| `NO_RESPONDERS` | nothing listens on `cmd.hft.<pf>` | yes |
| `DISCONNECTED` | no NATS connection | no |
| `BAD_REPLY` / `ENGINE_ERROR` | the engine's answer was unreadable / a refusal (`detail`) | yes |

### Data structures (all fixed size; `AlgoQuote` and `AlgoBar` are one 64-byte cache line)

| struct | key fields |
|---|---|
| `AlgoBar` | token, idx, start_minute, minute_of_day, open, high, low, close, volume, cvd |
| `AlgoQuote` | token, idx, ltp, bid, ask, bid_qty, ask_qty, ltq, cum_volume, ltt_ns |
| `AlgoContract` | token, future/spot/alt token, lot_size, expiry, strike, tick, divisor, kind, opt_type, symbol, underlying |
| `AlgoAnalytics` | now, abs_idx / n_valid / pctile, rel_idx / n_valid / pctile, minute_of_day |
| `AlgoAssignment` | user, pf, old_pf, instrument, option, slot, is_new |
| `AlgoPfState` | user, pf, option, subscribed, paused, squared, by_operator, cmd, at_ns, by |
| `AlgoCmdResult` | job_id, user, pf, status, cmd, detail |
