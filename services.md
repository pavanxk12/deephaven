# The services: what each one does, and that they do not affect each other

Written for: you, the person running the stack. One page that says what every part does, what it depends on, how much memory and CPU it takes,
and what changed in the last pass.

Using them day to day: **`howto.md`** (plain function calls for each service) and `algo_help()` in the terminal.

## 1. They are independent

Each service is its own plugin. They find each other **by name in the app, only when they need to**, and none of the data services knows the
others exist.

```
 master ──► candles                      (data: each one is built from the one before it)
                  └───► straddle
 nats service        (sends commands; knows nothing about the data)
 registry            (hands out portfolio numbers; knows nothing about the data or NATS)
 book / bridge / algo (use the registry and NATS; read candles; indicators are the algo's own)
 console / UI         (reads and clicks; uses the others)
```

| if this stops, is busy or breaks | what happens to the others |
|---|---|
| **NATS service** (stopped, or the real NATS server is down) | candles, straddle, master, registry: **not touched**. Algos keep computing signals; their commands come back as an error status (`service_down` or `disconnected`) and a swap rolls back and retries later. |
| **registry** | candles, straddle, NATS: **not touched**. Only creating a *new* portfolio number fails, with a clear message. |
| **candles** | the straddle stops getting new rows (it is built from candles) but does not break; it follows when candles restart. NATS and the registry are not touched. |
| **an algo** | the others do not know it exists. |

**Speed.** The NATS service runs on its own thread and its own event loop. A Deephaven listener only *hands* it a command and returns at once, so
sending (or waiting for NATS) never holds up a table update. The registry does a tiny file append per new portfolio. Neither shares a lock with
candles.

**This is checked, not just claimed.** `tests/test_independence.py` reads every module's import lines and fails if a package imports one it must
not know about: `core` imports nothing of the others; `data` (master, candles, EOD) uses `core` only; `comms` (NATS) and the
registry use `core` only; the data modules never mention NATS, the registry or a strategy. So a later change cannot quietly tie them together.

**Shared limit, honestly:** they all run inside one Deephaven process, so they share its memory and its table-update thread. A listener doing
slow work would delay every table. The algo's listener only queues a command, so this is fine today; keep that rule for anything added later.

---

## 2. What each service does

### Master (`master_build`)
Reads today's contract file (`.db3`) once, builds the lookup tables (symbols, ticks, lots, divisors, option chain) and caches them.
Each contract's own `Divisor` sets its price scale, so NSE paise and US cents both come out right. A file that belongs to the other market is
refused. **Memory:** about 17 MB retained for NSE (85,591 instruments), plus about 7 MB for each lookup dictionary the first time it is used; about
60 MB while building, freed afterwards.

### Candles (`candles_start`)
Builds 30s / 1min / 5min / 15min candles from the live feed. Numbers only, one row per instrument per closed bar. The bar starts at the next full
bar after you start it. **Memory:** about 230 bytes per instrument for the open bar, and **56 bytes per candle row**. This table is by far the
biggest memory item of everything, so it has a size lever:

| NSE, 1-minute candles, one session | instruments | candles |
|---|---|---|
| everything (default) | 85,591 | about 1.8 GB |
| `expiries=1` (each underlying's nearest option expiry) | 33,275 | about 0.7 GB |
| `symbols=["NIFTY","BANKNIFTY"]`, `expiries=2` | 3,139 + futures | about 66 MB |
| `kind="FUT"` (futures only) | 671 | 14 MB |

This pass added **`expiries=`** (`candles_start("1min", expiries=1)`) and a log line at start with the estimated size, which is a warning when it
is over 500 MB. Nothing changes if you do not use it. The straddle needs options of its `n+1` nearest expiries, so `straddle_start` warns if candles
were started with fewer. (Numbers are for every instrument ticking all session; the real feed may carry fewer.)

### Straddle (`straddle_start`)
V and D series per underlying and expiry, built from candles. Unchanged in this pass.

### EOD (`eod_save`, `eod_load`, `eod_auto`)
Saves candles and straddle to Parquet, written through a temporary file and a rename. Unchanged in this pass.

### NATS service (`nats_start`, `nats_connect`, `pf_pause` ...) — see `nats.md`
One connection for every algo and the UI, for subscribe, pause, unpause, squareup, unsubscribe, apply.
- **Started once, connected to by everyone:** `nats_start(...)` is run once for all the users and is safe to call again (it does nothing); an algo
  calls `nats_connect(user=..., client=...)`, which only checks. `nats_restart(...)` is the deliberate restart, `nats_add_user(...)` adds a user
  without one. The registry works the same way (`registry_start` / `registry_connect`).
- **A switch for each algo and the UI:** they all start OFF and each says `nats_send(True, client="bridge_nifty")` for itself, so one algo
  stopping never stops another. There is also a master switch (`nats_send(False)`) for everyone. A command goes out only when both are on.
- **Users and portfolios:** started with the users that may send (`users=[3,4]`); every command is for one user; a portfolio is user + number.
- **Order and speed:** one event loop on one thread, one queue per (user, portfolio) so commands for one portfolio stay in order and different
  portfolios run side by side, and a cap of 64 requests on the wire in all.
- **Errors come back, nothing raises:** `service_down`, `disconnected`, `timeout`, `no_responders`, `blocked`, `no_user`, `user_not_allowed`,
  `full`, `off`. It reconnects by itself about once a second. A command is never re-sent by itself.
- **Blocking:** `nats_block(user=4)` or `nats_block(user=4, pf=1913)`; blocks survive a restart of the service.
- **Memory and CPU:** about 2 KB for each command waiting; nothing is kept after a command finishes (20,000 commands left 0.01 MB behind and
  no queues or tasks); about 70 µs of work per command on the real path, measured as 230 µs with memory tracing switched on; one extra thread.

  **This pass:** a cap on the commands inside the service at once (`max_queued`, default 5000, about 10 MB at most), so a burst cannot grow
  memory without limit; extra commands are refused with `full`. (This also fixed a bug I found while testing it: a new queue's first command
  was counted twice, so the cap acted as half its value during a burst.)

### Portfolio registry (`registry_start`, `pf_create`, `pf_register`, `pf_list`) — see `registry.md`
The one place that hands out portfolio numbers so a user never has two portfolios with the same number. An algo makes one call,
`create(user, strategy, name)`, and gets the number back. It remembers, for each portfolio: the
**user, the number, the name (what it trades), the algo using it, and the book that owns the number**, in a file, so a restart remembers them.
- **This pass:** it used to rewrite the whole file on every change, which got slower with every portfolio (about 17 ms each at 1,000 portfolios
  and rising). It now appends one line per change. Measured now: **3,000 portfolios in 0.75 s (0.25 ms each, flat)**, reload in 15 ms, file
  328 KB. A line cut off by a crash is ignored and tidied; garbage in the middle of the file stops the start instead of reusing numbers; a file
  from the first version is converted automatically.
- **Memory:** about 420 bytes per portfolio.

### Automations as plain scripts (`algo/automation.py`) — see `howto.md` section 5
Functions, no class to define: `algo_start(user, strategy)` registers an automation (its own book and its own sending switch, OFF at every start),
`pf_assign(...)` asks the book for the portfolio (new, the same, or a replacement), `pf_activate(...)` queues the whole command sequence through the
NATS service and returns at once, and `pf_leg`, `pf_cooling`, `algo_sending`,
`algo_portfolios`, `algo_exit` ... look after it. It adds no new service: it is the book and the NATS service behind plain functions.

### Book and bridge
- **Book:** the per-algo bookkeeping (which stock has which portfolio, caps, paused flags). With `Book(user=3)` it takes its numbers from the
  registry, so two strategies for the same user can never be given the same number, and it records the algo and stock name for each portfolio.
- **Bridge:** the order of commands for a swap, retire or exit. It sends through the NATS service, found by name each time, so stopping and
  restarting NATS is picked up with no restart. It sends as its own user, or its book's user.

---

## 3. Numbers behind this page

Measured on the real contract files (NSE 2026-10-16, US 2026-10-14) with a fake NATS connection, outside the package (nothing added to the code).
Not measured: a live Deephaven (table memory is the arithmetic above), and a real NATS outage.

| thing | result |
|---|---|
| tests | 488 pass, including the independence checks |
| master, NSE | 17 MB retained, 60 MB peak while building |
| open candle per instrument | 230 bytes |
| candle row | 56 bytes |
| NATS, 20,000 commands over 500 portfolios | no leftover queues, tasks or memory; 2 threads in the process |
| registry, 3,000 portfolios | 0.25 ms per new portfolio, flat |

## 4. What I did not change

Master's lookup dictionaries (about 7 MB each) could be replaced by arrays to save roughly 15 MB; not worth the risk to working code. The
feed, EOD, straddle maths are untouched. Nothing here is deployed to the host or committed.

---

## The console, one window per strategy

`app.run(algo.Console())` (needs the registry running) publishes **`operator_console`** (all strategies, with a strategy picker and a time window) and, for
every strategy that has portfolios, **`console_<strategy>`**: that strategy's own **summary, net positions, message window and order book**, with the
tick box / Pause / Square up / controls of *its* portfolios only. Starting a new algo (`algo_start(user, "name")`) opens its window by itself;
`console_open("name")` makes one by hand.

How a row finds its strategy: the registry records, for every (user, portfolio number), the strategy (the algo) that uses it. The order, trade and
message tables carry `PF = user * 10000 + portfolio number`, so each of those rows is tagged with a `Strategy` column from the registry, live (a trade
of a portfolio that is registered a moment later is tagged as soon as the registry row arrives). A row of an unknown portfolio has no strategy and
shows only on the all-strategies window. Strategy names may hold letters, digits, `_ . -` only (they go into queries and table names).
The pause / square up / subscribe state in these windows is the shared state described in `nats.md`.
