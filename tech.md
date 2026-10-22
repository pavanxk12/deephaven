# Technical overview: what runs, how it is wired, how a signal becomes an order

Written for: an engineer who has to understand, operate or extend the whole stack. Nothing here has been run on a live Deephaven yet;
what is verified is the logic, by tests with fakes (475 passing), and that is said where it matters (section 12).

---

## 1. The whole picture

Everything runs **inside one Deephaven session** (one Python process in the Deephaven container) plus **one real NATS server** outside it.
The package `algo` is a wheel installed in that session. The session holds a small number of long-lived pieces ("plugins"); your trading
scripts (algos) are plain Python pasted into the same session and call plain functions.

```
 exchange feed ──► fo_market_data (ticking table, one row per token: ltp, ltt, volume ...)      [Deephaven, from your feed]
                        │ listener
                        ▼
   contract file ─► MASTER ──► CANDLES ──► table `candles`  ─────────────┬──► STRADDLE ──► `straddle_series`
   (.db3, daily)    (lookups)   (bars)      (closed bars only)           │
                        │                                                ├──► END OF DAY ──► parquet files (+ .meta.json)
                        │                                                └──► your algo scripts (watch("candles", fn))
                        ▼
              lookups: contract, strikes, last_price ...   (plain functions, read-only)

   your algo script ── algo_start(user, "name") ─► its own BOOK + BRIDGE ─┐
        pf_assign()  = BOOK picks the portfolio number (REGISTRY keeps it, unique per user)
        pf_activate()= BRIDGE queues: [retire old] subscribe, pause, apply, unpause  ──► NATS SERVICE ──► NATS server ──► order bridge (C++/other)
                                                                                         │ (one service for everybody)
   console (UI) ── click a row, then Pause / Square up / Subscribe ───────────────────────┘
                                                                                         │ every confirmed command
                                                                                         ▼
                                                                  SHARED PORTFOLIO STATE (algo.core.pfstate)  ──► read by algos AND the console
   order/trade/ack tables from the bridge (strategy_events, orders_trade, orders_ack) ──► CONSOLE: one window per strategy
```

Three rules hold the design together:

1. **Services are independent.** None imports another one (a test reads every import line and fails if one is added). They find each other
   *by name in the `App`* only when needed. If one stops or is busy, the others carry on (section 9).
2. **Algos are plain functions and scripts**, not classes of the package. The package gives infrastructure only: master, candles, straddle,
   end of day, NATS, registry/book/bridge, console. Indicators, signals and sizing are written in the algo script.
3. **One place for each fact.** Contract data in the master; bars in `candles`; commands only through the NATS service; portfolio identity in the
   registry; what the commands did in the shared portfolio state.

---

## 2. What is running (and on what thread)

| piece | what it is | started by | thread | published tables |
|---|---|---|---|---|
| **App** | one per session: the running plugins, the published tables, `app.shared` (objects that must outlive a plugin restart) | `algo.install(globals(), "NSE")` | - | - |
| **master** | parses the day's contract `.db3` once (cached as a pickle), builds the lookups | `master_build("NSE")` | caller's | `master` (dict) |
| **candles** | turns the live feed into closed bars | `candles_start("1min", ...)` | Deephaven's listener thread (no own thread) | `candles` |
| **straddle** | V / D straddle series per underlying and expiry, from candles | `straddle_start(0)` | one sampler thread `algo-straddle` + candle listener | `straddle_series` |
| **eod** | saves whole tables to Parquet once a day | `eod_auto()` (or `eod_save()` now) | one thread `algo-eod` | files, `candles_hist`... on load |
| **schedule** | starts the day by itself and closes it: master, candles, registry, NATS, console at set times; at the close saves the candles and straddle tables, then stops the straddle and candles | `day_auto()` | one thread `algo-schedule` | none of its own (it starts the others) |
| **registry** | the portfolio registry: (user, number) -> strategy, name, owner | `registry_start()` | caller's (a small file append) | `portfolio_registry`, `portfolio_registry_log` |
| **nats** | the ONE command service | `nats_start(ip, port, users=[...])` | one asyncio loop thread `algo-nats` | `cmd_queue_automated`, `nats_events`, `portfolio_state_log` |
| **console** | the operator dashboards | `app.run(algo.Console())` | Deephaven UI render threads | `operator_console`, `<strategy>` |
| **per algo**: `<name>_book`, `<name>_bridge`, `<name>` | what `algo_start` creates for one automation | your script | none of their own (the bridge uses the NATS loop) | - |

Plain functions (not plugins) read the master and candles: `contract`, `strikes`, `nearest_strikes`, `last_price`, `last_candles` (`algo/lookup.py`).

`algo_status()` lists what is running; `algo_help()` lists every function by service.

---

### Start-up at container start

The India host's `init.py` ends with a guarded block that runs the start-up sequence (install, master from Postgres or the file, candles, registry, NATS, console) each time the Deephaven
container starts; see `run.md` section 0 for what that means for the day. The single-order strategy (`07_basket_rsi_crossorder.py`) is started by one `subscribe` per trade on a new
portfolio with `"strategyname": "single"` and closed with `squareup`: the engine's plugin reads its parameters once per portfolio.

## 3. The plugin framework (`algo/core`)

* `App` (`app.py`): `plugins` (name -> running plugin), `tables` (published), `shared`, `ns` (the session globals, so a published table is also a
  variable in the IDE). `app.run(plugin)` first stops a running plugin of the same name, so **a re-paste never leaves a duplicate listener behind**.
* `Plugin` (`plugin.py`): `requires = (...)` names other plugins it needs (a clear message if one is missing: *"straddle needs ['candles']. Call
  candles_start() first."*); `start()` builds its output; `listen(table, fn)` for table listeners and `spawn(fn)` for threads, **both stopped for
  you** when the plugin stops; `follow("candles", cb)` = "call me when another plugin republishes this table" (how the straddle notices a candle
  restart); `on_stop()` for cleanup.
* `config.py`: `Market` (NSE / US: timezone, price scale, session times, file formats, feed table name) and `Paths` (where state, cache, EOD and the
  market data live). `FIRST_PF = 1001`, the first portfolio number.
* `pfstate.py`: the shared portfolio state (section 8). `files.py`: safe atomic writes (temp file + rename, retrying a briefly locked rename).
  `nulls.py`, `timeframe.py`, `log.py`: small helpers.
* `algo.install(globals(), "NSE")` builds the `App` and puts the plain functions of `api.py` into the session.

Start-up order (each step needs the ones before it; calling out of order prints what to call first):

```
algo.install(globals(), "NSE")      the App and the functions
master_build("NSE")                 contract master            (needs the .db3 in /data/fo_market_data)
candles_start("1min")               needs master and the live table fo_market_data
straddle_start(0)                   needs master + candles                (optional)
eod_auto()                          needs candles / straddle              (optional)
registry_start()                    portfolio numbers                     (once)
nats_start("192.168.1.130", 4222, users=[3, 4])   the command service     (once)
app.run(algo.Console())             needs master + registry (+ strategy_events, orders_trade, orders_ack tables)
--- then, per algo script ---
nats_connect(user=, client=) ; registry_connect() ; algo_start(user, "name") ; ... listeners ... ; nats_send(True, client="name")
```

"Start once, connect many": `nats_start` and `registry_start` are safe to call twice (second call does nothing and refuses to change a running
service with other settings; that is `nats_restart` on purpose). A script only **connects** (`nats_connect`, `registry_connect` check that the
service is up and the user may send; they start and change nothing).

---

## 4. The data path: feed -> candles -> everything derived

**Feed.** `fo_market_data` is a Deephaven table with one row per token holding the last value (`ltp`, `ltt` last trade time, `volume`). It is your table; the
code assumes `ltp` is in the exchange's minor unit (NSE: paise) and applies the multiplier (default `1/price_div`; or each contract's own `Divisor`).

**Master** (`algo/data/master.py`). Reads the day's `.db3` (refuses the other market's file), keeps dictionaries token -> symbol, divisor, tick, expiry,
strike ...; `tokens_for(symbols, kind, expiries)` selects instruments. Cached as a pickle keyed by file name, mtime and size. About 17 MB retained for all NSE.

**Candles** (`algo/data/candles.py`). The listener gets each batch of changed feed rows; numpy cleans and scales the batch in one pass (drops NaN, inf,
null, zero or negative prices, negative volume, unparseable times); a `CandleAggregator` keeps one open bar per token (~230 B) and emits **only closed
bars** after a 10 s grace, on epoch-aligned buckets (30s / 1min / 5min / 15min). A bar is written as one row of numbers to a `DynamicTableWriter`; the
`Symbol` text is joined from a small static table, not stored per row. Row = 56 bytes. A quiet token produces no row for that bar (no forward fill).
Starting in the middle of a minute starts at the next full bar. Options to bound memory: `symbols=`, `kind="FUT"|"OPT"|"EQ"`, `expiries=1`
(about 1.8 GB/day for everything on 1-minute bars; 0.7 GB with `expiries=1`; 14 MB futures only; a size estimate is logged at start).
`candles_start` again restarts the table empty and **republishes** it; whoever `follow`s it moves to the new table.

**Straddle** (`algo/series/straddle.py`). For each underlying and expiry it uses the future that goes with that expiry (nearest future on or after it;
spot if no future exists, e.g. US) and builds V / D series from the candles of the future and the options, one point per minute (per candle if candles
are larger). Backfills from existing candles, then runs live (listener + sampler thread). Restarts itself when candles restart.

**End of day** (`algo/data/store.py`). `eod_save()` writes whole tables (all rows and columns plus a `Date` column) to Parquet with a `.meta.json`, via a
temporary file renamed on completion (a crash never leaves half a file or damages yesterday's). `eod_auto()` does it daily 5 minutes after the close;
`eod_load()` brings files back as tables. By default it snapshots first (briefly one extra copy of the table in memory).

**Algos and candles.** `watch("candles", fn)` (in `algo/automation.py`) listens to the `candles` table **and follows it** across restarts; `fn(update,
is_replay)` gets each batch of newly closed candles. `last_candles(x, n)` seeds an indicator from today's history. Any time frame at or above the candle size is
the algo's own roll-up. The indicator (e.g. Wilder RSI) is computed in the script (`examples/03_algo_template.py`).

---

## 5. The command path: signal -> portfolio -> NATS -> bridge

### 5.1 Identity: registry and book

* A **portfolio** is identified by **(user, number)**. The same number under two users is two portfolios; a user never gets a number twice.
  Numbers start at **1001** (`FIRST_PF`) unless an algo gives `pf_start`.
* **Registry** (`algo/strategy/registry.py`): the Postgres table `algo_portfolios` when `ALGO_STATE_STORE=postgres` (see "State in Postgres" below), else the append-only JSONL file `portfolio_registry.json` in the state folder. For each (user, number) it
  remembers `owner` (the book), `algo` (the strategy), `name` (what it trades), `created`, `note`. `allocate/create` reserve a number *before* returning it.
  Tables `portfolio_registry` (current record) and `portfolio_registry_log` (every change).
* **Book** (`algo/strategy/book.py`): one per automation (`<name>_book`). It decides *which portfolio an instrument and option map to*:
  none yet -> a new number from the registry; same option -> the same portfolio; another option -> a **new** portfolio and the old one is reported as
  `old_pf` (to retire). The algo never picks a number. State kept: `pf_map` (instrument -> pf, file), `subscribed` (pf -> option, file), caps, paused,
  live leg, allow flags, last failure. Slots: `instrument*100 + slot` lets one instrument have several portfolios (multi-strike).
  Old files can be adopted (`algo_start(..., map_file=, sub_file=)`); clashes are reported, wrong-format files are refused, never overwritten.

### 5.2 Your script

```python
algo_start(USER, STRATEGY, pf_start=1001)       # registers: its own book + bridge + sending switch, starts OFF; opens its console window
pf_track(STRATEGY, [future], slots=N)           # find the portfolios it had before a restart
watch("candles", on_candle)                     # your listener
a = pf_assign(STRATEGY, future, option, name="NIFTY", slot=k)   # the BOOK picks the number; None while OFF or while unticked on the console
pf_activate(a, "CALL", params)                  # the BRIDGE queues the sequence; returns at once
nats_send(True, client=STRATEGY)                # last line: switch this automation on
```

`params` (ival, jval, sval, trp, ranges ...) come from **your** code (`order_params` in the examples): sizing is strategy logic.

### 5.3 Bridge (`algo/strategy/bridge.py`)

One per algo (`<name>_bridge`); it holds **no connection**. It knows the *order of commands* and gives the NATS service a workflow:

```
swap(pf, option, params):   [retire old: pause -> (squareup) -> (unsubscribe)]   then   subscribe (if new) -> pause -> apply (maxqty = cap + add_qty) -> unpause
```

Any step that is not `ok` stops the sequence, leaves the portfolio **paused**, rolls the book's bookkeeping back, sets a cooldown (`pf_cooling`) and writes a
`swap failed` row to `nats_events`. Each bridge is one **client** of the service with **its own send switch** (starts OFF).

### 5.4 NATS service (`algo/comms/nats_service.py`) — the only thing that talks to NATS

* **One thread, one asyncio loop**, no locks. A **lane per (user, portfolio)** is a bounded FIFO: commands of one portfolio always run in the order
  given (pause, apply, unpause can never overtake each other); different portfolios run side by side. `max_in_flight` (64) caps requests on the wire for all;
  `lane_depth` caps waiting jobs per lane; `max_queued` (5000) caps everything inside. Over a cap the command is refused with status `full`, never silently lost.
* Each command is a NATS request on subject `cmd.hft.<pf>` with `{"Cmd", "Payload", "ReplyId"}`; the payload is stamped with `USERIDENTIFIER`/`user_id` and `SENDER`.
  The answer's `Status` ("ok" or an error) becomes a `Reply(ok, status, ...)`.
* **Never raises to the caller; never resends by itself.** After a timeout it is unknown whether the bridge acted, so the caller sees `timeout` and decides.
  Statuses besides the bridge's own: `timeout, no_responders, disconnected, service_down, off, dry, full, stopped, bad_reply, error, no_user, user_not_allowed, blocked`.
  Every refusal and failure is also a row in `nats_events`; every accepted command is a row in `cmd_queue_automated` (audit).
* **Connection**: if NATS drops, commands fail fast with `disconnected` and the client reconnects about once a second (no restart needed). The first
  connect times out after about 3 s with a clear error. `NatsService` stopped/never started -> `NO_SERVICE` stand-in answers `service_down`: algos and
  console keep working and say what is wrong.
* **Users and blocks**: `nats_start(users=[3,4])` lists who may send (`nats_add_user` adds one live); `nats_block(user=)` / `(user=, pf=)` stops sending for a user
  or one portfolio; blocks live in `app.shared` and **survive a service restart** (a restart that names a user lifts that user's block).
* **Switches**: master (`nats_send(True/False)`) and one per strategy (`nats_send(True/False, client="name")`, the name given to `algo_start`; a name that is not a running strategy is refused). A command goes out only if both are on. Typed commands (`pf_pause(...)`) and the
  console buttons have no client and need only the master switch.
* **Dry run**: `nats_start(dry_run=True)` opens no connection and only logs.

### 5.5 Shared portfolio state (`algo/core/pfstate.py`)

After every command the bridge answers `ok`, the service records it in one shared object (`app.shared["pf_states"]`), **whoever sent it**: per (user, pf)
`option, subscribed, paused, squared, by, cmd, at`. A failed command changes nothing. Each change is a row in `portfolio_state_log`. Listeners are told.

```
 algo command  ──►  NATS service ──ok──►  PfStates  ──►  console (State column of the positions table, toolbar)
 console click ──►  NATS service ──ok──►  PfStates  ──►  the algo's bridge hears it and updates its book:
                                                            pause / resume   -> book.paused; the algo's next order APPLIES but does not unpause (their pause stands)
                                                            square up        -> pf_leg() = None: the slot may start again
                                                            Unsubscribe      -> slot blocked: pf_allowed() False, pf_assign() None, until subscribed again
```

An algo's *own* unsubscribe (retiring a portfolio) never blocks its slot; only the operator's Unsubscribe does. After a Deephaven restart the states start
empty and each book re-adopts what it had subscribed.

---

## 6. The console (`algo/ui`)

* Inputs: `strategy_events`, `orders_trade`, `orders_ack` (from your order bridge, via Kafka) and the live feed table; the registry and the shared state.
* **Strategy mapping**: trade/ack/message rows carry `PF = user * 10000 + portfolio number`; they are tagged with a `Strategy` column joined live from
  `portfolio_registry` (`PfKey = User*10000 + PF` -> `Algo`). Unknown portfolio -> null strategy (visible only on the all-strategies window).
* **Windows**: `operator_console` (all strategies, strategy picker, time window) and `<strategy>`: **its own** summary, net positions, message window,
  order book, controls. A new `algo_start` opens its window; `console_open("name")` by hand. Names may only hold letters, digits, `_ . -`.
* **Net positions** are a **native table** (the browser draws it; the engine streams only the cells that changed, so a price tick costs no Python redraw). Click a row and a toolbar
  above it acts on that row's portfolio: Pause / Resume, Square up (two clicks, only while paused), Subscribe / Unsubscribe. A State column shows RUNNING / PAUSED / SQUARED /
  UNSUBSCRIBED. Rules kept: Square up needs Pause first; no Unsubscribe while the net position is open. Commands go through the NATS service with no client name (operator);
  failures are written beside the buttons. The state is read from two live tables the console publishes: `portfolio_states_log` (one row per confirmed command, the table form of
  `PfStates`) and `pf_by_option` (for every contract, the portfolio that trades it, a subscribed one first, with its strategy). A portfolio is linked to its contract by the
  subscribe's `T1` (churning) or `TOKEN` (the single-order strategy). Rows are bigger (`density="spacious"`). Code: `fast_positions`, `fast_toolbar` in `algo/ui/console.py`.
* **ON / OFF button** (Controls panel, `algo/ui/controls.py`): in `operator_console` it flips the service's master switch for everyone; in a strategy's own window it flips only that
  strategy's switch (the same as `nats_send(..., client="name")`) and shows ON only when the master is on too. Pause all / Resume all / Square off all act on that strategy's portfolios only.
* **Limits** are also a live table: `strategy_limits` (one row per strategy: both limits, turnover and M2M now, breached or not; `strategy_limits_log` keeps every change), built in `Console._limits_table` from `Limits`, the registry and `q.strategy_totals`; the strategy window shows its row, `operator_console` shows the table. One table listener feeds the guard for every strategy.
* **Limits** (`algo/core/limits.py` numbers and latches, `algo/strategy/guard.py` the action): a strategy window has a **turnover limit (₹)** and an **M2M limit (₹)** box in its Summary,
  changeable any time (0 = none). The console feeds the guard the strategy's whole-day turnover and gross M2M from a server-side table listener on every change, so it works with no browser open.
  Turnover at/above its limit: that strategy's sending switch goes OFF. M2M at/below its limit (the number as typed, -5000 means -5000): its subscribed portfolios (registry algo = the strategy, each under its own user) are paused, squared up and unsubscribed
  as operator commands, then its switch goes OFF. Either way a row goes to `nats_events` (`cmd = limit`), the window shows LIMIT BREACHED and the ON / OFF button flips to OFF. The breach is **latched**: ON (the button,
  or `nats_send(True, client=...)`) is refused until the limit is set to a value the strategy is no longer past; changing it does not switch ON by itself. State: `<work>/state/limits.json`.
* Memory/CPU: every table is a lazy chain; per-row text is joined, not stored; the whole-day net table per strategy is created when its window renders.
* **Redraws.** A panel is a Python function; Deephaven re-runs it (in the one Python thread) when its state or a table it reads changes, and sends the difference to the browser.
  `use_redraw` asks for a redraw on every portfolio-state change. Heavy panels therefore wait behind the candle work; the native positions table avoids that for price ticks.
  Known noise: `KeyError: 'Key N not initialized'` from deephaven.ui when a state-change listener fires for a page that was closed or reloaded; it is dropped and harmless.

---

## 7. Files and tables

| where | what |
|---|---|
| `/data/fo_market_data/` (read-only mount) | contract `.db3`, `stocks.txt`, `logo.png`, history to load |
| `/data/deephaven/state/` | `portfolio_registry.json`, per-algo `pf_map`/`pf_sub` files, `limits.json` (the state itself; with `ALGO_STATE_STORE=postgres` they are a backup copy of what is in Postgres) |
| `/data/deephaven/cache/` | master pickles |
| `/data/fo_market_data/` (the market-data folder, mounted writable) or your `eod_dir` | `candles_<day>.parquet`, `straddle_<day>.parquet` (+ `.meta.json`) |

Published tables (Deephaven IDE variables): `candles`, `straddle_series`, `portfolio_registry`, `portfolio_registry_log`, `cmd_queue_automated`, `nats_events`,
`portfolio_state_log` (the NATS service's), `strategy_limits`, `strategy_limits_log`, `portfolio_states_log`, `pf_by_option` (the console's), and the windows `operator_console` and one
per strategy, named after it (`basket_rsi`, `nifty_rsi`, ...); after `eod_load`: `candles_<day>`, `candles_hist`, ...

---

## 8. Memory and CPU

* Candles dominate: **56 B per closed bar row**; ~230 B per instrument for the open bar. Control with `symbols=`, `kind=`, `expiries=`, the bar size.
* Master ~17 MB; registry a few hundred bytes per portfolio (3,000 portfolios load in under a second); shared portfolio state ~100 B each.
* NATS: one thread; queued commands are tiny; bounded by `max_queued`.
* No extra threads for candles (Deephaven's listener thread); straddle and EOD one thread each; NATS one. Measurements are external; the code has no instrumentation.
* **The ceiling is one Python thread.** All Python in the Deephaven process (the candle builder, every strategy's listener, every console callback) shares one lock, so it can use one
  core. Measured on the India host (16 cores, 2026-10-19, all 85,957 instruments): the container used about 110% CPU (one core), the Python thread averaged ~70% and peaked at 100%,
  memory was flat at ~2.9 GiB of a 16 GiB limit, and candle batches took a median of 0.4 to 0.9 s against a 250 ms limit. Cutting the candle instruments (`expiries=1`, `kind="FUT"`)
  is the lever; adding strategies or panels only adds to that thread. Docker's memory figure is the whole process; the Deephaven UI's is only the Java heap in use.

---

## 9. What happens when something breaks

| breaks | effect on the rest |
|---|---|
| **NATS server down / connection lost** | candles, straddle, master, registry untouched; algos keep computing signals; commands return `disconnected`, swaps roll back and retry after the cooldown; reconnects by itself |
| **NATS service stopped** (`nats_stop`) | same, answers `service_down`; restart with `nats_start`, algos need not restart; every algo is OFF again and switches itself on |
| **registry** | only creating a *new* number fails, with a message |
| **candles restarted** | straddle and `watch()` listeners follow the new table; a plain `listen()` would stay on the old, silent one |
| **an algo** | the others do not know it exists |
| **a command times out** | not resent; caller sees `timeout`; look at the bridge before sending again |
| **a bad feed row** | dropped and counted, never stored |
| **Deephaven restarted during the day** | scripts pasted into the terminal are gone (positions in them are no longer known: square them up from the console); a stop that was waiting out `squareup_settle_s` loses its unsubscribe, so those portfolios stay subscribed (flat) in the engine; the candles table starts again, so "open" and "VWAP" mean the restart time; `orders_ack` / `orders_trade` / `orders_error` continue from the committed Kafka offset of group `deephaven-hft` and do not show what was consumed before |
| **the engine's plugin slots are used up** | seen on 130 on 2026-10-22: PFs 31001 to 31032 (32 plugin instances) were created and none after that, while every later `subscribe` was still acknowledged "ok" and nothing reached `orders_ack` / `orders_trade`. The cap is inferred from that pattern, not read from the engine. A portfolio that is squared up but not unsubscribed keeps its slot |

---

## 10. Where each thing lives in the code

```
algo/core/      app.py plugin.py config.py pfstate.py limits.py files.py log.py nulls.py timeframe.py
algo/data/      master.py candles.py store.py symbols.py
algo/series/    straddle.py
algo/comms/     nats_service.py
algo/strategy/  registry.py book.py bridge.py config.py guard.py
algo/ui/        console.py tables.py filters.py
algo/           api.py (functions you type)  automation.py (functions an algo script uses)  lookup.py (contract/price lookups)
examples/       00_services.py .. 10_us_zscore_crossorder.py   (plain scripts; 03/04/06/07/08 compute their own RSI and sizing, 10 a z-score; 07, 08 and 10 keep all their names inside one function)
scripts/        00_install .. 05_start_day.py   (the lines you paste; 05 is the whole start-up, which the India host's init.py also runs at every container start)
tests/          pytest, no Deephaven needed
```
Allowed imports (test-enforced): `core` knows nothing; `data` uses core; `series` core+data; `comms` and the registry core only; `strategy` core+comms+data; `ui` anything.

---

## 10b. History, `absolute()` and `relative()`

Saved days (`candles_<day>`, `straddle_<day>` Parquet files, from `eod_save` and the converters in `tools/`) feed two plain functions that compute on call and keep only a size-limited cache: `absolute(series, level, symbol, n)` compares a symbol with its own past at the same exchange session minute, and `relative(series, level, symbol, basket)` compares it with a basket from `baskets.txt`. Details, formulas, costs and checks: `docs/history_absolute_relative.md`.

---

## 11. Extending it

* **A new algo**: copy `examples/03_algo_template.py`, change `STRATEGY`, `USER`, the signal and the sizing. Nothing in the package changes.
* **A new data service**: a `Plugin` with `requires`, `start()` publishing a table, listeners via `self.listen`; add a function in `api.py`.
* **A new command**: add it in `nats_service.py` (and, if it changes what the console shows, in `pfstate.PfStates.record`).

---

## 12. What is verified and what is not

Verified by tests (fakes for Deephaven tables and the NATS connection; real registry, book, bridge, service logic): candle aggregation and timing, master parsing for NSE
and US files, straddle maths and replay, NATS ordering/caps/errors/reconnect/blocks, registry persistence and uniqueness, the command sequences and their rollbacks,
shared state both ways, the example scripts end to end, Wilder RSI against textbook values, import-direction independence.

**Not verified:** anything on a live Deephaven (query semantics of `natural_join`, `where_in`, listeners, `deephaven.ui` rendering of the console), the real order bridge's
replies, real feed row shapes, the join of the integer `PF` column with the registry (cast to long on both sides), and real memory/CPU numbers (the sizes above are
arithmetic from row sizes). The wheel built from this tree has not been deployed to the dev host.

Since then, on the live India Deephaven (2026-10-19): start-up from `init.py`, the Postgres master, the limits table and its guard, the native positions table with its toolbar,
and orders from `07_basket_rsi_crossorder.py` to the engine have been run. What is still only reasoned about: the toolbar's Unsubscribe for a single-order portfolio (it sends `T1`,
the single-order strategy names its contract `TOKEN`), and the live read of `relative()` for a whole basket under load.

Open items: the subscribe payload defaults are service-wide (a strategy adds its own fields with `algo_start(..., subscribe={...})`); one NATS server per service; the candle
builder is a Python loop (the main CPU cost); `candles_start`/`master_build` are not start-once; the trades-event mapping (contracts, qty, M2M per portfolio) is the next piece.

## State in Postgres (`algo/core/statedb.py`)

With `ALGO_STATE_STORE=postgres` in the Deephaven service's environment, the registry, each strategy's book and the limits live in four small tables of the `algofusion` database
instead of JSON files (created if missing; nothing existing is touched): `algo_portfolios (user_id, pf, owner, name, algo, created, note, updated_at)` primary key (user_id, pf);
`algo_pf_map (book, token, pf)` and `algo_pf_sub (book, pf, option_token)`, where `book` is the book's file name such as `pf_map_nifty_rsi_book.json`; `algo_limits (strategy, turnover, m2m, turnover_hit, m2m_hit, updated_at)`.
The login is the one the contract master uses (`AUTOMATION_DATABASE_URL`, or `ALGO_PG_PASSWORD`; host `postgres`). Each write is one upsert (or one transaction for a book's map) committed before the caller is answered, and a failed write is
raised and undone in memory, so a number is never handed out and then lost. The same change is then also written to the old JSON file as a BACKUP COPY (best effort: a failure there is only logged and never stops a write), and at every start the files are rewritten from the tables, so they always say what Postgres says. A dropped connection is retried once on a new one. If Postgres cannot be reached when the registry, a book or the limits start, they REFUSE to start and say why (starting empty would hand out numbers
already in use); `day_auto` retries the registry step every minute. The first time a table is empty and the old JSON file exists, the file is imported; after that the table is the only source (a file that was changed by hand is overwritten at the next start).
Each server has its own Postgres, so each keeps its own numbers. Without the variable nothing changes: the JSON files are used.

## Expense, net M2M and currency (the console summary)

`net_positions` (algo/ui/tables.py) gives every traded instrument `M2M` = the GROSS result (realised + unrealised), `Expense` and `NetM2M = M2M - Expense`. The summary window shows, in this order: REALISED, UNREALISED, GROSS M2M (their sum), EXPENSE,
NET M2M (gross less expense); the positions table has the same two columns. The expense is rupees per crore (1.0E7) of the value BOUGHT and of the value SOLD, by instrument kind, from `Market.expense_per_crore`:
NSE futures 600 buy / 5,400 sell, options 5,625 / 20,328, equity 200 / 200 (US: none, so no expense tiles). The kind of each instrument comes from the contract master (`kind_table`: its own option, future and equity lists), and only a token the master
does not know falls back to the symbol text ("... CE" / "... PE" option, "... FUT" future; the space matters, RELIANCE is a stock). The limits still use the gross M2M. Every money figure, TURNOVER included, is the actual amount (not crores or millions), with `Market.currency` (rupee for NSE, `$` for the US); the per-crore rates are only used to work out the expense. The queries were run in a real Deephaven session against hand-computed values.

The console divides every price by ITS contract's own divisor, read from the contract master (the `divisor` column of `resultset_<day>`: US stocks 100, US options 10000, NSE 100), not by one number per market; `Market.price_div` is only the fallback for a token the master does not know. A fill the engine published twice (the CrossOrder plugin's) is counted once (`dedup_fills`: one row per portfolio, order number and fill number).

A fill the engine publishes twice (the CrossOrder plugin's) is counted once: `dedup_fills` keeps one row per (`pf`, `response_order_number`, `fill_number`) before positions, totals and the order book are built. The raw `orders_trade` still holds both rows, and the plugin's own position can still double count.

**Which portfolio belongs to which strategy, and the stop.** The book records every confirmed subscribe of one of its portfolios, whoever sent it (the strategy's own bridge, a script that subscribes by itself such as the CrossOrder scripts, the console, a typed command), in `algo_pf_sub`. If the book has nothing, `Bridge.subscribed_option(pf)` asks the NATS service's record of the confirmed subscribe. `algo_stop(strategy)` and the M2M breach use it: for every subscribed portfolio of that strategy they pause, square up, wait `BridgeConfig.squareup_settle_s` (60 s) and unsubscribe. The wait lets a plugin send and fill its closing order, because the unsubscribe destroys the plugin. `algo_stop` waits up to 10 s for the commands, then stops the automation; the unsubscribes finish in the background, and are lost if Deephaven restarts inside the wait. A portfolio nobody subscribed is left alone.

**Money display.** Amounts in the console keep two decimals when the market's currency is not the rupee (`$`), and are whole numbers for NSE.

## Contract picker, null symbols and speed (`algo/series/absolute.py`)

`absolute("pct", ..., "SBIN FUT", n)` needs, for each of the n earlier days, the contract that was the nearest unexpired future of SBIN on that day. `ParquetSource._futures_of` finds it in the
day file's `Symbol` column. Until 2026-10-22 its cache held 64 answers, but a 68-day look-back over 230 stocks needs about 15,000, so every call read the `Symbol` column of 68 files again:
on 130 that step alone took about 380 ms of the 360 ms a call took; on a PC with the real history, 60 stocks x 68 days: **331 ms per stock before, 0.4 ms after** (first pass 7 ms per stock). Now `_index_of` reads
each file's `Symbol` column once into `{underlying: [its future names]}` (800 files kept) and the answers are cached up to 200,000 (file, spec). Measured live on 130 with the new wheel: z-scores of 209
futures in 2.9 s (about 14 ms each; before about 240 to 360 ms each).

A day file can hold a row with a null `Symbol` (130's own end-of-day save of 2026-10-12 had 1,675 of them, tokens the master did not know). `month_contract` used to raise `TypeError` on it, which made
`absolute_warm` and every later `absolute()` fail; it now skips names that are not text. Scripts 11 and 12 also wrap `month_contract` themselves when they are pasted (once), so they work
on a wheel that does not have the fix yet.

## India stock-futures scripts (`examples/11_*`, `12_*`)

11 is 08 and 12 is 10 for the NSE. The universe is every stock future in the master that has not expired (index futures NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50 left out; 232 on 2026-10-22),
dealt one by one into `N_BASKETS = 4` baskets (`fut_1` .. `fut_4`, 58 each). A basket entry is `"SBIN FUT"`: the nearest unexpired future, on each day, so `relative()` ranks today's nearest futures
against each other and `absolute()` compares a future with the nearest future of each earlier day (open and VWAP are taken from the same contract of that day, so a roll does not add the calendar
spread). The trade goes to `contract("SBIN")`, the nearest future. 11 seeds its RSI from the last `HIST_DAYS` saved days, picking each day's nearest future by expiry (`future_names`); a stock's RSI can
show one jump on the morning it rolls. History needs the futures in the saved candle files: on 130 the converted GDFL futures (`history/india_futures_2026` and `..._aug_oct` on the PC,
2026-01-01 to 2026-10-16, copied into `fo_market_data`) plus the server's own saved days; 2026-07-31, 2026-10-19 and the exchange holidays have no file.

**Time exit.** In 08, 10, 11 and 12 `EXIT_AT` now starts the same stop as `algo_stop` (in its own thread, once): pause and squareup every portfolio, unsubscribe after `squareup_settle_s`, stop the
listener. It refuses while sending is OFF (it prints that; run `<strategy>_stop()` by hand). Take-profit, stop-loss and the z-exit send `squareup` for that portfolio. Each trade uses a new portfolio and the engine keeps one plugin instance per subscribed portfolio
until it is unsubscribed, so (given the engine finding in section 9) a squareup alone leaves the slot used and the slots ran out after about 32 trades (seen twice on 130 on 2026-10-22: from the 33rd
entry on, the script printed trades that never reached the engine). All four scripts (08, 10, 11, 12) now **release** the portfolio: `UNSUB_AFTER_S` (60) seconds after an exit is confirmed they send
`unsubscribe` for it (the same command `algo_stop` uses), and `MAX_PORTFOLIOS` (28) stops new entries while the script holds that many portfolios (open positions plus closed ones waiting for their
unsubscribe); a blocked entry is not remembered as "traded in this zone", so the next signal tries again. Two scripts running together share the engine's slots, so their limits must add up to less
than the cap. A release waiting in its timer is lost if Deephaven restarts (the stop at the end of the day still unsubscribes everything). Not yet run against the live engine; the tests check the
command sequence (subscribe, squareup, unsubscribe) and the limit.

**Entries seen on 2026-10-22.** The z-score script (`Z_ENTRY = 3.0`) made no entry: the largest |z| of 209 futures was 1.74 (99th percentile 1.69), and an entry also needs the future's % from open and from
VWAP in the middle 50% of its basket, which works against a 3-sigma move. How many entries a setting would give has not been measured over the saved days.

## Console redraws (`algo/ui/redraw.py`)

The console components redraw when the shared portfolio states change (`use_redraw`). A stop of forty portfolios was forty changes, each queuing a redraw in every open window, and a window whose
browser session had gone could still be called ("Key N not initialized" in the Deephaven log, 114 times on 2026-10-22). `Redraw.soon()` now batches a burst into one redraw per 0.5 s per window and does
nothing once the window is closed; a click still redraws at once. The cause of the old errors was not reproduced, so this is a mitigation; check the log after the console has been open for a while.

## Compose and servers

`docker-compose.yml` on 130 adds `ALGO_STATE_STORE: "postgres"` and mounts `fo_market_data` writable (no `:ro`) so the end-of-day save can write there. 131 also sets `ALGO_MARKET=US` and `ALGO_NATS_IP`;
104 sets `ALGO_NATS_IP`. The compose files are not in this repository. The day timer (`day_auto`) is started from `init.py`, not from the compose file; `day_auto(stop=True)` switches it off until the next restart.
