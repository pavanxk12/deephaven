# algo — plugin framework for the Deephaven trading stack

Everything runs inside one Deephaven session. Each component is a **plugin**: it declares what it needs, builds its
output when started, and cleans up after itself when stopped or replaced. Plugins do not import each other's globals;
they find each other through the `App`.

```
algo/
  core/        app.py (App: running plugins + published tables)   plugin.py (lifecycle, listeners, threads)
               pfstate.py (what the commands did to each portfolio: shared by algos and the console)
               limits.py (the turnover and M2M limit of each strategy, and whether it is breached)
               config.py (Market NSE/US, Paths)                    log.py   nulls.py
  data/        master.py     contract master from the dated .db3, cached      -> master
               candles.py    1-minute candles from the tick table             -> candles
               store.py      end-of-day save, history load
  series/      straddle.py   V / D straddle series                            -> straddle_series
  comms/       nats_service.py  the ONE NATS service: a queue per portfolio, switches, blocks, errors   -> nats_events, portfolio_state_log
  strategy/    book.py       the book: portfolio numbers, caps, paused, allow (state files)
               registry.py   portfolio numbers, unique per user, with strategy and name         -> portfolio_registry
               bridge.py     the command sequence of a portfolio (retire, subscribe, pause, apply, unpause)
               config.py     the bridge's settings
               guard.py      a turnover / M2M breach: switch the strategy off (and square up its portfolios for M2M)
  ui/          console.py    operator console, one window per strategy (same pause / squareup / tick state as the algos)  -> operator_console, console_<strategy>
  lookup.py    plain lookups: contract, strikes, nearest_strikes, last_price ...
  automation.py  plain functions for a script: algo_start, pf_assign, pf_activate ...
  api.py       the functions you type in the console (algo_help() lists them)
tests/         pure logic and wiring, runs without Deephaven:  python -m pytest
scripts/       the few lines you paste each day
```

Pure logic (candle aggregation, ltt parsing, straddle maths, command sequences) is separate from the Deephaven wiring and covered by tests. Deephaven queries are not testable outside a
server, so they are kept small and in one place per plugin.

How master and candles work inside (workflow, which steps are numpy / DataFrame / engine, threads, memory, error handling): [docs/master_and_candles.md](docs/master_and_candles.md).

Indicators are not part of the package: a script computes the ones it needs from the candles it reads (`examples/03_algo_template.py` does an RSI).

History and comparisons: how saved days, `absolute()` (now against the same minute of past periods) and `relative()` (now against a basket) work, with the numbers measured: [docs/history_absolute_relative.md](docs/history_absolute_relative.md).

## A day

Once per session (or the last line of `init.py`): `import algo; algo.install(globals(), "NSE")`. That puts plain
functions in the session, so the day is a few calls (`scripts/01_day.py`):

```python
master_build("NSE")                    # contract master for today ("NSE" or "US")
candles_start("1min")                 # "30s" | "1min" | "5min" | "15min"
straddle_start(0)                     # nearest option expiry only; 1 = two nearest, 2 = three nearest, ...
```

* `candles_start("5min")` again restarts the candles on the new size (the table starts empty). The straddle
  follows the new table by itself, and so does a script listening with `watch("candles", fn)`.
* `straddle_start(n)`: expiries are counted over everything listed, weeklies included. Each expiry uses the future that
  goes with it: the nearest future expiring on or after it, so weekly options use the monthly future and next month's
  options use next month's future. One point per minute (per candle if candles are larger than a minute).
  `straddle_start(1, symbols=["NIFTY"])` limits it. **A symbol with no future uses its spot (cash) price as the reference**
  (US stocks and ETFs); a symbol with neither is skipped and named in the log (US SPX, SPXW).
* `master_build("US")` switches the exchange (it stops plugins running for the other one); `master_build(force=True)` ignores the cache, `algo_status()` shows what is running.

The console is a plugin (`scripts/03_console.py`). Running a plugin whose name is
already running replaces it, so re-running a call is always safe. `algo.reload()` after editing the package.

## Prices and units

`candles_start("1min", 0.01)`: the **multiplier** is applied to every feed price, so a feed in paise (x100) gets
`0.01` and the candles hold proper rates. The default is the exchange's own scale (NSE 0.01, US 1). From the candles on,
**everything is in proper currency units as doubles** (candles, straddle): there is no `price_div`
anywhere downstream, so there is one place where the scale can be wrong. Volume stays int64.

* Why doubles: a multiplier like 0.01 makes prices fractional, so int64 would round 245.67 to 246. Prices are rounded to 6
  decimals on the way in (so 24567 x 0.01 is stored as 245.67, not 245.67000000000002); averages are not rounded.
* Orders are the one place prices become integers again: sizes are floored/ceiled to the instrument's tick with a small
  nudge so the bridge's truncation (x100, int) lands on the tick.
* The code assumes `fo_market_data.ltp` is in the exchange's minor unit (NSE: paise). Set the multiplier to match the real feed.
* Bad input is dropped, never stored: NaN, infinity, null, zero or negative prices, negative volume, unparseable times.
  Dropped rows are counted (`app.status()` shows how many).

## End of day: whole tables to Parquet files

```python
eod_save()                       # today: candles_2026-10-15.parquet, straddle_2026-10-15.parquet (+ .meta.json each)
eod_save(tables=["candles"])                                              # choose which tables
eod_save(day="2026-10-14", folder="/data/eod")                           # another day / another folder
eod_auto()                       # saves by itself every day, 5 minutes after the session ends (NSE 15:35, US 16:05)
eod_auto("15:40")                # or at a time you choose;  eod_auto(stop=True) switches it off
eod_load("2026-10-15")           # -> tables candles_20261015, straddle_20261015
eod_load(days=5)                # the last 5 saved days merged -> candles_hist, straddle_hist
eod_load()                       # every saved day merged -> candles_hist, straddle_hist
```
* **The whole table is written**, every row and column, with an added `Date` column (the tables carry only a time of day).
  The `.meta.json` beside each file records the market, timezone, row count, columns, when it was saved and how the table was built.
* **Safe:** a file is written under a temporary name and renamed when complete, so a crash never leaves half a file and never
  damages the previous day's. A missing or empty table is skipped and reported; one failing table does not stop the other.
* **Memory:** by default a frozen copy of the table is taken first (so it can keep ticking while it is written), which
  briefly needs the memory of that table (~770 MB for 32,000 instruments). `eod_save(snapshot=False)` writes the live table instead.
* **Getting the files outside Docker:** `/data/fo_market_data` is a read-only mount, so by default the files stay in the
  Deephaven volume and need `docker cp`. To see them on the host straight away, mount a writable folder and point the
  exporter at it:
  ```yaml
      volumes:
        - ./deephaven/eod:/data/eod            # in docker-compose.yml, under the deephaven service
  ```
  ```python
  import algo; algo.install(globals(), "NSE", paths=algo.Paths(eod_dir="/data/eod"))
  ```

## Memory and speed

The candle table grows all day (one row per instrument per bar), so **how many instruments you build candles for** decides the memory. A rough guide, 1-minute bars:

| instruments | rows per day | candles (numbers only, ~64 B/row) |
|---|---|---|
| 700 (futures + a few chains) | 262,500 | ~17 MB |
| 32,000 (every option of every symbol) | 12,000,000 | ~770 MB |

* Restrict at the source: `candles_start("1min", symbols=["NIFTY","BANKNIFTY"])`, `kind="FUT"` (or `"OPT"`, `"EQ"`).
  `expiries=1` keeps only the nearest expiry of the options.
* The candle table stores numbers only; the `Symbol` text is joined from one small table, not kept per row.
* The feed batch is cleaned and scaled with numpy in one pass; only usable rows reach Python (about 2.4x faster than row by
  row on a feed where a third of the rows have no trade yet, with identical candles).
* No extra threads: candles run on Deephaven's own listener thread; only the straddle sampler, the NATS service and the
  bridge have a thread each.
* These numbers are estimates from row sizes, not measurements of a running Deephaven.

## Adding things

**A plugin** — subclass `algo.Plugin`, set `name` and `requires`, implement `start()`. Use `self.listen(table, fn)`
for table listeners and `self.spawn(fn)` for threads (both are stopped for you), `self.app.publish(name, table)` for
output, `self.log` for logging.

## Deploying into the container

The package is plain Python. Mount the folder, no image rebuild:

```yaml
  deephaven:
    environment:
      PYTHONPATH: /opt
      PYTHONDONTWRITEBYTECODE: "1"
    volumes:
      - ./deephaven/algo:/opt/algo:ro        # a git checkout of this repo
```
Writable locations are on the `deephaven_data` volume (`/data/deephaven`), because `/data/fo_market_data` is a
read-only mount:

| what | where |
|---|---|
| portfolio state (`pf_map.json`, `pf_sub.json`) | `/data/deephaven/state/` |
| master cache | `/data/deephaven/cache/` |
| end-of-day files (`candles_<day>.parquet`, `straddle_<day>.parquet`) | `/data/deephaven/fo_market_data/` by default, or any folder you choose (below) |
| contract `.db3`, `stocks.txt`, `logo.png`, history to load | `/data/fo_market_data/` (read-only) |

**Before the first run copy the old state files** (`/data/stocks_pf.json`, `/data/stocks_pf_sub.json`) to
`/data/deephaven/state/pf_map.json` and `pf_sub.json`. Without them portfolio numbering restarts at `pf_start`.

## Where the old scripts went

| old | now |
|---|---|
| masterbuild.py | `algo.Master` |
| candles.py | `algo.Candles` |
| indicators.py | not in the package: a script computes its own from the candles |
| straddle.py | `algo.Straddle` |
| eod.py / bod.py | `eod_save()` / `eod_auto()` / `eod_load()` (`algo.Eod`, `algo.History`) |
| automation_rsi.py | a plain script on the services: `examples/03_algo_template.py` is the shape; the stocks logic (baskets, mid filter) is yours to write in it |
| niftyrsi.py | `examples/04_nifty_multistrike.py` (a plain script, with the multi-strike part) |
| dashboard_blackbox.py | `algo.Console` |

The numbers that sat in the USER INPUT blocks stay as constants at the top of your script; the connection, the users and the subscribe payload
are `nats_start(...)`, the portfolio numbers are `algo_start(...)`, and `StraddleConfig`, `Market` and `Paths` are as before.

## Behaviour to know about

* `PCT()` (percent from the open) is measured from the first bar `candles` built (candles start at the next full minute
  after `Candles` starts), so after a late start it is not the real day open.
* US: checked against the 2026-10-14 contract file and a real feed export (tokens 1-8): strikes / ticks / equity prices use the
  file's `Divisor`, `ltt` is text `yyyy-mm-dd HH:MM:SS` in UTC. Not yet checked: the live option prices against their divisor
  (10000), and the US option tick (the file lists 0.0001; an order script should refuse anything under 0.01). The straddle works for US
  stocks and ETFs through the spot fallback; `contract("AAPL")` falls back to the cash instrument when there is no future, and a script can do the same.
