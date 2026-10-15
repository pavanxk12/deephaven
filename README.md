# algo — plugin framework for the Deephaven trading stack

Everything runs inside one Deephaven session. Each component is a **plugin**: it declares what it needs, builds its
output when started, and cleans up after itself when stopped or replaced. Plugins do not import each other's globals;
they find each other through the `App`.

```
algo/
  core/        app.py (App: running plugins + published tables)   plugin.py (lifecycle, listeners, threads)
               config.py (Market NSE/US, Paths)                    log.py   nulls.py
  data/        master.py     contract master from the dated .db3, cached      -> master
               candles.py    1-minute candles from the tick table             -> candles
               indicators.py SMA / EMA / RSI / MACD / ATR / PCT via update_by                    -> indicators
               store.py      end-of-day save, history load
  series/      straddle.py   V / D straddle series                            -> straddle_series
  strategy/    book.py       portfolio numbers, caps, paused, allow (state files)
               bridge.py     NATS commands, send on/off switch, operator actions
               rsi_options.py  RSI signal -> buy the nearest option          (stocks, or NIFTY with synthetic strike)
               picker.py  sizing.py  universe.py  config.py   (pure logic)
  ui/          console.py    operator console dashboard                       -> operator_console
tests/         pure logic and wiring, runs without Deephaven:  python -m pytest
scripts/       the few lines you paste each day
```

Pure logic (candle aggregation, ltt parsing, sizing, basket ranking, option picking, straddle maths, command
sequences) is separate from the Deephaven wiring and covered by tests. Deephaven queries are not testable outside a
server, so they are kept small and in one place per plugin.

How master, candles and indicators work inside (workflow, which steps are numpy / DataFrame / engine, threads, memory, error handling): [docs/master_candles_indicators.md](docs/master_candles_indicators.md).

## A day

Once per session (or the last line of `init.py`): `import algo; algo.install(globals(), "NSE")`. That puts plain
functions in the session, so the day is a few calls (`scripts/01_day.py`):

```python
master_build("NSE")                    # contract master for today ("NSE" or "US")
candles_start("1min")                 # "30s" | "1min" | "5min" | "15min"
indicators(EMA(9), RSI(5))            # start computing these
indicators(EMA(6))                    # ADDS EMA_6; EMA_9 and RSI_5 keep running
straddle_start(0)                     # nearest option expiry only; 1 = two nearest, 2 = three nearest, ...
```

* `EMA(9)`, `RSI(5)` only describe an indicator; `indicators(...)` starts the computation and each call adds to the
  running set. The result is the table **`indicator_values`** (the name `indicators` is the function).
  `indicators()` lists the set, `indicators_remove("EMA_6")` drops one.
* **Indicators, one column each:**

  | call | column | notes |
  |---|---|---|
  | `SMA(20)` | `SMA_20` | average of the last 20 closes |
  | `EMA(9)` | `EMA_9` | exponential, alpha 2/(n+1) |
  | `RSI(14)` | `RSI_14` | RSI x 100, Wilder |
  | `MACD(12, 26)` | `MACD_12_26` | EMA(12) - EMA(26) |
  | `MACD_SIGNAL(12, 26, 9)` | `MACD_SIGNAL_12_26_9` | EMA(9) of the MACD line |
  | `MACD_HIST(12, 26, 9)` | `MACD_HIST_12_26_9` | MACD line - signal |
  | `ATR(14)` | `ATR_14` | Wilder smoothing of the true range (uses high, low, previous close) |
  | `PCT()` | `PCT_OPEN` | percent change from the first bar's open |
  | `PCT("5min")` | `PCT_5min` | percent change against the close 5 minutes earlier (any span from 1 s to 6 h: `"90s"`, `"10min"`, `"2h"`) |
  | `PCT(5)` | `PCT_5` | percent change against the close 5 bars earlier |

  Prices come out in the unit of `Close` (paise for NSE), as whole numbers. An indicator is blank (RSI, ATR, MACD) or a
  partial average (SMA, EMA) until it has enough bars. For several MACD lines, ask for each:
  `indicators(MACD(), MACD_SIGNAL(), MACD_HIST())`.
  `PCT` is opt-in like the others: nothing is added to the table unless you ask. A time lookback compares with the latest bar
  that started at or before (this bar - lookback), so a quiet instrument is compared with the last price it really had; a bar
  lookback counts bars. Both are blank until such a bar exists. The stock strategy reads `PCT_OPEN` for its basket ranking, so
  run `indicators(PCT())` before it (only needed while `mid_filter` is on).
* **Bar size per indicator:** `EMA(9)` runs on the candles as they are, `EMA(9, "5min")` on 5-minute bars rolled up from
  them (column `EMA_9_5min`), `RSI(14, "15min")` on 15-minute bars. A size must be a whole multiple of the candle size.
  The table has one row per bar of the finest size in use; a coarser indicator shows the value of the bar that row falls
  in (still forming, or final once closed). `indicators(..., timeframe="5min")` sets the default for those that name none.
* `candles_start("5min")` again restarts the candles on the new size (the table starts empty). Indicators, the
  straddle and the strategy follow the new table by themselves.
* `straddle_start(n)`: expiries are counted over everything listed, weeklies included. Each expiry uses the future that
  goes with it: the nearest future expiring on or after it, so weekly options use the monthly future and next month's
  options use next month's future. One point per minute (per candle if candles are larger than a minute).
  `straddle_start(1, symbols=["NIFTY"])` limits it. **A symbol with no future uses its spot (cash) price as the reference**
  (US stocks and ETFs); a symbol with neither is skipped and named in the log (US SPX, SPXW).
* `master_build("US")` switches the exchange (it stops plugins running for the other one); `master_build(force=True)` ignores the cache, `algo_status()` shows what is running.

The strategy and the console are plugins (`scripts/02_strategy.py`, `03_console.py`). Running a plugin whose name is
already running replaces it, so re-running a call is always safe. `algo.reload()` after editing the package.

## Prices and units

`candles_start("1min", 0.01)`: the **multiplier** is applied to every feed price, so a feed in paise (x100) gets
`0.01` and the candles hold proper rates. The default is the exchange's own scale (NSE 0.01, US 1). From the candles on,
**everything is in proper currency units as doubles** (candles, indicators, straddle, strategy): there is no `price_div`
anywhere downstream, so there is one place where the scale can be wrong. Volume stays int64.

* Why doubles: a multiplier like 0.01 makes prices fractional, so int64 would round 245.67 to 246. Prices are rounded to 6
  decimals on the way in (so 24567 x 0.01 is stored as 245.67, not 245.67000000000002); averages are not rounded.
* Orders are the one place prices become integers again: sizes are floored/ceiled to the instrument's tick with a small
  nudge so the bridge's truncation (x100, int) lands on the tick.
* RSI is 0 to 100, `PCT_...` columns are percents (1.25 = +1.25%), a blank is a Deephaven null.
* The code assumes `fo_market_data.ltp` is in the exchange's minor unit (NSE: paise). Set the multiplier to match the real feed.
* Bad input is dropped, never stored: NaN, infinity, null, zero or negative prices, negative volume, unparseable times.
  Dropped rows are counted (`app.status()` shows how many).

## End of day: whole tables to Parquet files

```python
eod_save()                       # today: candles_2026-10-15.parquet, straddle_2026-10-15.parquet (+ .meta.json each)
eod_save(tables=["candles", "straddle_series", "indicator_values"])    # choose; indicators go to indicators_<day>.parquet
eod_save(day="2026-10-14", folder="/data/eod")                           # another day / another folder
eod_auto()                       # saves by itself every day, 5 minutes after the session ends (NSE 15:35, US 16:05)
eod_auto("15:40")                # or at a time you choose;  eod_auto(stop=True) switches it off
eod_load("2026-10-15")           # -> tables candles_20261015, straddle_20261015
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

The candle table grows all day (one row per instrument per bar) and every indicator column adds to it, so **how many
instruments you build candles and indicators for** decides the memory. A rough guide, 1-minute bars:

| instruments | rows per day | candles (numbers only, ~64 B/row) |
|---|---|---|
| 700 (futures + a few chains) | 262,500 | ~17 MB |
| 32,000 (every option of every symbol) | 12,000,000 | ~770 MB, plus ~8 B per row per indicator column |

* Restrict at the source: `candles_start("1min", symbols=["NIFTY","BANKNIFTY"])`, `kind="FUT"` (or `"OPT"`, `"EQ"`).
  Restrict indicators separately with `indicators(..., symbols=[...], kind="FUT")` (they can be narrower than candles).
* The candle table stores numbers only; the `Symbol` text is joined from one small table, not kept per row.
* Indicator formulas that need no memory per row are lazy (`update_view`); only the running state an indicator really needs
  is stored (the running price sum only for EMA, the price change only for RSI / ATR, high / low only for ATR).
* The feed batch is cleaned and scaled with numpy in one pass; only usable rows reach Python (about 2.4x faster than row by
  row on a feed where a third of the rows have no trade yet, with identical candles).
* No extra threads: candles and indicators run on Deephaven's own listener thread; only the straddle sampler and the
  bridge (strategy) have a thread each.
* These numbers are estimates from row sizes, not measurements of a running Deephaven.

## Adding things

**An indicator** — one small class in `algo/data/indicators.py` (see `EMA`, `RSI`): `column`, `ops()` (the
`update_by` operations) and `final()` (the output expression). Then `algo.Indicators([algo.RSI(14), algo.EMA(20)])`
and re-run that line; the whole day is recomputed.

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
| indicators.py | `algo.Indicators` (+ `RSI`, `EMA`) |
| straddle.py | `algo.Straddle` |
| eod.py / bod.py | `eod_save()` / `eod_auto()` / `eod_load()` (`algo.Eod`, `algo.History`) |
| automation_rsi.py | `algo.Book` + `algo.Bridge` + `algo.RsiOptions(RsiOptionsConfig.stocks())` |
| niftyrsi.py | the same with `RsiOptionsConfig.nifty()` |
| dashboard_blackbox.py | `algo.Console` |

All the numbers that sat in the USER INPUT blocks are fields of `RsiOptionsConfig`, `BridgeConfig`,
`StraddleConfig`, `Market` and `Paths`.

## Behaviour to know about

* `RsiOptionsConfig.stocks()` still has `rsi_high = rsi_low = 50` and `reset_on_start = True` as in the old script.
  The plugin warns about the first at start and about cleared caps at the second.
* `PCT()` (percent from the open) is measured from the first bar `candles` built (candles start at the next full minute
  after `Candles` starts), so after a late start it is not the real day open.
* US: checked against the 2026-10-14 contract file and a real feed export (tokens 1-8): strikes / ticks / equity prices use the
  file's `Divisor`, `ltt` is text `yyyy-mm-dd HH:MM:SS` in UTC. Not yet checked: the live option prices against their divisor
  (10000), and the US option tick (the file lists 0.0001; sizing refuses anything under 0.01). The straddle works for US
  stocks and ETFs through the spot fallback; the strategy still needs the same fallback.
