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
               indicators.py RSI / EMA / Pct via update_by                    -> indicators
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

  Prices come out in the unit of `Close` (paise for NSE), as whole numbers. An indicator is blank (RSI, ATR, MACD) or a
  partial average (SMA, EMA) until it has enough bars. For several MACD lines, ask for each:
  `indicators(MACD(), MACD_SIGNAL(), MACD_HIST())`.
* **Bar size per indicator:** `EMA(9)` runs on the candles as they are, `EMA(9, "5min")` on 5-minute bars rolled up from
  them (column `EMA_9_5min`), `RSI(14, "15min")` on 15-minute bars. A size must be a whole multiple of the candle size.
  The table has one row per bar of the finest size in use; a coarser indicator shows the value of the bar that row falls
  in (still forming, or final once closed). `indicators(..., timeframe="5min")` sets the default for those that name none.
* `candles_start("5min")` again restarts the candles on the new size (the table starts empty). Indicators, the
  straddle and the strategy follow the new table by themselves.
* `straddle_start(n)`: expiries are counted over everything listed, weeklies included. Each expiry uses the future that
  goes with it: the nearest future expiring on or after it, so weekly options use the monthly future and next month's
  options use next month's future. One point per minute (per candle if candles are larger than a minute).
  `straddle_start(1, symbols=["NIFTY"])` limits it.
* `master_build("US")` switches the exchange (it stops plugins running for the other one); `master_build(force=True)` ignores the cache, `algo_status()` shows what is running.

The strategy and the console are plugins (`scripts/02_strategy.py`, `03_console.py`). Running a plugin whose name is
already running replaces it, so re-running a call is always safe. `algo.reload()` after editing the package.

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
| end-of-day files (`candles_<day>.parquet`, `straddle_<day>.parquet`) | `/data/deephaven/fo_market_data/` |
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
| eod.py / bod.py | `algo.Eod` / `algo.History` |
| automation_rsi.py | `algo.Book` + `algo.Bridge` + `algo.RsiOptions(RsiOptionsConfig.stocks())` |
| niftyrsi.py | the same with `RsiOptionsConfig.nifty()` |
| dashboard_blackbox.py | `algo.Console` |

All the numbers that sat in the USER INPUT blocks are fields of `RsiOptionsConfig`, `BridgeConfig`,
`StraddleConfig`, `Market` and `Paths`.

## Behaviour to know about

* `RsiOptionsConfig.stocks()` still has `rsi_high = rsi_low = 50` and `reset_on_start = True` as in the old script.
  The plugin warns about the first at start and about cleared caps at the second.
* `Pct` is measured from the first bar `candles` built (candles start at the next full minute after `Candles` starts),
  so after a late start it is not the real day open.
* `Market` US values (price scale, session, tz) are placeholders to check against a real feed row.
