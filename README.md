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

```python
# scripts/01_day.py
import algo
app = algo.connect(globals(), market="NSE")
app.run(algo.Master())
app.run(algo.Candles())
app.run(algo.Indicators())
app.run(algo.Straddle())
app.run(algo.Eod(auto_at=dtime(15, 35)))
```
Then `scripts/02_strategy.py` (Book, Bridge, RsiOptions; sending starts **off**) and `scripts/03_console.py`.
`app.status()` lists what is running. Running a plugin whose name is already running replaces it (its listeners and
threads are stopped first), so re-pasting a script is always safe.

Other calls: `app["bridge"].send_on()`, `app["rsi_stocks"].status()`, `app["rsi_stocks"].sell("SYMBOL")`,
`app["eod"].save()`, `app.stop("straddle")`, `algo.reload()` after editing the package.

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
