# How to run it

Every command below is typed in the **Deephaven console** (the Python console in the web IDE), except the last section.
Order: **master → candles → indicators** (then the straddle and the end-of-day save if you want them).
Each step needs the one before it; calling them out of order gives a message that names which one to call first.

---

## 0. One time

**Make the package visible to Deephaven** (docker-compose.yml, under the `deephaven` service):

```yaml
    environment:
      PYTHONPATH: /opt
      PYTHONDONTWRITEBYTECODE: "1"
    volumes:
      - ./deephaven/algo:/opt/algo:ro          # a checkout of this repo's  algo/  folder
      - ./deephaven/eod:/data/eod              # optional: a writable folder you can see on the host (end-of-day files)
```
then `docker compose up -d deephaven` (this restarts the container, so do it outside market hours).

**In the console, once per session** (or as the last line of `init.py`):

```python
import algo; algo.install(globals(), "NSE")
# with end-of-day files going to the folder you mounted above:
# import algo; algo.install(globals(), "NSE", paths=algo.Paths(eod_dir="/data/eod"))
```
This puts the functions below into the console. It prints their names.

---

## 1. The contract master

Reads today's `<yyyymmdd>DB.db3` from `/data/fo_market_data`, once, and caches it.

```python
master_build("NSE")                      # India. Today's file
master_build("US")                       # US. Switching the exchange stops what was running for the other one
master_build("NSE", day="20261015")      # a specific day's file
master_build("NSE", force=True)          # ignore the cache and rebuild
master_build("US", file="20261015DB_US.db3")   # name the file when more than one in the folder matches the day
```
**What the folder `/data/fo_market_data` (host: `./deephaven/fo_market_data`) holds:** the contract file `<yyyymmdd>.db3` (or
`*<yyyymmdd>*.db3`, e.g. `20261015DB.db3`; the date is today in the market's own timezone, New York's date for the US),
the saved days `candles_<day>.parquet` / `straddle_<day>.parquet` for `eod_load`, and `stocks.txt` / `logo.png` for the strategy
and console. If two files match one day (an NSE one and a US one), it uses the last by name and logs a warning that lists them;
a file that belongs to the other market is refused with a message, never read as the wrong market.
Result: the dict `master`, and everything below uses it. Look at it: `algo_status()`.

---

## 2. Candles

Builds candles from the live feed table `fo_market_data`, **from the next full bar onward** (nothing before it).

```python
candles_start()                          # 1-minute candles, each contract scaled by its own Divisor from the master file
candles_start("1min")                    # sizes: "30s"  "1min"  "5min"  "15min"
candles_start("5min")
candles_start("1min", 0.01)              # one multiplier for every price (a feed in paise x100 needs 0.01)

# save memory: only the instruments you need
candles_start("1min", symbols=["NIFTY", "BANKNIFTY"])        # only these underlyings
candles_start("1min", kind="FUT")                            # only futures   ("FUT", "OPT" or "EQ")
candles_start("1min", symbols=["NIFTY"], kind="OPT")         # NIFTY options only

candles_stop()
```
Result: the table `candles` (`Minute, Symbol, token, Open, High, Low, Close, Volume`).
**Calling `candles_start` again restarts the candles empty**; indicators and the straddle rebuild on the new table by themselves.
Run `eod_save()` first if you need what was built.

---

## 3. Indicators

`EMA(9)`, `RSI(14)` ... only *describe* an indicator. `indicators(...)` starts the computation, and **each call adds to what is
already running**. The result is the table `indicator_values`.

```python
indicators(EMA(9), RSI(14))                                  # start these
indicators(SMA(20), ATR(14))                                 # ADDS these; EMA_9 and RSI_14 keep running
indicators(MACD(12, 26), MACD_SIGNAL(12, 26, 9), MACD_HIST(12, 26, 9))
indicators(PCT())                                            # percent from the first bar's open   -> PCT_OPEN
indicators(PCT("5min"), PCT("10min"))                        # percent against the close 5 / 10 minutes earlier
indicators(PCT(5))                                           # percent against the close 5 bars earlier
```

| call | column | meaning |
|---|---|---|
| `SMA(n)` | `SMA_n` | average of the last n closes |
| `EMA(n)` | `EMA_n` | exponential average |
| `RSI(n)` | `RSI_n` | RSI, 0 to 100 (Wilder) |
| `MACD(f, s)` | `MACD_f_s` | EMA(f) − EMA(s) |
| `MACD_SIGNAL(f, s, g)` | `MACD_SIGNAL_f_s_g` | EMA(g) of the MACD line |
| `MACD_HIST(f, s, g)` | `MACD_HIST_f_s_g` | MACD line − signal |
| `ATR(n)` | `ATR_n` | average true range (Wilder) |
| `PCT()` | `PCT_OPEN` | percent from the first bar's open |
| `PCT("5min")` | `PCT_5min` | percent against the close 5 minutes earlier (`"90s"`, `"10min"`, `"2h"` ... up to 6 h) |
| `PCT(5)` | `PCT_5` | percent against the close 5 bars earlier |

**Bar size per indicator** (the default is the candle size; a size must be a whole multiple of it):

```python
indicators(EMA(9, "5min"), RSI(14, "15min"))                 # own bar size  -> EMA_9_5min, RSI_14_15min
indicators(EMA(21), timeframe="5min")                        # default bar size for the ones that name none
```

**Only some instruments** (saves memory; can be narrower than the candles):

```python
indicators(EMA(9), RSI(14), kind="FUT")
indicators(symbols=["NIFTY", "BANKNIFTY"])                   # change the scope of everything already running
```

**Look at it, remove it:**

```python
indicators()                                                 # lists what is being computed
indicators_remove("EMA_9")                                   # stop one (use the column name)
```

**Read the values:**

```python
indicator_values                                             # the whole table
indicator_values.last_by("token")                            # latest row per instrument
indicator_values.where("Symbol = `NIFTY 27OCT26 FUT`").view(["Minute", "EMA_9", "RSI_14"])
candles.natural_join(indicator_values, on=["token", "Minute"], joins=["EMA_9", "RSI_14"])   # next to the candle
```
A blank value (a Deephaven null) means "not enough history yet".

---

## 4. Straddle (optional)

```python
straddle_start(0)                        # nearest option expiry only
straddle_start(1)                        # two nearest expiries     (2 = three nearest ...)
straddle_start(1, symbols=["NIFTY", "BANKNIFTY"])
straddle_stop()
```
Result: the table `straddle_series`. Weekly options use the monthly future; a symbol with no future uses its spot.

---

## 5. End of day (optional)

```python
eod_save()                               # candles + straddle -> candles_<day>.parquet, straddle_<day>.parquet (+ .meta.json)
eod_save(tables=["candles", "straddle_series", "indicator_values"])
eod_save(day="2026-10-14", folder="/data/eod")
eod_auto()                               # saves every day 5 minutes after the close;  eod_auto("15:40"),  eod_auto(stop=True)
eod_load("2026-10-15")                   # -> candles_20261015, straddle_20261015
eod_load()                               # every saved day merged -> candles_hist, straddle_hist
```

---

## A normal morning

**India**
```python
import algo; algo.install(globals(), "NSE")
master_build("NSE")
candles_start("1min")
indicators(EMA(9), EMA(18), RSI(14), ATR(14), PCT(), PCT("5min"))
straddle_start(1)
eod_auto()
algo_status()
```

**US**
```python
import algo; algo.install(globals(), "US")
master_build("US")
candles_start("1min")
indicators(EMA(9), RSI(14), PCT())
straddle_start(0)
eod_auto()
algo_status()
```

---

## What is running, and stopping things

```python
algo_status()                            # every running part and a one-line summary of each
candles_stop()   straddle_stop()   eod_auto(stop=True)
```

If you edit files in the package: `import algo; algo.reload()`, then `import algo; algo.install(globals(), "NSE")`
and run the steps again.

---

## Messages you may see

| message | meaning |
|---|---|
| `indicators needs ['candles']. Call candles_start() first.` | you skipped a step; run the one it names |
| `unknown market ...` | the exchange must be `"NSE"` or `"US"` |
| `no .db3 for <day> in /data/fo_market_data` | today's contract file is not there yet |
| `... lists exchange [...], which is not NSE` (or US) | the file is the other market's; pass `file=` with the right name |
| `bar size ... must be a whole multiple of the candle size` | an indicator's size such as `"30s"` on 1-minute candles |
| `... is not writable (a read-only mount?)` | `eod_save`: use `folder=` or mount a writable folder (section 0) |
| `no instruments in master for symbols [...]` | a name in `symbols=` is not in the contract file |

---

## In a terminal (your PC, project folder)

```
python -m pytest -q                                              # all the tests
set ALGO_TEST_DB=C:/Users/PC/Documents/20261015DB.db3
python -m pytest tests/test_real_master.py -q                    # check a day's contract file (NSE or US)
git switch algo-refactor                                         # the branch with all of this
```
