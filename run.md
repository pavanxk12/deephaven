# How to run it

**Short version: `howto.md`.** In the terminal, `algo_help()` lists every function by service.

Every command below is typed in the **Deephaven console** (the Python console in the web IDE), except the last section.
Order: **master → candles** (then the straddle and the end-of-day save if you want them). Indicators are not a service: your script computes them from the candles.
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
the saved days `candles_<day>.parquet` / `straddle_<day>.parquet` for `eod_load`, and `stocks.txt` / `logo.png` for your scripts
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
**Calling `candles_start` again restarts the candles empty**; the straddle rebuilds on the new table by itself.
Run `eod_save()` first if you need what was built.

---

## 3. Straddle (optional)

```python
straddle_start(0)                        # nearest option expiry only
straddle_start(1)                        # two nearest expiries     (2 = three nearest ...)
straddle_start(1, symbols=["NIFTY", "BANKNIFTY"])
straddle_stop()
```
Result: the table `straddle_series`. Weekly options use the monthly future; a symbol with no future uses its spot.

Columns: `Time, Symbol, Expiry, Fut, ATMStrike, ATMStraddle, MinStrike, V, Cost, CumCost, D, AltCost, AltCumCost, AltD, Synthetic, IV`.
`IV = V / Synthetic * sqrt(max(dte, 1)) * 100`, where `V` is the minimum straddle, `Synthetic` is CE - PE + strike at that strike, and `dte` is the calendar
days from the sample day to the expiry (the expiry day counts as 1). It is NaN when the synthetic price is not positive.
Straddle files saved before `IV` existed (`straddle_<day>.parquet`) do not have the column. A merge of days with different columns is refused with the days named (the other table still loads); load such days one at a time (`eod_load("<day>", tables=["straddle_series"])`),
not merged with newer days in one `eod_load()`.

---

## 4. End of day (optional)

```python
eod_save()                               # candles + straddle -> candles_<day>.parquet, straddle_<day>.parquet (+ .meta.json)
eod_save(tables=["candles"])
eod_save(day="2026-10-14", folder="/data/eod")
eod_auto()                               # saves every day 5 minutes after the close;  eod_auto("15:40"),  eod_auto(stop=True)
eod_load("2026-10-15")                   # -> candles_20261015, straddle_20261015
eod_load(days=5)                         # the LAST 5 saved days merged -> candles_hist, straddle_hist   (eod_load(5) is the same)
eod_load()                               # every saved day merged -> candles_hist, straddle_hist
```

---

## 4b. History files: storing them small, and loading old days (optional)

* **How files are stored.** `eod_save()` writes one Parquet file per table per day, now compressed with ZSTD (about 25% smaller than before; every Parquet reader
  still reads them). Parquet is compressed already, so putting the files in a tar or zip saves nothing; a plain `tar` is only for copying many files as one.
  Keep the files flat in one folder, one per day: `eod_load("2026-05-15")` finds `candles_2026-05-15.parquet` in the EOD folder and in `/data/fo_market_data`.
* **Loading.** Load the days you need: `eod_load("2026-05-15")` for one day, `eod_load(days=20)` for the last 20 saved days merged. Avoid `eod_load()` with no day on a big history: it merges every file into one table
  (the US 2026 set below is 208 million rows).
* **Vendor 1-minute OHLCV files** (`ts_event, symbol, open, high, low, close, volume, instrument_id`, one file per day) are not in our format (other column
  names, no `Date`, no `token`). Convert them with the stand-alone tool (needs `pyarrow`, not part of the wheel):
  ```
  python tools/convert_us_ohlcv.py  <folder of daily files>  <output folder>  [--symbols AAPL,SPY | --symbols-file list.txt]
                                    [--session 09:30-16:00]  [--zstd-level 9]  [--tar out.tar]
  ```
  It writes `candles_<day>.parquet` (+ `.meta.json`) with the columns of the `candles` table (Minute, Symbol, token, Open, High, Low, Close, Volume) and a
  `Date`, sorted by Symbol then Minute with ZSTD (about 28% smaller than the vendor file), and a `manifest.json` with row counts and SHA-256 per file. Each file is
  checked (rows, volume) before it counts; nothing is overwritten without `--force`; a bad file is reported and the rest carry on. `token` is the vendor's
  `instrument_id`, which is **not** a token of the contract master: join to the master by `Symbol`. To save disk, convert only the symbols you use (`--symbols-file`) and
  the hours you need (`--session`): 9,600 symbols for pre-market to after-hours is 12 MB a day, a few hundred symbols is a small fraction of that.
  Copy the output folder (or the tar, then `tar -xf`) into `/data/fo_market_data/` of the server that needs it.
* **GDFL India cash CSVs** (`GFDLCM_STOCK_ddmmyyyy.csv`, columns `Ticker, Date, Time, Open, High, Low, Close, Volume, Open Interest`; the folders may be nested):
  `python tools/convert_gdfl_csv.py <root folder> <output folder> [--symbols-file list.txt] [--session 09:15-15:30] [--tar out.tar]`. Same output as above
  (`candles_<day>.parquet` + `.meta.json` + `manifest.json`). Details: the times are India time and end in `:59`, so a bar's `Minute` is the time without the
  seconds (09:15:59 is the 09:15 bar), stored as UTC like the live candles; `Symbol` is the ticker without `.NSE` (`RELIANCE`, and `ABC.BE` for another series);
  Open Interest is dropped. The CSV has no instrument ids, so `token` is a made-up number per ticker starting at 10,000,000, kept in `tickers.json` in the output
  folder and reused by every later run (do not delete it; copy it with the files). It is **not** a token of the contract master: join by `Symbol`. A file whose
  name and `Date` column disagree, with a missing column or with a zero price is reported and skipped; a second file for the same day is reported too.

---

## 4c. absolute(): where is the value now compared with the same time before (optional)

A plain function: it computes when you call it and returns; nothing runs in the background (no listener, no thread, no start or stop).

```python
hist, idx = absolute("iv", "intraday", "NIFTY", 22)          # IV now against the IV at this minute on each of the last 22 days
hist, idx = absolute("pct", "intraday", "NIFTY FUT", 22, ref="10min")   # % change from 10 minutes ago, now vs the same minute of the last 22 days
hist, idx = absolute("pct", "intraweek", "RELIANCE FUT", 5, ref="vwap")  # % from the weekly VWAP, same weekday and minute, last 5 weeks
r = absolute("pct", "intramonth", "AAPL", 4, ref="09:30")    # % from the 09:30 price; r.now  r.n_valid  r.pctile  r.minute  r.dates  r.ref
absolute("pct", "intraday", "NIFTY FUT", 22, ref="prev_close", at="2026-03-20 11:30")   # as of another moment (exchange time)
absolute("iv", "intraday", "NIFTY", 22, now=3.1)             # you give the current value
absolute_warm("pct", "intraday", ["AAPL", "MSFT"], 22, ref="10min")   # read the history of many symbols ahead, one read per day file (same series and ref as the calls to come)
absolute_settings(memory_mb=300)                              # the cache limit (default 200 MB); no arguments just shows it
```
* `hist` is the n past values at the same session minute (oldest first; NaN where that day had no value). `idx` is how many of them are below the value now:
  0 = lowest, n_valid = highest, -1 = cannot tell (no value now, or fewer than half of n past values; `absolute_settings(min_valid=...)`).
* **series and the reference point.** `pct` is the percent change of the price from a **reference point you choose with `ref=`** (the default is `"open"`), so one function covers every
  case and nothing has to be written again; `iv` is the `IV` of the nearest expiry in `straddle_series` (with `ref=` it is the percent change of the IV from that point). The choices:

  | `ref` | the price (or IV) is measured from |
  |---|---|
  | `"open"` | the open of the period: the day's (`intraday`), the week's (`intraweek`) or the month's (`intramonth`) |
  | `"vwap"` | the VWAP since that open ((H+L+C)/3 x volume), prices only |
  | `"prev_close"` | the last value of the previous trading day |
  | `"10min"` (any `"1min"` to `"389min"`; a bare number is minutes too) | the value that many minutes earlier (NaN before the session has that many minutes) |
  | `"09:30"` (any time inside the session, exchange time) | the value at that minute of the same day (NaN until that time has come) |

  `pct_open` and `pct_vwap` still work as shorthands for `pct` with `ref="open"` / `"vwap"`. With `level="intraweek"` and `ref="10min"` the comparison points are the same weekday and minute
  of the previous weeks, each measured from 10 minutes before itself. **level:** `intraday` (previous n trading days; open and VWAP restart each day), `intraweek` (same weekday and minute of the previous n weeks;
  weekly open and VWAP), `intramonth` (same trading-day-of-the-month and minute of the previous n months; monthly open and VWAP).
  **symbol:** `"RELIANCE"` (cash), `"NIFTY FUT"` (the nearest unexpired future of each day, so it rolls), `"NIFTY 27OCT26 FUT"` (one contract); for `iv` the underlying.
* **Time** is the exchange's own session minute (minutes since the open, from its time zone), not UTC and not a position in an array: a bar missing on one day cannot shift the
  rest, and the US clock change needs no care (09:30 New York is session minute 0 in summer and winter). A price is carried forward at most 5 minutes (an IV 3); beyond that the
  point is NaN and left out. The comparison minute is the latest closed bar; before the open it is the first minute, after the close the last.
* **Where the data comes from:** history from the saved `candles_<day>.parquet` / `straddle_<day>.parquet` files (the EOD folder and `/data/fo_market_data`), read with pyarrow one
  symbol and one day at a time into ONE shared cache with a size limit, about 5 KB per symbol per day as float32 (22 days of 1,900 symbols fit in 200 MB; the oldest entries go first);
  today so far is read from the `candles` / `straddle_series` table for that one symbol at the moment of the call. Deephaven's own memory is not used for history.
* **Speed (measured on the real files):** the first call for a symbol reads its history, about 0.5 s for 22 days (0.1 s from smaller row groups); after that about 0.1 ms plus the read of
  today's rows. `absolute_warm` reads each day file once for many symbols: 300 symbols x 22 days in 1.5 s, instead of 143 s one symbol at a time. Warm in your script before a
  listener starts calling.
* **History for each kind of symbol:** cash stocks: `candles_<day>` files (US vendor files and the GDFL cash files converted with `tools/`); futures: `tools/convert_gdfl_futures.py`
  turns the futures of the F&O files into `candles_<day>` files named like the master (`NIFTY 27JAN26 FUT`); IV: `straddle_<day>` files (`tools/build_straddle_history.py`).
  Put them in the EOD folder or `/data/fo_market_data/`. A missing day simply gives NaN for that day.

---

## 4d. relative(): where is a symbol now compared with a basket of other symbols (optional)

Also a plain function that computes when called. The same three series and levels as `absolute()`, but the comparison is across symbols, now:

```python
values, idx = relative("pct", "intraday", "HDFCBANK", ref="10min")      # % change over the last 10 minutes: HDFCBANK against the other symbols of the basket it is defined in (baskets.txt), now
values, idx = relative("pct", "intraday", "HDFCBANK", "banks")          # explicitly against that basket (it may or may not hold HDFCBANK); the default ref is the open
values, idx = relative("pct", "intraweek", "RELIANCE", ["TCS", "INFY"], ref="vwap")   # a list works too; each from its weekly VWAP
r = relative("iv", "intraday", "NIFTY", "index_complex")                  # r.now  r.symbols  r.n_valid  r.pctile  r.minute
relative("pct", "intraday", "HDFCBANK", "banks", ref="09:45", at="2026-03-20 11:30")   # from the 09:45 price, as of another moment (exchange time)
```
* **Which basket:** with no basket given, the symbol is ranked in the basket it is defined in. If it is in no basket, or in several, the call says so and names the baskets to choose from
  (`relative(..., "banks")`). With a basket name or a list, it is ranked there, whether or not it is one of its members; `r.basket` tells which basket was used.
* `values` is the series now for each basket symbol (the symbol itself is left out; NaN where one has no value, and `r.symbols` names them in the same order). `idx` is how many of
  them are below the symbol's own value: 0 = lowest, n = highest, -1 = cannot tell (no value for the symbol, or fewer than half of the basket has a value). `level` chooses what the
  open / VWAP is measured from for every symbol: today's, this week's or this month's; the `ref` (open, vwap, prev_close, N minutes ago, a fixed time) is the same for every symbol; `iv`
  uses the underlying's IV.
* **Cost:** today's rows of the whole basket come from ONE read of the `candles` table; for a week or a month the earlier days of that period are read once for the whole basket from the
  saved files. Measured: 60 symbols in 0.04 s (day) to 0.4 s (month); a basket of 1,500 symbols in 0.2 s (day) and 0.9 s (week).
* **Baskets** live in a text file, `baskets.txt`, in `/data/fo_market_data/` (next to `stocks.txt`; else in the state folder; `absolute_settings(basket_file=...)` names another):
  ```
  # a comment
  [banks]
  HDFCBANK
  ICICIBANK, SBIN, AXISBANK        # one symbol per line or separated by commas
  [nifty_complex]
  NIFTY FUT
  BANKNIFTY FUT
  ```
  A symbol is written as in `absolute()` (`RELIANCE`, `NIFTY FUT`, `NIFTY 27OCT26 FUT`; spaces inside a symbol are part of it). Names use letters, digits and `_ . -`. The file is read again
  whenever it changes (its time and size are checked at each call; nothing runs in the background). A mistake is reported with its line number and `relative()` refuses until it is fixed.
* **From the terminal:** `basket()` lists the baskets, `basket("banks")` shows one, `basket("banks", ["HDFCBANK", "SBIN"])` creates or replaces it, `basket("banks", add=["AXISBANK"])`,
  `basket("banks", remove=["SBIN"])`, `basket("banks", delete=True)`. These write into the same `baskets.txt`: only that basket's lines change, your comments and the other baskets stay.
  A file in a read-only mounted folder is not changed from here (it says so); edit it on the host or point `basket_file` to a writable one. With no file yet, the first change creates it in the
  first writable place.

---

## 5. NATS: the one connection for commands (optional)

Strategies, the console and you all send portfolio commands through one service, so there is one connection and one set of rules.

```python
# ---- ONCE, for everyone (examples/00_services.py) ----
nats_start("192.168.1.130", 4222, users=[3, 4])   # starts the shared service for ALL the users. Calling it again changes nothing.
nats_start(dry_run=True)                       # no connection, commands are only logged
nats_add_user(5)                               # one more user, without a restart
nats_restart(port=4223)                        # on purpose: stop and start again (cuts every algo off, all go back to OFF)
nats_stop()

# ---- in every algo script ----
nats_connect(user=4, client="bridge_nifty")    # only checks the service is up and user 4 may send; starts nothing
nats_send(True, client="bridge_nifty")         # THIS algo may send (every algo and the UI starts OFF)
nats_send(False, client="bridge_nifty")        # stops only this algo
nats_send(False)                               # the master switch: nothing is sent for anyone;  nats_send(True) lets them again

pf_subscribe(1913, 47162, user=4)              # portfolio 1913 -> option token 47162, sent as user 4
pf_pause(1913, user=4)                         # user= may be left out when the service has a single user
pf_apply(1913, user=4, ival=1.1, jval=0.8, sval=3.3, maxqty=3, side="BUY")
pf_unpause(1913, user=4)
pf_squareup(1913, user=4)
pf_unsubscribe(1913, 47162, user=4)

nats_block(user=4)                             # nothing is sent for user 4 or ANY portfolio under user 4
nats_block(user=4, pf=1913)                    # only portfolio 1913 of user 4 (a portfolio is always named with its user)
nats_block()                                   # list what is blocked
nats_unblock(user=4)                           # lifts user 4 and every portfolio blocked under user 4
nats_unblock(user=4, pf=1913)                  # lifts only that portfolio
```
Each `pf_...` prints the outcome (`PF 1913 apply: ok - ok`). Statuses: `ok`, the bridge's own refusal, `timeout` (not known whether the
bridge acted: it is never re-sent by itself), `no_responders` (nothing listens on `cmd.hft.<pf>`), `disconnected`, `off` (sending is off),
`full` (too many waiting for that portfolio), `dry`.
Every command the bridge accepted is also a row in the table `cmd_queue_automated`.

From code (a strategy, a UI callback): `svc = app["nats"]`, then `svc.pause(pf)` etc. Each returns a `Future` at once and never blocks;
`.result(5)` gives the `Reply` (`.ok`, `.status`). `svc.submit(pf, job)` runs several commands back to back on one portfolio.
`svc.stats()` and `algo_status()` show connected, sent, ok, failed, timeouts, waiting and the last round trip.

**How the load is shared:** one connection and one event loop (NATS requests are waiting on the network, not computing, so extra threads
would only add contention). Each portfolio has its own queue, so its commands always run in the order given, while different portfolios run at
the same time. `max_in_flight` (default 64) caps requests on the wire across all portfolios, first come first served. Every command is sent, in
order; none is replaced or dropped. Each portfolio queue holds at most `lane_depth` (256) jobs and refuses the rest with `full`.

---

## 6. Portfolio numbers: one registry (optional)

A user never has two portfolios with the same number, whichever strategy or screen creates them. Details in `registry.md`.

```python
registry_start()                               # once; remembers its numbers in a file
pf_create(3, "rsi_stocks", "RELIANCE")         # create(user, strategy, name) -> the portfolio number (the next free one for user 3)
pf_create(3, "rsi_nifty", "NIFTY", start=1555) # this strategy's numbers begin at 1555
pf_register(3, 1001)                           # claim a number you already use (refused, with who holds it, if it is taken)
pf_register(3, 1001, algo="rsi_stocks", name="TCS")      # yours already: records the algo and what it trades
pf_list(3)                                     # user, number, name, algo and book for user 3;  pf_list() for everyone
```
An automation takes its numbers from it: `algo_start(3, "stocks", pf_start=1001)` then `pf_assign(...)`, and the registry records its name and the stock
for each portfolio it uses. The same number under two different users is fine. The tables are `portfolio_registry` (current) and `portfolio_registry_log` (every change).

---

## 7. Writing your own algo (optional)

Write a plain script in the terminal: `algo_start(3, "my_algo")`, then `pf_assign(...)` to get a portfolio from the book and `pf_activate(...)` to
send the commands; it starts OFF (`nats_send(True, client="my_algo")` switches it on). See `howto.md` section 5; a copy-and-change template in
`examples/03_algo_template.py`.

**Several strategies at once.** Run as many as you like, each from its own script with its own `STRATEGY` name (`nifty_sma`, `banknifty_sma` ...). Each gets its
own book, bridge, send switch (`nats_send(True/False, client="name")`), candle listener and console window (`console_<name>`); portfolio numbers never clash and
stopping or re-pasting one does not touch the others. One catch: all scripts are pasted into the same terminal, so top-level variables with the same name
(`closes`, `ltp`, `state`, `future`, `candle_listener`, and so on) are shared and the later script overwrites the earlier one's. Give each script its own names (a prefix such as `bn_`).

**Pasting a script again is safe.** `watch("candles", on_candle, owner=STRATEGY)` ties the listener to the strategy, so a re-paste (or `algo_stop("name")`) replaces the
old listener instead of adding a second. Without `owner=` every paste adds another listener and your function runs once per paste. A re-paste also restarts
the automation OFF; its last line switches it on again.

**After a new wheel is deployed (one time per deploy).** The session keeps running the code it loaded. Either restart Deephaven, or (market closed, nothing running) reload:
```python
algo_stop("name")                       # for each automation first: the reloaded code would not recognise a running one
app = algo.connect(globals(), market="NSE")
for cb in list(app._publish_cbs.get("candles", [])):         # drop candle listeners left by pastes made before owner= existed
    w = getattr(cb, "__self__", None)
    if type(w).__name__ == "_Watch":
        w.stop()
import importlib, algo.automation, algo.lookup, algo.api
for m in (algo.automation, algo.lookup, algo.api):
    importlib.reload(m)
algo.api.install(globals(), "NSE")      # re-binds the functions; then paste the scripts again
```
`algo.reload()` is the blunt alternative: it stops every service, so run the day's start-up lines again afterwards. None of this is needed on a normal day.

---

## A normal morning

**India**
```python
import algo; algo.install(globals(), "NSE")
master_build("NSE")
candles_start("1min")
straddle_start(1)
eod_auto()
algo_status()
```

**US**
```python
import algo; algo.install(globals(), "US")
master_build("US")
candles_start("1min")
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
| `straddle needs ['candles']. Call candles_start() first.` | you skipped a step; run the one it names |
| `unknown market ...` | the exchange must be `"NSE"` or `"US"` |
| `no .db3 for <day> in /data/fo_market_data` | today's contract file is not there yet |
| `... lists exchange [...], which is not NSE` (or US) | the file is the other market's; pass `file=` with the right name |
| `... NOT sent - the NATS service is not running. Start it once with nats_start(...)` | a `pf_...`, `nats_send` or `nats_add_user` call before the service was started (nothing raises) |
| `the NATS service is already running (...) and these settings differ ... nats_restart(...)` | an algo script called `nats_start` with other settings; it must not restart the shared service |
| `the NATS service is running for users [...], not [9]. Add them ... nats_add_user(9)` | `nats_start(users=...)` named a user the running service does not have |
| `user 9 may not send on the running NATS service` | `nats_connect(user=9)`; add the user with `nats_add_user(9)` |
| `... (NATS is down; it reconnects by itself, send again when it is back)` | the NATS server cannot be reached right now |
| `... (several users are allowed: add user=...)` | `pf_...` without `user=` while `nats_start` was given several users |
| `... (that user was not given to nats_start)` | `user=` names a user that is not one of the service's users |
| `... (blocked: nats_unblock(...) lifts it)` | that user or portfolio is blocked |
| `The portfolio registry is not running. Call registry_start() first.` | `pf_create`, `pf_register` or `pf_list` before `registry_start()` (nothing raises) |
| `... TAKEN by <name>, not available to <you>` | that user already has a portfolio with that number |
| `CLASH: ...` (in the log, when a book starts) | a number in the book's files is held by somebody else for that user |
| `cannot connect to nats://...` | no NATS server answered within a few seconds; check the url |
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
