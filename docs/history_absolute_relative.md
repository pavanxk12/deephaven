# The tech behind history, `absolute()` and `relative()`

Written for: an engineer who has to maintain or extend this part, or who wants to know exactly what the numbers mean and what they cost.

Code: `algo/series/absolute.py` (the computation), `algo/api.py` (the console functions), `algo/data/store.py` (saving and loading days), `tools/` (the converters and the straddle builder).
Tests: `tests/test_absolute*.py`, `test_relative.py`, `test_baskets.py`, `test_convert_*.py`, `test_build_straddle.py`, `test_eod.py`. How it was checked on real files is in section 10.

---

## 1. The three pieces

```
  saved days (Parquet, one file per table per day)                  today so far (Deephaven tables)
  candles_<day>.parquet   straddle_<day>.parquet                    candles   straddle_series
          │  read with pyarrow: one symbol, one day                          │  read ONCE per call, filtered
          ▼                                                                  ▼
   shared size-limited cache of small float32 arrays (per symbol, per day)   today's Day / IvDay (never cached)
          └──────────────────────────────┬───────────────────────────────────┘
                                         ▼
        absolute(series, level, symbol, n)  ──►  (hist[n], idx)      "now vs the same minute of the last n periods"
        relative(series, level, symbol, basket) ──►  (values[k], idx)  "now vs the other symbols of a basket, now"
```

* **History** is produced by `eod_save()` (live days) and by the converters in `tools/` (older data). Both write the same two file layouts (section 2).
* **`absolute()`** compares a symbol with its own past. **`relative()`** compares a symbol with other symbols, at the same moment. Both use the same series definitions, the same time model and the same cache.
* **Nothing runs in the background.** There is no plugin, listener or thread for either. A call reads what it needs, computes and returns. The only thing kept between calls is the cache (section 5).

---

## 2. The saved history

### 2.1 Two layouts

| file | one row is | columns |
|---|---|---|
| `candles_<yyyy-mm-dd>.parquet` | one instrument, one closed 1-minute bar | `Minute` (timestamp, UTC, the **start** of the bar), `Symbol`, `token`, `Open`, `High`, `Low`, `Close`, `Volume`, `Date` |
| `straddle_<yyyy-mm-dd>.parquet` | one straddle series at one minute | `Time` ("HH:MM:SS" exchange time), `Symbol`, `Expiry`, `Fut`, `ATMStrike`, `ATMStraddle`, `MinStrike`, `V`, `Cost`, `CumCost`, `D`, `AltCost`, `AltCumCost`, `AltD`, `Synthetic`, `IV`, `Date` |

`absolute()` and `relative()` read only `Minute`, `Symbol`, `Open`, `High`, `Low`, `Close`, `Volume` from candles and `Time`, `Symbol`, `Expiry`, `IV` from the straddle. They ignore `token`, `Date`, the `.meta.json` files, `manifest.json` and `tickers.json`.

`IV = V / Synthetic * sqrt(max(dte, 1)) * 100` (dte = calendar days to the expiry), computed by the straddle engine for every row (`algo/series/straddle.py`, `implied_vol`).

### 2.2 Where the files come from

| source | tool | notes |
|---|---|---|
| live Deephaven session | `eod_save()` / `eod_auto()` | both tables, ZSTD, a `Date` column added, written under a temporary name and renamed |
| US vendor 1-minute OHLCV | `tools/convert_us_ohlcv.py` | `ts_event, symbol, open ... instrument_id` renamed to the layout; `token` = the vendor's `instrument_id` |
| GDFL India cash CSV | `tools/convert_gdfl_csv.py` | times are India time ending in `:59`, so the bar's minute is the time without the seconds; converted to UTC; `Symbol` = ticker without `.NSE`; `token` is a made-up number per ticker, kept in `tickers.json` |
| GDFL India F&O files (futures) | `tools/convert_gdfl_futures.py` | `NIFTY27JAN26FUT.NFO` becomes the master's text `NIFTY 27JAN26 FUT`; options are not converted |
| GDFL India F&O files (straddle) | `tools/build_straddle_history.py` | runs the **same** `StraddleEngine` and `replay()` as `straddle_start(0)` over each day's option and future bars, so the columns and `IV` are the live table's |

All converters: sorted by `Symbol` then `Minute`, ZSTD, one file per day, each day checked after writing (rows, volume), a bad file is reported and the rest carry on, resumable, never overwrite without `--force`, a `manifest.json` with row counts and SHA-256, optional plain tar (Parquet is compressed already, so a tar saves nothing).

### 2.3 Contract lists for the straddle history

The F&O files hold no contract master. A day's chain is every contract that traded that day or in the 5 days before it and has not expired; a strike needs both its CE and PE. A listed strike that has not traded is missing, which is harmless because it has no price to use. Nothing from the future is used.

### 2.4 `eod_load`

`eod_load("2026-03-12")` loads one day; `eod_load(days=N)` (or `eod_load(N)`) merges the last N saved days into `candles_hist` / `straddle_hist`; `eod_load()` merges all. A day found in two folders is taken from the first; days whose columns differ (a file from before `IV` existed) are refused with the days named. This is for loading tables into Deephaven; `absolute()` does **not** use it and keeps history out of Deephaven's memory.

---

## 3. The time model

The key is **the exchange's own session minute**, not UTC and not a position in an array.

* `Clock(zone, session)` turns epoch seconds into (local day, minute of the day) using the exchange time zone, looking up the offset **per distinct hour**, so a clock change is exact. A slot is `minute_of_day - session_start`; NSE has 375 slots (09:15 to 15:30), the US 390 (09:30 to 16:00). Bars outside `[0, slots)` (pre-market, after-hours, the GDFL bars after 15:30) are ignored.
* **Why not UTC or a position:** 09:30 New York is 13:30 UTC in summer and 14:30 in winter, but session minute 0 in both, so the clock change needs no special case. If a bar is missing on one day, an index would shift every later point of that day; a minute key does not.
* **A bar's slot is its start.** The 09:15 bar is slot 0. A straddle sample stamped `09:16:02` (the 09:15 bar's close plus 2 s) is slot 1: `HH*60+MM - start`.
* **"Now" is the latest closed bar.** The slot used for the comparison is `wall minute - start - 1` (the bar that started a minute ago), clipped to the session: before the open it is slot 0, after the close the last slot. `at=` replaces the wall clock.
* **Quiet minutes.** A symbol with no bar in a minute keeps its last close for at most `max_age` minutes (5; an IV 3). Beyond that the point is NaN and is left out. This is the same carry-forward the straddle already uses for stale prices.
* **Trading days** are the days that exist as files, not a calendar; a missing day file is indistinguishable from a holiday.

### 3.1 Which past points a level compares with

| level | the n comparison dates | the period whose open / VWAP is used |
|---|---|---|
| `intraday` | the previous n trading days (files before today) | that day |
| `intraweek` | the same weekday of the previous n weeks (`today - 7k`), NaN if that date has no file | the ISO week of that date, from its first trading day |
| `intramonth` | the t-th trading day of each of the previous n months, where t is today's rank in its month | the calendar month of that date, from its first trading day |

Today's own file is never part of its own history. A week or month that opened on a different weekday (a holiday Monday) simply opens on its first day with data.

---

## 4. The series and the reference point

A series is **a value measured from a reference point you choose**. `series="pct"` is the percent change of the price from `ref`; `series="iv"` is the IV itself, or with a `ref` its percent change; `pct_open` and `pct_vwap` are shorthands for `pct` with `ref="open"` / `"vwap"`.

| `ref` | meaning | what it needs besides the day itself |
|---|---|---|
| `"open"` (default) | the open of the period: the first bar's open of the day, the week or the month (for an IV, its first value) | for a week or month, the earlier days of that period |
| `"vwap"` | `sum(typical price x volume) / sum(volume)` since the period's open (prices only) | the same |
| `"prev_close"` | the last bar's close (the last IV) of the previous trading day | that previous day |
| `"10min"` | the carried value 10 minutes earlier in the same day (1 to 389; a bare number is minutes) | nothing |
| `"09:30"` | the carried value at that fixed minute of the same day (exchange time) | nothing |

`parse_ref` reads the text once and refuses what makes no sense (a time outside the session, `"375min"`, an unknown word, `"vwap"` for an IV); a reference never stands for a number that does not exist: before the session has that many minutes, or before the fixed time has come, the value is NaN.
A reference is part of the call, not of the data: the same cached day arrays serve every choice, and the list of days a call needs (`_needed_days`, `_ref_days`) follows from it, so `absolute_warm` with the same `ref` reads exactly those days.

Per (symbol, day) the service keeps a `Day`: `close` (carried over minutes, float32 per slot), `cum_pv` (running sum of `(H+L+C)/3 * volume`, float32), `cum_v` (running volume, float32), plus the day's `open` (first bar's open), `last` (the last bar's close), total `pv` and total `v`. An `IvDay` is one float32 array per slot. About 4.7 KB per symbol per day (0.11 MB for 22 days).

* **`pct`** `= (price - reference) / reference * 100`, price = the carried close at the slot. For `ref="open"` the reference is the open of the period's first bar (a week's or month's: the first day's open); for `"vwap"` it is (sum of `pv` over the period's earlier days + the day's `cum_pv` at the slot) / (the same for volume), typical price (H+L+C)/3 times volume.
* **`iv`** = the IV of the symbol's **nearest expiry** that day (`min(Expiry)`), carried at most 3 slots. For a future or a contract the underlying is used. With a `ref` it is the percent change of the IV from that reference (the IV day keeps its first and last value for `"open"` and `"prev_close"`).
* The percent is computed as `(price - anchor) / anchor` after converting float32 to float64, which is exact for round cases; storing float32 costs at most about 1e-6 percent.

---

## 5. Cost: reading, caching, memory

* **Reading** uses pyarrow with a filter: `Symbol = X` for one symbol, `Symbol in [...]` for many, reading only the columns needed. Deephaven's heap is not used for history.
* **One shared cache** (`Cache`): an ordered map with a **byte budget** (default 200 MB, `absolute_settings(memory_mb=...)`), least recently used out first. Each key is **loaded once** even if many threads ask at once (a per-key gate), so many algos asking about the same symbol cost one read. Values are the small `Day` / `IvDay` objects (a `None` is cached too, so a missing day is not read again).
* **Batching.** `absolute_warm(series, level, symbols, n)` and `relative()` read each day file **once for all** the symbols that need it (`bars_many`, `iv_many`), not once per symbol. Measured: one day file, 300 symbols in a single filtered read 24 ms, against 22 ms for one symbol read alone. `warm` only warms as many symbols as the cache holds and says so, because warming more would evict what it just loaded.
* **Giving memory back.** After a bulk read the service calls `pyarrow.default_memory_pool().release_unused()`, so the process does not keep the read buffers.
* **Files and row groups.** The converted files have 1M-row groups; a single-symbol read is about 22 ms. The same file with 65,536-row groups reads one symbol in 2.8 ms but is 17% larger (14.8 against 12.6 MB). Batching removes most of the need; the row-group size is a converter option, not changed.
* **No threads, no locks in the hot path** except the cache's own, so nothing here can hold up Deephaven's update cycle.

---

## 5b. A reference on a future stays on one contract

A rolling future changes contract at each expiry, and two contracts trade a few points apart (the calendar spread). So the **earlier days a reference needs** are read for the contract that is
the nearest on the day being measured, not for whatever was the nearest on those earlier days (`_pin`): the previous close, and a week's or month's open and VWAP, all come from that one contract
(the contract of today comes from the master, the contract of a past day from its file). Without this, on the first day after an expiry `ref="prev_close"` would measure the new contract against
the old one's close: on 2026-01-28 that was +0.97% for NIFTY, against -0.03% on the same contract. A contract named by its month or full name is already one contract and needs no pin.

## 6. `absolute(series, level, symbol, n)`

1. Validate `series`, `level`, `n` (1 to 400) and parse the symbol. The instrument is exactly what is written, never another: `"RELIANCE"` = the cash stock; `"NIFTY FUT"` = the nearest unexpired future **of each day** (the history rolls: on 2026-01-28 the 27 Jan contract is gone and the Feb one is used); `"NIFTY JUN FUT"` / `"NIFTY JUN26 FUT"` = the contract expiring in that month (without a year, the next such month on or after the day), its own days only; `"NIFTY 27OCT26 FUT"` = that contract. A day on which the instrument has no file or no rows is NaN (a future never falls back to the equity, nor the equity to a future).
2. Work out the date and slot from `at` (or the clock), list the saved days (the folder is listed at most once a minute), pick the n comparison dates (section 3.1).
3. For each comparison date, `_value_at`: get that day's `Day` / `IvDay` from the cache (reading the file once if absent), and for a week or month also the earlier days of that period; compute the value at the slot, NaN if absent.
4. **Now:** today's rows for this one symbol are read from the live `candles` / `straddle_series` table (a filtered snapshot, section 8) and turned into a `Day` the same way; if the live table has nothing, today's saved file is used (after the close).
5. `idx` = the number of valid past values strictly below now; `pctile = 100 * idx / n_valid`. **-1** (and NaN `pctile`) when now is missing or fewer than `min_valid` past values exist (default `max(1, ceil(n/2))`).

Result: a tuple `(hist, idx)` (`hist` float64, oldest first) with attributes `now, n_valid, pctile, minute, dates, today, series, level, symbol`. `now=` supplies the value yourself.

---

## 7. `relative(series, level, symbol, basket=None)`

The same series, measured for every symbol of a basket **now**, then a rank:

1. **Which basket.** `None` = the basket the symbol is defined in, found in `baskets.txt` by exact symbol match (a future and its cash symbol are different symbols); none found or several found stops with the names to choose from. A name or a list ranks the symbol against that basket whether or not it is a member. The symbol itself and repeats are dropped from the values.
2. **Today for everyone in one read:** `today_bars_many` is **one** `where_in` over the candles table for all tokens, split by token (`today_iv_many` for the straddle). Symbols the table did not have fall back to today's saved file, read once for all of them.
3. **For a week or month:** the earlier days of the period (the open and VWAP anchors) are read once for the whole basket through `bars_many` and cached; intraday needs no history at all.
4. `values[k]` = the series now for basket symbol k (NaN where it has no value), `idx` = how many valid values are strictly below the symbol's. `-1` when the symbol has no value or fewer than half of the basket has one. Attributes: `now, n_valid, pctile, symbols` (aligned with `values`), `basket` (the name, None for a list), `minute, today`.

### 7.1 `baskets.txt`

```
# a comment
[banks]
HDFCBANK
ICICIBANK, SBIN, AXISBANK      # one per line or comma separated; spaces inside a symbol are part of it
[nifty_complex]
NIFTY FUT
BANKNIFTY FUT
```

* **Location:** `baskets.txt` in the market-data folder, else the state folder, or `absolute_settings(basket_file=...)`.
* **Parsing** is strict and reports `file line N`: text before a heading, a bad heading, a duplicate basket, an empty basket, more than 5,000 symbols.
* **Freshness without a thread:** at each call the file's path, modification time and size are compared with the last read; it is parsed again only when they differ. A file with a mistake makes the calls say so (with the line) until it is fixed.
* **Editing from the terminal** (`basket("banks", [...])`, `add=`, `remove=`, `delete=True`) edits **only that basket's block** of the text, keeping comments, order and the other baskets; the new text is parsed before it is written, written to a temporary file and renamed, and a file that already has a mistake or is read-only is not touched. A new file is created in the first writable place.

---

## 8. Reading today from Deephaven (`deephaven_readers`)

Four small functions, built when the service is first used:

| function | does |
|---|---|
| `today_bars(spec, day)` | `candles.where("token = T")` for ONE symbol, as arrays of `Ts, Open, High, Low, Close, Volume` |
| `today_iv(und, day)` | `straddle_series.where("Symbol = `X`")`, the nearest expiry, as (minute of the day, IV) |
| `today_bars_many(specs, day)` | ONE `where_in` over all tokens, grouped by token |
| `today_iv_many(unders, day)` | the same for many underlyings |

The token of a symbol comes from the contract master (cash = its spot token, a contract by name, `X FUT` = the nearest unexpired future). A symbol with no token, a stopped table or a missing table gives nothing, and the saved file is used instead. Only plain names go into a query (letters, digits and `& _ . -`).

---

## 9. The console functions

`absolute`, `relative`, `basket`, `absolute_warm`, `absolute_settings` (all in `algo_help()`). The service object is created on first use and kept in the session's shared store; `absolute_settings(memory_mb, max_age, iv_max_age, min_valid, folder, basket_file, clear)` changes a setting, empties the cache and rebuilds it, and with no arguments just shows the cache numbers. There is no `start` or `stop`.

---

## 10. What was measured and checked

Real files (US 2026 candles, India cash and futures candles, the India straddle history):

| check | result |
|---|---|
| `absolute` US `pct_open` and `pct_vwap`, AAPL, 22 days across the 8 March clock change | equals an independent pandas computation on all 22 days |
| `absolute` India `iv`, NIFTY, 22 days | equals pandas |
| `absolute` `"NIFTY FUT"`, 22 days across the Jan expiry | equals pandas; the contract changes on 2026-01-28 |
| `relative`, one symbol against 59, intraday | values equal pandas, `idx` exact |
| first call, one symbol, 22 days | about 0.5 s (0.25 s for a future); then about 0.1 ms plus the read of today |
| `absolute_warm`, 300 symbols x 22 days | 1.5 s, against 143 s one symbol at a time |
| `relative`, 60 symbols | 0.04 s (day), 0.14 s (week), 0.4 s (month) |
| `relative`, 1,500 symbols | 0.23 s (day), 0.86 s (week) |
| cache | 22 days of one symbol 0.11 MB; 300 symbols x 22 days 32 MB |

The test suite (594 tests at the last run) covers the clock and the clock change, carry-forward, each level and its anchors, holidays and missing days, NaN and `min_valid` rules, `at`, the cache budget and eviction, many threads loading once, batch versus single reads, futures rolling, the baskets file (parsing, errors, re-reading, edits that leave the rest alone, read-only), and the converters.

---

## 11. Limits and assumptions

* **Not run on a live Deephaven.** The reads of today's tables (`where_in`, `to_pandas`) are tested against fakes only.
* **Trading days come from the files.** A day with no file is treated as a holiday; there is no separate calendar. `intramonth` over 2026 data can only reach back to January.
* **Regular session only.** Pre-market, after-hours and the GDFL bars after 15:30 are ignored (the `extended` option of `Clock` exists but is not used). A special session with different hours (a Muhurat evening) is outside the slots.
* **Half days.** An early close simply has no points after it; they are NaN, and the days are still aligned on the open.
* **float32 storage** limits the percent to about 1e-6 percent error; ties are not counted as below.
* **IV** is the nearest expiry's IV from the straddle history; it is not annualised (`V / synthetic * sqrt(dte) * 100`).
* **History of a symbol that did not trade** at the compared minute is carried for at most `max_age` minutes and is NaN beyond that.
* **Cash and futures candle files are both named `candles_<day>.parquet`**: keep them in separate folders (or rename one kind), because a folder can hold only one of them per day.
* **Old straddle files** (from before `IV` existed) lack the column; `eod_load` refuses to merge them with newer days.
