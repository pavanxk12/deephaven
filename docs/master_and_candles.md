# Master and candles: how it works inside

This covers the two data steps, in the order you run them:

```python
master_build("NSE")                         # 1. the contract master
candles_start("1min", symbols=["NIFTY"])    # 2. candles form from the live feed
```
Indicators are not a service. A script computes the ones it needs from the candles it reads (`examples/03_algo_template.py` does an RSI).

Code: `algo/data/master.py`, `algo/data/candles.py`, `algo/data/symbols.py`, `algo/api.py` (the two functions above),
`algo/core/` (plugin and app plumbing).

---

## 0. Where the data comes from

There are two different sources, and they behave differently.

| | **Contract master** | **Live feed `fo_market_data`** |
|---|---|---|
| what | the day's instrument list: token, symbol, expiry, strike, tick, lot, divisor | the latest trade of every instrument |
| where | file `<yyyymmdd>DB.db3` (SQLite, one table `ResultSet`) in `/data/fo_market_data` (read-only mount) | a Deephaven table in the session, fed from Kafka (`hft.marketdata.fo` in your compose file) |
| shape | static, written once a day | **ticking, updated in place** |
| read by | `master_build()`, once, with pandas | `candles_start()`, continuously, with a table listener |
| joined by | the integer `token` | the integer `token` |

### The master file (db3)

`20261015DB.db3` is an ordinary SQLite file with one table, `ResultSet`, one row per contract (85,272 rows for NSE F&O on
2026-10-15, 79,728 for the US file of 2026-10-14). Columns used: `Token, Symbol, ExpiryDate, InstType, OptionType,
StrikePrice, LotSize, TickSize, Divisor`. It is opened read-only, read once, turned into `MasterData` and cached. Nothing in it
changes during the day. It answers *"what is token 48704?"*, *"what is the tick of this option?"*, *"how many raw units make
one rupee?"* (the `Divisor`).

### The feed table `fo_market_data`: updated, not appended

This is the key point. The table has **one row per token**, and when a new trade arrives for a token **that row is changed in
place**; no new row is added. So the table never grows; it always shows the last known state of each instrument.

```
 time ->     cycle 1                 cycle 2                 cycle 3
 token 48704 added   ltp 2456700     modified ltp 2456750    modified ltp 2456725
 token 48987 added   ltp 305000                              modified ltp 305100
             (first sight = "added")      (every later change = "modified")
```
Columns the candle builder reads: `token`, `ltp` (last traded price, raw units), `vol` (**cumulative** volume for the day),
`ltt` (time of that trade, as text).

What this means for the code:

* **Both kinds of change are processed.** The listener reads `update.added(...)` and `update.modified(...)` of every update
  cycle. A token is "added" the first time it is seen and "modified" every time after.
* **Unchanged rows come again.** Other fields of the row (quotes, for example) can change while the last trade does not, which
  also shows up as a modification. The builder compares `(ltt, vol)` with the last trade it saw for that token and skips
  repeats.
* **The first batch is the current state.** The listener starts with `replay=True`, so it first receives the whole table as
  "added" rows (one latest value per token). Rows older than the first bar build no candle; they only record the
  cumulative volume, so the first candle's volume is correct.
* **You see the last value per cycle, not every trade.** Deephaven delivers changes in update cycles (about one a second by
  default). If a token trades three times inside one cycle, only the last trade is visible. Consequences:
  * **Close** is exact (the last value at the end of the bar), and **Volume** is exact (it is a cumulative number, so
    differences add up whatever was missed).
  * **Open, High and Low are built from the sampled values**, so a spike that starts and ends inside one cycle can be
    missed. For liquid, fast instruments the High / Low may be slightly narrower than the exchange's.
  * If you need exact highs and lows, the source should be an **append-only stream** of every trade (the Kafka topic behind
    this table), not the last-value table. `Candles` would then read only `added` rows. I have not built that; say if you want it.
* **A quiet token produces no candle** for a bar in which it did not trade (no forward fill). A script that computes an indicator
  from the candles then sees only the bars that exist for that token: an illiquid option's "EMA 9" spans nine traded bars, not nine minutes.
* **Time is the feed's own.** Bars are cut by `ltt`, and a bar is closed when the latest `ltt` seen moves 10 s past it.
* **The table that creates the feed is not in this repo** (it is set up by your `init.py` / Kafka consumer), so "updated in
  place, keyed by token" is read from how the old scripts and the console use it (they `natural_join` it on `token`, which
  only works when each token appears once). Please confirm it against `init.py`.

### How the two meet

```
 master (static)                                feed (streaming)
 token -> symbol, divisor, tick, expiry         token -> ltp, vol (cumulative), ltt
        \                                           /
         `---------- joined on token ------------'
   candles: price = ltp * (1 / Divisor of that token) ;  Symbol joined from master ;  scope (symbols / kind) taken from master
```
A token in the feed but not in the master has no symbol (null) and uses the default scale; with `symbols=` / `kind=` set it is
dropped. A token in the master that never trades never produces a candle.

---

## 1. The big picture

```
 contract file (.db3)                 live feed table `fo_market_data`
        |                                      |   (one row per token, last value, ticking)
        v                                      v
  master_build()  ----- symbols, ticks, ----> candles_start()
  MasterData in Python   divisors, tokens      listener on the feed
        |                                      |  numpy cleaning + Python state machine
        |                                      v
        |                              table `candles`   (Deephaven table, grows all day)
        |                                      |
        +--- symbols / kind / expiries ---->  read by the straddle, by end of day, and by your scripts
```

Rules of the design:

* **Candles are the source of everything derived.** The straddle, end of day and your scripts read `candles`; `candles` never reads them.
* **Heavy numeric work stays in vectors or in the Deephaven engine.** Python only does what is inherently sequential
  (building a candle tick by tick).
* **Nothing is stored that is not used.** The candle table holds numbers only; helper columns are lazy formulas.
* **A bad row is dropped, never stored and never allowed to raise out of a listener.**

---

## 2. Plumbing shared by the services (`algo/core`)

| piece | what it does |
|---|---|
| `algo.install(globals(), "NSE")` | creates the one `App` for the session and puts `master_build`, `candles_start` ... into the console (`algo_help()` lists them) |
| `App` | holds the running plugins (`app.plugins`), the published tables (`app.tables`), the market (`NSE`/`US`), paths and a small `shared` dict |
| `Plugin` | base class: `start()`, automatic clean-up of everything it created (`listen`, `spawn`, `follow`), `describe()` for `algo_status()` |
| `app.run(plugin)` | checks `requires`, **stops an older plugin of the same name first**, calls `start()`, registers it; if `start()` fails the plugin is torn down and not registered |
| `app.publish(name, table)` | stores the table and puts it in the session as a variable; **if the name already existed it tells everyone who `follow`s it** |
| `Plugin.listen(table, fn)` | a Deephaven table listener, wrapped so an exception inside `fn` is logged and cannot kill the listener; stopped automatically with the plugin |
| `Plugin.follow(name, cb)` | "call me when another plugin republishes this table" (how the straddle notices a candle restart) |

`requires`: master needs nothing, candles need `master`, the straddle needs both. Calling a function out of order gives
a message such as *"straddle needs ['candles']. Call candles_start() first."*

---

## 3. `master_build()` (`algo/data/master.py`)

Runs once per day (or when the file changes). Output: `MasterData` (pandas frames and plain dicts), also published as the
dict `master` for old notebooks.

### Workflow

```
master_build("NSE")
 1. exchange: if different from the running one, stop the plugins of the other exchange
 2. find the file        <market_data>/<yyyymmdd>.db3  or  *<yyyymmdd>*.db3        (os / glob)
 3. cache check          master_v4_<MKT>_<day>_<mtime>_<size>.pkl in <work>/cache   -> hit: load and stop here
 4. read_contracts       SQLite opened read-only; only the 9 columns used are selected
 5. normalise            raw rows -> clean frame (vector operations)
 6. classify             split into options / chain / futures / equities, build symbol map
 7. save cache           write to a .tmp file, then os.replace (a crash never leaves a half file)
 8. publish              master dict into the session; MasterData kept on the plugin (app.master)
```

### What is vectorised

| step | how | vectorised? |
|---|---|---|
| read | one `SELECT` of 9 of 17 columns (8.3 MB instead of 19.6 MB for today's file) | pandas |
| numbers | `pd.to_numeric(..., errors="coerce")`, strike and tick divided by the row's **own `Divisor`** (NSE 100, US options 10000, US equities 100), market constant if the column is missing or not above zero | pandas column maths |
| expiry | `parse_expiry`: yyyymmdd, epoch seconds / milliseconds (epoch 1980 NSE, 1970 US) or date text, via `to_timedelta` | pandas |
| text | `astype(str).str.strip().str.upper()` | pandas |
| classify | boolean masks on `InstType` / `OptionType` / expiry; CE and PE paired with one `merge` on (symbol, expiry, strike) | pandas |
| symbol text | `"NIFTY " + "27OCT26" + " " + "25000" + " CE"` built with column string operations; each distinct strike formatted once (`.10g`, so 16.25 stays 16.25 and nothing is cut at 6 digits) | pandas (0.25 s vs 0.45 s per row) |
| dict lookups | `tick_of`, `divisor_of`: numpy mask `isfinite & > 0`, then `dict(zip(tokens.tolist(), values.tolist()))` | numpy |
| `tokens_for(symbols, kind)` | `isin` on the symbol column, `np.unique` of the token arrays | numpy |

Lazy (built on first use, then cached on the object): `token_of`, `tick_of`, `divisor_of`, `fut_info`, option index
(`strikes()`, `expiries()`), so a plugin that does not need them pays nothing.

### Rules that matter

* A row without a usable expiry cannot be an option or a future; **equities have no expiry and are kept**.
* An equity's tick is its own; if it has none it takes its future's tick.
* Options without a partner (a CE with no PE) stay in `opt` but are not in `chain`.
* Strikes are exact floats (`260`, `16.25`, `307.5`); the strike in the symbol text always equals the strike value.

Python loops: `_option_index` (one loop over option groups, only when asked) and the cache I/O. Everything else is column
operations. Memory is small (tens of MB for ~85,000 contracts).

---

## 4. `candles_start()` (`algo/data/candles.py`)

Builds `candles` from the feed table `fo_market_data` (columns `token`, `ltp`, `vol` = cumulative volume, `ltt` = time text).

### Output table

| column | type | note |
|---|---|---|
| `Minute` | Instant | **start** of the bar (a view formula over `MinuteEpoch`, not stored) |
| `Symbol` | String | joined from a small static table, **not stored per row** |
| `token` | long | |
| `Open High Low Close` | double | proper currency units (feed price x multiplier) |
| `Volume` | long | volume traded inside the bar |

Stored per row: `MinuteEpoch`, `token`, 4 doubles, `Volume` (about 64 bytes).

### Start-up (`Candles.start`)

```
 1. scale        multiplier given        -> one number for every price
                 multiplier not given    -> 1 / Divisor of each contract, from master (NSE 0.01, US options 0.0001, US equities 0.01)
 2. scope        master.tokens_for(symbols, kind)   -> sorted token array, or None = everything in the feed
 3. lookups      dense numpy arrays indexed by token:  scale_arr[token], allowed[token]
 4. table        DynamicTableWriter (numbers only)  ->  natural_join(symbol table)  ->  view(Minute ...)   -> publish("candles")
 5. state        CandleAggregator(first bar = next full bar, bar length)  +  LttParser(time format, feed timezone)
 6. listen       listener on the feed with replay=True (the rows already in the feed are processed first)
```
`app.shared["candle_seconds"]` is set **before** publishing, because plugins that follow `candles` read it the moment they
are told.

### What happens to every feed batch (`_on_update`, runs on Deephaven's update thread)

```
 feed batch (added rows, then modified rows)
   |
   | _clean()   NUMPY, whole batch at once
   |   tok  = asarray(token).astype(int64)           ltp, vol = asarray(..., float64)           ltt = object array
   |   ok   = isfinite(ltp) & ltp>0 & isfinite(vol) & vol>=0 & ltt is not None
   |   ok  &= token in range [0, size)  & allowed[token]            (only when symbols=/kind= was given)
   |   scale = scale_arr[clip(token)]   (unknown tokens use the default scale)
   |   price = round(ltp * scale, decimals)      ok &= price > 0                 <- the multiplier cannot round a price to zero
   |   survivors -> .tolist()   (plain Python numbers)
   v
 for each surviving row                                   PYTHON, sequential (order matters for open/close/volume)
   |  is_new?        (ltt, vol) differs from the last trade of this token   (the feed re-sends unchanged rows)
   |  LttParser      "14-10-2026 10:15:05" -> epoch seconds (strptime once per minute, then slice + dict lookup)
   |  mark + tick    CandleAggregator.tick(token, price, vol, time)
   v
 flush(cutoff)        close every open candle whose bar started before  floor((latest time in this batch - 10 s) / bar) * bar
```

### The state machine (`CandleAggregator`, pure Python, no Deephaven)

* One open candle per token: `[bar start, open, high, low, close, cum volume, first volume]`.
* A tick in the **same** bar updates high / low / close. A tick in a **later** bar closes the old candle and opens a new one.
  A tick in an **earlier** bar (late / out of order) is ignored.
* A candle is also closed by `flush` once the feed's own clock has moved 10 s past the end of its bar. **The clock is the
  latest `ltt` seen, not the wall clock**: if no instrument ticks at all, the last candle stays open until the next tick.
* `Volume` = cumulative volume at the end of the candle minus cumulative volume at the end of the previous candle. The very
  first candle of a token counts from its first tick. Ticks before the first bar (`build_from`) only record the cumulative
  volume, they build no candle.
* Closed candles go to `DynamicTableWriter.write_row` (one Python-to-Java call per closed candle, not per tick).

### Numbers and errors

| case | handled by |
|---|---|
| null, NaN, infinity, zero or negative price | dropped by the `_clean` mask |
| negative or NaN volume | dropped by the mask |
| missing / unparseable time | dropped (`LttParser` returns None) |
| `.123` milliseconds in the time text | cut before parsing |
| multiplier that is 0, negative, NaN, text or `True` | refused when you call `candles_start` |
| float noise (24567 x 0.01 = 245.67000000000002) | `np.round(..., decimals=6)` before the price is used anywhere |
| an exception on one row | caught in `_feed`; counted in `skipped`, first 10 logged, then every 1000th |
| token outside the lookup range | default scale; dropped if a symbols/kind filter is active |

### Threads, memory, restart

* **No thread is created.** The listener runs on Deephaven's update-graph thread, one batch at a time, so the aggregator
  needs no lock. `describe()` reads counters from another thread; a slightly stale number is harmless.
* **Memory:** open candles (one list per token), `_seen` (last trade per token), the minute cache (capped at 5,000 entries), the
  dense arrays (two arrays of `max token + 1`), and the candle table itself. The table is the big one:
  `instruments x bars` rows. 700 instruments x 375 one-minute bars = 262,500 rows (~17 MB); 32,000 instruments = 12 million
  rows (~770 MB). **`symbols=` / `kind=` is the lever.**
* **Calling `candles_start` again** stops the old builder (its listener is removed), starts a new one and publishes a new,
  empty `candles` table. Everything that follows `candles` is told (see section 6).

---

## 5. What follows what (restart cascade)

```
 candles_start(...)  -- publishes a NEW candles table -->  app.on_publish("candles") callbacks
                                                              |- straddle      : restart on the new table
                                                              `- your scripts  : a listener made with watch("candles", fn) moves to the
                                                                               new table; a plain listen() would stay on the old, silent one
```

The old tables are released when their listeners are stopped (nothing else holds them). Restarting candles loses the candles
built so far; the straddle restarts from the first new bar. Run `eod_save()` first if you need the day (see the README, "End of day").

---

## 6. Error handling at a glance

| layer | what is guarded |
|---|---|
| `app.run` | missing dependency (message names the function to call), failed start (torn down, not registered) |
| listener wrapper | any exception in a callback is logged with its traceback, the listener keeps running |
| candles | per-batch numpy mask, per-row `try/except` in `_feed`, bounded caches, argument validation |
| master | unreadable cache is ignored and rebuilt; cache writes are atomic; all-NaT expiry raises with a hint |
| numbers | `algo/core/nulls.py`: one definition of "not a number" (None, NaN, inf, Deephaven null sentinels), used everywhere |

---

## 7. What is verified and how

| what | test file | how |
|---|---|---|
| master parsing, symbols, fractional strikes, US divisors | `test_master.py`, `test_master_us.py` | synthetic contract frames |
| the real contract files | `test_real_master.py` | `ALGO_TEST_DB=<file>`: every row accounted for, symbol uniqueness, strike text, ticks, expiries, chain pairs (NSE and US files pass) |
| candle state machine, time parsing | `test_candles.py` | pure Python |
| batch (numpy) path equals the row-by-row rules | `test_candles_vector.py` | randomised dirty data, 8 seeds, with and without the instrument filter |
| bad rows, multiplier, threads | `test_hardening.py` | NaN / inf / None / garbage rows, 8 threads creating portfolios |
| the RSI in the example scripts | `test_examples.py` | against the published values of the textbook 14-period example, and against the definition |

**Not verified here:** the behaviour of Deephaven's own operators (`natural_join`, `where_in`, listeners) on a live server, memory use on a live server (the numbers above are estimates from row sizes), and the live feed's real
column types. Run it once on the container and compare a few values with the old scripts.
