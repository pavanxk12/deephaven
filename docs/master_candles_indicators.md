# Master, candles and indicators: how it works inside

This covers only the three data steps, in the order you run them:

```python
master_build("NSE")                         # 1. the contract master
candles_start("1min", symbols=["NIFTY"])    # 2. candles form from the live feed
indicators(EMA(9), RSI(14))                 # 3. indicators are computed on the candles
```

Code: `algo/data/master.py`, `algo/data/candles.py`, `algo/data/indicators.py`, `algo/data/symbols.py`,
`algo/api.py` (the three functions above), `algo/core/` (plugin and app plumbing).

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
* **A quiet token produces no candle** for a bar in which it did not trade (no forward fill). Indicators then see only the
  bars that exist for that token: an illiquid option's "EMA 9" spans nine traded bars, not nine minutes.
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
        |                                      v
        +--- symbols / kind filter ----> indicators(...)
                                               |  Deephaven engine operations (update_by ...)
                                               v
                                       table `indicator_values`
```

Rules of the design:

* **Candles are the source of everything derived.** Indicators (and the straddle, strategy) read `candles`; `candles` never reads them.
* **Heavy numeric work stays in vectors or in the Deephaven engine.** Python only does what is inherently sequential
  (building a candle tick by tick).
* **Nothing is stored that is not used.** The candle table holds numbers only; helper columns are lazy formulas.
* **A bad row is dropped, never stored and never allowed to raise out of a listener.**

---

## 2. Plumbing shared by all three (`algo/core`)

| piece | what it does |
|---|---|
| `algo.install(globals(), "NSE")` | creates the one `App` for the session and puts `master_build`, `candles_start`, `indicators`, `EMA`, `RSI` ... into the console |
| `App` | holds the running plugins (`app.plugins`), the published tables (`app.tables`), the market (`NSE`/`US`), paths and a small `shared` dict |
| `Plugin` | base class: `start()`, automatic clean-up of everything it created (`listen`, `spawn`, `follow`), `describe()` for `algo_status()` |
| `app.run(plugin)` | checks `requires`, **stops an older plugin of the same name first**, calls `start()`, registers it; if `start()` fails the plugin is torn down and not registered |
| `app.publish(name, table)` | stores the table and puts it in the session as a variable; **if the name already existed it tells everyone who `follow`s it** |
| `Plugin.listen(table, fn)` | a Deephaven table listener, wrapped so an exception inside `fn` is logged and cannot kill the listener; stopped automatically with the plugin |
| `Plugin.follow(name, cb)` | "call me when another plugin republishes this table" (how indicators notice a candle restart) |

`requires`: master needs nothing, candles need `master`, indicators need `candles`. Calling a function out of order gives
a message such as *"indicators needs ['candles']. Call candles_start() first."*

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

## 5. `indicators(...)` (`algo/data/indicators.py`)

Reads `candles`, produces `indicator_values`. **All the arithmetic is done by Deephaven's engine** (incremental, in Java), not
by Python; Python only builds the query once and publishes it.

### Calling it

```python
indicators(EMA(9), RSI(14))                     # starts the computation
indicators(SMA(20), ATR(14))                    # ADDS (existing columns untouched); the table is rebuilt
indicators(EMA(9, "5min"))                      # own bar size -> column EMA_9_5min
indicators(EMA(21), timeframe="5min")           # default bar size for the ones that name none
indicators(RSI(14), symbols=["NIFTY"], kind="FUT")   # compute only for these instruments
indicators()                                    # list;      indicators_remove("EMA_9") removes
```
`EMA(9)` only *describes* an indicator (a small object). `indicators(...)` is what starts it. Duplicates (same column) are
ignored; a bad argument (`indicators(9)`, an impossible bar size) raises and **leaves the running set unchanged**.

### The pipeline (`build_indicator_table`)

```
 candles
   |  where_in(token table)                 only when symbols=/kind= was given          (engine, join-like filter)
   v
 _bars        view:  Px = Close*1.0, Op = Open*1.0, One = 1  (+ Hi, Lo only if ATR)    (lazy formulas)
              coarser bars: lowerBin(Minute, N*SECOND)  ->  agg_by first / last (+max / min)  (engine aggregation)
   v
 update_by(by = token)    cum_sum  Cnt = One  (+ PxSum = Px only if EMA)              (engine, one pass per token)
                          delta    D = Px     only if RSI / ATR
   v
 update_view              Gain, Loss (RSI)                                             (lazy formulas, no memory per row)
   v
 steps, step by step      step i of EVERY indicator runs together:
                            "ops"    -> one update_by  (ema_tick, rolling_avg_tick, forward_fill)
                            "update" -> one update_view (ATR true range, MACD line)
   v
 view                     Minute, Symbol, token, then each indicator's final expression
   v
 (coarser groups)         each coarser bar size is computed the same way, then natural_join on (token, bar start)
```

Per token state is kept by `update_by(..., by="token")`, which is why many tokens and many indicators in one table do not
mix. All the operations are in a single `update_by` per step, so adding an indicator adds columns, not passes.

### What each indicator computes

| call | column | formula | blank until |
|---|---|---|---|
| `SMA(n)` | `SMA_n` | `rolling_avg_tick` over the last n bars | never: the average of the bars there are until n |
| `EMA(n)` | `EMA_n` | `ema_tick` with alpha 2/(n+1); until n bars the running average `PxSum / Cnt` | never |
| `RSI(n)` | `RSI_n` | 100 - 100/(1 + AvgGain/AvgLoss), Wilder smoothing (alpha 1/n); AvgLoss = 0 gives 100 if there were gains, else 50 | n price changes |
| `MACD(f, s)` | `MACD_f_s` | EMA(f) - EMA(s) | s bars |
| `MACD_SIGNAL(f, s, g)` | `MACD_SIGNAL_f_s_g` | EMA(g) of the MACD line | s + g - 1 bars |
| `MACD_HIST(f, s, g)` | `MACD_HIST_f_s_g` | MACD line - signal | s + g - 1 bars |
| `ATR(n)` | `ATR_n` | Wilder smoothing of true range = max(H-L, abs(H-prevC), abs(L-prevC)); first bar H-L; prevC = Px - D | n bars |
| `PCT()` | `PCT_OPEN` | 100 * (Px - first bar's Open) / first bar's Open, per token | the first bar |
| `PCT("5min")` | `PCT_5min` | 100 * (Px - close of the latest bar at or before now - 5 min) / that close; as-of join on (token, time) | a bar that old exists |
| `PCT(5)` | `PCT_5` | 100 * (Px - close 5 bars earlier) / that close; join on (token, bar number - 5) | 5 bars |

`ema_tick(decay_ticks)`: the engine weight per bar is `exp(-1/decay_ticks)`, and `decay_ticks(alpha) = -1/ln(1-alpha)`
makes that equal `1 - alpha`. A blank is a Deephaven null. Every division has a guard or a denominator that cannot be zero
(`Cnt >= 1`, `AvgL == 0` test, and a zero or missing reference price gives a blank in PCT).

### Stored versus lazy (this is the memory)

| column | stored? | present when |
|---|---|---|
| `Cnt` | stored (long) | always |
| `PxSum` | stored (double) | an EMA is computed |
| `D` | stored (double) | RSI or ATR is computed |
| `DayOpen_PCT_OPEN` | stored (double) | `PCT()` |
| each indicator's smoothing state (`EMA_9_raw`, `AvgG_`, `AvgL_`, `MACD_.._f/_s/_g`, `ATR_.._atr`, `SMA_.._raw`) | stored (double) | that indicator |
| `Px Op One Hi Lo`, `Gain Loss`, `OpenFirst`, ATR true range, MACD line, every final expression | **lazy** (`view` / `update_view`) | evaluated when read, no memory per row |
| `Symbol` | reference into the small symbol table | |

### Bar sizes

* `candles_start("1min")` and `EMA(9)` -> the table has one row per 1-minute bar.
* A size must be a **whole multiple of the candle size** (`EMA(9, "5min")` on 1-minute candles works; `"30s"` on 1-minute
  candles is refused).
* With mixed sizes the table has one row per bar of the **finest** size, and a coarser column shows the value of the coarser
  bar that row belongs to (still forming, or final once closed; earlier rows of a closed coarse bar also show the final value).

### Threads and cost

* **No thread is created by this plugin.** Building the table is a query definition done on your console thread; after that
  the engine updates it on its own threads as candles arrive.
* Adding or removing an indicator, or changing symbols / kind / timeframe, **rebuilds the table over the whole day** and
  republishes it (an `update_by` over the existing rows). Nothing else is touched.
* Cost per indicator column is roughly one double per row per stored state column; narrowing `symbols=` / `kind=` reduces it
  proportionally.

---

## 6. What follows what (restart cascade)

```
 candles_start(...)  -- publishes a NEW candles table -->  app.on_publish("candles") callbacks
                                                              |- indicators : rebuild on the new table (same indicator set, same scope)
                                                              |- straddle   : restart on the new table
                                                              `- strategy   : move its listener to the new table
 indicators(...)     -- republishes indicator_values   -->  strategy moves its listener to the new table
```

The old tables are released when their listeners are stopped (nothing else holds them). Restarting candles loses the candles
built so far; indicators then restart from the first new bar (EMA/SMA start as running averages, RSI/ATR/MACD blank, `PCT_OPEN`
measured from the first new bar). Run `eod_save()` first if you need the day (see the README, "End of day").

---

## 7. Error handling at a glance

| layer | what is guarded |
|---|---|
| `app.run` | missing dependency (message names the function to call), failed start (torn down, not registered) |
| listener wrapper | any exception in a callback is logged with its traceback, the listener keeps running |
| candles | per-batch numpy mask, per-row `try/except` in `_feed`, bounded caches, argument validation |
| indicators | argument types and lengths validated in the constructors; `add` / `remove` roll the set back if the rebuild fails |
| master | unreadable cache is ignored and rebuilt; cache writes are atomic; all-NaT expiry raises with a hint |
| numbers | `algo/core/nulls.py`: one definition of "not a number" (None, NaN, inf, Deephaven null sentinels), used everywhere |

---

## 8. What is verified and how

| what | test file | how |
|---|---|---|
| master parsing, symbols, fractional strikes, US divisors | `test_master.py`, `test_master_us.py` | synthetic contract frames |
| the real contract files | `test_real_master.py` | `ALGO_TEST_DB=<file>`: every row accounted for, symbol uniqueness, strike text, ticks, expiries, chain pairs (NSE and US files pass) |
| candle state machine, time parsing | `test_candles.py` | pure Python |
| batch (numpy) path equals the row-by-row rules | `test_candles_vector.py` | randomised dirty data, 8 seeds, with and without the instrument filter |
| bad rows, multiplier, threads | `test_hardening.py` | NaN / inf / None / garbage rows, 8 threads creating portfolios |
| indicator query shape | `test_indicator_library.py` | recording stand-in for the engine |
| **indicator numbers** | `test_indicator_math.py` | the real `build_indicator_table` run on a pandas stand-in for the engine, 3 tokens x 7 indicators, compared with textbook formulas |
| independent check | `test_talib_parity.py` | TA-Lib (skipped if not installed): SMA exact, EMA 1e-9, RSI / ATR / MACD 1e-7 after the seed has decayed |

**Not verified here:** the behaviour of Deephaven's own operators (`ema_tick`, `update_by`, `natural_join`, `where_in`) on a
live server, memory use on a live server (the numbers above are estimates from row sizes), and the live feed's real
column types. Run it once on the container and compare a few values with the old scripts.

**One documented difference from TA-Lib / charting platforms:** the engine's EMA starts from the first value, TA-Lib starts
from a simple average of the first n bars. The gap shrinks every bar, so the first bars after a candle restart differ slightly.
