# INDIA STOCK FUTURES BASKET + EMA 5/10 CROSS + EXTREME FILTERS + CrossOrder: 11 without the RSI. A plain script (functions, no class). Paste in the India server's terminal after the services are up (init.py).
#
#   The universe      as in 11: every STOCK future of the master that has not expired (ONLY = [...] limits it), "SBIN FUT" = the nearest future of each day, dealt into N_BASKETS baskets.
#   The signal        at the end of every EMA_BAR bar (the candle that ends it: :05, :10, :15 ...), for each stock that has no position, four filters in this order:
#                     0. TODAY      the latest EMA_BAR close must be the HIGHEST of today's EMA_BAR closes so far (a BUY) or the LOWEST (a SELL) (DAY_EXTREME); a stock that is neither is
#                                   dropped here, before anything else is asked.
#                     1. RELATIVE   the stock must be EXTREME against its basket (relative()), for the % change from the OPEN, the % change from the VWAP and the IV (USE_IV): percentile above EXTREME[1] (75) = strong,
#                                   below EXTREME[0] (25) = weak. DIRECTION_MATCH = True: all STRONG points to a BUY, all WEAK to a SELL. False: either extreme passes and the EMAs choose the side.
#                     2. EMA        the EMA(EMA_FAST) of the 5-minute closes (EMA_BAR = "5min", made from the 1-minute candles; seeded from the last HIST_DAYS saved days) against the EMA(EMA_SLOW), no crossover
#                                   needed: fast ABOVE slow = BUY, fast BELOW slow = SELL; with DIRECTION_MATCH it must agree with the side the ranking pointed to.
#                     3. ABSOLUTE   the stock must also be EXTREME against its own earlier days at the same minute (absolute(), ABS_DAYS = 22 saved days), on the same side.
#                     Each filter runs only for the stocks that passed the one before it. With fewer than half of the days in the files absolute() cannot tell and there is no trade.
#                     The IV of a future is the IV of its underlying (the straddle series of the nearest expiry): the script STARTS the straddle itself if it is not running (straddle_start(STRADDLE_N); the candles must
#                     hold the OPTIONS too, not futures only) and straddle_<day>.parquet files must exist for the earlier days. Without them the IV filter says "cannot tell" and nothing trades; USE_IV = False
#                     leaves the IV out.
#   The exit          the same as 11: +TARGET_PCT in your favour or -STOP_PCT against you, on every price tick of the live feed for the open positions (TICK_EXITS) and on each 1-minute close;
#                     then `squareup`, and the portfolio is unsubscribed UNSUB_AFTER_S seconds later. At most MAX_PORTFOLIOS portfolios are held at once.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:15 India time): the automation is stopped the way algo_stop does it (pause, square up, a minute later unsubscribe every portfolio, then the script stops).
#   The order         a NEW portfolio for every trade: ONE `subscribe` with the CrossOrder fields, as for 11:
#                     {"strategyname": "CrossOrder", "TOKEN": <the future>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#
# EVERYTHING is inside the function run_india_fut_ema(); the only names left in the terminal start with india_fut_ema:
#   india_fut_ema_status()   india_fut_ema_exit_all()   india_fut_ema_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).

def run_india_fut_ema():
    import contextlib
    import glob
    import io
    import os
    import queue
    import threading
    import time
    from contextlib import nullcontext
    from datetime import date, datetime, time as dtime
    from zoneinfo import ZoneInfo

    import numpy as np
    import pandas as pd

    from algo import current
    from algo.comms.nats_service import NO_SERVICE
    from algo.core.timeframe import parse_timeframe

    import algo.series.absolute as absolute_module                 # the installed library crashes on a day file with a null Symbol (a token the master did not know, in an end-of-day save):
    if not getattr(absolute_module.month_contract, "skips_null_names", False):        # until the library is updated, this wraps its contract picker once so those names are skipped
        original_month_contract = absolute_module.month_contract

        def month_contract_without_nulls(spec, names, day):
            return original_month_contract(spec, [n for n in names if isinstance(n, str)], day)

        month_contract_without_nulls.skips_null_names = True
        absolute_module.month_contract = month_contract_without_nulls

    # =============================== USER INPUT ===============================
    USER = 4
    STRATEGY = "india_fut_ema"               # its name: recorded with every portfolio, the name of its sending switch and of its console window
    N_BASKETS = 1                            # every stock future (about 200) is dealt into this many baskets, fut_1 ... fut_4; a symbol is ranked only against the others of ITS basket
    INDEX_FUTURES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}      # index futures are left out: stock futures only
    ONLY = [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
        "BSE", "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT",
        "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
        "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDIGO", "INFY",
        "ITC", "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "MAXHEALTH", "NESTLEIND", "NTPC",
        "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
        "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATASTEEL", "TCS",
        "TECHM", "TITAN", "TMPV", "TRENT", "ULTRACEMCO",
    ]                               # optional: trade only these underlyings, e.g. ["SBIN", "TCS", "INFY"]; empty = every stock future
    EXCLUDE = []                             # optional: underlyings to leave out
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    EXTREME = (25.0, 75.0)                   # a percentile is EXTREME when it is below EXTREME[0] (weak) or above EXTREME[1] (strong); the middle band does not trade
    DIRECTION_MATCH = True                   # True: a BUY (fast EMA above the slow) needs the STRONG side (above 75) everywhere, a SELL (fast below slow) the WEAK side (below 25).
                                             # False: either extreme passes, whatever the side of the EMAs
    USE_IV = True                            # True: the IV must be extreme too (against the basket and against its own days). Needs the straddle running and straddle_<day> files. False: % from open / VWAP only
    STRADDLE_N = 0                           # straddle_start(STRADDLE_N) when the script starts it itself: 0 = the nearest expiry only (what the IV filters read)
    DAY_EXTREME = True                       # True: filter 0, the latest EMA_BAR close (5min) must be the highest (BUY) or the lowest (SELL) of today's EMA_BAR closes so far (at least 2 bars today). False: off
    ABS_DAYS = 22                            # absolute(): compare the stock with itself at the same minute of its previous 22 trading days (saved files); 0 = off
    EMA_BAR = "5min"                         # the bar the EMAs are computed on (made from the 1-minute candles)
    EMA_FAST, EMA_SLOW = 5, 10               # the two EMAs: fast above slow = long, fast below slow = short
    ACT_ON_BAR_END = True                    # True: look for an ENTRY only on the 1-minute candle that ends an EMA_BAR bar (:05, :10, :15 ... for 5min). Take-profit and stop-loss are still checked on every
                                             # 1-minute close. False: look for an entry on every 1-minute candle
    HIST_DAYS = 1                            # how many earlier saved days seed the EMAs (0 = none)
    TARGET_PCT, STOP_PCT = 1.5, 0.5
    ONE_ENTRY_PER_STOCK = True               # True: each stock gets ONE entry; once it has been entered, a take-profit, stop-loss or any exit ends its trading (no new entry in it). False: it may be entered again
                                             # (the pasted script forgets this on a paste or a restart)
    QTY = 1                                  # in LOTS (the plugin multiplies it by the contract's lot size)
    ENGINE = {"strategyname": "CrossOrder", "SERVERCODE": 26286, "ACCOUNT": "", "ALGOID": "34500"}      # "CrossOrder" is the engine's name for the CrossOrder plugin
    WINDOW = (dtime(9, 30), dtime(15, 0))    # entries only inside it (India time, the session is 09:15 to 15:30); an exit is sent at any time
    EXIT_AT = dtime(15, 15)                  # time exit (India time): 15 minutes before the 15:30 close, the automation is stopped like algo_stop (pause, square up, unsubscribe every portfolio) and no entry is made after it
    MAX_PORTFOLIOS = 32                      # portfolios this script may hold in the engine at once: open positions plus closed ones waiting for their unsubscribe. The engine seems to keep about
                                             # 32 plugin instances in all (all scripts together), so two scripts at the same time need smaller numbers. No entry while the script is at the limit.
    UNSUB_AFTER_S = 60                       # a closed position's portfolio is unsubscribed this many seconds after its exit (take-profit, stop-loss, z-exit) so the engine frees the slot;
                                             # the wait lets the plugin send and fill its closing order first. None = never (the slot is then held until the stop at the end of the day)
    TICK_EXITS = True                        # True: target and stop-loss are also checked on every price tick of the live feed (FEED_TABLE) for the open positions only, not just on each 1-minute close.
                                             # The candle check stays as the backup, and is used alone if the tick listener cannot be set up (it says so). Needs TARGET_PCT or STOP_PCT.
    FEED_TABLE = "fo_market_data"            # the live feed: columns token and ltp (raw price; the contract's own divisor from the master turns it into the price)
    FAIL_COOLDOWN_S = 30
    RETRY_RANKING_S = 20                     # a ranking read that fails (a rare clash with the update cycle: "Previous values requested or inconsistent") is tried again on the next candle batches
                                             # for this many seconds instead of giving up on that stock for the whole bar. 0 = no retry
    # ==========================================================================

    def make_baskets():
        """The underlyings of every stock future that has not expired (the master), sorted, dealt one by one into N_BASKETS baskets so each basket is a similar cross-section of the market.
        A basket entry is "SBIN FUT": the NEAREST future that has not expired, on each day. relative() ranks today's nearest futures against each other, absolute() compares a future with
        the nearest future of each earlier day, so a stock is only ever compared with its own futures, and a roll never mixes two contracts inside one day."""
        futs = current().master.futs
        today = pd.Timestamp(datetime.now(ZoneInfo("Asia/Kolkata")).date())
        unders = sorted({str(u).upper() for u in futs.loc[futs["expiry"] >= today, "TckrSymb"]} - set(INDEX_FUTURES) - {u.upper() for u in EXCLUDE})
        if ONLY:
            unders = [u for u in unders if u in {o.upper() for o in ONLY}]
        names = [f"{u} FUT" for u in unders]
        if len(names) < 3 * N_BASKETS:
            raise ValueError(f"{len(names)} stock futures found: that is too few for {N_BASKETS} baskets of at least 3 (is the master loaded for today?)")
        return {f"fut_{i + 1}": names[i::N_BASKETS] for i in range(N_BASKETS)}

    BASKETS = make_baskets()
    if not (0 <= EXTREME[0] < EXTREME[1] <= 100 and 0 < EMA_FAST < EMA_SLOW and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and HIST_DAYS >= 0 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)):
        raise ValueError("EXTREME must be inside 0 to 100, EMA_FAST below EMA_SLOW, TARGET_PCT and STOP_PCT above 0, QTY at least 1, HIST_DAYS 0 or more, ABS_DAYS 0 to 400, EXIT_AT after the window opens, "
                         "and every basket needs at least 3 symbols")
    BAR_S = parse_timeframe(EMA_BAR)                               # 300; refuses a size that does not line up with the clock
    basket_of = {}                                                 # symbol -> the name of its basket
    for name, members in BASKETS.items():
        for symbol in members:
            if symbol in basket_of:
                raise ValueError(f"{symbol!r} is in two baskets ({basket_of[symbol]!r} and {name!r}): a symbol belongs to one basket only")
            basket_of[symbol] = name

    try:
        from deephaven.execution_context import get_exec_ctx
        exec_ctx = get_exec_ctx()                                  # taken here, in the terminal: the candle listener thread has none, and the ranking reads tables
    except Exception:                                              # (not in Deephaven, such as in a test)
        exec_ctx = nullcontext()

    nats_connect(user=USER, client=STRATEGY)                       # only checks the NATS service is up and this user may send
    registry_connect()                                             # ... and the portfolio registry. Neither starts anything.
    algo_start(USER, STRATEGY)                                     # its book (portfolio numbers) and its own sending switch, which starts OFF
    for name, members in BASKETS.items():
        try:
            basket(name, members)                                  # saved in baskets.txt; the ranking below uses the list itself, so a read-only folder does not matter
        except Exception as e:
            print(f"basket {name!r} was not saved ({e}); the ranking still works from BASKETS")

    if USE_IV:
        if current().get("straddle") is None:                      # the IV of today comes from the straddle series: start it here (it builds the whole day from the candles first, so a late start is fine)
            try:
                straddle_start(STRADDLE_N)
                print(f"{STRATEGY}: the straddle was not running: straddle_start({STRADDLE_N}) sent from here (it needs the candles WITH the options; it is stopped by the end-of-day step)")
            except Exception as e:
                print(f"{STRATEGY}: the straddle could not be started ({e}); the IV filters will refuse every entry until it runs: straddle_start({STRADDLE_N}), with the options in the candles")
        else:
            print(f"{STRATEGY}: the straddle is already running, its IV is used as it is")

    symbol_of = {}                                                 # token -> the basket entry it trades for
    for name in basket_of:
        info = contract(name[:-4], show=False)                    # "SBIN FUT" -> contract("SBIN") = the nearest future of SBIN
        if info is None:
            print(f"{name}: no such contract, left out of the trading (it still counts in the basket)")
            continue
        symbol_of[int(info["token"])] = name
    token_of = {name: token for token, name in symbol_of.items()}
    pf_track(STRATEGY, list(symbol_of), slots=100)                 # the portfolios it had before (a restart or a paste again); every trade takes the next free slot
    with contextlib.redirect_stdout(io.StringIO()):                # (algo_portfolios prints a table; only the numbers are needed)
        had = algo_portfolios(STRATEGY)
    next_slot = {t: (int(had[had["instrument"] == t]["slot"].max()) + 1 if had is not None and (had["instrument"] == t).any() else 0) for t in symbol_of}

    bars = {t: {} for t in symbol_of}                              # token -> {5-minute bar number: the last 1-minute close inside it}: what the EMAs are computed from
    last_px = {}                                                   # token -> the latest 1-minute close
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    last_cross = {}                                                # token -> the bar it was last entered on: not twice on one bar
    entered = set()                                                # tokens already entered by this script: with ONE_ENTRY_PER_STOCK they are not entered again
    last_fail = {}
    counts = {}                                                    # where the signals went (shown by status()): looked at, refused at which filter, ranking errors, entered
    retry = {}                                                     # token -> when its ranking first failed; looked at again on the next candle batches for RETRY_RANKING_S seconds

    def count(key):
        counts[key] = counts.get(key, 0) + 1

    def ranking_failed(token, first, e):
        """A ranking read raised: count it and, for RETRY_RANKING_S seconds from the first failure, look at this stock again on the next candle batch."""
        count("ranking errors")
        now = time.time()
        first = first or now
        said = (str(e).splitlines() or [""])[0][:150]
        if now - first < RETRY_RANKING_S:
            retry[token] = first
            print(f"{symbol_of[token]}: the ranking is not available ({said}); trying again on the next candle batch")
        else:
            print(f"{symbol_of[token]}: the ranking is still not available ({said}); no trade this time")


    def epoch_s(minute):
        """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in India too (its offset, 5 h 30 min, is a whole number of 5-minute bars)."""
        return int(pd.Timestamp(minute).timestamp())

    def add_close(token, minute, close):
        b = bars[token]
        b[epoch_s(minute) // BAR_S] = float(close)
        for old in sorted(b)[:-600]:
            del b[old]

    def closes(token, finished_only):
        """The bar closes, oldest first. On the candle that ENDS a bar the newest bar is complete and counts (finished_only False); on any other minute it is still forming and is left out."""
        keys = sorted(bars[token])
        return [bars[token][k] for k in (keys[:-1] if finished_only else keys)]

    # ---- the history: earlier days' 1-minute closes -> 5-minute closes, before today's -------------------------------------------------------
    def future_names(pq, path, day):
        """{a contract's name in that day's file: its basket entry}: for every stock the nearest future that had NOT expired on that day ("SBIN 27OCT26 FUT" -> "SBIN FUT"),
        so the seed is the stock's own futures and a roll never mixes two contracts inside one day."""
        import re
        pattern = re.compile(r"^(.+) (\d{2}[A-Z]{3}\d{2}) FUT$")
        day_date = date.fromisoformat(day)
        best = {}
        for sym in pq.read_table(path, columns=["Symbol"])["Symbol"].unique().to_pylist():
            m = pattern.match(sym)
            if m is None or f"{m[1]} FUT" not in token_of:
                continue
            expiry = datetime.strptime(m[2], "%d%b%y").date()
            if expiry >= day_date and (m[1] not in best or expiry < best[m[1]][0]):
                best[m[1]] = (expiry, sym)
        return {sym: f"{und} FUT" for und, (_, sym) in best.items()}

    def load_history():
        """Seed `bars` from the last HIST_DAYS saved days (candles_<yyyy-mm-dd>.parquet) before today. Returns the days used."""
        if HIST_DAYS <= 0 or not symbol_of:
            return []
        try:
            import pyarrow.parquet as pq
        except ImportError:
            print("history: pyarrow is not installed, the EMAs start from today's candles")
            return []
        paths = current().paths
        folders = [d for d in (paths.saved, paths.market_data) if d and os.path.isdir(d)]
        today = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
        files = {}
        for d in folders:
            for f in sorted(glob.glob(os.path.join(d, "candles_*.parquet"))):
                day = os.path.basename(f)[len("candles_"):-len(".parquet")]
                if len(day) == 10 and day[4] == "-" and day < today:           # ISO days; today's own candles come from the live table
                    files.setdefault(day, f)
        used = sorted(files)[-HIST_DAYS:]
        if not used:
            print(f"history: no candles_<day>.parquet before {today} in {', '.join(folders) or 'any folder'}: the EMAs start from today's candles")
            return []
        loaded = []
        for day in used:
            try:
                file_name = future_names(pq, files[day], day)
                df = pq.read_table(files[day], columns=["Minute", "Symbol", "Close"], filters=[("Symbol", "in", sorted(file_name))]).to_pandas()
                df["Symbol"] = df["Symbol"].map(file_name)
            except Exception as e:
                print(f"history: {day} could not be read ({e}); left out")
                continue
            if df.empty:
                continue
            loaded.append(day)
            minute = df["Minute"]
            if getattr(minute.dt, "tz", None) is not None:
                minute = minute.dt.tz_convert("UTC").dt.tz_localize(None)
            df["ts"] = minute.to_numpy().astype("datetime64[s]").astype("int64")
            df["bar"] = df["ts"] // BAR_S
            last = df.sort_values(["Symbol", "ts"]).groupby(["Symbol", "bar"], sort=True)["Close"].last()          # the last 1-minute close inside each 5-minute bar
            for (name, bar), close in last.items():
                if name in token_of and close > 0:
                    bars[token_of[name]][int(bar)] = float(close)
        if not loaded:
            print("history: none of the earlier days could be used, the EMAs start from today's candles")
            return []
        print(f"history: {len(loaded)} earlier day(s) ({', '.join(loaded)}) seed the {EMA_BAR} EMAs of {sum(1 for t in bars if bars[t])} of {len(symbol_of)} stocks")
        return loaded

    load_history()
    if ABS_DAYS > 0:                                               # read the earlier days of every stock NOW (one read per day file for all of them), not in the candle thread at the first signal
        try:
            for warm_ref in ("open", "vwap"):
                absolute_warm("pct", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS, ref=warm_ref)
            if USE_IV:
                absolute_warm("iv", LEVEL, sorted({n[:-4] for n in symbol_of.values()}), ABS_DAYS)            # the IV is the underlying's
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for t in symbol_of:                                            # today's candles already built, over the history
        h = last_candles(t, 400, show=False)
        if h is not None:
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(t, m, c)
            if len(h):
                last_px[t] = float(h["Close"].iloc[-1])

    # ---- the indicator: the EMA fast against slow --------------------------------------------------------------------------------------------------
    def ema(values, n):
        """The exponential moving average of the closes, one value per close (alpha = 2 / (n + 1), started from the first close)."""
        k = 2.0 / (n + 1.0)
        e = float(values[0])
        out = [e]
        for v in values[1:]:
            e = float(v) * k + e * (1.0 - k)
            out.append(e)
        return out

    def trend_of(values):
        """1 = the fast EMA is ABOVE the slow one on the last bar (long), 2 = BELOW (short), None = equal, or too few bars to trust an EMA yet."""
        if len(values) < 2 * EMA_SLOW:
            return None
        fast, slow = ema(values, EMA_FAST), ema(values, EMA_SLOW)
        diff = fast[-1] - slow[-1]
        return 1 if diff > 0 else 2 if diff < 0 else None

    def day_sides(token):
        """Filter 0: the sides today's EMA_BAR closes allow. {1} (BUY) when the latest bar close is the highest of today so far, {2} (SELL) when it is the lowest, empty when it is neither or when
        there are fewer than 2 bars today (the first one is both). Only today's bars count, not the history that seeds the EMAs. Everything is allowed when DAY_EXTREME is False."""
        if not DAY_EXTREME:
            return {1, 2}
        b = bars[token]
        if not b:
            return set()
        keys = sorted(b, reverse=True)
        day = datetime.fromtimestamp(keys[0] * BAR_S, ZoneInfo("Asia/Kolkata")).date()
        today = []
        for k in keys:                                             # newest first, until an earlier day begins
            if datetime.fromtimestamp(k * BAR_S, ZoneInfo("Asia/Kolkata")).date() != day:
                break
            today.append(b[k])
        if len(today) < 2:
            return set()
        return ({1} if today[0] >= max(today) else set()) | ({2} if today[0] <= min(today) else set())

    # ---- the filters: EXTREME against the basket (relative), the EMAs, EXTREME against the stock's own days (absolute) ------------------------
    def sides_of(pctiles):
        """The sides these percentiles allow. {1} (BUY) when every one is STRONG (above EXTREME[1]), {2} (SELL) when every one is WEAK (below EXTREME[0]). With DIRECTION_MATCH False, {1, 2} when
        every one is extreme on either side (the EMAs then choose the side). Empty when one of them is in the middle, or unknown."""
        if not pctiles or any(p is None or p != p for p in pctiles):
            return set()
        strong, weak = all(p > EXTREME[1] for p in pctiles), all(p < EXTREME[0] for p in pctiles)
        if DIRECTION_MATCH:
            return ({1} if strong else set()) | ({2} if weak else set())
        return {1, 2} if all(p > EXTREME[1] or p < EXTREME[0] for p in pctiles) else set()

    def under_of(symbol):
        return symbol[:-4]                                         # "SBIN FUT" -> "SBIN": the IV is the underlying's

    def basket_sides(symbol):
        """Filter 1, relative(): the sides the ranking of the symbol among the OTHER symbols of ITS basket allows (the % from the open, from the VWAP and the IV)."""
        members = BASKETS[basket_of[symbol]]
        pctiles = []
        for ref in ("open", "vwap"):
            with exec_ctx:                                         # the call reads the candles table; a listener thread needs the terminal's query context for that
                result = relative("pct", LEVEL, symbol, members, ref=ref)
            if result[1] < 0:                                      # (the second item is -1 when it cannot be told)
                return set()
            pctiles.append(result.pctile)
        if USE_IV:
            with exec_ctx:
                result = relative("iv", LEVEL, under_of(symbol), [under_of(m) for m in members])
            if result[1] < 0:
                return set()
            pctiles.append(result.pctile)
        return sides_of(pctiles)

    def own_days_sides(symbol):
        """Filter 3, absolute(): the sides the ranking of the symbol among the same minute of its previous ABS_DAYS trading days allows; -1 (cannot tell) is none."""
        pctiles = []
        for ref in ("open", "vwap"):
            with exec_ctx:                                         # the call reads the candles table; a listener thread needs the terminal's query context for that
                result = absolute("pct", LEVEL, symbol, ABS_DAYS, ref=ref)
            if result[1] < 0:                                      # (the second item is -1 when it cannot be told)
                return set()
            pctiles.append(result.pctile)
        if USE_IV:
            with exec_ctx:
                result = absolute("iv", LEVEL, under_of(symbol), ABS_DAYS)
            if result[1] < 0:
                return set()
            pctiles.append(result.pctile)
        return sides_of(pctiles)

    # ---- the orders ------------------------------------------------------------------------------------------------------------------
    def in_window():
        now = datetime.now(ZoneInfo("Asia/Kolkata")).time()
        return WINDOW[0] <= now <= WINDOW[1] and now < EXIT_AT         # no entry once the time exit has come, whatever WINDOW says

    def past_exit():
        return datetime.now(ZoneInfo("Asia/Kolkata")).time() >= EXIT_AT

    def cross_order(token, side, qty):
        return {**ENGINE, "TOKEN": int(token), "SIDE": int(side), "QTY": int(qty)}

    def send_entry(pf, token, side, qty):
        """The one subscribe that starts the strategy: exactly its own fields, sent as they are (not the churning subscribe defaults). A Future with the reply."""
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "subscribe", cross_order(token, side, qty), user=USER, client=STRATEGY)       # honours this strategy's own switch

    def send_exit(pf):
        svc = current().get("nats") or NO_SERVICE
        return svc.squareup(pf, user=USER, client=STRATEGY)                              # the plugin's SquareUpEvent closes the position

    live = set()                                                   # portfolios this script holds in the engine: open positions, and closed ones waiting for their unsubscribe
    at_limit_said = []                                             # the "at the limit" message is printed once until a slot frees

    def release(pf, token):
        """Unsubscribe a portfolio whose position is closed. It destroys the plugin instance, which frees one of the engine's slots (a squareup alone leaves it there, idle)."""
        try:
            svc = current().get("nats") or NO_SERVICE
            fut = svc.unsubscribe(pf, token, user=USER, client=STRATEGY)
            fut.add_done_callback(lambda f: (live.discard(pf), at_limit_said.clear()))
        except Exception as e:
            print(f"{STRATEGY}: PF {pf} could not be unsubscribed ({e}); it keeps its slot until the stop")

    tick_state = []                                                # [the table of open tokens, the tick listener] once the tick exits are on

    def divisor_of(token):
        """The contract's own divisor from the master (100 for NSE futures and US stocks), else the market's default."""
        app = current()
        return app.master.divisor_of.get(int(token)) or app.market.price_div

    def tick_exit(token, pos, px):
        """Target / stop-loss of an open position on the price `px` (the same rule as on the candle close)."""
        move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)                 # % in the position's favour (negative = against it)
        if TARGET_PCT is not None and move >= TARGET_PCT:
            last_px[token] = px
            leave(token, f"target {move:+.2f}% (tick)")
        elif STOP_PCT is not None and move <= -STOP_PCT:
            last_px[token] = px
            leave(token, f"stop-loss {move:+.2f}% (tick)")

    def on_tick(update, is_replay):
        """Every price tick of a token with an open position (the engine filters the feed; the other ticks never reach Python)."""
        ch = update.added(["token", "ltp"])
        if ch is None:
            return
        for tok, ltp in zip(ch["token"], ch["ltp"]):
            tok = int(tok)
            pos = position.get(tok)
            if pos is None or pos["state"] != "open" or ltp <= 0:
                continue
            div = divisor_of(tok)
            if div:
                tick_exit(tok, pos, float(ltp) / float(div))

    tick_jobs = queue.Queue()                                      # (token, add?) jobs for the one worker thread: Deephaven refuses an input-table edit made from inside a listener or a callback

    def tick_worker():
        """Adds / removes the tokens of open positions, one at a time and in order, outside any listener."""
        while True:
            item = tick_jobs.get()
            if item is None:
                return
            token, on = item
            if not tick_state:
                continue
            try:
                from deephaven import new_table
                from deephaven.column import int_col
                with exec_ctx:
                    (tick_state[0].add if on else tick_state[0].delete)(new_table([int_col("token", [token])]))
            except Exception as e:
                print(f"{STRATEGY}: tick exits: token {token} could not be {'added' if on else 'removed'} ({e})")

    def tick_track(token, on):
        """Add / remove a token to the set whose ticks are followed (a position opened / closed). Only queued here: it is done by the worker."""
        if tick_state:
            tick_jobs.put((int(token), bool(on)))

    def tick_start():
        if not TICK_EXITS or (TARGET_PCT is None and STOP_PCT is None):
            return
        try:
            feed = globals().get(FEED_TABLE)
            if feed is None:
                raise RuntimeError(f"no table {FEED_TABLE!r} in this session")
            from deephaven import dtypes, input_table
            from deephaven.table_listener import listen
            keys = input_table(col_defs={"token": dtypes.int32}, key_cols="token")
            ticks = feed.where_in(keys, "token").where("ltp > 0").view(["token", "ltp"])           # in the engine: only the open positions' ticks come through
            old = current().shared.setdefault("tick_handles", {}).pop(STRATEGY, None)
            if old is not None:
                old.stop()                                                                      # pasted again: the old listener goes
            handle = listen(ticks, on_tick)
            current().shared["tick_handles"][STRATEGY] = handle
            old_jobs = current().shared.setdefault("tick_workers", {}).pop(STRATEGY, None)
            if old_jobs is not None:
                old_jobs.put(None)                                                              # pasted again: the old worker goes too
            current().shared["tick_workers"][STRATEGY] = tick_jobs
            tick_state[:] = [keys, handle]
            threading.Thread(target=tick_worker, name=f"{STRATEGY}-tick-keys", daemon=True).start()
            print(f"{STRATEGY}: target and stop-loss are checked on every price tick of {FEED_TABLE} (and on each candle as the backup)")
        except Exception as e:
            tick_state.clear()
            print(f"{STRATEGY}: tick exits not available ({e}); target and stop-loss are checked on each 1-minute candle")

    def settled(token, what, future):
        """The bridge answered (called from the NATS thread)."""
        try:
            ok = bool(future.result().ok)
            why = "" if ok else future.result().status
        except Exception as e:
            ok, why = False, repr(e)
        pos = position.get(token)
        if what == "enter":
            if ok and pos is not None:
                pos["state"] = "open"
            else:
                if pos is not None:
                    live.discard(pos["pf"])
                position.pop(token, None)
                tick_track(token, False)
                last_fail[token] = time.time()
                entered.discard(token)                                 # nothing was opened: this stock may still be entered
                print(f"{symbol_of[token]}: entry FAILED ({why}); nothing is open")
        elif ok:
            position.pop(token, None)
            tick_track(token, False)
            if pos is not None and UNSUB_AFTER_S is not None:
                timer = threading.Timer(UNSUB_AFTER_S, release, (pos["pf"], token))        # the slot is freed a little later, not at once: the plugin may still be filling its closing order
                timer.daemon = True
                timer.start()
        elif pos is not None:
            pos["state"] = "open"
            last_fail[token] = time.time()
            print(f"{symbol_of[token]}: exit FAILED ({why}); the position is still open, it is tried again")

    def enter(token, side):
        """Send the entry. True when it was sent; False when it was not (the limit, no portfolio number, all 100 of the day used): the caller may try again at the next signal."""
        if len(live) >= MAX_PORTFOLIOS:
            if not at_limit_said:
                at_limit_said.append(True)
                print(f"{STRATEGY}: {len(live)} portfolios held (MAX_PORTFOLIOS {MAX_PORTFOLIOS}); no new entry until an exit has been unsubscribed")
            return False
        if next_slot[token] >= 100:
            print(f"{symbol_of[token]}: all 100 portfolios of the day are used, no trade")
            return False
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])       # the BOOK gives a NEW portfolio number (None while OFF or unticked)
        if not a:
            return False
        next_slot[token] += 1
        live.add(a["pf"])
        entered.add(token)
        position[token] = {"side": side, "entry": last_px[token], "qty": QTY, "pf": a["pf"], "state": "entering"}
        tick_track(token, True)
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {QTY} lot(s) at {last_px[token]:g} on PF {a['pf']}")
        send_entry(a["pf"], token, side, QTY).add_done_callback(lambda f: settled(token, "enter", f))
        return True

    def leave(token, why):
        pos = position.get(token)
        if pos is None or pos["state"] != "open":
            return
        pos["state"] = "exiting"
        print(f"{symbol_of[token]}: {why}, closing at {last_px[token]:g}")
        send_exit(pos["pf"]).add_done_callback(lambda f: settled(token, "exit", f))

    def step(token, entry_ok=True, ends_bar=True):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry (only when `entry_ok`: with ACT_ON_BAR_END that is the candle that ends a bar).
        `ends_bar` says if this candle completes a bar, so the newest bar counts in the EMAs."""
        px = last_px[token]
        pos = position.get(token)
        if pos is not None:
            if pos["state"] == "open":
                move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)         # % in the position's favour (negative = against it)
                if move >= TARGET_PCT:
                    leave(token, f"target {move:+.2f}%")
                elif move <= -STOP_PCT:
                    leave(token, f"stop-loss {move:+.2f}%")
            return
        if not entry_ok or (ONE_ENTRY_PER_STOCK and token in entered):
            return
        keys = sorted(bars[token])
        bar = keys[-1] if ends_bar else (keys[-2] if len(keys) > 1 else None)       # the bar this look is at
        if last_cross.get(token) == bar or not in_window() or not algo_sending(STRATEGY) or not pf_allowed(STRATEGY, token):
            return
        if time.time() - last_fail.get(token, 0.0) < FAIL_COOLDOWN_S:
            return
        first = retry.pop(token, None)
        if first is None:
            count("looked at")
        try:
            today = day_sides(token)                               # 0. today's bar closes: the latest is the day's highest (buy) or lowest (sell)
            if not today:
                count("not the high/low of the day")
                return
            allowed = basket_sides(symbol_of[token])               # 1. relative: extreme in its basket (and on which side)
            if not allowed:
                count("refused by basket")
                return
            side = trend_of(closes(token, finished_only=not ends_bar))        # 2. the EMAs: fast above slow = buy, below = sell
            if side is None or side not in allowed or side not in today:
                count("refused by EMAs")
                return
            if ABS_DAYS > 0 and side not in own_days_sides(symbol_of[token]):         # 3. absolute: extreme against its own days, on the same side
                count("refused by own days")
                return
        except Exception as e:
            ranking_failed(token, first, e)
            return
        if enter(token, side):
            count("entered")
            last_cross[token] = bar

    stopping = []                                                  # set once the time exit has started the stop

    def time_stop():
        """The time exit: what stop() does by hand. algo_stop pauses and squares up every portfolio of this strategy, unsubscribes them a minute later (so the plugin can send and
        fill its closing order first) and stops the automation. It refuses while sending is OFF; it is then said, and stop() can be run by hand."""
        print(f"{STRATEGY}: time exit {EXIT_AT:%H:%M}: pausing, squaring up and unsubscribing every portfolio, then stopping")
        try:
            stop()
        except Exception as e:
            print(f"{STRATEGY}: the time exit could not stop the automation ({e}); run {STRATEGY}_stop() by hand")

    def on_candle(update, is_replay):
        """Every batch of newly closed 1-minute candles: keep the closes of the traded stocks and run the rules on each."""
        ch = update.added(["token", "Close", "Minute"])
        if ch is None:
            return
        due = list(retry.items())                                  # stocks whose ranking failed in an earlier batch: looked at again below, once this batch has been taken in
        for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
            tok = int(tok)
            if tok in bars and close > 0:
                add_close(tok, minute, close)
                last_px[tok] = float(close)
                ends_bar = (epoch_s(minute) + 60) % BAR_S == 0                              # this candle completes a bar of EMA_BAR
                step(tok, not ACT_ON_BAR_END or ends_bar, ends_bar)
        for tok, t0 in due:
            if time.time() - t0 >= RETRY_RANKING_S:
                retry.pop(tok, None)                               # waited long enough: given up on this bar
            elif tok in retry:
                step(tok, True, True)
        if past_exit() and not stopping:                           # the time exit: stop the whole automation, once (in its own thread: the stop waits for its commands)
            stopping.append(True)
            threading.Thread(target=time_stop, name=f"{STRATEGY}-time-exit", daemon=True).start()

    # ---- by hand ---------------------------------------------------------------------------------------------------------------------
    def exit_all():
        """Close every open position now (the squareup of each portfolio)."""
        for token in list(position):
            leave(token, "exit_all")

    def status():
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(position)} open position(s), baskets {', '.join(f'{n} ({len(m)})' for n, m in BASKETS.items())}")
        for token, p in position.items():
            print(f"   {symbol_of[token]:16} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} lot(s) entry {p['entry']:g} now {last_px.get(token, float('nan')):g} PF {p['pf']} {p['state']}")
        ready = sum(1 for t in symbol_of if len(closes(t, True)) >= 2 * EMA_SLOW)
        print(f"   the EMAs are ready for {ready} of {len(symbol_of)} stocks ({EMA_BAR} bars, {2 * EMA_SLOW} needed)")
        print("   signals: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none yet"))

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY)                                        # pause + square up + unsubscribe every portfolio, wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["india_fut_ema_status"], shared["india_fut_ema_exit_all"], shared["india_fut_ema_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_india_fut_ema()
