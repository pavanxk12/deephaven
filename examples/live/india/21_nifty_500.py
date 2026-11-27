# INDIA CASH EQUITY BASKET + EXTREME FILTERS + CrossOrder: the NIFTY 500 sub-sector version, with RTURNOVER in place of the straddle V. A plain script (functions, no class).
# Paste in the India server's terminal after the services are up (init.py).
#
#   The universe      SECTOR_BASKETS names each basket's underlyings: the 50 NIFTY 500 sub-sectors (PSU banks apart from private banks, API/CDMO apart from formulations, and so on).
#                     Each basket entry is the plain CASH equity symbol ("SBIN"), not a future. An underlying with NO cash listing in the master is dropped by make_baskets(), and a
#                     basket left with fewer than MIN_BASKET members is dropped whole (it is SAID, not an error).
#   The signal        at the end of every SIGNAL_BAR bar, for each stock that has no position, four filters in this order:
#                     0. TODAY      the latest SIGNAL_BAR close must be the HIGHEST of today's SIGNAL_BAR closes so far (a BUY) or the LOWEST (a SELL) (DAY_EXTREME); a stock that is
#                                   neither is dropped here, before anything else is asked.
#                     1. BASKET AVG the basket's own AVERAGE % change from the open (across every member, from the same relative() read filter 2 uses): BUY needs it at least
#                                   BASKAVG_LONG, SELL needs it at or below BASKAVG_SHORT. A basket-wide momentum gate, not a per-stock ranking.
#                     2. RTURNOVER  the stock's RELATIVE TURNOVER: its cumulative traded VALUE (price x volume) from today's open, over the AVERAGE of its own cumulative
#                                   turnover at the SAME minute on its previous RTURNOVER_DAYS days, must be at least RTURNOVER_MIN. This REPLACES the straddle V of the
#                                   earlier script: it needs no options and no straddle, only absolute("turnover", ...) (the candles' Close x Volume). The same gate is used
#                                   on both sides (unlike V, high turnover is not a directional statement). absolute() says "cannot tell" (and nothing trades) with fewer
#                                   than half of RTURNOVER_DAYS earlier days in the saved files.
#                     3. RELATIVE   the stock's % change from the OPEN against its basket (relative()): BUY needs pct_open at least REL_BUY, SELL needs it at or below REL_SELL.
#                     4. ABSOLUTE   the same, against its own earlier days at the same minute (absolute(), ABS_DAYS = 22 saved days): BUY needs ABS_BUY, SELL needs ABS_SELL.
#                     Each filter runs only for the stocks that passed the one before it. With fewer than half of the days in the files absolute() cannot tell and there is no trade.
#                     A trade needs the sides of 0 and 1 (and 3, and 4 when ABS_DAYS > 0) to agree on exactly one side; filter 2 is a gate, it picks no side.
#   The exit          the same as before: +TARGET_PCT in your favour or -STOP_PCT against you, on every price tick of the live feed for the open positions (TICK_EXITS) and on each
#                     1-minute close; then `squareup`, and the portfolio is unsubscribed UNSUB_AFTER_S seconds later. At most MAX_PORTFOLIOS portfolios are held at once.
#                     TRAIL_TRIGGER_PCT / PROFIT_LOCK_PCT (both None, or both set): once the move reaches TRAIL_TRIGGER_PCT the stop shifts, for the rest of that position's life,
#                     from -STOP_PCT to PROFIT_LOCK_PCT.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:10 India time): the automation is stopped the way algo_stop does it (pause, square up, a minute later unsubscribe every portfolio, then the script stops).
#   The order         a NEW portfolio for every trade: ONE `subscribe` with the CrossOrder fields:
#                     {"strategyname": "CrossOrder", "TOKEN": <the equity>, "SIDE": 1|2, "QTY": n shares, "SERVERCODE": 26286, "ACCOUNT": "", "ALGOID": "34500"}
#                     QTY is worked out from ENTRY_VALUE: the number of shares (rounded, at least 1) that costs about ENTRY_VALUE rupees at the entry price.
#
# WHAT THE BACKTEST SAID (NIFTY 500 cash 1-minute, 2026-01-01 .. 2026-09-16, 175 days, these filters, cash closes, no futures, RVOL not RTURNOVER, and no V):
#   the RVOL gate is monotone -- mean per trade -0.016% with no gate, +0.019% at 1.5, +0.053% at 4.0, +0.082% at 5.0, and the win rate rises 44% -> 47% with it.
#   At RVOL_MIN 5 it made 1,492 trades over 164 days (9.1/day), +0.082% a trade gross, and it was still positive at 0.05% round-trip cost (+0.032%) but NOT at 0.10% (-0.018%).
#   SHORTS carried it: +0.109% a trade at a 50% win rate, against +0.062% and 45% for the longs.
#   RTURNOVER has not been backtested separately: RTURNOVER_MIN = 8.0 below is a starting guess (turnover = volume x price moves more than volume alone), not a tuned number.
#   The +10% target NEVER fired in 20,000+ backtested trades and the MEDIAN trade was -0.758%, i.e. the stop: the exits, not the filters, are where the rest of the money is.
#   So: this is a thin edge that lives or dies on your real all-in cost. Run it small, and measure your actual slippage before sizing it up.
#
# EVERYTHING is inside the function run_nifty_500_directional(); the only names left in the terminal start with nifty_500_directional:
#   nifty_500_directional_status()   nifty_500_directional_exit_all()   nifty_500_directional_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).

def run_nifty_500_directional():
    import contextlib
    import io
    import queue
    import threading
    import time
    from contextlib import nullcontext
    from datetime import datetime, time as dtime
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

    _INI_NAME = "params.ini"
    
    def _params(section):
        """Every USER INPUT value of this script, from params.ini, section [section]. Values are Python
        literals (ast.literal_eval): numbers, strings, True/False/None, lists, tuples, dicts. A value that is
        not a literal is kept as text, which is how a time like 15:45 survives for P.time(). '#' is NOT an
        inline comment here, so a value keeps any '#' it contains; put comments on their own line.
        init.py exec()s this script, so __file__ may not exist: the file is looked for in $ALGO_PARAMS, then
        the container's strategies folder, then beside this script, then the working directory."""
        import ast, configparser, os
    
        def _lit(text):
            """A Python literal, also allowing dict(a=1, b=2) -- the shape the scripts already used. No other
            call is accepted, so a params.ini can never run anything."""
            class _D(ast.NodeTransformer):
                def visit_Call(self, n):
                    self.generic_visit(n)
                    if isinstance(n.func, ast.Name) and n.func.id == "dict" and not n.args:
                        return ast.Dict([ast.Constant(kw.arg) for kw in n.keywords], [kw.value for kw in n.keywords])
                    raise ValueError("only dict(...) may be called in a value")
            tree = _D().visit(ast.parse(text.strip(), mode="eval"))
            return ast.literal_eval(ast.fix_missing_locations(tree))
        _here = globals().get("__file__")
        cands = [os.environ.get("ALGO_PARAMS", ""),
                 "/app/scripts/strategies/" + _INI_NAME,
                 os.path.join(os.path.dirname(os.path.abspath(_here)), _INI_NAME) if _here else "",
                 _INI_NAME]
        path = next((p for p in cands if p and os.path.isfile(p)), None)
        if path is None:
            raise FileNotFoundError(f"{_INI_NAME} not found: looked at $ALGO_PARAMS, /app/scripts/strategies/, "
                                    f"beside this script, and {os.getcwd()}")
        cfg = configparser.ConfigParser(inline_comment_prefixes=None, interpolation=None)
        cfg.optionxform = str                                          # keys stay UPPER CASE
        with open(path, encoding="utf-8") as fh:
            cfg.read_file(fh)
        if not cfg.has_section(section):
            raise KeyError(f"{path}: no section [{section}]; it has {cfg.sections()}")
    
        class _P(dict):
            path, section = "", ""
            def __missing__(self, k):
                raise KeyError(f"{self.path} [{self.section}]: {k} is not set")
            def time(self, k):
                """A HH:MM or HH:MM:SS value as a datetime.time."""
                from datetime import time as _t
                parts = [int(x) for x in str(self[k]).strip().split(":")]
                if not 2 <= len(parts) <= 3:
                    raise ValueError(f"{self.path} [{self.section}]: {k} = {self[k]!r} is not HH:MM")
                return _t(*parts)
    
        out = _P()
        out.path, out.section = path, section
        for k, v in cfg.items(section):
            v = v.strip()
            try:
                out[k] = _lit(v)
            except (ValueError, SyntaxError, TypeError):
                out[k] = v                                             # a time like 15:45, or plain text
        return out
    
    # =============================== USER INPUT ===============================
    # Every value below is read from params.ini in this folder, section [nifty_500_directional].
    # Change a number there, not here; this block only names what the script needs.
    PARAMS_SECTION = "nifty_500_directional"
    P = _params(PARAMS_SECTION)
    print(f"{P.section}: {len(P)} parameters from {P.path}")
    
    USER = P["USER"]
    STRATEGY = P["STRATEGY"]
    INDEX_FUTURES = P["INDEX_FUTURES"]
    SECTOR_BASKETS = P["SECTOR_BASKETS"]
    EXCLUDE = P["EXCLUDE"]
    MIN_BASKET = P["MIN_BASKET"]
    ONE_BASKET = P["ONE_BASKET"]
    LEVEL = P["LEVEL"]
    BASKAVG_LONG = P["BASKAVG_LONG"]
    BASKAVG_SHORT = P["BASKAVG_SHORT"]
    RTURNOVER_MIN = P["RTURNOVER_MIN"]
    RTURNOVER_DAYS = P["RTURNOVER_DAYS"]
    REL_BUY = P["REL_BUY"]
    REL_SELL = P["REL_SELL"]
    ABS_BUY = P["ABS_BUY"]
    ABS_SELL = P["ABS_SELL"]
    DIRECTION_MATCH = P["DIRECTION_MATCH"]
    SIDES = P["SIDES"]
    DAY_EXTREME = P["DAY_EXTREME"]
    ABS_DAYS = P["ABS_DAYS"]
    SIGNAL_BAR = P["SIGNAL_BAR"]
    ACT_ON_BAR_END = P["ACT_ON_BAR_END"]
    TARGET_PCT = P["TARGET_PCT"]
    STOP_PCT = P["STOP_PCT"]
    TRAIL_TRIGGER_PCT = P["TRAIL_TRIGGER_PCT"]
    PROFIT_LOCK_PCT = P["PROFIT_LOCK_PCT"]
    TRAIL_STEP_PCT = P["TRAIL_STEP_PCT"]
    ONE_ENTRY_PER_STOCK = P["ONE_ENTRY_PER_STOCK"]
    ENTRY_VALUE = P["ENTRY_VALUE"]
    ENGINE = P["ENGINE"]
    WINDOW = (P.time("WINDOW_START"), P.time("WINDOW_END"))
    EXIT_AT = P.time("EXIT_AT")
    MAX_PORTFOLIOS = P["MAX_PORTFOLIOS"]
    UNSUB_AFTER_S = P["UNSUB_AFTER_S"]
    TICK_EXITS = P["TICK_EXITS"]
    FEED_TABLE = P["FEED_TABLE"]
    FAIL_COOLDOWN_S = P["FAIL_COOLDOWN_S"]
    RETRY_RANKING_S = P["RETRY_RANKING_S"]
    # ==========================================================================

    def make_baskets():
        """SECTOR_BASKETS gives each basket's underlyings by name. A basket entry is the plain CASH equity symbol ("SBIN"), among the underlyings of that sub-sector that are
        listed in the master's cash equities (and are not in INDEX_FUTURES/EXCLUDE). A sub-sector left with fewer than MIN_BASKET live cash equities is dropped whole and said."""
        eqs = current().master.eqs
        live = {str(u).upper() for u in eqs["TckrSymb"]} - set(INDEX_FUTURES) - {u.upper() for u in EXCLUDE}
        baskets, dropped = {}, []
        for name, unders in SECTOR_BASKETS.items():
            members = [u.upper() for u in unders if u.upper() in live]
            if len(members) >= MIN_BASKET:
                baskets[name] = members
            elif members:
                dropped.append(f"{name} ({len(members)})")
        if dropped:
            print(f"{STRATEGY}: dropped, under MIN_BASKET {MIN_BASKET} live cash equities: {', '.join(dropped)}")
        return baskets

    BASKETS = make_baskets()
    if ONE_BASKET:                                                 # rank every stock against the WHOLE NIFTY 500 instead of its own sub-sector.
        members = sorted({s for v in BASKETS.values() for s in v})  # the set also dedupes, so the "in two baskets" guard below cannot trip on it.
        print(f"{STRATEGY}: ONE_BASKET -- the {len(BASKETS)} sub-sectors are merged into one basket of {len(members)} equities; "
              f"REL_BUY/REL_SELL are now a rank against the whole market, so 80/20 would mean the top/bottom 100 names")
        BASKETS = {"all": members}
    if not (BASKAVG_SHORT < BASKAVG_LONG and 0 <= REL_SELL < REL_BUY <= 100 and 0 <= ABS_SELL < ABS_BUY <= 100
            and TARGET_PCT > 0 and STOP_PCT > 0 and ENTRY_VALUE > 0 and BASKETS and all(len(v) >= MIN_BASKET for v in BASKETS.values())
            and (RTURNOVER_MIN is None or (RTURNOVER_MIN > 0 and isinstance(RTURNOVER_DAYS, int) and 1 <= RTURNOVER_DAYS <= 400))
            and SIDES and set(SIDES) <= {1, 2}
            and ((TRAIL_TRIGGER_PCT is None) == (PROFIT_LOCK_PCT is None)) and (TRAIL_TRIGGER_PCT is None or TRAIL_TRIGGER_PCT > 0)
            and (TRAIL_STEP_PCT is None or (TRAIL_TRIGGER_PCT is not None and TRAIL_STEP_PCT > 0))
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0
            and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)):
        raise ValueError("REL_SELL/ABS_SELL must be below REL_BUY/ABS_BUY, all inside 0 to 100, TARGET_PCT and STOP_PCT above 0, "
                         "RTURNOVER_MIN (if set) above 0 with RTURNOVER_DAYS a whole number 1 to 400, SIDES a non-empty subset of {1, 2}, "
                         "TRAIL_TRIGGER_PCT and PROFIT_LOCK_PCT both None or both set (and TRAIL_TRIGGER_PCT above 0), "
                         f"ENTRY_VALUE above 0, ABS_DAYS 0 to 400, EXIT_AT after the window opens, and at least one basket with {MIN_BASKET}+ live cash equities, "
                         "and TRAIL_STEP_PCT (if set) needs TRAIL_TRIGGER_PCT set too and must be above 0")
    BAR_S = parse_timeframe(SIGNAL_BAR)
    basket_of = {}                                                 # symbol -> the name of its basket
    for name, members in BASKETS.items():
        for symbol in members:
            if symbol in basket_of:
                raise ValueError(f"{symbol!r} is in two baskets ({basket_of[symbol]!r} and {name!r}): a symbol belongs to one basket only")
            basket_of[symbol] = name
    print(f"{STRATEGY}: {len(BASKETS)} baskets, {len(basket_of)} equities: " + ", ".join(f"{n} ({len(m)})" for n, m in BASKETS.items()))

    try:
        from deephaven.execution_context import get_exec_ctx
        exec_ctx = get_exec_ctx()                                  # taken here, in the terminal: the candle listener thread has none, and the ranking reads tables
    except Exception:
        exec_ctx = nullcontext()

    nats_connect(user=USER, client=STRATEGY)                       # only checks the NATS service is up and this user may send
    registry_connect()                                             # ... and the portfolio registry. Neither starts anything.
    algo_start(USER, STRATEGY)                                     # its book (portfolio numbers) and its own sending switch, which starts OFF
    for name, members in BASKETS.items():
        try:
            basket(name, members)                                  # saved in baskets.txt; the ranking below uses the list itself, so a read-only folder does not matter
        except Exception as e:
            print(f"basket {name!r} was not saved ({e}); the ranking still works from BASKETS")

    symbol_of = {}                                                 # token -> the basket entry it trades for
    _master_eqs = current().master.spot_of                        # underlying -> CASH equity token; NOT contract(name), which gives the nearest FUTURE
                                                                    # first for a bare underlying name (most of these stocks have one) and would silently trade futures
    for name in basket_of:
        token = _master_eqs.get(name)
        if token is None:
            print(f"{name}: no cash contract in the master, left out of the trading (it still counts in the basket)")
            continue
        symbol_of[int(token)] = name
    pf_track(STRATEGY, list(symbol_of), slots=100)                 # the portfolios it had before (a restart or a paste again); every trade takes the next free slot
    with contextlib.redirect_stdout(io.StringIO()):
        had = algo_portfolios(STRATEGY)
    next_slot = {t: (int(had[had["instrument"] == t]["slot"].max()) + 1 if had is not None and (had["instrument"] == t).any() else 0) for t in symbol_of}

    bars = {t: {} for t in symbol_of}                              # token -> {bar number: the last 1-minute close inside it}: what the TODAY filter checks
    last_px = {}                                                   # token -> the latest 1-minute close
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    last_cross = {}                                                # token -> the bar it was last entered on: not twice on one bar
    entered = set()                                                # tokens already entered by this script
    last_fail = {}
    counts = {}                                                    # where the signals went (shown by status())
    retry = {}                                                     # token -> when its ranking first failed

    # ---- RTURNOVER: today's cumulative traded value (price x volume) from the open, over its own average at the same minute on earlier days -----
    # Uses the library's absolute("turnover", ...): no hand-rolled tracking needed, it reads the live table and the saved candles_<day>.parquet
    # files itself (the same files and the same RTURNOVER_DAYS as absolute("pct", ...) in filter 4).
    def rturnover_ok(symbol):
        """Filter 2. True when this stock's cumulative traded value from today's open is at least RTURNOVER_MIN times its own average at the SAME minute of the session
        over its earlier RTURNOVER_DAYS days. False when it is below, or when absolute() cannot tell (fewer than half those days in the saved files)."""
        if RTURNOVER_MIN is None:
            return True
        with exec_ctx:                                              # the call reads the candles table; a listener thread needs the terminal's query context for that
            r = absolute("turnover", LEVEL, symbol, RTURNOVER_DAYS)
        if r[1] < 0:                                                 # (the second item is -1 when it cannot be told)
            count("RTURNOVER cannot be told")
            return False
        hist = r[0]                                                  # Absolute is a tuple (hist, idx); hist/idx are not attributes, only tuple positions
        valid = hist[np.isfinite(hist)]
        avg = float(np.mean(valid)) if valid.size else 0.0
        if avg <= 0:
            count("RTURNOVER cannot be told")
            return False
        if r.now / avg >= RTURNOVER_MIN:
            return True
        count("refused by RTURNOVER")
        return False

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
        """A candle's Minute as UTC epoch seconds."""
        return int(pd.Timestamp(minute).timestamp())

    def add_close(token, minute, close):
        b = bars[token]
        b[epoch_s(minute) // BAR_S] = float(close)
        for old in sorted(b)[:-600]:
            del b[old]

    if ABS_DAYS > 0:                                               # read the earlier days of every stock NOW (one read per day file for all of them), not in the candle thread
        try:
            for warm_ref in ("open", "vwap"):
                absolute_warm("pct", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS, ref=warm_ref)
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")

    for t in symbol_of:                                            # today's candles already built, over the history
        h = last_candles(t, 400, show=False)
        if h is not None and len(h):
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(t, m, c)
            last_px[t] = float(h["Close"].iloc[-1])
    if RTURNOVER_MIN is not None:
        try:
            absolute_warm("turnover", LEVEL, sorted(set(symbol_of.values())), RTURNOVER_DAYS)
        except Exception as e:
            print(f"absolute('turnover', ...): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
        print(f"{STRATEGY}: RTURNOVER >= {RTURNOVER_MIN} over {RTURNOVER_DAYS} days (absolute('turnover', ...): cannot tell with fewer than half those days saved)")

    # ---- the TODAY filter: today's highest/lowest close so far --------------------------------------------------------
    def day_sides(token):
        """Filter 0: the sides today's SIGNAL_BAR closes allow. {1} (BUY) when the latest bar close is the highest of today so far, {2} (SELL) when it is the lowest, empty when it is
        neither or when there are fewer than 2 bars today. Everything is allowed when DAY_EXTREME is False."""
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

    # ---- the filters: against the basket (relative), against the stock's own days (absolute) --------------------------
    def sides_of(strong, weak):
        """{1} (BUY) when `strong` holds, {2} (SELL) when `weak` holds. With DIRECTION_MATCH False, {1, 2} when either one holds. Empty when neither."""
        if DIRECTION_MATCH:
            return ({1} if strong else set()) | ({2} if weak else set())
        return {1, 2} if (strong or weak) else set()

    def ranked_sides(pct, buy, sell):
        """`pct`: a no-argument relative()/absolute() call. BUY needs the percentile at least `buy`; SELL needs it at or below `sell`."""
        with exec_ctx:                                             # the call reads the candles table; a listener thread needs the terminal's query context for that
            result = pct()
        if result[1] < 0:                                          # (the second item is -1 when it cannot be told)
            return set()
        p = result.pctile
        if p != p:                                                 # unknown (NaN)
            return set()
        return sides_of(p >= buy, p <= sell)

    _avg_cache = {}                                                # (basket, bar) -> the sides that basket's average allows on that bar

    def basket_avg_sides(symbol, bar):
        """Filter 1: the basket's own AVERAGE % change from the open (across every member). BUY needs it at least BASKAVG_LONG; SELL needs it at or below BASKAVG_SHORT.
        The average is the SAME number for every member of a basket, so it is read ONCE a bar and kept: with ONE_BASKET the member list is the whole NIFTY 500, and
        reading a 500-name ranking once per stock per bar is what makes the ranking fall behind. A "cannot tell" is never cached, so it is retried."""
        name = basket_of[symbol]
        hit = _avg_cache.get((name, bar))
        if hit is not None:
            return hit
        if len(_avg_cache) > 400:                                  # only today's bars matter; this keeps it from growing all session
            _avg_cache.clear()
        members = BASKETS[name]
        with exec_ctx:
            result = relative("pct", LEVEL, symbol, members, ref="open")
        if result[1] < 0:
            return set()
        avg = float(np.mean(result[0]))                            # result[0]: every basket member's own % change from the open, right now
        out = sides_of(avg >= BASKAVG_LONG, avg <= BASKAVG_SHORT)
        _avg_cache[(name, bar)] = out
        return out

    def relative_sides(symbol):
        """Filter 3, relative(): the ranking of pct_open among the OTHER symbols of ITS basket, against REL_BUY / REL_SELL."""
        members = BASKETS[basket_of[symbol]]
        return ranked_sides(lambda: relative("pct", LEVEL, symbol, members, ref="open"), REL_BUY, REL_SELL)

    def absolute_sides(symbol):
        """Filter 4, absolute(): the ranking of pct_open among the same minute of its previous ABS_DAYS trading days, against ABS_BUY / ABS_SELL."""
        return ranked_sides(lambda: absolute("pct", LEVEL, symbol, ABS_DAYS, ref="open"), ABS_BUY, ABS_SELL)

    # ---- the orders ----------------------------------------------------------------------------------------------------
    def in_window():
        now = datetime.now(ZoneInfo("Asia/Kolkata")).time()
        return WINDOW[0] <= now <= WINDOW[1] and now < EXIT_AT         # no entry once the time exit has come, whatever WINDOW says

    def past_exit():
        return datetime.now(ZoneInfo("Asia/Kolkata")).time() >= EXIT_AT

    def cross_order(token, side, qty):
        return {**ENGINE, "TOKEN": int(token), "SIDE": int(side), "QTY": int(qty)}

    def send_entry(pf, token, side, qty):
        """The one subscribe that starts the strategy: exactly its own fields, sent as they are. A Future with the reply."""
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "subscribe", cross_order(token, side, qty), user=USER, client=STRATEGY)

    def send_exit(pf):
        svc = current().get("nats") or NO_SERVICE
        return svc.squareup(pf, user=USER, client=STRATEGY)                              # the plugin's SquareUpEvent closes the position

    live = set()                                                   # portfolios this script holds in the engine
    at_limit_said = []

    def release(pf, token):
        """Unsubscribe a portfolio whose position is closed. It destroys the plugin instance, which frees one of the engine's slots."""
        try:
            svc = current().get("nats") or NO_SERVICE
            fut = svc.unsubscribe(pf, token, user=USER, client=STRATEGY)
            fut.add_done_callback(lambda f: (live.discard(pf), at_limit_said.clear()))
        except Exception as e:
            print(f"{STRATEGY}: PF {pf} could not be unsubscribed ({e}); it keeps its slot until the stop")

    tick_state = []

    def divisor_of(token):
        """The contract's own divisor from the master (100 for NSE equities and futures), else the market's default."""
        app = current()
        return app.master.divisor_of.get(int(token)) or app.market.price_div

    def exit_reason(pos, move):
        """Why `move` (the % in the position's favour) should exit now, or None."""
        if TRAIL_TRIGGER_PCT is not None and move >= TRAIL_TRIGGER_PCT:
            if not pos["trailing"]:
                pos["trailing"] = True
                pos["lock"] = PROFIT_LOCK_PCT
            if TRAIL_STEP_PCT is not None:
                pos["lock"] = max(pos["lock"], move - TRAIL_STEP_PCT)
        if TARGET_PCT is not None and move >= TARGET_PCT:
            return f"target {move:+.2f}%"
        if pos["trailing"]:
            if move <= pos["lock"]:
                return f"profit-lock {move:+.2f}% (lock {pos['lock']:+.2f}%)"
        elif STOP_PCT is not None and move <= -STOP_PCT:
            return f"stop-loss {move:+.2f}%"
        return None

    def tick_exit(token, pos, px):
        """Target / stop-loss (or trailing profit-lock) of an open position on the price `px`."""
        move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)
        why = exit_reason(pos, move)
        if why is not None:
            last_px[token] = px
            leave(token, f"{why} (tick)")

    def on_tick(update, is_replay):
        """Every price tick of a token with an open position. fo_market_data is a last_by("PF") table: a token's first tick arrives as an added row, every one after that as a
        modified row -- both must be read or only the very first tick of each position is seen."""
        for ch in (update.added(["token", "ltp"]), update.modified(["token", "ltp"])):
            if ch is None:
                continue
            for tok, ltp in zip(ch["token"], ch["ltp"]):
                tok = int(tok)
                pos = position.get(tok)
                if pos is None or pos["state"] != "open" or ltp <= 0:
                    continue
                div = divisor_of(tok)
                if div:
                    tick_exit(tok, pos, float(ltp) / float(div))

    tick_jobs = queue.Queue()                                      # Deephaven refuses an input-table edit made from inside a listener or a callback

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
        """Add / remove a token to the set whose ticks are followed. Only queued here: it is done by the worker."""
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
            ticks = feed.where_in(keys, "token").where("ltp > 0").view(["token", "ltp"])
            old = current().shared.setdefault("tick_handles", {}).pop(STRATEGY, None)
            if old is not None:
                old.stop()
            handle = listen(ticks, on_tick)
            current().shared["tick_handles"][STRATEGY] = handle
            old_jobs = current().shared.setdefault("tick_workers", {}).pop(STRATEGY, None)
            if old_jobs is not None:
                old_jobs.put(None)
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
                entered.discard(token)                             # nothing was opened: this stock may still be entered
                print(f"{symbol_of[token]}: entry FAILED ({why}); nothing is open")
        elif ok:
            position.pop(token, None)
            tick_track(token, False)
            if pos is not None and UNSUB_AFTER_S is not None:
                timer = threading.Timer(UNSUB_AFTER_S, release, (pos["pf"], token))
                timer.daemon = True
                timer.start()
        elif pos is not None:
            pos["state"] = "open"
            last_fail[token] = time.time()
            print(f"{symbol_of[token]}: exit FAILED ({why}); the position is still open, it is tried again")

    def enter(token, side):
        """Send the entry. True when it was sent; False when it was not."""
        if len(live) >= MAX_PORTFOLIOS:
            if not at_limit_said:
                at_limit_said.append(True)
                print(f"{STRATEGY}: {len(live)} portfolios held (MAX_PORTFOLIOS {MAX_PORTFOLIOS}); no new entry until an exit has been unsubscribed")
            return False
        if next_slot[token] >= 100:
            print(f"{symbol_of[token]}: all 100 portfolios of the day are used, no trade")
            return False
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])
        if not a:
            return False
        next_slot[token] += 1
        live.add(a["pf"])
        entered.add(token)
        px = last_px[token]
        qty = max(1, round(ENTRY_VALUE / px)) if px > 0 else 1      # shares that cost about ENTRY_VALUE rupees at the entry price
        position[token] = {"side": side, "entry": px, "qty": qty, "pf": a["pf"], "state": "entering", "trailing": False, "lock": None}
        tick_track(token, True)
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {qty} share(s) (~Rs {qty * px:,.0f}) at {px:g} on PF {a['pf']}")
        send_entry(a["pf"], token, side, qty).add_done_callback(lambda f: settled(token, "enter", f))
        return True

    def leave(token, why):
        pos = position.get(token)
        if pos is None or pos["state"] != "open":
            return
        pos["state"] = "exiting"
        print(f"{symbol_of[token]}: {why}, closing at {last_px[token]:g}")
        send_exit(pos["pf"]).add_done_callback(lambda f: settled(token, "exit", f))

    def step(token, entry_ok=True, ends_bar=True):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry."""
        px = last_px[token]
        pos = position.get(token)
        if pos is not None:
            if pos["state"] == "open":
                move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)
                why = exit_reason(pos, move)
                if why is not None:
                    leave(token, why)
            return
        if not entry_ok or (ONE_ENTRY_PER_STOCK and token in entered):
            return
        keys = sorted(bars[token])
        bar = keys[-1] if ends_bar else (keys[-2] if len(keys) > 1 else None)
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
            sides = today & basket_avg_sides(symbol_of[token], bar)  # 1. basket avg: the basket's own average % move from the open (one read a bar)
            if not sides:
                count("refused by basket avg")
                return
            if not rturnover_ok(symbol_of[token]):                 # 2. RTURNOVER: this stock is trading far above its own normal traded value by this minute
                return
            sides &= relative_sides(symbol_of[token])              # 3. relative: extreme in its basket (and on which side)
            if not sides:
                count("refused by basket")
                return
            if ABS_DAYS > 0:
                sides &= absolute_sides(symbol_of[token])          # 4. absolute: extreme against its own days, on the same side
            sides &= set(SIDES)                                    # SHORT ONLY / long only, if asked for
            if len(sides) != 1:
                count("refused by own days" if ABS_DAYS > 0 and not sides else "no single side agreed")
                return
            side = next(iter(sides))
        except Exception as e:
            ranking_failed(token, first, e)
            return
        if enter(token, side):
            count("entered")
            last_cross[token] = bar

    stopping = []

    def time_stop():
        """The time exit: what stop() does by hand."""
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
        due = list(retry.items())
        for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
            tok = int(tok)
            if tok in bars and close > 0:
                add_close(tok, minute, close)
                last_px[tok] = float(close)
                ends_bar = (epoch_s(minute) + 60) % BAR_S == 0
                step(tok, not ACT_ON_BAR_END or ends_bar, ends_bar)
        for tok, t0 in due:
            if time.time() - t0 >= RETRY_RANKING_S:
                retry.pop(tok, None)
            elif tok in retry:
                step(tok, True, True)
        if past_exit() and not stopping:
            stopping.append(True)
            threading.Thread(target=time_stop, name=f"{STRATEGY}-time-exit", daemon=True).start()

    # ---- by hand ---------------------------------------------------------------------------------------------------
    def exit_all():
        """Close every open position now (the squareup of each portfolio)."""
        for token in list(position):
            leave(token, "exit_all")

    def status():
        sides = {1: "LONG only", 2: "SHORT only"}.get(SIDES[0]) if len(SIDES) == 1 else "both sides"
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(position)} open position(s), {sides}, "
              f"RTURNOVER >= {RTURNOVER_MIN}, {len(BASKETS)} baskets / {len(symbol_of)} equities, ENTRY_VALUE Rs {ENTRY_VALUE:,.0f}")
        for token, p in position.items():
            print(f"   {symbol_of[token]:16} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} share(s) entry {p['entry']:g} now {last_px.get(token, float('nan')):g} PF {p['pf']} {p['state']}")
        print("   signals: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none yet"))

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY, pause=False)                           # square up + unsubscribe every portfolio, wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["nifty_500_directional_status"], shared["nifty_500_directional_exit_all"], shared["nifty_500_directional_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_nifty_500_directional()
