# INDIA STOCK FUTURES BASKET + EXTREME FILTERS + CrossOrder: 11 without the RSI. A plain script (functions, no class). Paste in the India server's terminal after the services are up (init.py).
#
#   The universe      SECTOR_BASKETS names each basket's underlyings (e.g. "bank", "fin"); "SBIN FUT" = the nearest future of each day, among those that have not expired.
#   The signal        at the end of every SIGNAL_BAR bar (the candle that ends it: :05, :10, :15 ...), for each stock that has no position, four filters in this order:
#                     0. TODAY      the latest SIGNAL_BAR close must be the HIGHEST of today's SIGNAL_BAR closes so far (a BUY) or the LOWEST (a SELL) (DAY_EXTREME); a stock that is neither is
#                                   dropped here, before anything else is asked.
#                     1. BASKET AVG the basket's own AVERAGE % change from the open (across every member, from the same relative() read filter 2 uses): BUY needs it at least BASKAVG_LONG,
#                                   SELL needs it at or below BASKAVG_SHORT. A basket-wide momentum gate, not a per-stock ranking.
#                     2. RELATIVE   the stock's % change from the OPEN against its basket (relative()), and its underlying's straddle V % change from the open (USE_V): BUY needs pct_open
#                                   and v both at least REL_BUY's; SELL needs pct_open at or below REL_SELL's and v at least REL_SELL's own v threshold. DIRECTION_MATCH = True: each side
#                                   needs its own. False: either passes.
#                     3. ABSOLUTE   the same, against its own earlier days at the same minute (absolute(), ABS_DAYS = 22 saved days): BUY needs ABS_BUY's pct_open and v; SELL needs
#                                   ABS_SELL's pct_open and its own v threshold.
#                     Each filter runs only for the stocks that passed the one before it. With fewer than half of the days in the files absolute() cannot tell and there is no trade.
#                     A trade needs the sides of 0 and 1 (and 2, and 3 when ABS_DAYS > 0) to agree on exactly one side.
#                     The V of a future is the straddle cost (V column) of its underlying's nearest expiry, as its % change from today's open: the script STARTS the straddle itself if it
#                     is not running (straddle_start(STRADDLE_N); the candles must hold the OPTIONS too, not futures only) and straddle_<day>.parquet files must exist for the earlier days.
#                     Without them the V filter says "cannot tell" and nothing trades; USE_V = False leaves it out.
#   The exit          the same as 11: +TARGET_PCT in your favour or -STOP_PCT against you, on every price tick of the live feed for the open positions (TICK_EXITS) and on each 1-minute close;
#                     then `squareup`, and the portfolio is unsubscribed UNSUB_AFTER_S seconds later. At most MAX_PORTFOLIOS portfolios are held at once. TRAIL_TRIGGER_PCT / PROFIT_LOCK_PCT
#                     (both None, or both set): once the move reaches TRAIL_TRIGGER_PCT the stop shifts, for the rest of that position's life, from -STOP_PCT to PROFIT_LOCK_PCT.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:15 India time): the automation is stopped the way algo_stop does it (pause, square up, a minute later unsubscribe every portfolio, then the script stops).
#   The order         a NEW portfolio for every trade: ONE `subscribe` with the CrossOrder fields, as for 11:
#                     {"strategyname": "CrossOrder", "TOKEN": <the future>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#
# EVERYTHING is inside the function run_directional(); the only names left in the terminal start with directional:
#   directional_status()   directional_exit_all()   directional_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).

def run_directional():
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
        literals: numbers, strings, True/False/None, lists, tuples, dicts, and dict(a=1) -- no other call is
        accepted, so a params.ini can never run anything. A value that is not a literal is kept as text, which
        is how a time like 15:45 survives for _ptime(). '#' is NOT an inline comment here, so a value keeps any
        '#' it contains; put comments on their own line.
        init.py exec()s this script, so __file__ may not exist: the file is looked for in $ALGO_PARAMS, then
        the container's strategies folder, then beside this script, then the working directory."""
        import ast, configparser, os
    
        def value_of(n):
            """One parsed node as a value. Literals, and dict(a=1) because the scripts already spelled some
            settings that way. Any other call, name or operator is refused, so nothing in a params.ini runs."""
            if isinstance(n, ast.Constant):
                return n.value
            if isinstance(n, ast.Dict):
                return {value_of(k): value_of(v) for k, v in zip(n.keys, n.values)}
            if isinstance(n, ast.List):
                return [value_of(x) for x in n.elts]
            if isinstance(n, ast.Tuple):
                return tuple(value_of(x) for x in n.elts)
            if isinstance(n, ast.Set):
                return {value_of(x) for x in n.elts}
            if isinstance(n, ast.UnaryOp) and isinstance(n.op, (ast.USub, ast.UAdd)):
                v = value_of(n.operand)                                # -2000, -0.1 ...
                return -v if isinstance(n.op, ast.USub) else +v
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "dict" and not n.args:
                return {kw.arg: value_of(kw.value) for kw in n.keywords}
            raise ValueError(f"not a literal: {ast.dump(n)[:60]}")
    
        def literal(text):
            return value_of(ast.parse(text.strip(), mode="eval").body)
    
        here = globals().get("__file__")
        for path in (os.environ.get("ALGO_PARAMS", ""),
                     "/app/scripts/strategies/" + _INI_NAME,
                     os.path.join(os.path.dirname(os.path.abspath(here)), _INI_NAME) if here else "",
                     _INI_NAME):
            if path and os.path.isfile(path):
                break
        else:
            raise FileNotFoundError(f"{_INI_NAME} not found: looked at $ALGO_PARAMS, /app/scripts/strategies/, "
                                    f"beside this script, and {os.getcwd()}")
        cfg = configparser.ConfigParser(inline_comment_prefixes=None, interpolation=None)
        cfg.optionxform = str                                          # keys stay UPPER CASE
        with open(path, encoding="utf-8") as fh:
            cfg.read_file(fh)
        if not cfg.has_section(section):
            raise KeyError(f"{path}: no section [{section}]; it has {cfg.sections()}")
        out = {}
        for key, text in cfg.items(section):
            text = text.strip()
            try:
                out[key] = literal(text)
            except (ValueError, SyntaxError, TypeError):
                out[key] = text                                        # a time like 15:45, or plain text
        print(f"{section}: {len(out)} parameters from {path}")
        return out
    
    def _ptime(P, key):
        """A HH:MM or HH:MM:SS value from params.ini as a datetime.time."""
        from datetime import time as _t
        parts = [int(x) for x in str(P[key]).strip().split(":")]
        if not 2 <= len(parts) <= 3:
            raise ValueError(f"params.ini: {key} = {P[key]!r} is not HH:MM")
        return _t(*parts)
    
    # =============================== USER INPUT ===============================
    # Every value below is read from params.ini in this folder, section [directional].
    # Change a number there, not here; this block only names what the script needs.
    PARAMS_SECTION = "directional"
    P = _params(PARAMS_SECTION)
    
    USER = P["USER"]
    STRATEGY = P["STRATEGY"]
    INDEX_FUTURES = P["INDEX_FUTURES"]
    SECTOR_BASKETS = P["SECTOR_BASKETS"]
    EXCLUDE = P["EXCLUDE"]
    LEVEL = P["LEVEL"]
    BASKAVG_LONG = P["BASKAVG_LONG"]
    BASKAVG_SHORT = P["BASKAVG_SHORT"]
    REL_BUY = P["REL_BUY"]
    REL_SELL = P["REL_SELL"]
    ABS_BUY = P["ABS_BUY"]
    ABS_SELL = P["ABS_SELL"]
    DIRECTION_MATCH = P["DIRECTION_MATCH"]
    USE_V = P["USE_V"]
    STRADDLE_N = P["STRADDLE_N"]
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
    QTY = P["QTY"]
    ENGINE = P["ENGINE"]
    WINDOW = (_ptime(P, "WINDOW_START"), _ptime(P, "WINDOW_END"))
    EXIT_AT = _ptime(P, "EXIT_AT")
    MAX_PORTFOLIOS = P["MAX_PORTFOLIOS"]
    UNSUB_AFTER_S = P["UNSUB_AFTER_S"]
    TICK_EXITS = P["TICK_EXITS"]
    FEED_TABLE = P["FEED_TABLE"]
    FAIL_COOLDOWN_S = P["FAIL_COOLDOWN_S"]
    RETRY_RANKING_S = P["RETRY_RANKING_S"]
    # ==========================================================================

    def make_baskets():
        """SECTOR_BASKETS gives each basket's underlyings by name (e.g. "bank", "fin"). A basket entry is "SBIN FUT": the NEAREST future that has not expired, on each day, among the underlyings
        of that sector that have not expired (the master) and are not in INDEX_FUTURES/EXCLUDE. relative() then ranks today's nearest futures against the others in the SAME sector, absolute()
        compares a future with the nearest future of each earlier day, so a stock is only ever compared with its own futures, and a roll never mixes two contracts inside one day."""
        futs = current().master.futs
        today = pd.Timestamp(datetime.now(ZoneInfo("Asia/Kolkata")).date())
        live = {str(u).upper() for u in futs.loc[futs["expiry"] >= today, "TckrSymb"]} - set(INDEX_FUTURES) - {u.upper() for u in EXCLUDE}
        baskets = {}
        for name, unders in SECTOR_BASKETS.items():
            members = [f"{u.upper()} FUT" for u in unders if u.upper() in live]
            if members:
                baskets[name] = members
        return baskets

    BASKETS = make_baskets()
    if not (BASKAVG_SHORT < BASKAVG_LONG and 0 <= REL_SELL["pct_open"] < REL_BUY["pct_open"] <= 100 and 0 <= ABS_SELL["pct_open"] < ABS_BUY["pct_open"] <= 100 and 0 <= REL_BUY["v"] <= 100 and 0 <= ABS_BUY["v"] <= 100
            and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and ((TRAIL_TRIGGER_PCT is None) == (PROFIT_LOCK_PCT is None)) and (TRAIL_TRIGGER_PCT is None or TRAIL_TRIGGER_PCT > 0)
            and (TRAIL_STEP_PCT is None or (TRAIL_TRIGGER_PCT is not None and TRAIL_STEP_PCT > 0))
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)):
        raise ValueError("REL_SELL/ABS_SELL pct_open must be below REL_BUY/ABS_BUY pct_open, all inside 0 to 100 along with the v thresholds, TARGET_PCT and STOP_PCT above 0, "
                         "TRAIL_TRIGGER_PCT and PROFIT_LOCK_PCT must both be None or both set (and TRAIL_TRIGGER_PCT above 0), "
                         "QTY at least 1, ABS_DAYS 0 to 400, EXIT_AT after the window opens, every basket needs at least 3 symbols, "
                         "and TRAIL_STEP_PCT (if set) needs TRAIL_TRIGGER_PCT set too and must be above 0")
    BAR_S = parse_timeframe(SIGNAL_BAR)                               # 300; refuses a size that does not line up with the clock
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

    if USE_V:
        if current().get("straddle") is None:                      # the V of today comes from the straddle series: start it here (it builds the whole day from the candles first, so a late start is fine)
            try:
                straddle_start(STRADDLE_N)
                print(f"{STRATEGY}: the straddle was not running: straddle_start({STRADDLE_N}) sent from here (it needs the candles WITH the options; it is stopped by the end-of-day step)")
            except Exception as e:
                print(f"{STRATEGY}: the straddle could not be started ({e}); the V filters will refuse every entry until it runs: straddle_start({STRADDLE_N}), with the options in the candles")
        else:
            print(f"{STRATEGY}: the straddle is already running, its V is used as it is")

    symbol_of = {}                                                 # token -> the basket entry it trades for
    for name in basket_of:
        info = contract(name[:-4], show=False)                    # "SBIN FUT" -> contract("SBIN") = the nearest future of SBIN
        if info is None:
            print(f"{name}: no such contract, left out of the trading (it still counts in the basket)")
            continue
        symbol_of[int(info["token"])] = name
    pf_track(STRATEGY, list(symbol_of), slots=100)                 # the portfolios it had before (a restart or a paste again); every trade takes the next free slot
    with contextlib.redirect_stdout(io.StringIO()):                # (algo_portfolios prints a table; only the numbers are needed)
        had = algo_portfolios(STRATEGY)
    next_slot = {t: (int(had[had["instrument"] == t]["slot"].max()) + 1 if had is not None and (had["instrument"] == t).any() else 0) for t in symbol_of}

    bars = {t: {} for t in symbol_of}                              # token -> {5-minute bar number: the last 1-minute close inside it}: what the TODAY filter checks
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

    if ABS_DAYS > 0:                                               # read the earlier days of every stock NOW (one read per day file for all of them), not in the candle thread at the first signal
        try:
            for warm_ref in ("open", "vwap"):
                absolute_warm("pct", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS, ref=warm_ref)
            if USE_V:
                absolute_warm("v", LEVEL, sorted({n[:-4] for n in symbol_of.values()}), ABS_DAYS, ref="open")      # the V is the underlying's, as its % change from the open
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for t in symbol_of:                                            # today's candles already built, over the history
        h = last_candles(t, 400, show=False)
        if h is not None:
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(t, m, c)
            if len(h):
                last_px[t] = float(h["Close"].iloc[-1])

    # ---- the TODAY filter: today's highest/lowest close so far -----------------------------------------------------------------------------------
    def day_sides(token):
        """Filter 0: the sides today's SIGNAL_BAR closes allow. {1} (BUY) when the latest bar close is the highest of today so far, {2} (SELL) when it is the lowest, empty when it is neither or when
        there are fewer than 2 bars today (the first one is both). Only today's bars count. Everything is allowed when DAY_EXTREME is False."""
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

    # ---- the filters: against the basket (relative), against the stock's own days (absolute) ------------------------
    def sides_of(strong, weak):
        """{1} (BUY) when `strong` holds, {2} (SELL) when `weak` holds. With DIRECTION_MATCH False, {1, 2} when either one holds. Empty when neither."""
        if DIRECTION_MATCH:
            return ({1} if strong else set()) | ({2} if weak else set())
        return {1, 2} if (strong or weak) else set()

    def under_of(symbol):
        return symbol[:-4]                                         # "SBIN FUT" -> "SBIN": the V is the underlying's

    def ranked_sides(pct, v, buy, sell):
        """`pct`, `v`: no-argument relative()/absolute() calls for pct_open and the straddle V % from open. BUY needs pct_open at least buy["pct_open"]; SELL needs it at or below sell["pct_open"].
        V, when USE_V, is checked per side: BUY needs it at least buy["v"]; SELL needs it at least sell["v"] (its own threshold, or buy["v"] when sell has none)."""
        with exec_ctx:                                             # the call reads the candles table; a listener thread needs the terminal's query context for that
            result = pct()
        if result[1] < 0:                                          # (the second item is -1 when it cannot be told)
            return set()
        p = result.pctile
        if p != p:                                                 # unknown (NaN)
            return set()
        sides = sides_of(p >= buy["pct_open"], p <= sell["pct_open"])
        if not sides:
            return set()
        if USE_V:
            with exec_ctx:
                result = v()
            if result[1] < 0:                                      # cannot tell: no side
                return set()
            v_p = result.pctile
            sides = ({1} if 1 in sides and v_p >= buy["v"] else set()) | ({2} if 2 in sides and v_p >= sell.get("v", buy["v"]) else set())
        return sides

    def basket_avg_sides(symbol):
        """Filter 1: the basket's own AVERAGE % change from the open (across every member). BUY needs it at least BASKAVG_LONG; SELL needs it at or below BASKAVG_SHORT.
        A basket-wide momentum gate, not a per-stock ranking."""
        members = BASKETS[basket_of[symbol]]
        with exec_ctx:                                             # the call reads the candles table; a listener thread needs the terminal's query context for that
            result = relative("pct", LEVEL, symbol, members, ref="open")
        if result[1] < 0:                                          # (the second item is -1 when it cannot be told)
            return set()
        avg = float(np.mean(result[0]))                            # result[0]: every basket member's own % change from the open, right now
        return sides_of(avg >= BASKAVG_LONG, avg <= BASKAVG_SHORT)

    def relative_sides(symbol):
        """Filter 2, relative(): the ranking among the OTHER symbols of ITS basket, for pct_open and the straddle V % from open (if USE_V), against REL_BUY / REL_SELL."""
        members = BASKETS[basket_of[symbol]]
        return ranked_sides(
            pct=lambda: relative("pct", LEVEL, symbol, members, ref="open"),
            v=lambda: relative("v", LEVEL, under_of(symbol), [under_of(m) for m in members], ref="open"),
            buy=REL_BUY, sell=REL_SELL,
        )

    def absolute_sides(symbol):
        """Filter 3, absolute(): the ranking among the same minute of its previous ABS_DAYS trading days, for pct_open and the straddle V % from open (if USE_V), against ABS_BUY / ABS_SELL."""
        return ranked_sides(
            pct=lambda: absolute("pct", LEVEL, symbol, ABS_DAYS, ref="open"),
            v=lambda: absolute("v", LEVEL, under_of(symbol), ABS_DAYS, ref="open"),
            buy=ABS_BUY, sell=ABS_SELL,
        )

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

    def exit_reason(pos, move):
        """Why `move` (the % in the position's favour) should exit now, or None. TRAIL_TRIGGER_PCT off: plain TARGET_PCT / STOP_PCT. On: once `move` reaches TRAIL_TRIGGER_PCT the
        position's own "trailing" flag turns on (sticky) and its lock starts at PROFIT_LOCK_PCT instead of -STOP_PCT, for the rest of its life. TRAIL_STEP_PCT set: from then on, for every
        TRAIL_STEP_PCT set, the lock then continuously follows the move, held at move - TRAIL_STEP_PCT (pos["lock"] only ever goes up, never down)."""
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
        """Target / stop-loss (or trailing profit-lock) of an open position on the price `px` (the same rule as on the candle close)."""
        move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)                 # % in the position's favour (negative = against it)
        why = exit_reason(pos, move)
        if why is not None:
            last_px[token] = px
            leave(token, f"{why} (tick)")

    def on_tick(update, is_replay):
        """Every price tick of a token with an open position (the engine filters the feed; the other ticks never reach Python).
        fo_market_data is a last_by("PF") table: a token's first tick arrives as an added row, every one after that as a
        modified row (same row, updated in place) -- both must be read or only the very first tick of each position is seen."""
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
        position[token] = {"side": side, "entry": last_px[token], "qty": QTY, "pf": a["pf"], "state": "entering", "trailing": False, "lock": None}
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
        `ends_bar` says if this candle completes a bar, so the newest bar counts for the TODAY filter."""
        px = last_px[token]
        pos = position.get(token)
        if pos is not None:
            if pos["state"] == "open":
                move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)         # % in the position's favour (negative = against it)
                why = exit_reason(pos, move)
                if why is not None:
                    leave(token, why)
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
            sides = today & basket_avg_sides(symbol_of[token])     # 1. basket avg: the basket's own average % move from the open
            if not sides:
                count("refused by basket avg")
                return
            sides &= relative_sides(symbol_of[token])              # 2. relative: extreme in its basket (and on which side)
            if not sides:
                count("refused by basket")
                return
            if ABS_DAYS > 0:
                sides &= absolute_sides(symbol_of[token])          # 3. absolute: extreme against its own days, on the same side
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
                ends_bar = (epoch_s(minute) + 60) % BAR_S == 0                              # this candle completes a bar of SIGNAL_BAR
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
        print("   signals: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none yet"))

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY, pause=False)                           # square up + unsubscribe every portfolio (no pause step), wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["directional_status"], shared["directional_exit_all"], shared["directional_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_directional()
