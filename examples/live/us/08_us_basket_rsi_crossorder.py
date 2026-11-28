# US BASKET + 5-MINUTE RSI + CrossOrder: the US version of 07. A plain script (functions, no class). Paste in the US server's terminal after the services are up (init.py).
#
#   The basket        BASKETS = {"tech": [...], "chips": [...], "other": [...]}: named sector baskets. A symbol is ranked against the OTHERS of its own basket only (each saved in baskets.txt too).
#   The filter        a symbol may be traded only while its % change from the OPEN stands in the MIDDLE of its basket (REL_ZONE["pct_open"], Q2 to Q3), AND its % from the VWAP,
#                     among that same basket, sits on the side matching the RSI zone -- ABOVE REL_ZONE["pct_vwap"]'s high for OVERBOUGHT (SELL), BELOW its low for OVERSOLD (BUY).
#   The own history   AND, against the same minute on its previous ABS_DAYS (22) trading days: its own % from the OPEN, now, must sit INSIDE ABS_ZONE["pct_open"] (Q2 to Q3);
#                     its own % from the VWAP, now, must sit on the side matching the RSI zone -- ABOVE ABS_ZONE["pct_vwap"]'s high for OVERBOUGHT (SELL), BELOW its low for OVERSOLD (BUY).
#                     absolute() reads those days from the saved candle files, one stock and one day at a time. With fewer than half of the days in the files there is no trade.
#   The signal        5-minute RSI, made from the 1-minute candles: it ENTERS oversold (< RSI_LOW) -> BUY, or overbought (> RSI_HIGH) -> SELL. One position per symbol at a time.
#   The history       the RSI does not start from nothing at the open: the 5-minute closes of the last HIST_DAYS saved days (candles_<day>.parquet in the market-data / EOD folders)
#                     come first, then today's candles. So the RSI at 09:35 is computed through yesterday's prices, overnight gap included. No files = it says so and starts from today.
#   The exit          the script's own target and stop-loss on the price: +TARGET_PCT in its favour, or -STOP_PCT against it (checked on every 1-minute close). TRAIL_TRIGGER_PCT /
#                     PROFIT_LOCK_PCT (both None, or both set): once the move reaches TRAIL_TRIGGER_PCT the stop shifts, for the rest of that position's life, from -STOP_PCT to PROFIT_LOCK_PCT.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:55 New York time, 5 minutes before the 16:00 close): the automation is stopped the way algo_stop does it: every portfolio is paused and squared up, a minute
#                     later unsubscribed (so the plugin can close first), and the script stops. No entry is made from then on. It is checked on every batch of 1-minute candles, so it
#                     starts within a minute of EXIT_AT. It refuses while sending is OFF (it says so; then run meanrevert_stop() by hand).
#   The order         ENGINE["strategyname"] = "LimitOrder" (the engine plugin that rests a limit order and MODIFYs it in place on later params, never CrossOrder's immediate fill):
#                     {"strategyname": "LimitOrder", "TOKEN": <contract>, "SIDE": 1|2, "QTY": n lots, "PRICE": <int>, "SERVERCODE": ..., "ACCOUNT": "PRO", "ALGOID": "1", "MAX_TRIES": 5}
#   The spread gate   ENTRY and EXIT both wait for the live spread (SPREAD_MAX, from fo_market_data's b0p/a0p/ltp) to tighten before sending anything -- see spread_ok()/the
#                     pending_entry/pending_exit state machine below. An ENTRY that never tightens within SPREAD_WAIT_S is dropped (no trade); an EXIT fires anyway once it times out
#                     (a stop-loss/target must never just hang unsent). Once resting, an order is "chased": every CHASE_EVERY_S, if the spread is still OK and the best price has moved,
#                     an "apply" (not a new subscribe) re-prices it in place, up to CHASE_TRIES times; after that it is paused (cancelled) and, for an entry, unsubscribed -- nothing more.
#                     An EXIT is a brand-new portfolio (opposite side, NOT the original entry's `squareup`): the original entry pf is left open, now offset by this new one. Its QTY is
#                     the REAL filled amount read from the `orders_trade` table (token, buy_sell, fill_quantity, fill_price, PF), never the original target -- so a partially filled
#                     entry is still closed exactly, not over- or under-closed. The book gives the portfolio numbers; each stock may use up to 100 a day.
#                     CHECK on the US server before real money: SERVERCODE and ACCOUNT are India's values.
#
# EVERYTHING is inside the function run_meanrevert() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of
# names shared by every script you paste, so top-level `rsi` or `closes` would overwrite another running script's. The only names left in the terminal start with meanrevert:
#   meanrevert_status()   meanrevert_exit_all()   meanrevert_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).


def run_meanrevert():
    import contextlib
    import glob
    import io
    import os
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
    from algo.ui.filters import PF_PER_USER             # orders_trade's own PF column is wire-level: USER * PF_PER_USER + the book's pf (31002 = user 3, book pf 1002)

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
    # Every value below is read from params.ini in this folder, section [meanrevert].
    # Change a number there, not here; this block only names what the script needs.
    PARAMS_SECTION = "meanrevert"
    P = _params(PARAMS_SECTION)
    
    USER = P["USER"]
    STRATEGY = P["STRATEGY"]
    BASKETS = P["BASKETS"]
    TRADE_AS = P["TRADE_AS"]
    LEVEL = P["LEVEL"]
    REL_ZONE = P["REL_ZONE"]
    ABS_DAYS = P["ABS_DAYS"]
    ABS_ZONE = P["ABS_ZONE"]
    RSI_BAR = P["RSI_BAR"]
    RSI_N = P["RSI_N"]
    RSI_HIGH = P["RSI_HIGH"]
    RSI_LOW = P["RSI_LOW"]
    RSI_LIVE = P["RSI_LIVE"]
    ACT_ON_BAR_END = P["ACT_ON_BAR_END"]
    HIST_DAYS = P["HIST_DAYS"]
    TARGET_PCT = P["TARGET_PCT"]
    STOP_PCT = P["STOP_PCT"]
    TRAIL_TRIGGER_PCT = P["TRAIL_TRIGGER_PCT"]
    PROFIT_LOCK_PCT = P["PROFIT_LOCK_PCT"]
    TRAIL_STEP_PCT = P["TRAIL_STEP_PCT"]
    ONE_ENTRY_PER_STOCK = P["ONE_ENTRY_PER_STOCK"]
    QTY = P["QTY"]
    ENTRY_VALUE = P["ENTRY_VALUE"]
    ENGINE = P["ENGINE"]
    WINDOW = (_ptime(P, "WINDOW_START"), _ptime(P, "WINDOW_END"))
    EXIT_AT = _ptime(P, "EXIT_AT")
    MAX_PORTFOLIOS = P["MAX_PORTFOLIOS"]
    UNSUB_AFTER_S = P["UNSUB_AFTER_S"]
    EXIT_SETTLE_TRIES = P["EXIT_SETTLE_TRIES"]
    TICK_EXITS = P["TICK_EXITS"]
    FEED_TABLE = P["FEED_TABLE"]
    SPREAD_MAX = P["SPREAD_MAX"]
    SPREAD_WAIT_S = P["SPREAD_WAIT_S"]
    CHASE_TRIES = P["CHASE_TRIES"]
    CHASE_EVERY_S = P["CHASE_EVERY_S"]
    FAIL_COOLDOWN_S = P["FAIL_COOLDOWN_S"]
    RETRY_RANKING_S = P["RETRY_RANKING_S"]
    # ==========================================================================
    REFS = ("pct_open", "pct_vwap")
    if not (all(0 <= ZONE[ref][0] < ZONE[ref][1] <= 100 for ZONE in (REL_ZONE, ABS_ZONE) for ref in REFS)
            and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and (ENTRY_VALUE is None or ENTRY_VALUE > 0) and HIST_DAYS >= 0 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and ((TRAIL_TRIGGER_PCT is None) == (PROFIT_LOCK_PCT is None)) and (TRAIL_TRIGGER_PCT is None or TRAIL_TRIGGER_PCT > 0)
            and (TRAIL_STEP_PCT is None or (TRAIL_TRIGGER_PCT is not None and TRAIL_STEP_PCT > 0))
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)
            and SPREAD_MAX > 0 and SPREAD_WAIT_S >= 0 and isinstance(CHASE_TRIES, int) and CHASE_TRIES >= 0 and CHASE_EVERY_S > 0):
        raise ValueError("every (low, high) band in REL_ZONE/ABS_ZONE must be inside 0 to 100 with low below high, TARGET_PCT and STOP_PCT above 0, "
                         "TRAIL_TRIGGER_PCT and PROFIT_LOCK_PCT must both be None or both set (and TRAIL_TRIGGER_PCT above 0), "
                         "QTY at least 1, HIST_DAYS 0 or more, ABS_DAYS 0 to 400, EXIT_AT after the window opens, every basket needs at least 3 symbols, "
                         "SPREAD_MAX above 0, SPREAD_WAIT_S 0 or more, CHASE_TRIES a whole number 0 or more, CHASE_EVERY_S above 0, "
                         "and TRAIL_STEP_PCT (if set) needs TRAIL_TRIGGER_PCT set too and must be above 0")
    BAR_S = parse_timeframe(RSI_BAR)                               # 300; refuses a size that does not line up with the clock
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

    symbol_of = {}                                                 # token -> the basket entry it trades for
    for name in basket_of:
        info = contract(TRADE_AS.get(name, name), show=False)
        if info is None:
            print(f"{name}: no such contract, left out of the trading (it still counts in the basket)")
            continue
        symbol_of[int(info["token"])] = name
    token_of = {name: token for token, name in symbol_of.items()}
    pf_track(STRATEGY, list(symbol_of), slots=100)                 # the portfolios it had before (a restart or a paste again); every trade takes the next free slot
    with contextlib.redirect_stdout(io.StringIO()):                # (algo_portfolios prints a table; only the numbers are needed)
        had = algo_portfolios(STRATEGY)
    next_slot = {t: (int(had[had["instrument"] == t]["slot"].max()) + 1 if had is not None and (had["instrument"] == t).any() else 0) for t in symbol_of}

    bars = {t: {} for t in symbol_of}                              # token -> {5-minute bar number: the last 1-minute close inside it}: what the RSI is computed from
    last_px = {}                                                   # token -> the latest 1-minute close (candle)
    last_tick = {}                                                 # token -> the latest ltp from fo_market_data (tick, not candle): what spread_ok() compares bid/ask against
    best = {}                                                      # token -> (bid, ask), the latest best quotes from fo_market_data's b0p/a0p
    filled = {}                                                    # pf -> cumulative fill_quantity from orders_trade, for every pf this script holds: the REAL filled size
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting (qty is the TARGET; filled[pf] is the real size)
    pending_entry = {}                                             # token -> {side, deadline}: a signal passed, waiting for SPREAD_MAX before the LimitOrder is actually sent
    pending_exit = {}                                              # token -> {why, qty, deadline}: an exit was triggered, waiting for SPREAD_MAX (falls back to firing anyway past the deadline);
                                                                   # qty is the real filled amount of the entry pf at the moment it was armed, not the original target
    chase = {}                                                     # pf -> {token, side, qty, price, tries, kind}: a resting LimitOrder being re-priced ("entry" or "exit")
    traded_in_zone = {}                                            # token -> the zone it was traded in: no second trade until the RSI has left it
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

    zone_side = {"OVERSOLD": 1, "OVERBOUGHT": 2}                   # 1 = buy, 2 = sell

    def epoch_s(minute):
        """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in New York (its offsets are whole hours)."""
        return int(pd.Timestamp(minute).timestamp())

    def add_close(token, minute, close):
        b = bars[token]
        b[epoch_s(minute) // BAR_S] = float(close)
        for old in sorted(b)[:-600]:
            del b[old]

    def closes(token):
        """The bar closes, oldest first. The newest bar is still forming, so it is left out when RSI_LIVE is False."""
        keys = sorted(bars[token])
        return [bars[token][k] for k in (keys if RSI_LIVE else keys[:-1])]

    # ---- the history: earlier days' 1-minute closes -> 5-minute closes, before today's -------------------------------------------------------
    def load_history():
        """Seed `bars` from the last HIST_DAYS saved days (candles_<yyyy-mm-dd>.parquet) before today. Returns the days used."""
        if HIST_DAYS <= 0 or not symbol_of:
            return []
        try:
            import pyarrow.parquet as pq
        except ImportError:
            print("history: pyarrow is not installed, the RSI starts from today's candles")
            return []
        paths = current().paths
        folders = [d for d in (paths.saved, paths.market_data) if d and os.path.isdir(d)]
        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
        files = {}
        for d in folders:
            for f in sorted(glob.glob(os.path.join(d, "candles_*.parquet"))):
                day = os.path.basename(f)[len("candles_"):-len(".parquet")]
                if len(day) == 10 and day[4] == "-" and day < today:           # ISO days; today's own candles come from the live table
                    files.setdefault(day, f)
        used = sorted(files)[-HIST_DAYS:]
        if not used:
            print(f"history: no candles_<day>.parquet before {today} in {', '.join(folders) or 'any folder'}: the RSI starts from today's candles")
            return []
        names = sorted(set(symbol_of.values()))
        loaded = []
        for day in used:
            try:
                df = pq.read_table(files[day], columns=["Minute", "Symbol", "Close"], filters=[("Symbol", "in", names)]).to_pandas()
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
            print("history: none of the earlier days could be used, the RSI starts from today's candles")
            return []
        print(f"history: {len(loaded)} earlier day(s) ({', '.join(loaded)}) seed the {RSI_BAR} RSI of {sum(1 for t in bars if bars[t])} of {len(symbol_of)} stocks")
        return loaded

    load_history()
    if ABS_DAYS > 0:                                               # read the earlier days of every stock NOW (one read per day file for all of them), not in the candle thread at the first signal
        try:
            for warm_ref in ("open", "vwap"):
                absolute_warm("pct", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS, ref=warm_ref)
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for t in symbol_of:                                            # today's candles already built, over the history
        h = last_candles(t, 400, show=False)
        if h is not None:
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(t, m, c)
            if len(h):
                last_px[t] = float(h["Close"].iloc[-1])

    # ---- the indicator: Wilder's RSI ------------------------------------------------------------------------------------------------
    def rsi(values, n=RSI_N):
        """RSI of the last closes, or None until there are n + 1 of them.
        Wilder smoothing written as the weighted sum it is, instead of a Python loop over the bars: it is an EWMA with
        alpha = 1/n, so each step multiplies the running average by (n - 1) / n and adds the newest move / n. The bar k
        steps after the seed therefore carries ((n - 1) / n) ** (steps - k) / n, and the seed itself carries
        ((n - 1) / n) ** steps. Same numbers to the last bit as the loop; about five times less work, which matters
        because this runs once per symbol per bar on Deephaven's update thread."""
        v = np.asarray(values, dtype=float)
        if v.size <= n:
            return None
        d = np.diff(v)
        up, down = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
        decay = (n - 1) / n
        steps = v.size - 1 - n                                     # smoothing steps after the seed; 0 when there are exactly n + 1 closes
        w = decay ** np.arange(steps - 1, -1, -1) / n              # each post-seed move's weight, oldest first: the newest carries 1/n, the one before it decay/n ...
        avg_up = up[:n].mean() * decay ** steps + up[n:] @ w
        avg_down = down[:n].mean() * decay ** steps + down[n:] @ w
        if avg_down == 0:
            return 100.0 if avg_up > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + avg_up / avg_down)

    def zone_of(value):
        return None if value is None else "OVERBOUGHT" if value > RSI_HIGH else "OVERSOLD" if value < RSI_LOW else None

    # ---- the filters: the middle of the basket (relative), the middle of its own days (absolute) --------------------------------------
    def relative_sides(symbol, zone):
        """True when the symbol's % from the open ranks inside its REL_ZONE band, among ITS basket (the other symbols of it), AND its % from the VWAP ranks on the side
        matching the zone, among that same basket: OVERBOUGHT (SELL) needs it ABOVE the band's high (> 75), OVERSOLD (BUY) needs it BELOW the band's low (< 25)."""
        with exec_ctx:                                             # relative() reads the candles table; a listener thread needs the terminal's query context for that
            open_result = relative("pct", LEVEL, symbol, BASKETS[basket_of[symbol]], ref="open")
        if not (open_result[1] >= 0 and REL_ZONE["pct_open"][0] <= open_result.pctile <= REL_ZONE["pct_open"][1]):      # (the second item is -1 when it cannot be told)
            return False
        with exec_ctx:
            vwap_result = relative("pct", LEVEL, symbol, BASKETS[basket_of[symbol]], ref="vwap")
        if vwap_result[1] < 0:
            return False
        return vwap_result.pctile > REL_ZONE["pct_vwap"][1] if zone == "OVERBOUGHT" else vwap_result.pctile < REL_ZONE["pct_vwap"][0]

    def absolute_sides(symbol, zone):
        """True when the symbol's % from the open, now, ranks INSIDE its ABS_ZONE band (Q2-Q3) against the same minute on its previous ABS_DAYS trading days,
        AND its % from the VWAP ranks on the side matching the zone: OVERBOUGHT (SELL) needs it ABOVE the band's high (> 75), OVERSOLD (BUY) needs it BELOW
        the band's low (< 25). absolute() answers idx = -1 (cannot tell) when fewer than half of those days are in the saved files; that is no trade."""
        with exec_ctx:                                             # absolute() reads today's candles table once, and the saved day files
            open_result = absolute("pct", LEVEL, symbol, ABS_DAYS, ref="open")
        if open_result[1] < 0 or not (ABS_ZONE["pct_open"][0] <= open_result.pctile <= ABS_ZONE["pct_open"][1]):
            return False
        with exec_ctx:
            vwap_result = absolute("pct", LEVEL, symbol, ABS_DAYS, ref="vwap")
        if vwap_result[1] < 0:
            return False
        return vwap_result.pctile > ABS_ZONE["pct_vwap"][1] if zone == "OVERBOUGHT" else vwap_result.pctile < ABS_ZONE["pct_vwap"][0]

    # ---- the orders ------------------------------------------------------------------------------------------------------------------
    def in_window():
        now = datetime.now(ZoneInfo("America/New_York")).time()
        return WINDOW[0] <= now <= WINDOW[1] and now < EXIT_AT         # no entry once the time exit has come, whatever WINDOW says

    def past_exit():
        return datetime.now(ZoneInfo("America/New_York")).time() >= EXIT_AT

    def limit_order(token, side, qty, price):
        return {**ENGINE, "TOKEN": int(token), "SIDE": int(side), "QTY": int(qty), "PRICE": int(price)}

    def send_place(pf, token, side, qty, price):
        """The one subscribe that starts a LimitOrder plugin instance: its own fields, sent as they are. A Future with the reply (an ACK that the order is working, not a fill)."""
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "subscribe", limit_order(token, side, qty, price), user=USER, client=STRATEGY)       # honours this strategy's own switch

    def send_modify(pf, qty, price):
        """Re-price the resting order in place: the plugin's onParamsUpdate MODIFYs it (no cancel/restart). qty is always the ORIGINAL target -- the plugin works out the
        remaining amount itself from its own fill count. Used only while chasing."""
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "apply", {"QTY": int(qty), "PRICE": int(price), "MAX_TRIES": ENGINE["MAX_TRIES"]}, user=USER, client=STRATEGY)

    def send_cancel(pf):
        """Cancel a resting order: the plugin's only cancel path (PauseUnPauseEvent). Used only once CHASE_TRIES is used up."""
        svc = current().get("nats") or NO_SERVICE
        return svc.pause(pf, user=USER, client=STRATEGY)

    def best_price(token, order_side):
        b, a = best.get(token, (0, 0))
        return a if order_side == 1 else b

    def spread_ok(token, order_side):
        """True once the live spread is tight enough to fire THIS side of an order: best ask vs ltp for a BUY leg, ltp vs best bid for a SELL leg (today's divisor applied).
        False (keep waiting) while there is no live quote yet."""
        ref = best_price(token, order_side)
        ltp = last_tick.get(token, 0)
        div = divisor_of(token)
        if ref <= 0 or ltp <= 0 or not div:
            return False
        return abs(ref - ltp) / float(div) <= SPREAD_MAX

    live = set()                                                   # portfolios this script holds in the engine: open positions, and closed ones waiting for their unsubscribe
    pf_side = {}                                                   # pf -> the side that OPENS exposure on it (1 BUY, 2 SELL). on_trade nets every fill against it, so a
                                                                   # closing fill (a console "Square off all", or the LimitOrder plugin's own SquareUpEvent order) SUBTRACTS.

    def pf_state(pf):
        """The portfolio as the NATS bridge last confirmed it: PfState with .squared / .paused / .by ("operator" for the console), or None if nothing is known yet.
        Used to defer to the operator -- a console Pause all / Square off all must not be undone or doubled up by this script."""
        try:
            return current().get("nats").states.get(USER, pf)
        except Exception:
            return None
    at_limit_said = []                                             # the "at the limit" message is printed once until a slot frees

    def release_exit(pf, token, qty, attempt=1):
        """The exit portfolio, UNSUB_AFTER_S after its ack. An unsubscribe destroys the plugin instance and any order still working with it, so this only lets go once
        the exit has actually filled. While the fills are short it waits again (up to EXIT_SETTLE_TRIES waits in all); if they never come it cancels what is left,
        gives the slot back, and puts the position back to "open" with the amount still uncovered, so the next signal or tick exit sends a fresh exit. Before this, a
        forced exit that never filled was unsubscribed anyway and the entry's shares were left open with nothing watching them until the time exit."""
        got = filled.get(pf, 0)
        if qty <= 0 or got >= qty:                                 # done: the position really is closed
            position.pop(token, None)
            maybe_untrack(token)
            if UNSUB_AFTER_S is None:
                live.discard(pf)                                   # the owner asked for the slot to be held until the stop
            else:
                release(pf, token)
            return
        if attempt < EXIT_SETTLE_TRIES:
            print(f"{symbol_of[token]}: exit PF {pf} has filled {got} of {qty}; waiting before its unsubscribe ({attempt} of {EXIT_SETTLE_TRIES})")
            timer = threading.Timer(UNSUB_AFTER_S if UNSUB_AFTER_S is not None else 60.0,
                                    release_exit, (pf, token, qty, attempt + 1))
            timer.daemon = True
            timer.start()
            return
        chase.pop(pf, None)
        send_cancel(pf)                                            # whatever is still resting goes, then the slot
        if UNSUB_AFTER_S is None:
            live.discard(pf)
        else:
            release(pf, token)
        left = qty - got
        pos = position.get(token)
        if pos is not None:
            pos["qty"], pos["state"] = left, "open"                # STILL OPEN: armed again on the next signal or tick exit
            pos["closed"] = pos.get("closed", 0) + got             # so the next exit asks for what is left, not for the whole entry again
            pos.pop("exit_pf", None)
            pos.pop("exit_qty", None)
            print(f"{symbol_of[token]}: exit PF {pf} filled only {got} of {qty} after {EXIT_SETTLE_TRIES} waits; {left} is STILL OPEN and will be exited again")
        else:
            print(f"{STRATEGY}: exit PF {pf} filled {got} of {qty} and the position is already gone; {left} may still be open -- check the book")

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
            arm_exit(token, f"{why} (tick)")

    def chase_check(pf):
        """One check of a resting order, at most one re-price ("apply") every CHASE_EVERY_S: re-price it if the spread is still ok AND the price moved since the
        last one sent; cancel (and, for an entry, unsubscribe) once CHASE_TRIES is used up. An EXIT is chased for real: it is a fresh `subscribe` on its own portfolio, so
        that plugin instance is in its ENTRY state and onParamsUpdate MODIFYs its order exactly like an entry's. (Only a `squareup` order cannot be modified, and these
        scripts never send one.) A FORCED exit does not wait for the spread -- it was forced because the spread never tightened."""
        c = chase.get(pf)
        if c is None:
            return
        if time.time() - c["last"] < CHASE_EVERY_S:
            return                                                  # on_tick calls this on EVERY tick of the token, so without this gate all CHASE_TRIES re-prices
                                                                    # could be spent inside a fraction of a second on a fast-moving stock
        st = pf_state(pf)
        if st is not None and (st.paused or st.squared):            # the LimitOrder plugin's PauseUnPauseEvent(False) is a NO-OP ("staying paused until params are
            chase.pop(pf, None)                                     # sent again"), so an "apply" from the chase would UN-PAUSE it and re-arm the entry the
            print(f"{STRATEGY}: PF {pf} {'squared up' if st.squared else 'paused'} by {st.by}; chase stopped")
            maybe_untrack(c["token"])                               # operator just cancelled. Stop chasing instead.
            return
        if filled.get(pf, 0) >= c["qty"]:
            chase.pop(pf, None)                                     # fully filled: nothing left to re-price or cancel
            maybe_untrack(c["token"])
            return
        if c["tries"] >= CHASE_TRIES:
            chase.pop(pf, None)
            send_cancel(pf)
            got = filled.get(pf, 0)
            print(f"{symbol_of[c['token']]}: {c['kind']} chase used up {CHASE_TRIES} tries, PF {pf} cancelled (filled {got} of {c['qty']})")
            if c["kind"] == "entry":
                if got > 0:                                        # partially filled before the cancel: keep tracking it as an open position of that size, don't orphan it
                    pos = position.get(c["token"])
                    if pos is not None:
                        pos["qty"] = got
                        pos["state"] = "open"
                else:
                    release(pf, c["token"])
                    entered.discard(c["token"])
                    last_fail[c["token"]] = time.time()
                    position.pop(c["token"], None)
                    maybe_untrack(c["token"])
            return
        if not spread_ok(c["token"], c["side"]) and not c["forced"]:
            return                                                # spread went bad again: wait, this check doesn't count as a try. A forced exit is exempt: its
                                                                  # spread is bad by definition, so waiting for it would stall the chase of the one order that
                                                                  # most needs to be re-priced -- a stop-loss already past its level
        if not spread_ok(c["token"], c["side"]) and c["tries"] == 0 and c["kind"] == "exit":
            print(f"{symbol_of[c['token']]}: chasing the forced exit on PF {pf} with the spread still wide")
        price = best_price(c["token"], c["side"])
        if price == c["price"]:
            return                                                # unchanged: wait, doesn't count as a try either
        send_modify(pf, c["qty"], price)
        c["price"] = price
        c["tries"] += 1
        c["last"] = time.time()

    def maybe_untrack(token):
        """Stop following a token's ticks once nothing still needs them: no pending entry/exit, no open position, no live chase."""
        if token in pending_entry or token in pending_exit or token in position:
            return
        if any(c["token"] == token for c in chase.values()):
            return
        tick_track(token, False)

    def fire_entry(token):
        """The signal's spread finally tightened (or was already tight): send the real entry now. True when sent."""
        side = pending_entry.pop(token, {}).get("side")
        if side is None:
            return False
        if len(live) >= MAX_PORTFOLIOS:
            if not at_limit_said:
                at_limit_said.append(True)
                print(f"{STRATEGY}: {len(live)} portfolios held (MAX_PORTFOLIOS {MAX_PORTFOLIOS}); no new entry until an exit has been unsubscribed")
            maybe_untrack(token)
            return False
        if next_slot[token] >= 100:
            print(f"{symbol_of[token]}: all 100 portfolios of the day are used, no trade")
            maybe_untrack(token)
            return False
        qty = QTY if ENTRY_VALUE is None else int(ENTRY_VALUE / last_px[token])         # by value: whole units for ENTRY_VALUE at the last close
        if qty < 1:
            print(f"{symbol_of[token]}: ENTRY_VALUE {ENTRY_VALUE:g} buys less than one unit at {last_px[token]:g}, no trade")
            maybe_untrack(token)
            return False
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])       # the BOOK gives a NEW portfolio number (None while OFF or unticked)
        if not a:
            maybe_untrack(token)
            return False
        next_slot[token] += 1
        pf = a["pf"]
        price = best_price(token, side)
        live.add(pf)
        pf_side[pf] = side
        entered.add(token)
        position[token] = {"side": side, "entry": last_px[token], "qty": qty, "pf": pf, "state": "entering", "trailing": False, "lock": None}
        chase[pf] = {"token": token, "side": side, "qty": qty, "price": price, "tries": 0, "kind": "entry",
                     "last": time.time(), "forced": False}      # `last`: CHASE_EVERY_S is measured from the last re-price, not from every tick
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {qty} lot(s) at {price} (LimitOrder) on PF {pf}")
        send_place(pf, token, side, qty, price).add_done_callback(lambda f: settled(token, f))
        return True

    def arm_entry(token, side):
        if token in pending_entry or token in position:
            return
        pending_entry[token] = {"side": side, "deadline": time.time() + SPREAD_WAIT_S}
        tick_track(token, True)

    def fire_exit(token, qty, forced=False):
        """The exit's spread tightened, or it timed out and must fire anyway: open a NEW portfolio (opposite side), never the original entry's squareup."""
        pos = position.get(token)
        if pos is None:
            return
        closing_side = 2 if pos["side"] == 1 else 1
        price = best_price(token, closing_side)
        if price <= 0:
            pending_exit[token] = {"why": "no quote yet", "qty": qty, "deadline": time.time() + SPREAD_WAIT_S}      # can't even force it without a quote: try again shortly
            return
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])
        if not a:
            pending_exit[token] = {"why": "no portfolio yet", "qty": qty, "deadline": time.time() + SPREAD_WAIT_S}
            return
        next_slot[token] += 1
        pf = a["pf"]
        live.add(pf)
        pf_side[pf] = closing_side
        pos["state"] = "exiting"
        chase[pf] = {"token": token, "side": closing_side, "qty": qty, "price": price, "tries": 0, "kind": "exit",
                     "last": time.time(), "forced": bool(forced)}   # a forced exit chases even while the spread is bad: that is why it was forced
        print(f"{symbol_of[token]}: closing {qty} on NEW PF {pf} at {price}{' (forced, spread never tightened)' if forced else ''}")
        send_place(pf, token, closing_side, qty, price).add_done_callback(lambda f: settled_exit(token, pf, f, qty))

    def arm_exit(token, why):
        if token in pending_exit:
            return
        pos = position.get(token)
        if pos is None or pos["state"] != "open":
            return
        st = pf_state(pos["pf"])
        if st is not None and (st.squared or st.paused):            # the operator closed or froze this portfolio from the console: an exit of ours would open a
            print(f"{symbol_of[token]}: {why}, but PF {pos['pf']} was "   # brand-new opposite position against a book that is already flat
                  f"{'squared up' if st.squared else 'paused'} by {st.by}; dropping it, no exit sent")
            position.pop(token, None)
            maybe_untrack(token)
            return
        qty_open = filled.get(pos["pf"], 0) - pos.get("closed", 0)      # the entry's own fills, less whatever an earlier exit of this position already closed
        if qty_open <= 0:
            print(f"{symbol_of[token]}: {why}, but PF {pos['pf']} shows no confirmed fill yet; nothing to close")
            position.pop(token, None)
            maybe_untrack(token)
            return
        pending_exit[token] = {"why": why, "qty": qty_open, "deadline": time.time() + SPREAD_WAIT_S}
        print(f"{symbol_of[token]}: {why}, waiting up to {SPREAD_WAIT_S}s for the spread before closing {qty_open}")

    def on_trade(update, is_replay):
        """orders_trade: token, buy_sell, fill_quantity, fill_price, response_order_number, PF, activity_time. Its PF is wire-level (USER * PF_PER_USER + the book's pf,
        e.g. 31002 for user 3's book pf 1002) -- converted back to the book's own pf before matching, since `live`/`chase`/`filled` are all keyed by the book's pf everywhere
        else. Only this strategy's own live pfs are kept."""
        ch = update.added(["PF", "buy_sell", "fill_quantity"])
        if ch is None:
            return
        for wire_pf, bs, qty in zip(ch["PF"], ch["buy_sell"], ch["fill_quantity"]):
            pf = int(wire_pf) - USER * PF_PER_USER
            if pf in live:
                q = int(qty)
                opens = pf_side.get(pf)                             # fills are NETTED, not summed: a fill on the opening side adds, one on the other side
                filled[pf] = filled.get(pf, 0) + (q if opens is None or int(bs) == opens else -q)   # (a squareup) takes away, so `filled` is the real live size

    def start_trade_listener():
        """watch() checks app.tables/app.ns, which orders_trade may never be registered in even once it exists as a bare console
        global (the same gap tick_start() already works around for FEED_TABLE via globals().get(...)) -- so this reads it the same
        direct way instead of going through watch()."""
        from deephaven.table_listener import listen
        table = globals().get("orders_trade")
        if table is None:
            return None
        old = current().shared.setdefault("trade_handles", {}).pop(STRATEGY, None)
        if old is not None:
            old.stop()
        handle = listen(table, on_trade, do_replay=False)
        current().shared["trade_handles"][STRATEGY] = handle
        return handle

    def on_tick(update, is_replay):
        """Every price tick of a token being followed (an open position, a pending entry, or a chasing order; the engine filters the feed, other ticks never reach Python).
        fo_market_data is a last_by("PF") table: a token's first tick arrives as an added row, every one after that as a modified row (same row, updated in place) -- both
        must be read or only the very first tick of each position is seen."""
        for ch in (update.added(["token", "ltp", "b0p", "a0p"]), update.modified(["token", "ltp", "b0p", "a0p"])):
            if ch is None:
                continue
            for tok, ltp, b, a in zip(ch["token"], ch["ltp"], ch["b0p"], ch["a0p"]):
                tok = int(tok)
                if ltp > 0:
                    last_tick[tok] = float(ltp)
                if b > 0 and a > 0:
                    best[tok] = (float(b), float(a))
                pe = pending_entry.get(tok)
                if pe is not None:
                    if spread_ok(tok, pe["side"]):
                        fire_entry(tok)
                    elif time.time() >= pe["deadline"]:
                        pending_entry.pop(tok, None)
                        count("entry spread never tightened")
                        maybe_untrack(tok)
                pos = position.get(tok)
                if pos is not None and pos["state"] == "open" and ltp > 0:
                    div = divisor_of(tok)
                    if div:
                        tick_exit(tok, pos, float(ltp) / float(div))
                px = pending_exit.get(tok)
                if px is not None:
                    closing_side = 2 if pos["side"] == 1 else 1 if pos is not None else None
                    if closing_side is not None and spread_ok(tok, closing_side):
                        pending_exit.pop(tok, None)
                        fire_exit(tok, px["qty"])
                    elif time.time() >= px["deadline"]:
                        pending_exit.pop(tok, None)
                        fire_exit(tok, px["qty"], forced=True)
        for pf in list(chase):
            chase_check(pf)

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
        """Ticks are no longer optional (TICK_EXITS only adds the candle-close target/stop check on top): every entry and exit waits for the spread and chases its order on
        them, so without this the whole spread-gate/chase machinery has nothing driving it."""
        try:
            feed = globals().get(FEED_TABLE)
            if feed is None:
                raise RuntimeError(f"no table {FEED_TABLE!r} in this session")
            from deephaven import dtypes, input_table
            from deephaven.table_listener import listen
            keys = input_table(col_defs={"token": dtypes.int32}, key_cols="token")
            ticks = feed.where_in(keys, "token").where("ltp > 0").view(["token", "ltp", "b0p", "a0p"])    # only the followed tokens' ticks come through
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
            print(f"{STRATEGY}: spread gate / chase / {'tick exits, ' if TICK_EXITS else ''}live on {FEED_TABLE} (candle close is the backup)")
        except Exception as e:
            tick_state.clear()
            print(f"{STRATEGY}: live ticks not available ({e}); spread gate / chase cannot run, only the candle-close checks will")


    def settled(token, future):
        """The entry's own pf answered (called from the NATS thread)."""
        try:
            ok = bool(future.result().ok)
            why = "" if ok else future.result().status
        except Exception as e:
            ok, why = False, repr(e)
        pos = position.get(token)
        if ok and pos is not None:
            pos["state"] = "open"
        else:
            if pos is not None:
                live.discard(pos["pf"])
                chase.pop(pos["pf"], None)
            position.pop(token, None)
            maybe_untrack(token)
            last_fail[token] = time.time()
            entered.discard(token)                                 # nothing was opened: this stock may still be entered
            print(f"{symbol_of[token]}: entry FAILED ({why}); nothing is open")

    def settled_exit(token, pf, future, qty=0):
        """The exit's own pf answered (called from the NATS thread)."""
        try:
            ok = bool(future.result().ok)
            why = "" if ok else future.result().status
        except Exception as e:
            ok, why = False, repr(e)
        if ok:
            pos = position.get(token)
            if pos is not None:                                    # an ack only says the order is WORKING. The position is NOT dropped here any more: it stays in
                pos["state"] = "exiting"                           # "exiting" until the fills are in, so an exit that half-fills or never fills cannot be forgotten
                pos["exit_pf"], pos["exit_qty"] = pf, qty          # while the entry portfolio still holds the shares
            timer = threading.Timer(UNSUB_AFTER_S if UNSUB_AFTER_S is not None else 60.0,
                                    release_exit, (pf, token, qty, 1))              # checks the fills before it lets the portfolio go
            timer.daemon = True
            timer.start()
        else:
            live.discard(pf)
            chase.pop(pf, None)
            pos = position.get(token)
            if pos is not None:
                pos["state"] = "open"
            print(f"{symbol_of[token]}: exit FAILED ({why}); still open, will retry on the next signal")

    def step(token, entry_ok=True):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry (only when `entry_ok`: with ACT_ON_BAR_END that is the candle that ends a bar)."""
        px = last_px[token]
        pos = position.get(token)
        if pos is not None:
            if pos["state"] == "open":
                move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)         # % in the position's favour (negative = against it)
                why = exit_reason(pos, move)
                if why is not None:
                    arm_exit(token, why)
            return
        if token in pending_entry or not entry_ok or (ONE_ENTRY_PER_STOCK and token in entered):
            return
        zone = zone_of(rsi(closes(token)))
        if zone is None:
            traded_in_zone.pop(token, None)                        # out of the zone: a new entry may trade it again
            return
        if traded_in_zone.get(token) == zone or not in_window() or not algo_sending(STRATEGY) or not pf_allowed(STRATEGY, token):
            return
        if time.time() - last_fail.get(token, 0.0) < FAIL_COOLDOWN_S:
            return
        first = retry.pop(token, None)
        if first is None:
            count("signals")
        try:
            own = ABS_DAYS == 0 or absolute_sides(symbol_of[token], zone)         # its own days first (cached), then the basket
            middle = own and relative_sides(symbol_of[token], zone)
        except Exception as e:
            ranking_failed(token, first, e)
            return
        if not own:
            count("refused by own days")
        elif not middle:
            count("refused by basket")
        else:
            traded_in_zone[token] = zone
            arm_entry(token, zone_side[zone])
            count("armed")

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
                step(tok, not ACT_ON_BAR_END or (epoch_s(minute) + 60) % BAR_S == 0)       # this candle ends a bar of RSI_BAR: an entry may be looked for
        for tok, t0 in due:
            if time.time() - t0 >= RETRY_RANKING_S:
                retry.pop(tok, None)                               # waited long enough: given up on this bar
            elif tok in retry:
                step(tok, True)
        if trade_listener[0] is None:                               # orders_trade was not there yet when this script started: keep trying every candle batch, not just once
            trade_listener[0] = start_trade_listener()
            if trade_listener[0] is not None:
                print(f"{STRATEGY}: orders_trade is up now -- real fill tracking is live")
        if past_exit() and not stopping:                           # the time exit: stop the whole automation, once (in its own thread: the stop waits for its commands)
            stopping.append(True)
            threading.Thread(target=time_stop, name=f"{STRATEGY}-time-exit", daemon=True).start()

    # ---- by hand ---------------------------------------------------------------------------------------------------------------------
    def exit_all():
        """Close every open position now (arms the spread-gated exit for each, same as a normal exit signal -- it is not instant)."""
        for token in list(position):
            arm_exit(token, "exit_all")

    def status():
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(position)} open position(s), baskets {', '.join(f'{n} ({len(m)})' for n, m in BASKETS.items())}")
        for token, p in position.items():
            print(f"   {symbol_of[token]:8} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} lot(s) entry {p['entry']:g} now {last_px.get(token, float('nan')):g} "
                  f"PF {p['pf']} filled {filled.get(p['pf'], 0)} {p['state']}")
        ready = sum(1 for t in symbol_of if rsi(closes(t)) is not None)
        print(f"   RSI is ready for {ready} of {len(symbol_of)} stocks ({RSI_BAR} bars, {RSI_N + 1} needed)")
        print(f"   waiting on spread: {len(pending_entry)} entr{'y' if len(pending_entry) == 1 else 'ies'}, {len(pending_exit)} exit(s); chasing: {len(chase)} order(s)")
        print("   signals: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none yet"))

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener
    trade_listener = [start_trade_listener()]                      # [handle], mutable so on_candle can retry it later if it was not there yet when this started

    def stop():
        listener.stop()
        if trade_listener[0] is not None:
            trade_listener[0].stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY, pause=False)                           # square up + unsubscribe every portfolio (no pause step), wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["meanrevert_status"], shared["meanrevert_exit_all"], shared["meanrevert_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_meanrevert()
