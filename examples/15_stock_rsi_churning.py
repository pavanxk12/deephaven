# STOCK (CASH) churning: BUY the stock when it is OVERSOLD, SELL it when OVERBOUGHT -- trading the stock itself, never its options. Same churning
# machinery as 06 (pf_assign / pf_activate on the "Churning" plugin, SUBSCRIBE defaults, order_params from the instrument's own price/tick), just
# pointed at each stock's own CASH price instead of buying CE/PE on it.
# Paste in the terminal after 00_services.py (and candles_start, with saved candles_<day>.parquet files for the absolute() history) have started.
# Needs the master and candles running for EQ (candles_start's default already includes EQ alongside FUT/OPT; kind="EQ" alone if you want to narrow it).
#
#   The stocks         STOCKS: the underlyings you give. Each trades its OWN cash instrument; its basket (for relative()) is this same list, and
#                       every relative()/absolute() entry is the bare symbol ("SBIN"), which both functions read as cash.
#   The signal         RSI_N=14 on RSI_BAR (5min) closes of the stock's CASH price (HIST_DAYS earlier saved days seed it, then today's candles).
#                       RSI < RSI_LOW (30) -> OVERSOLD: churn BUY.   RSI > RSI_HIGH (70) -> OVERBOUGHT: churn SELL.
#   The filter          a stock may churn only while BOTH relative_sides() (its % from open/VWAP rank Q2-Q3 of REL_ZONE among the basket) and
#                       absolute_sides() (the same, against its own last ABS_DAYS trading days) are true -- same filters and bands as 11.
#   The churning        once a stock is in a zone and passes both filters, it churns like 06: send the side (BUY or SELL) into every one of its
#                       N_SLOTS that is free (closed by hand, cooled down), for as long as the zone + filters hold. A slot stopped by hand (console
#                       squareup/unsubscribe) stays off until the RSI leaves the zone (the old `blocked` behaviour).
#   The order           ONE `subscribe` per entry: {"strategyname": "Churning", ...SUBSCRIBE..., "jval"/"ival"/... from order_params on the
#                       stock's own CASH price/tick, "side": "BUY"/"SELL"}, same mechanics as 06. The book gives the portfolio numbers; each
#                       stock's CASH token is the only instrument there is -- no separate contract to pick: N_SLOTS per stock.
#
# Everything is inside run_stock_churning() so the terminal keeps one set of names. Left in the terminal: stockchurn_status(), stockchurn_exit_all(),
# stockchurn_stop(), buy(), sell() (by hand, force=True, every stock).

def run_stock_churning():
    import math
    from datetime import datetime, time as dtime
    from zoneinfo import ZoneInfo

    import numpy as np
    import pandas as pd

    from algo import current
    from algo.core.timeframe import parse_timeframe

    # =============================== USER INPUT ===============================
    USER = 4
    STRATEGY = "stock_churning"      # its name: recorded with every portfolio, the name of its sending switch and of its console window
    PF_START = 1500
    ADD_QTY = 10
    STOCKS = ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK","BAJFINANCE", "BAJAJFINSV", "SBILIFE", "HDFCLIFE", "BSE","TCS", "INFY", "HCLTECH", "WIPRO", "TECHM","M&M", "MARUTI", "TATAMOTORS", "EICHERMOT", "BAJAJ-AUTO","SUNPHARMA","DRREDDY","CIPLA","APOLLOHOSP","MAXHEALTH","ITC","NESTLEIND","TITAN","HINDUNILVR","ETERNAL","ONGC","BPCL","RELIANCE","NTPC","POWERGRID","HINDALCO","JSWSTEEL","TATASTEEL","COALINDIA"]   # give your own list here: this is also the basket for relative()
    N_SLOTS = 1                      # how many concurrent churn entries a single stock may hold on one side at once
    LEVEL = "intraday"                # relative()/absolute() level: today's open and VWAP
    REL_ZONE = {"pct_open": (25.0, 75.0), "pct_vwap": (25.0, 75.0)}      # relative(): percentile within the basket (STOCKS) must land in (low, high)
    ABS_DAYS = 22                      # absolute(): against the same minute on its previous 22 trading days (saved files); 0 = off
    ABS_ZONE = {"pct_open": (25.0, 75.0), "pct_vwap": (25.0, 75.0)}
    RSI_BAR = "5min"
    RSI_N, RSI_HIGH, RSI_LOW = 14, 70, 30
    RSI_LIVE = False                   # True: the bar still forming counts too (acts every minute); False: finished bars only (hist day 1 == 11's default)
    HIST_DAYS = 1                       # earlier saved days (candles_<day>.parquet) that seed each stock's RSI before today's candles
    FAIL_COOLDOWN_S = 30
    WINDOW = (dtime(9, 20), dtime(15, 20))
    SUBSCRIBE = {"strategyname": "Churning", "maxloss": -2000, "stoploss": -2000, "max_unhedge": 10, "gtw": 26286}
    SIZING = dict(ival_pct=0.002, range_pct=0.03, trp_pct=0.001, jval_frac=0.75, sval_mult=5,
                  jqty1=1, jqty2=1, target=6000, stoploss=-2000, maxloss=-2000, max_unhedge=10, PRNT=1)
    # ==========================================================================
    if not (N_SLOTS >= 1 and N_SLOTS < 100 and len(STOCKS) >= 3):
        raise ValueError("N_SLOTS 1 to 99, and at least 3 STOCKS (relative() needs a basket) are required")
    if not (all(0 <= ZONE[ref][0] < ZONE[ref][1] <= 100 for ZONE in (REL_ZONE, ABS_ZONE) for ref in ("pct_open", "pct_vwap"))
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and HIST_DAYS >= 0):
        raise ValueError("every (low, high) band in REL_ZONE/ABS_ZONE must be inside 0 to 100 with low below high, and ABS_DAYS 0 to 400")
    BAR_S = parse_timeframe(RSI_BAR)
    STOCKS = [s.upper() for s in STOCKS]
    BASKET = list(STOCKS)      # relative()'s basket entries: a bare name means the CASH instrument (not " FUT"), so the churn is driven by the stock itself

    nats_connect(user=USER, client=STRATEGY)
    registry_connect()
    algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=ADD_QTY, subscribe=SUBSCRIBE)
    try:
        basket(STRATEGY, BASKET)
    except Exception as e:
        print(f"basket {STRATEGY!r} was not saved ({e}); the ranking still works from BASKET")

    cash_of = {}                                   # symbol (e.g. "SBIN") -> its CASH (EQ) instrument's token
    symbol_of = {}                                 # token -> symbol
    for s in STOCKS:
        eq = contracts(s, kind="EQ", limit=1)
        if eq is None or eq.empty:
            print(f"{s}: no EQ (cash) contract found, left out")
            continue
        cash_of[s] = int(eq["token"].iloc[0])
        symbol_of[int(eq["token"].iloc[0])] = s
    pf_track(STRATEGY, list(cash_of.values()), slots=N_SLOTS)

    ltp = {}                                       # token -> last close (the stocks' own cash prices)
    bars = {s: {} for s in STOCKS}                 # symbol -> {bar number: last 1-minute close inside it}
    blocked = {s: set() for s in STOCKS}           # symbol -> slots stopped by hand: off until the RSI leaves its zone
    ZONE_SIDE = {"OVERSOLD": "BUY", "OVERBOUGHT": "SELL"}

    try:
        from deephaven.execution_context import get_exec_ctx
        from contextlib import nullcontext
        exec_ctx = get_exec_ctx()
    except Exception:
        from contextlib import nullcontext
        exec_ctx = nullcontext()

    def epoch_s(minute):
        return int(pd.Timestamp(minute).timestamp())

    def add_close(s, minute, close):
        b = bars[s]
        b[epoch_s(minute) // BAR_S] = float(close)
        for old in sorted(b)[:-600]:
            del b[old]

    def closes(s):
        keys = sorted(bars[s])
        return [bars[s][k] for k in (keys if RSI_LIVE else keys[:-1])]

    # ---- history: earlier saved days seed each stock's bars, before today's candles. A cash (EQ) symbol is the plain stock name every
    # day (no roll / expiry matching needed, unlike a future) -----------------------------------------------------------------------------
    def load_history():
        if HIST_DAYS <= 0 or not cash_of:
            return
        import glob, os
        try:
            import pyarrow.parquet as pq
        except ImportError:
            print("history: pyarrow is not installed, the RSI starts from today's candles")
            return
        paths = current().paths
        folders = [d for d in (paths.saved, paths.market_data) if d and os.path.isdir(d)]
        today = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
        files = {}
        for d in folders:
            for f in sorted(glob.glob(os.path.join(d, "candles_*.parquet"))):
                day = os.path.basename(f)[len("candles_"):-len(".parquet")]
                if len(day) == 10 and day[4] == "-" and day < today:
                    files.setdefault(day, f)
        used = sorted(files)[-HIST_DAYS:]
        if not used:
            print(f"history: no candles_<day>.parquet before {today}: the RSI starts from today's candles")
            return
        loaded = []
        for day in used:
            try:
                df = pq.read_table(files[day], columns=["Minute", "Symbol", "Close"], filters=[("Symbol", "in", STOCKS)]).to_pandas()
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
            last = df.sort_values(["Symbol", "ts"]).groupby(["Symbol", "bar"], sort=True)["Close"].last()
            for (s, b), close in last.items():
                if s in bars and close > 0:
                    bars[s][int(b)] = float(close)
        if loaded:
            print(f"history: {len(loaded)} earlier day(s) ({', '.join(loaded)}) seed the {RSI_BAR} RSI of {sum(1 for s in bars if bars[s])} of {len(STOCKS)} stocks")
        else:
            print("history: none of the earlier days could be used, the RSI starts from today's candles")

    load_history()
    if ABS_DAYS > 0:
        try:
            for warm_ref in ("open", "vwap"):
                absolute_warm("pct", LEVEL, BASKET, ABS_DAYS, ref=warm_ref)
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for s, tok in cash_of.items():
        h = last_candles(tok, 400, show=False)
        if h is not None:
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(s, m, c)
                ltp[tok] = float(c)

    # ---- the indicator: Wilder's RSI --------------------------------------------------------------------------------------------------
    def rsi(values, n=RSI_N):
        if len(values) <= n:
            return None
        d = np.diff(np.asarray(values, dtype=float))
        up, down = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
        avg_up, avg_down = up[:n].mean(), down[:n].mean()
        for u, v in zip(up[n:], down[n:]):
            avg_up, avg_down = (avg_up * (n - 1) + u) / n, (avg_down * (n - 1) + v) / n
        if avg_down == 0:
            return 100.0 if avg_up > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + avg_up / avg_down)

    def zone_of(value):
        return None if value is None else "OVERBOUGHT" if value > RSI_HIGH else "OVERSOLD" if value < RSI_LOW else None

    # ---- the filters: same as 11 -------------------------------------------------------------------------------------------------------
    def relative_sides(sym):
        for ref in ("pct_open", "pct_vwap"):
            with exec_ctx:
                result = relative("pct", LEVEL, sym, BASKET, ref=ref[len("pct_"):])          # a bare name = the CASH instrument
            if result[1] < 0 or not (REL_ZONE[ref][0] <= result.pctile <= REL_ZONE[ref][1]):
                return False
        return True

    def absolute_sides(sym):
        for ref in ("pct_open", "pct_vwap"):
            with exec_ctx:
                result = absolute("pct", LEVEL, sym, ABS_DAYS, ref=ref[len("pct_"):])
            if result[1] < 0 or not (ABS_ZONE[ref][0] <= result.pctile <= ABS_ZONE[ref][1]):
                return False
        return True

    def passes_filters(sym):
        try:
            return (ABS_DAYS == 0 or absolute_sides(sym)) and relative_sides(sym)
        except Exception as e:
            print(f"{sym}: the ranking is not available ({e}); no churn this time")
            return False

    # ---- order sizing, from the stock's own CASH price/tick (same shape as 06's order_params, side is now BUY or SELL) ------------------
    def floor_tick(x, tick):
        return round(math.floor(x / tick + 1e-9) * tick, 2) + 1e-6

    def ceil_tick(x, tick):
        return round(math.ceil(x / tick - 1e-9) * tick, 2) + 1e-6

    def order_params(side, premium, tick, ival_pct=0.01, range_pct=0.05, trp_pct=0.01, jval_frac=0.75, sval_mult=3, **apply_fields):
        if not (premium and premium > 0 and tick and tick >= 0.01):
            return None
        ival = max(floor_tick(premium * ival_pct, tick), tick)
        return {"ival": ival, "jval": max(floor_tick(ival * jval_frac, tick), tick), "sval": max(floor_tick(ival * sval_mult, tick), tick),
                "trp": ceil_tick(premium * (1 + trp_pct / 100), tick),
                "lowerrange": max(floor_tick(premium * (1 - range_pct), tick), tick), "upperrange": floor_tick(premium * (1 + range_pct), tick),
                "side": side, **apply_fields}

    # ---- WHEN: churn while the zone + both filters hold (same shape as 06's on_zone, just cash both times) ----
    def in_window():
        return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]

    def on_zone(sym, zone, force=False):
        side = ZONE_SIDE[zone]
        cash = cash_of[sym]
        if not in_window() or not algo_sending(STRATEGY):
            return
        if not force and not passes_filters(sym):
            return                                              # in the zone, but not in the middle of its basket / its own days: no churn
        tick = contract(cash, show=False)
        if tick is None:
            return
        params = order_params(side, ltp.get(cash), tick["tick"], **SIZING)
        if params is None:
            print(f"{sym} on_zone {zone}: skipped, no usable price or tick")
            return
        if force:
            blocked[sym].clear()
        for k in range(N_SLOTS):
            if k in blocked[sym]:
                continue
            if not pf_allowed(STRATEGY, cash, k):
                continue
            if pf_leg(STRATEGY, cash, k) == side or (not force and pf_cooling(STRATEGY, cash, FAIL_COOLDOWN_S, k)):
                continue                                         # this slot already runs this side, or failed a moment ago
            a = pf_assign(STRATEGY, cash, cash, name=sym, slot=k)   # it's just cash, both times
            if a:
                pf_activate(a, side, params)

    def check(sym):
        value = rsi(closes(sym))
        zone = zone_of(value)
        if zone is None:
            blocked[sym].clear()
        else:
            on_zone(sym, zone)
        return value

    def on_candle(update, is_replay):
        ch = update.added(["token", "Close", "Minute"])
        if ch is None:
            return
        touched = set()
        for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
            if close > 0:
                ltp[int(tok)] = float(close)
                sym = symbol_of.get(int(tok))
                if sym is not None:
                    add_close(sym, minute, close)
                    touched.add(sym)
        for sym in touched:
            check(sym)

    # ---- the old `blocked` set: a slot squared up or unsubscribed from the console stays off -------------------------------------------
    def slot_of(sym, pf):
        ids = current().get(STRATEGY).book.state.current_pf_map()
        cash = cash_of[sym]
        for k in range(N_SLOTS):
            if ids.get(cash if k == 0 else cash * 100 + k) == pf:
                return k
        return None

    def on_state(user, pf, st):
        if user == USER and st.cmd in ("squareup", "unsubscribe") and st.by != STRATEGY:
            for sym in STOCKS:
                k = slot_of(sym, pf)
                if k is not None:
                    blocked[sym].add(k)
                    print(f"{STRATEGY}: {sym} slot {k} (PF {pf}) was stopped by {st.by}: off until the RSI leaves its zone, or buy() / sell()")
                    break

    old = current().shared.setdefault("stock_churning_state", {}).pop(STRATEGY, None)
    if old is not None:
        old()                                                     # pasted again: take the old listener off first
    undo_state = current().get("nats").states.on_change(on_state)
    current().shared["stock_churning_state"][STRATEGY] = undo_state

    # ---- by hand ------------------------------------------------------------------------------------------------------------------------
    def sell():
        for sym in STOCKS:
            on_zone(sym, "OVERBOUGHT", force=True)

    def buy():
        for sym in STOCKS:
            on_zone(sym, "OVERSOLD", force=True)

    def exit_all():
        algo_exit(STRATEGY)
        for sym in STOCKS:
            blocked[sym].update(range(N_SLOTS))

    def status():
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(STOCKS)} stocks, basket size {len(BASKET)}")
        for sym in STOCKS:
            value = rsi(closes(sym))
            zone = zone_of(value)
            rsi_text = f"{value:.1f}" if value is not None else "n/a"
            print(f"   {sym:12} RSI {rsi_text}" + ("" if zone is None else f" zone {zone}"))

    listener = watch("candles", on_candle, owner=STRATEGY)

    shared = globals()
    shared["stockchurn_status"], shared["stockchurn_exit_all"] = status, exit_all
    shared["stockchurn_stop"] = lambda: (listener.stop(), algo_stop(STRATEGY))
    shared["buy"], shared["sell"] = buy, sell

    nats_send(True, client=STRATEGY)
    for sym in STOCKS:
        check(sym)
    print(f"{STRATEGY}: watching {', '.join(STOCKS)}; RSI({RSI_N}) on {RSI_BAR} bars, <{RSI_LOW} churns BUY, >{RSI_HIGH} churns SELL, "
          f"gated by relative()/absolute() both in {REL_ZONE['pct_open']}")


run_stock_churning()
