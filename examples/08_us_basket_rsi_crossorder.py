# US BASKET + 5-MINUTE RSI + CrossOrder: the US version of 07. A plain script (functions, no class). Paste in the US server's terminal after the services are up (init.py).
#
#   The basket        BASKETS = {"us_stocks": [...]}: ONE basket of the 19 stocks. A symbol is ranked against the OTHER 18 (saved as the basket "us_stocks" in baskets.txt too).
#   The filter        a symbol may be traded only while BOTH its % change from the OPEN and from the VWAP stand in the MIDDLE of the basket (percentile MID_ZONE = Q2 to Q3).
#   The own history   AND the stock's own move (% from the OPEN and from the VWAP, now) must sit in Q2 to Q3 of the same minute on its previous ABS_DAYS (22) trading days:
#                     absolute() reads those days from the saved candle files, one stock and one day at a time. With fewer than half of the days in the files there is no trade.
#   The signal        5-minute RSI, made from the 1-minute candles: it ENTERS oversold (< RSI_LOW) -> BUY, or overbought (> RSI_HIGH) -> SELL. One position per symbol at a time.
#   The history       the RSI does not start from nothing at the open: the 5-minute closes of the last HIST_DAYS saved days (candles_<day>.parquet in the market-data / EOD folders)
#                     come first, then today's candles. So the RSI at 09:35 is computed through yesterday's prices, overnight gap included. No files = it says so and starts from today.
#   The exit          the script's own target and stop-loss on the price: +TARGET_PCT in its favour, or -STOP_PCT against it (checked on every 1-minute close).
#                     The engine's CrossOrder has no target or stop and reads its parameters ONCE per portfolio, so an exit is the portfolio's `squareup` command.
#   The order         a NEW portfolio for every trade: ONE `subscribe` that carries the fields, as for 07 (the engine knows the strategy as "CrossOrder"; SIDE 1 = buy, 2 = sell):
#                     {"strategyname": "CrossOrder", "TOKEN": <contract>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#                     The NATS service adds USERIDENTIFIER and user_id. The book gives the portfolio numbers; each stock may use up to 100 a day.
#                     CHECK on the US server before real money: the strategy name "CrossOrder", SERVERCODE and ACCOUNT are India's values.
#
# EVERYTHING is inside the function run_us_basket_rsi() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of
# names shared by every script you paste, so top-level `rsi` or `closes` would overwrite another running script's. The only names left in the terminal start with us_basket_rsi:
#   us_basket_rsi_status()   us_basket_rsi_exit_all()   us_basket_rsi_stop()   (stop does NOT close positions: run exit_all first)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).


def run_us_basket_rsi():
    import contextlib
    import glob
    import io
    import os
    import time
    from contextlib import nullcontext
    from datetime import datetime, time as dtime
    from zoneinfo import ZoneInfo

    import numpy as np
    import pandas as pd

    from algo import current
    from algo.comms.nats_service import NO_SERVICE
    from algo.core.timeframe import parse_timeframe

    # =============================== USER INPUT ===============================
    USER = 3
    STRATEGY = "us_basket_rsi"               # its name: recorded with every portfolio, the name of its sending switch and of its console window
    BASKETS = {"us_stocks": ["AAPL", "TSLA", "NVDA", "MSFT", "SPY", "QQQ", "IWM"]}

    TRADE_AS = {}                            # optional: {"AAPL": "<another contract name or token>"} trade another contract than the basket entry
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    MID_ZONE = (25.0, 75.0)                  # percentile of the symbol among its basket, for the % from open AND the % from VWAP: Q2 to Q3
    ABS_DAYS = 22                            # absolute(): compare the stock with itself at the same minute of its previous 22 trading days (saved files); 0 = off
    ABS_ZONE = MID_ZONE                      # the percentile band that comparison must fall in (Q2 to Q3)
    RSI_BAR = "5min"                         # the bar the RSI is computed on (made from the 1-minute candles)
    RSI_N, RSI_HIGH, RSI_LOW = 9, 70, 30
    RSI_LIVE = False                          # True: the bar still forming counts too (acts every minute); False: finished bars only
    HIST_DAYS = 3                            # how many earlier saved days seed the RSI (0 = none)
    TARGET_PCT, STOP_PCT = 1.5, 0.5
    QTY = 1                                  # in LOTS (the plugin multiplies it by the contract's lot size)
    ENGINE = {"strategyname": "CrossOrder", "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}      # "CrossOrder" is the engine's name for the CrossOrder plugin
    WINDOW = (dtime(10, 0), dtime(15, 45))   # entries only inside it (New York time, the regular session is 09:30 to 16:00); an exit is sent at any time
    FAIL_COOLDOWN_S = 30
    # ==========================================================================
    if not (0 <= MID_ZONE[0] < MID_ZONE[1] <= 100 and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and HIST_DAYS >= 0 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and 0 <= ABS_ZONE[0] < ABS_ZONE[1] <= 100):
        raise ValueError("MID_ZONE and ABS_ZONE must be inside 0 to 100, TARGET_PCT and STOP_PCT above 0, QTY at least 1, HIST_DAYS 0 or more, ABS_DAYS 0 to 400, and every basket needs at least 3 symbols")
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
    last_px = {}                                                   # token -> the latest 1-minute close
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    traded_in_zone = {}                                            # token -> the zone it was traded in: no second trade until the RSI has left it
    last_fail = {}
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
        """RSI of the last closes, or None until there are n + 1 of them."""
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

    # ---- the filter: the middle of the basket --------------------------------------------------------------------------------------
    def in_middle(symbol):
        """True when the symbol's % from the open AND its % from the VWAP both rank between MID_ZONE[0] and MID_ZONE[1] percent of ITS basket (the other symbols of it)."""
        for ref in ("open", "vwap"):
            with exec_ctx:                                         # relative() reads the candles table; a listener thread needs the terminal's query context for that
                result = relative("pct", LEVEL, symbol, BASKETS[basket_of[symbol]], ref=ref)
            values, idx = result                                   # the other symbols' values, and how many of them are below this one (-1 = cannot tell)
            if idx < 0 or not (MID_ZONE[0] <= result.pctile <= MID_ZONE[1]):
                return False
        return True

    def in_own_history(symbol):
        """True when the symbol's % from the open AND from the VWAP, now, each rank between ABS_ZONE[0] and ABS_ZONE[1] percent of the same minute on its previous ABS_DAYS trading days.
        absolute() answers idx = -1 (cannot tell) when fewer than half of those days are in the saved files; that is no trade."""
        for ref in ("open", "vwap"):
            with exec_ctx:                                         # absolute() reads today's candles table once, and the saved day files
                result = absolute("pct", LEVEL, symbol, ABS_DAYS, ref=ref)
            hist, idx = result                                     # the earlier days' values (oldest first), and how many of them are below today's (-1 = cannot tell)
            if idx < 0 or not (ABS_ZONE[0] <= result.pctile <= ABS_ZONE[1]):
                return False
        return True

    # ---- the orders ------------------------------------------------------------------------------------------------------------------
    def in_window():
        return WINDOW[0] <= datetime.now(ZoneInfo("America/New_York")).time() <= WINDOW[1]

    def cross_order(token, side, qty):
        return {**ENGINE, "TOKEN": int(token), "SIDE": int(side), "QTY": int(qty)}

    def send_entry(pf, token, side, qty):
        """The one subscribe that starts the strategy: exactly its own fields, sent as they are (not the churning subscribe defaults). A Future with the reply."""
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "subscribe", cross_order(token, side, qty), user=USER, client=STRATEGY)       # honours this strategy's own switch

    def send_exit(pf):
        svc = current().get("nats") or NO_SERVICE
        return svc.squareup(pf, user=USER, client=STRATEGY)                              # the plugin's SquareUpEvent closes the position

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
                position.pop(token, None)
                last_fail[token] = time.time()
                print(f"{symbol_of[token]}: entry FAILED ({why}); nothing is open")
        elif ok:
            position.pop(token, None)
        elif pos is not None:
            pos["state"] = "open"
            last_fail[token] = time.time()
            print(f"{symbol_of[token]}: exit FAILED ({why}); the position is still open, it is tried again")

    def enter(token, side):
        if next_slot[token] >= 100:
            print(f"{symbol_of[token]}: all 100 portfolios of the day are used, no trade")
            return
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])       # the BOOK gives a NEW portfolio number (None while OFF or unticked)
        if not a:
            return
        next_slot[token] += 1
        position[token] = {"side": side, "entry": last_px[token], "qty": QTY, "pf": a["pf"], "state": "entering"}
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {QTY} lot(s) at {last_px[token]:g} on PF {a['pf']}")
        send_entry(a["pf"], token, side, QTY).add_done_callback(lambda f: settled(token, "enter", f))

    def leave(token, why):
        pos = position.get(token)
        if pos is None or pos["state"] != "open":
            return
        pos["state"] = "exiting"
        print(f"{symbol_of[token]}: {why}, closing at {last_px[token]:g}")
        send_exit(pos["pf"]).add_done_callback(lambda f: settled(token, "exit", f))

    def step(token):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry."""
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
        zone = zone_of(rsi(closes(token)))
        if zone is None:
            traded_in_zone.pop(token, None)                        # out of the zone: a new entry may trade it again
            return
        if traded_in_zone.get(token) == zone or not in_window() or not algo_sending(STRATEGY) or not pf_allowed(STRATEGY, token):
            return
        if time.time() - last_fail.get(token, 0.0) < FAIL_COOLDOWN_S:
            return
        try:
            middle = (ABS_DAYS == 0 or in_own_history(symbol_of[token])) and in_middle(symbol_of[token])      # its own days first (cached), then the basket
        except Exception as e:
            print(f"{symbol_of[token]}: the ranking is not available ({e}); no trade")
            return
        if middle:
            traded_in_zone[token] = zone
            enter(token, zone_side[zone])

    def on_candle(update, is_replay):
        """Every batch of newly closed 1-minute candles: keep the closes of the traded stocks and run the rules on each."""
        ch = update.added(["token", "Close", "Minute"])
        if ch is None:
            return
        for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
            tok = int(tok)
            if tok in bars and close > 0:
                add_close(tok, minute, close)
                last_px[tok] = float(close)
                step(tok)

    # ---- by hand ---------------------------------------------------------------------------------------------------------------------
    def exit_all():
        """Close every open position now (the squareup of each portfolio)."""
        for token in list(position):
            leave(token, "exit_all")

    def status():
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(position)} open position(s), baskets {', '.join(f'{n} ({len(m)})' for n, m in BASKETS.items())}")
        for token, p in position.items():
            print(f"   {symbol_of[token]:8} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} lot(s) entry {p['entry']:g} now {last_px.get(token, float('nan')):g} PF {p['pf']} {p['state']}")
        ready = sum(1 for t in symbol_of if rsi(closes(t)) is not None)
        print(f"   RSI is ready for {ready} of {len(symbol_of)} stocks ({RSI_BAR} bars, {RSI_N + 1} needed)")

    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        algo_stop(STRATEGY)                                        # open positions are NOT closed by this: us_basket_rsi_exit_all() first

    shared = globals()
    shared["us_basket_rsi_status"], shared["us_basket_rsi_exit_all"], shared["us_basket_rsi_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_us_basket_rsi()
