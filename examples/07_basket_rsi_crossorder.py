# BASKET + 1-MINUTE RSI + CrossOrder: a plain script (functions, no class). Paste in the terminal after the services are up (init.py or 05_start_day.py).
#
#   The baskets       BASKETS = {name: [symbols]}: as many as you like, each saved as a basket of that name (baskets.txt). A symbol is ranked against the OTHER symbols of its own
#                     basket only, and may sit in one basket only.
#   The filter        a symbol may be traded only while BOTH its % change from the OPEN and from the VWAP stand in the MIDDLE of its basket (percentile MID_ZONE = Q2 to Q3).
#   The signal        1-minute RSI: it ENTERS oversold (< RSI_LOW) -> BUY, or overbought (> RSI_HIGH) -> SELL. One position per symbol at a time.
#   The exit          the script's own target and stop-loss on the price: +TARGET_PCT in its favour, or -STOP_PCT against it (checked on every 1-minute close).
#                     The engine's CrossOrder has no target or stop, and it reads its parameters ONCE per portfolio, so an exit is the portfolio's `squareup` command
#                     (the plugin's SquareUpEvent closes the position at the touch), not a second order.
#   The order         a NEW portfolio for every trade (the plugin ignores parameters after the first): ONE `subscribe` that carries the fields, exactly as the one that worked
#                     for PF 1204 / 1205 (engine log: INITIATED, then onParamsUpdate, then the order). The engine knows this strategy as "single"; an `apply` alone creates nothing.
#                     {"strategyname": "single", "TOKEN": <contract>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}   SIDE 1 = buy, 2 = sell.
#                     Nothing else is sent with it (no churning defaults); the NATS service adds USERIDENTIFIER and user_id. The book gives the portfolio numbers; each contract may
#                     use up to 100 a day.
#                     The plugin works a limit order at the touch (it modifies up to 3 times, then cancels), so a fill is not instant.
#
# EVERYTHING is inside the function run_basket_rsi() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of names shared by every script you paste, so a script that defines `rsi`, `closes` or `on_candle`
# at the top level would overwrite the same names of another script that is still running. The only names left in the terminal all start with basket_rsi:
#   basket_rsi_status()   basket_rsi_exit_all()   basket_rsi_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).


def run_basket_rsi():
    import contextlib
    import io
    import time
    from contextlib import nullcontext
    from datetime import datetime, time as dtime
    from zoneinfo import ZoneInfo

    import numpy as np

    from algo import current
    from algo.comms.nats_service import NO_SERVICE

    # =============================== USER INPUT ===============================
    USER = 4
    STRATEGY = "basket_rsi"                  # its name: recorded with every portfolio, the name of its sending switch and of its console window
    BASKETS = {                              # edit: any number of baskets, at least 3 symbols each, a symbol in one basket only.
                                             # "SBIN 27OCT26 FUT" is that one contract: it does not roll, so change the date to the next expiry (23NOV26) before 27OCT26
        "banks": ["HDFCBANK 27OCT26 FUT", "ICICIBANK 27OCT26 FUT", "SBIN 27OCT26 FUT", "AXISBANK 27OCT26 FUT", "KOTAKBANK 27OCT26 FUT"],
        "it": ["INFY 27OCT26 FUT", "TCS 27OCT26 FUT", "WIPRO 27OCT26 FUT", "HCLTECH 27OCT26 FUT", "OFSS 27OCT26 FUT"],
    }
    TRADE_AS = {}                            # optional: {"SBIN 27OCT26 FUT": "SBIN 23NOV26 FUT"} trade another contract (name or token) than the basket entry
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    MID_ZONE = (25.0, 75.0)                  # percentile of the symbol among its basket, for the % from open AND the % from VWAP: Q2 to Q3
    RSI_N, RSI_HIGH, RSI_LOW = 9, 70, 30    # the 1-minute RSI
    TARGET_PCT, STOP_PCT = 1.5, 0.5
    QTY = 1                                  # in LOTS (the plugin multiplies it by the contract's lot size)
    ENGINE = {"strategyname": "single", "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}      # "single" is the engine's name for the CrossOrder plugin
    WINDOW = (dtime(9, 20), dtime(15, 20))   # entries only inside it (India time); an exit is sent at any time
    FAIL_COOLDOWN_S = 30
    # ==========================================================================
    if not (0 <= MID_ZONE[0] < MID_ZONE[1] <= 100 and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())):
        raise ValueError("MID_ZONE must be inside 0 to 100, TARGET_PCT and STOP_PCT above 0, QTY at least 1, and every basket needs at least 3 symbols")
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
    pf_track(STRATEGY, list(symbol_of), slots=100)                 # the portfolios it had before (a restart or a paste again); every trade takes the next free slot
    with contextlib.redirect_stdout(io.StringIO()):                # (algo_portfolios prints a table; only the numbers are needed)
        had = algo_portfolios(STRATEGY)
    next_slot = {t: (int(had[had["instrument"] == t]["slot"].max()) + 1 if had is not None and (had["instrument"] == t).any() else 0) for t in symbol_of}
    closes = {t: [] for t in symbol_of}                            # token -> 1-minute closes, oldest first
    for t in symbol_of:
        h = last_candles(t, 200, show=False)
        if h is not None:
            closes[t].extend(float(c) for c in h["Close"])
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    traded_in_zone = {}                                            # token -> the zone it was traded in: no second trade until the RSI has left it
    last_fail = {}
    zone_side = {"OVERSOLD": 1, "OVERBOUGHT": 2}                   # 1 = buy, 2 = sell

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

    # ---- the orders ------------------------------------------------------------------------------------------------------------------
    def in_window():
        return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]

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
        position[token] = {"side": side, "entry": closes[token][-1], "qty": QTY, "pf": a["pf"], "state": "entering"}
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {QTY} lot(s) at {closes[token][-1]:g} on PF {a['pf']}")
        send_entry(a["pf"], token, side, QTY).add_done_callback(lambda f: settled(token, "enter", f))

    def leave(token, why):
        pos = position.get(token)
        if pos is None or pos["state"] != "open":
            return
        pos["state"] = "exiting"
        print(f"{symbol_of[token]}: {why}, closing at {closes[token][-1]:g}")
        send_exit(pos["pf"]).add_done_callback(lambda f: settled(token, "exit", f))

    def step(token):
        """One new 1-minute close of a traded contract: manage its position, or look for an entry."""
        px = closes[token][-1]
        pos = position.get(token)
        if pos is not None:
            if pos["state"] == "open":
                move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)         # % in the position's favour (negative = against it)
                if move >= TARGET_PCT:
                    leave(token, f"target {move:+.2f}%")
                elif move <= -STOP_PCT:
                    leave(token, f"stop-loss {move:+.2f}%")
            return
        zone = zone_of(rsi(closes[token]))
        if zone is None:
            traded_in_zone.pop(token, None)                        # out of the zone: a new entry may trade it again
            return
        if traded_in_zone.get(token) == zone or not in_window() or not algo_sending(STRATEGY) or not pf_allowed(STRATEGY, token):
            return
        if time.time() - last_fail.get(token, 0.0) < FAIL_COOLDOWN_S:
            return
        try:
            middle = in_middle(symbol_of[token])
        except Exception as e:
            print(f"{symbol_of[token]}: the basket ranking is not available ({e}); no trade")
            return
        if middle:
            traded_in_zone[token] = zone
            enter(token, zone_side[zone])

    def on_candle(update, is_replay):
        """Every batch of newly closed 1-minute candles: keep the closes of the traded contracts and run the rules on each."""
        ch = update.added(["token", "Close"])
        if ch is None:
            return
        for tok, close in zip(ch["token"], ch["Close"]):
            tok = int(tok)
            if tok in closes and close > 0:
                closes[tok].append(float(close))
                del closes[tok][:-300]
                step(tok)

    # ---- by hand ---------------------------------------------------------------------------------------------------------------------
    def exit_all():
        """Close every open position now (the squareup of each portfolio)."""
        for token in list(position):
            leave(token, "exit_all")

    def status():
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {len(position)} open position(s), baskets {', '.join(f'{n} ({len(m)})' for n, m in BASKETS.items())}")
        for token, p in position.items():
            print(f"   {symbol_of[token]:22} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} lot(s) entry {p['entry']:g} now {closes[token][-1]:g} PF {p['pf']} {p['state']}")

    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        algo_stop(STRATEGY)                                        # pause + square up + unsubscribe every portfolio, wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["basket_rsi_status"], shared["basket_rsi_exit_all"], shared["basket_rsi_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_basket_rsi()
