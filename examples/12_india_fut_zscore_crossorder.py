# INDIA STOCK FUTURES BASKETS + Z-SCORE + CrossOrder: 10 for the NSE, driven by a z-score of the % from the VWAP on 5-MINUTE bars. A plain script (functions, no class).
# Paste in the India server's terminal after the services are up (init.py).
#
#   The universe      every STOCK future in the master that has not expired (about 200; index futures are left out), at the NEAREST expiry. A basket entry is "SBIN FUT" = the nearest
#                     future that has not expired, on each day: the script trades today's nearest future, and every comparison with the past uses the nearest future of THAT day,
#                     so a stock is only ever compared with its own futures and a roll never mixes two contracts inside one day.
#   The baskets       the sorted underlyings are dealt one by one into N_BASKETS (4) baskets, fut_1 ... fut_4, about 50 each (saved in baskets.txt too). A symbol is ranked against the OTHER
#                     symbols of its own basket only. ONLY = [...] limits the list, EXCLUDE = [...] removes some.
#   The filter        a symbol may be traded only while BOTH its % change from the OPEN and from the VWAP stand in the MIDDLE of its basket (percentile MID_ZONE = Q2 to Q3).
#   The z-score       z = (the future's % from the VWAP NOW  -  the mean of the % from the VWAP at the SAME MINUTE on the nearest future of its earlier days) / their standard deviation.
#                     The earlier days are those of the last Z_DAYS (90) CALENDAR days that are in the saved candle files (about 64 trading days).
#                     absolute() reads them one future and one day at a time. With fewer than Z_MIN_DAYS of them in the files there is no z, and so no trade and no z exit.
#   The timeframe     Z_BAR = "5min": z is looked at once per FINISHED 5-minute bar (the 1-minute candle that ends the bar, at :04, :09, :14 ...), for entries and for exits.
#                     Z_BAR_LIVE = True looks at it on EVERY 1-minute close instead (the bar still forming counts). The time exit runs every minute either way.
#   The entry         BUY when z <= -Z_ENTRY (the future is well BELOW its usual % from the VWAP at this minute), SELL when z >= +Z_ENTRY. One position per future at a time, and no second
#                     trade in the same direction until z has come back inside the band.
#   The exit          a BUY is closed when z > 0, a SELL when z < 0 (back through its usual value), checked on every 1-minute close.
#                     ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#                     And the time exit: EXIT_AT (15:15 India time) stops the automation the way algo_stop does it (pause, square up, a minute later unsubscribe every portfolio, then the
#                     script stops), and no entry is made after it.
#                     TARGET_PCT / STOP_PCT (a % move in your favour / against you) are OFF (None); give them a number to add them as well.
#   The order         a NEW portfolio for every trade: ONE `subscribe` that carries the fields, as for 07 (the engine knows the plugin as "CrossOrder"; SIDE 1 = buy, 2 = sell):
#                     {"strategyname": "CrossOrder", "TOKEN": <the future>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#                     The NATS service adds USERIDENTIFIER and user_id. The book gives the portfolio numbers; each future may use up to 100 a day.
# NEEDS about 64 trading days of saved candle files on the server (Z_DAYS = 90 calendar days) with the futures in them, and the master and the candles running for the futures
# (candles_start(kind="FUT") is enough); with fewer days there is a smaller sample, and below Z_MIN_DAYS no z at all.
#
# EVERYTHING is inside the function run_india_fut_zscore() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of
# names shared by every script you paste. The only names left in the terminal start with india_fut_zscore:
#   india_fut_zscore_status()   india_fut_zscore_exit_all()   india_fut_zscore_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).

def run_india_fut_zscore():
    import contextlib
    import io
    import math
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
    USER = 3
    STRATEGY = "india_fut_zscore"               # its name: recorded with every portfolio, the name of its sending switch and of its console window
    N_BASKETS = 4                            # every stock future (about 200) is dealt into this many baskets, fut_1 ... fut_4; a symbol is ranked only against the others of ITS basket
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
    ]                                # optional: trade only these underlyings, e.g. ["SBIN", "TCS", "INFY"]; empty = every stock future
    EXCLUDE = []                             # optional: underlyings to leave out
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    MID_ZONE = (25.0, 75.0)                  # percentile of the symbol among its basket, for the % from open AND the % from VWAP: Q2 to Q3
    Z_DAYS = 90                              # the z-score compares with the same minute of the earlier days within this many CALENDAR days (saved candle files)
    Z_MIN_DAYS = 20                          # fewer earlier days than this in the files: no z-score (no trade, no z exit)
    Z_ENTRY = 3.0                            # BUY when z <= -Z_ENTRY, SELL when z >= +Z_ENTRY (both exit through 0)
    Z_BAR = "5min"                           # the bar the rules run on: z is looked at once per finished bar of this size (made from the 1-minute candles)
    Z_BAR_LIVE = False                       # True: look at z on every 1-minute close (the bar still forming counts, acts every minute); False: finished bars only. The time exit runs every minute
    TARGET_PCT, STOP_PCT = None, None        # optional % move in your favour / against you that also closes a position; None = off (the z-score and the time exit close it)
    ONE_ENTRY_PER_STOCK = True               # True: each stock gets ONE entry; once it has been entered, a take-profit, stop-loss or any exit ends its trading (no new entry in it). False: it may be entered again
                                             # (the pasted script forgets this on a paste or a restart)
    QTY = 1                                  # in LOTS (the plugin multiplies it by the contract's lot size)
    ENGINE = {"strategyname": "CrossOrder", "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}      # "CrossOrder" is the engine's name for the CrossOrder plugin
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
    if not (0 <= MID_ZONE[0] < MID_ZONE[1] <= 100 and (TARGET_PCT is None or TARGET_PCT > 0) and (STOP_PCT is None or STOP_PCT > 0) and QTY >= 1 and BASKETS
            and all(len(v) >= 3 for v in BASKETS.values()) and isinstance(Z_DAYS, int) and 5 <= Z_DAYS <= 560 and isinstance(Z_MIN_DAYS, int) and 3 <= Z_MIN_DAYS and Z_ENTRY > 0
            and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)):
        raise ValueError("MID_ZONE must be inside 0 to 100, TARGET_PCT and STOP_PCT None or above 0, QTY at least 1, Z_DAYS 5 to 560, Z_MIN_DAYS at least 3, Z_ENTRY above 0, "
                         "EXIT_AT after the window opens, and every basket needs at least 3 symbols")
    BAR_S = parse_timeframe(Z_BAR)                                 # 300; refuses a size that does not line up with the clock
    Z_LOOKBACK = min(400, math.ceil(Z_DAYS * 5 / 7) + 5)           # trading days to ask absolute() for: more than fit in Z_DAYS calendar days; the older ones are dropped by date below
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

    last_px = {}                                                   # token -> the latest 1-minute close
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    traded_in_zone = {}                                            # token -> the zone it was traded in: no second trade until z has come back inside the band
    z_error_said = set()                                           # symbols whose z-score error was already printed
    entered = set()                                                # tokens already entered by this script: with ONE_ENTRY_PER_STOCK they are not entered again
    last_fail = {}
    zone_side = {"LOW": 1, "HIGH": 2}                              # z <= -Z_ENTRY -> buy (1), z >= Z_ENTRY -> sell (2)

    try:                                                           # read the earlier days of every stock NOW (one read per day file for all of them), not in the candle thread at the first signal
        absolute_warm("pct", LEVEL, sorted(set(symbol_of.values())), Z_LOOKBACK, ref="vwap")
    except Exception as e:
        print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for t in symbol_of:                                            # the last price of today's candles already built
        h = last_candles(t, 5, show=False)
        if h is not None and len(h):
            last_px[t] = float(h["Close"].iloc[-1])

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

    def zscore(symbol):
        """(z, n) for the symbol: z = (its % from the VWAP now - the mean of that % at the same minute on its earlier days) / their standard deviation, over the earlier days within Z_DAYS
        calendar days that are in the saved files; n is how many of them there were. z is None when n < Z_MIN_DAYS, the value now is missing or the days do not vary."""
        with exec_ctx:                                             # absolute() reads today's candles table once, and the saved day files
            result = absolute("pct", LEVEL, symbol, Z_LOOKBACK, ref="vwap")
        hist, _ = result
        today = date.fromisoformat(str(getattr(result, "today", None) or datetime.now(ZoneInfo("Asia/Kolkata")).date()))
        values = [float(h) for h, d in zip(hist, result.dates) if d is not None and np.isfinite(h) and 0 <= (today - date.fromisoformat(str(d))).days <= Z_DAYS]
        now = float(result.now)
        if len(values) < Z_MIN_DAYS or not np.isfinite(now):
            return None, len(values)
        sd = float(np.std(values, ddof=1))
        if not sd > 0:
            return None, len(values)
        return (now - float(np.mean(values))) / sd, len(values)

    def z_now(token):
        """The z-score of a traded stock, or None when it cannot be worked out (an error is said once per stock)."""
        try:
            return zscore(symbol_of[token])[0]
        except Exception as e:
            if token not in z_error_said:
                z_error_said.add(token)
                print(f"{symbol_of[token]}: the z-score is not available ({e})")
            return None

    def epoch_s(minute):
        """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in India too (its offset, 5 h 30 min, is a whole number of 5-minute bars)."""
        return int(pd.Timestamp(minute).timestamp())

    def z_zone(z):
        return None if z is None else "LOW" if z <= -Z_ENTRY else "HIGH" if z >= Z_ENTRY else None

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

    def step(token):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry."""
        px = last_px[token]
        pos = position.get(token)
        z = z_now(token)
        zone = z_zone(z)
        if zone is None:
            traded_in_zone.pop(token, None)                        # z is back inside the band (or unknown): a new entry may trade it again (also while a position is open)
        if pos is not None:
            if pos["state"] == "open":
                if z is not None and ((pos["side"] == 1 and z > 0) or (pos["side"] == 2 and z < 0)):
                    leave(token, f"z-score {z:+.2f} {'above' if pos['side'] == 1 else 'below'} 0")        # a BUY is closed above 0, a SELL below 0
                else:
                    move = 100.0 * (1 if pos["side"] == 1 else -1) * (px / pos["entry"] - 1.0)         # % in the position's favour (negative = against it)
                    if TARGET_PCT is not None and move >= TARGET_PCT:
                        leave(token, f"target {move:+.2f}%")
                    elif STOP_PCT is not None and move <= -STOP_PCT:
                        leave(token, f"stop-loss {move:+.2f}%")
            return
        if zone is None or (ONE_ENTRY_PER_STOCK and token in entered):
            return
        if traded_in_zone.get(token) == zone or not in_window() or not algo_sending(STRATEGY) or not pf_allowed(STRATEGY, token):
            return
        if time.time() - last_fail.get(token, 0.0) < FAIL_COOLDOWN_S:
            return
        try:
            if not in_middle(symbol_of[token]):                    # the middle of its basket
                return
        except Exception as e:
            print(f"{symbol_of[token]}: the ranking is not available ({e}); no trade")
            return
        if enter(token, zone_side[zone]):
            traded_in_zone[token] = zone

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
        for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
            tok = int(tok)
            if tok in symbol_of and close > 0:
                last_px[tok] = float(close)
                if Z_BAR_LIVE or (epoch_s(minute) + 60) % BAR_S == 0:  # Z_BAR_LIVE: every minute; else only the 1-minute candle that ends a Z_BAR bar (the bar is finished)
                    step(tok)
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
            z = z_now(token)
            print(f"   {symbol_of[token]:16} {'LONG' if p['side'] == 1 else 'SHORT'} {p['qty']} lot(s) entry {p['entry']:g} now {last_px.get(token, float('nan')):g} z {'-' if z is None else format(z, '+.2f')} PF {p['pf']} {p['state']}")
        zs = {symbol_of[t]: z_now(t) for t in symbol_of}
        near = {k: v for k, v in zs.items() if v is not None and abs(v) >= Z_ENTRY - 0.5}
        print(f"   z-score ready for {sum(1 for v in zs.values() if v is not None)} of {len(symbol_of)} futures (Z_DAYS {Z_DAYS}, at least {Z_MIN_DAYS} earlier days); "
              f"{len(near)} within 0.5 of the entry band" + (": " + ", ".join(f"{k} {v:+.2f}" for k, v in near.items()) if near else ""))

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY)                                        # pause + square up + unsubscribe every portfolio, wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["india_fut_zscore_status"], shared["india_fut_zscore_exit_all"], shared["india_fut_zscore_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_india_fut_zscore()
