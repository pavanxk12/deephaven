# US BASKET + EXTREME FILTERS + CrossOrder: the US version of 13. A plain script (functions, no class). Paste in the US server's terminal after the services are up (init.py).
#
#   The basket        BASKETS = {"tech": [...], "chips": [...], "other": [...]}: named sector baskets. A symbol is ranked against the OTHERS of its own basket only (each saved in baskets.txt too).
#   The signal        at the end of every SIGNAL_BAR bar (the candle that ends it: :05, :10, :15 ...), for each stock that has no position, four filters in this order:
#                     0. TODAY      the latest SIGNAL_BAR close must be the HIGHEST of today's SIGNAL_BAR closes so far (a BUY) or the LOWEST (a SELL) (DAY_EXTREME); a stock that is neither is
#                                   dropped here, before anything else is asked.
#                     1. BASKET AVG the basket's own AVERAGE % change from the open (across every member, from the same relative() read filter 2 uses): BUY needs it at least BASKAVG_LONG,
#                                   SELL needs it at or below BASKAVG_SHORT. A basket-wide momentum gate, not a per-stock ranking.
#                     2. RELATIVE   the stock must be EXTREME against its basket (relative()), for the % change from the OPEN and the % change from the VWAP. BUY needs it the BEST of the basket
#                                   (percentile 100) on both; SELL needs it WEAK (below EXTREME[0]) on both. DIRECTION_MATCH = True: BUY needs the best, SELL the weak side. False: either passes.
#                     3. ABSOLUTE   the stock must also be EXTREME against its own earlier days at the same minute (absolute(), ABS_DAYS = 22 saved days). BUY needs it above 90 on both refs;
#                                   SELL needs it below EXTREME[0] on both, on the same side as filter 2.
#                     4. RVOL       today's cumulative volume so far (absolute("volume", ...)) must be at least RVOL_MIN_MULT times the AVERAGE of the same point in the session over the
#                                   previous ABS_DAYS days. Not directional (no side of its own): a gate on both BUY and SELL alike. Off when RVOL_MIN_MULT is None, or ABS_DAYS is 0.
#                     5. TURNOVER   today's cumulative traded VALUE (price x volume, dollars) so far must be at least TURNOVER_MIN. A flat floor, not a ranking: it screens out stocks too
#                                   thin to trade in size, regardless of what the other filters say. Not directional. Off when TURNOVER_MIN is None.
#                     Each filter runs only for the stocks that passed the one before it. With fewer than half of the days in the files absolute() cannot tell and there is no trade.
#                     A trade needs the sides of 0 and 1 (and 2, and 3 when ABS_DAYS > 0) to agree on exactly one side.
#                     USE_IV = False: no IV filter (the US has no straddle series; True would make every entry say "cannot tell").
#   The exit          the script's own target and stop-loss on the price: +TARGET_PCT in its favour, or -STOP_PCT against it, on every price tick of the live feed for the open positions (TICK_EXITS)
#                     and on each 1-minute close. TRAIL_TRIGGER_PCT / PROFIT_LOCK_PCT (both None, or both set): once the move reaches TRAIL_TRIGGER_PCT the stop shifts, for the rest of that
#                     position's life, from -STOP_PCT to PROFIT_LOCK_PCT.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:55 New York time, 5 minutes before the 16:00 close): the automation is stopped the way algo_stop does it: every portfolio is paused and squared up, a minute
#                     later unsubscribed (so the plugin can close first), and the script stops. No entry is made from then on. It refuses while sending is OFF (it says so; then run directional_stop() by hand).
#   The order         ENGINE["strategyname"] = "LimitOrder" (the engine plugin that rests a limit order and MODIFYs it in place on later params, never CrossOrder's immediate fill):
#                     {"strategyname": "LimitOrder", "TOKEN": <contract>, "SIDE": 1|2, "QTY": n lots, "PRICE": <int>, "SERVERCODE": ..., "ACCOUNT": "PRO", "ALGOID": "1", "MAX_TRIES": 5}
#   The spread gate   ENTRY and EXIT both wait for the live spread (SPREAD_MAX, from fo_market_data's b0p/a0p/ltp) to tighten before sending anything. An ENTRY that never tightens within
#                     SPREAD_WAIT_S is dropped (no trade); an EXIT fires anyway once it times out (a stop-loss/target must never just hang unsent). Once resting, an order is "chased": every
#                     CHASE_EVERY_S, if the spread is still OK and the best price has moved, an "apply" (not a new subscribe) re-prices it in place, up to CHASE_TRIES times; after that it
#                     is paused (cancelled) and, for an entry, unsubscribed -- nothing more. An EXIT is a brand-new portfolio (opposite side, NOT the original entry's `squareup`): the
#                     original entry pf is left open, now offset by this new one. Its QTY is the REAL filled amount read from the `orders_trade` table (token, buy_sell, fill_quantity,
#                     fill_price, PF), never the original target -- so a partially filled entry is still closed exactly. The book gives the portfolio numbers; each stock may use up to 100 a day.
#                     CHECK on the US server before real money: SERVERCODE and ACCOUNT are India's values.
#
# EVERYTHING is inside the function run_directional() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of
# names shared by every script you paste, so top-level `step` or `closes` would overwrite another running script's. The only names left in the terminal start with directional:
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
    from algo.ui.filters import PF_PER_USER             # orders_trade's own PF column is wire-level: USER * PF_PER_USER + the book's pf (31002 = user 3, book pf 1002)

    # =============================== USER INPUT ===============================
    USER = 3
    STRATEGY = "directional"               # its name: recorded with every portfolio, the name of its sending switch and of its console window
    BASKETS = {                              # named sector baskets: a symbol is ranked only against the OTHERS of ITS basket (each saved as its own basket.txt entry)
        "tech":   ["MSFT", "GOOG", "META", "AMZN", "NFLX", "SHOP", "CRWD", "PANW", "PLTR", "AAPL", "TSLA"],
        "chips":  ["NVDA", "AVGO", "AMD", "MU", "ASML", "INTC", "AMAT", "MRVL", "KLAC", "ADI", "QCOM", "ARM", "SNDK", "TXN", "CSCO"],
        "other":  ["WMT", "COST", "PEP", "AMGN", "LIN", "GILD", "SPCX"],
    }

    TRADE_AS = {}                            # optional: {"AAPL": "<another contract name or token>"} trade another contract than the basket entry
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    BASKAVG_LONG = 0.1                       # filter 1: BUY needs the basket's AVERAGE % change from the open to be at least this
    BASKAVG_SHORT = -0.1                     # filter 1: SELL needs the basket's AVERAGE % change from the open to be at or below this
    REL_BUY = {"pct_open": 80.0, "pct_vwap": 51.0, "iv": 80.0}         # relative(): BUY needs every ref at least its own threshold here
    REL_SELL = {"pct_open": 20.0, "pct_vwap": 49.0, "iv": 20.0}           # relative(): SELL needs every ref below its own threshold here
    ABS_BUY = {"pct_open": 80.0, "pct_vwap": 51.0, "iv": 80.0}            # absolute(): BUY needs every ref at least its own threshold here
    ABS_SELL = {"pct_open": 20.0, "pct_vwap": 49.0, "iv": 20.0}           # absolute(): SELL needs every ref below its own threshold here
    DIRECTION_MATCH = True                   # True: a BUY needs the REL_BUY/ABS_BUY side everywhere, a SELL the REL_SELL/ABS_SELL side.
                                             # False: either side passes
    USE_IV = False                           # False: % from open / VWAP only. True would also need an IV series (the US has none), so every entry would say "cannot tell"
    DAY_EXTREME = True                       # True: filter 0, the latest SIGNAL_BAR close (5min) must be the highest (BUY) or the lowest (SELL) of today's SIGNAL_BAR closes so far (at least 2 bars today). False: off
    ABS_DAYS = 22                            # absolute(): compare the stock with itself at the same minute of its previous 22 trading days (saved files); 0 = off
    RVOL_MIN_MULT = 1.25                      # filter 4: today's cumulative volume so far must be at least this many times the AVERAGE of the same point in the session over the
                                             # previous ABS_DAYS days (relative volume). None = off; has no effect while ABS_DAYS is 0 (no history to average)
    TURNOVER_MIN = 100_000_000                # filter 5: today's cumulative traded VALUE (price x volume, dollars) so far must be at least this. A flat floor, not a ranking against
                                             # earlier days or other stocks: it only screens out stocks too thin to trade in size. None = off
    SIGNAL_BAR = "5min"                         # the bar filter 0 (TODAY) checks today's highest/lowest close on (made from the 1-minute candles)
    ACT_ON_BAR_END = True                    # True: look for an ENTRY only on the 1-minute candle that ends an SIGNAL_BAR bar (:05, :10, :15 ... for 5min). Take-profit and stop-loss are still checked on every
                                             # 1-minute close. False: look for an entry on every 1-minute candle
    TARGET_PCT, STOP_PCT = 6.0, 1.0
    TRAIL_TRIGGER_PCT = 1.0                 # None: plain TARGET_PCT / STOP_PCT. A number: once the move in its favour reaches this, the stop shifts from -STOP_PCT to PROFIT_LOCK_PCT
    PROFIT_LOCK_PCT = 0.5                   # the new stop level once TRAIL_TRIGGER_PCT is reached (e.g. 0.3 locks in at least +0.3%); must be set together with TRAIL_TRIGGER_PCT
    TRAIL_STEP_PCT = 1.0                     # None: the lock stays at PROFIT_LOCK_PCT once trailing starts. A number: from then on the lock continuously follows the move, held at
                                             # move - TRAIL_STEP_PCT (never lowered), so once the move is up X% the lock is up (X - TRAIL_STEP_PCT)% -- it never gives back more than TRAIL_STEP_PCT
    ONE_ENTRY_PER_STOCK = True               # True: each stock gets ONE entry; once it has been entered, a take-profit, stop-loss or any exit ends its trading (no new entry in it). False: it may be entered again
                                             # (the pasted script forgets this on a paste or a restart)
    QTY = 1                                  # in LOTS (the plugin multiplies it by the contract's lot size)
    ENTRY_VALUE = 5000                       # None: every entry is QTY lots. A number (dollars): the quantity at entry is int(ENTRY_VALUE / the last 1-minute close), QTY is then not used;
                                             # if one share costs more than ENTRY_VALUE the quantity would be 0 and that entry is skipped (it says so)
    ENGINE = {"strategyname": "LimitOrder", "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1","MAX_TRIES":5}      # "CrossOrder" is the engine's name for the CrossOrder plugin
    WINDOW = (dtime(10, 0), dtime(13, 45))   # entries only inside it (New York time, the regular session is 09:30 to 16:00); an exit is sent at any time
    EXIT_AT = dtime(15, 45)                  # time exit (New York time): 5 minutes before the 16:00 close, the automation is stopped like algo_stop (pause, square up, unsubscribe every portfolio) and no entry is made after it
    MAX_PORTFOLIOS = 28                      # portfolios this script may hold in the engine at once: open positions plus closed ones waiting for their unsubscribe. The engine seems to keep about
                                             # 32 plugin instances in all (all scripts together), so two scripts at the same time need smaller numbers. No entry while the script is at the limit.
    UNSUB_AFTER_S = 60                       # a closed position's portfolio is unsubscribed this many seconds after its exit (take-profit, stop-loss, z-exit) so the engine frees the slot;
                                             # the wait lets the plugin send and fill its closing order first. None = never (the slot is then held until the stop at the end of the day)
    TICK_EXITS = True                        # True: target and stop-loss are also checked on every price tick of the live feed (FEED_TABLE) for the open positions only, not just on each 1-minute close.
                                             # The candle check stays as the backup, and is used alone if the tick listener cannot be set up (it says so). Needs TARGET_PCT or STOP_PCT.
    FEED_TABLE = "fo_market_data"            # the live feed: columns token, ltp, b0p (best bid), a0p (best ask) (raw price; the contract's own divisor from the master turns it into the price)
    SPREAD_MAX = 0.30                        # entry and exit both wait for the live spread (best ask - ltp for a BUY-side order, ltp - best bid for a SELL-side order) to be at
                                             # most this (in price units, after the divisor) before the LimitOrder is actually sent, instead of firing it the instant the signal passes.
    SPREAD_WAIT_S = 60                       # how long an entry may wait for SPREAD_MAX before it is given up on (no trade this signal). An EXIT instead falls back to firing anyway
                                             # at the best price seen once this runs out: a stop-loss/target must not just hang unsent.
    CHASE_TRIES = 3                          # once a LimitOrder is resting, up to this many times it is re-priced ("apply", not a new subscribe) to the current best price, each time
                                             # the spread is still SPREAD_MAX-ok AND the price has moved since the last one sent. After CHASE_TRIES it is cancelled (paused) and,
                                             # for an entry, unsubscribed -- nothing more is tried for that signal.
    CHASE_EVERY_S = 2                        # how often (seconds) a resting order's chase re-checks the best price
    FAIL_COOLDOWN_S = 30
    RETRY_RANKING_S = 20                     # a ranking read that fails (a rare clash with the update cycle: "Previous values requested or inconsistent") is tried again on the next candle batches
                                             # for this many seconds instead of giving up on that stock for the whole bar. 0 = no retry
    # ==========================================================================
    REFS = ("pct_open", "pct_vwap", "iv")
    if not (BASKAVG_SHORT < BASKAVG_LONG and all(0 <= SELL[ref] < BUY[ref] <= 100 for SELL, BUY in ((REL_SELL, REL_BUY), (ABS_SELL, ABS_BUY)) for ref in REFS)
            and (TARGET_PCT is None or TARGET_PCT > 0) and (STOP_PCT is None or STOP_PCT > 0) and QTY >= 1 and (ENTRY_VALUE is None or ENTRY_VALUE > 0) and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and ((TRAIL_TRIGGER_PCT is None) == (PROFIT_LOCK_PCT is None)) and (TRAIL_TRIGGER_PCT is None or TRAIL_TRIGGER_PCT > 0)
            and (TRAIL_STEP_PCT is None or (TRAIL_TRIGGER_PCT is not None and TRAIL_STEP_PCT > 0))
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and (RVOL_MIN_MULT is None or RVOL_MIN_MULT > 0)
            and (TURNOVER_MIN is None or TURNOVER_MIN > 0)
            and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)
            and SPREAD_MAX > 0 and SPREAD_WAIT_S >= 0 and isinstance(CHASE_TRIES, int) and CHASE_TRIES >= 0 and CHASE_EVERY_S > 0):
        raise ValueError("BASKAVG_SHORT below BASKAVG_LONG; every SELL threshold must be below its BUY threshold, both inside 0 to 100, for pct_open, pct_vwap and iv in REL_BUY/REL_SELL/ABS_BUY/ABS_SELL, "
                         "TARGET_PCT and STOP_PCT (if set) above 0, TRAIL_TRIGGER_PCT and PROFIT_LOCK_PCT must both be None or both set (and TRAIL_TRIGGER_PCT above 0), "
                         "QTY at least 1, ABS_DAYS 0 to 400, RVOL_MIN_MULT None or above 0, TURNOVER_MIN None or above 0, EXIT_AT after the window opens, every basket needs at least 3 symbols, "
                         "SPREAD_MAX above 0, SPREAD_WAIT_S 0 or more, CHASE_TRIES a whole number 0 or more, CHASE_EVERY_S above 0, "
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

    bars = {t: {} for t in symbol_of}                              # token -> {5-minute bar number: the last 1-minute close inside it}: what the TODAY filter checks
    last_px = {}                                                   # token -> the latest 1-minute close (candle)
    last_tick = {}                                                 # token -> the latest ltp from fo_market_data (tick, not candle): what spread_ok() compares bid/ask against
    best = {}                                                      # token -> (bid, ask), the latest best quotes from fo_market_data's b0p/a0p
    filled = {}                                                    # pf -> cumulative fill_quantity from orders_trade, for every pf this script holds: the REAL filled size
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting (qty is the TARGET; filled[pf] is the real size)
    pending_entry = {}                                             # token -> {side, deadline}: a signal passed, waiting for SPREAD_MAX before the LimitOrder is actually sent
    pending_exit = {}                                              # token -> {why, qty, deadline}: an exit was triggered, waiting for SPREAD_MAX (falls back to firing anyway past the deadline);
                                                                   # qty is the real filled amount of the entry pf at the moment it was armed, not the original target
    chase = {}                                                     # pf -> {token, side, qty, price, tries, kind}: a resting LimitOrder being re-priced ("entry" or "exit")
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
        """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in New York (its offsets are whole hours)."""
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
            if USE_IV:
                absolute_warm("iv", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS)
            if RVOL_MIN_MULT is not None:
                absolute_warm("volume", LEVEL, sorted(set(symbol_of.values())), ABS_DAYS)
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    for t in symbol_of:                                            # today's candles already built
        h = last_candles(t, 400, show=False)
        if h is not None:
            for m, c in zip(h["Minute"], h["Close"]):
                add_close(t, m, c)
            if len(h):
                last_px[t] = float(h["Close"].iloc[-1])

    def day_sides(token):
        """Filter 0: the sides today's SIGNAL_BAR closes allow. {1} (BUY) when the latest bar close is the highest of today so far, {2} (SELL) when it is the lowest, empty when it is neither or when
        there are fewer than 2 bars today (the first one is both). Only today's bars count. Everything is allowed when DAY_EXTREME is False."""
        if not DAY_EXTREME:
            return {1, 2}
        b = bars[token]
        if not b:
            return set()
        keys = sorted(b, reverse=True)
        day = datetime.fromtimestamp(keys[0] * BAR_S, ZoneInfo("America/New_York")).date()
        today = []
        for k in keys:                                             # newest first, until an earlier day begins
            if datetime.fromtimestamp(k * BAR_S, ZoneInfo("America/New_York")).date() != day:
                break
            today.append(b[k])
        if len(today) < 2:
            return set()
        return ({1} if today[0] >= max(today) else set()) | ({2} if today[0] <= min(today) else set())

    # ---- the filters: EXTREME against the basket (relative), EXTREME against the stock's own days (absolute) ------------------------
    def sides_of(strong, weak):
        """{1} (BUY) when `strong` holds, {2} (SELL) when `weak` holds. With DIRECTION_MATCH False, {1, 2} when either one holds. Empty when neither."""
        if DIRECTION_MATCH:
            return ({1} if strong else set()) | ({2} if weak else set())
        return {1, 2} if (strong or weak) else set()

    def ranked_sides(call, buy, sell):
        """Collects pct_open, pct_vwap (and iv, when USE_IV) via `call(series, ref)`, a no-basket-no-level relative()/absolute() wrapper. BUY needs every ref at least `buy[ref]`;
        SELL needs every ref below `sell[ref]`. Empty when one of them cannot be told, or is unknown."""
        pctiles = {}
        for ref in ("pct_open", "pct_vwap"):
            with exec_ctx:                                         # the call reads the candles table; a listener thread needs the terminal's query context for that
                result = call("pct", ref[len("pct_"):])
            if result[1] < 0:                                      # (the second item is -1 when it cannot be told)
                return set()
            pctiles[ref] = result.pctile
        if USE_IV:
            with exec_ctx:
                result = call("iv", None)
            if result[1] < 0:
                return set()
            pctiles["iv"] = result.pctile
        if any(p != p for p in pctiles.values()):
            return set()
        return sides_of(all(p >= buy[ref] for ref, p in pctiles.items()), all(p < sell[ref] for ref, p in pctiles.items()))

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
        """Filter 2, relative(): the ranking among the OTHER symbols of ITS basket, for pct_open, pct_vwap and iv (if USE_IV), each against its own REL_BUY / REL_SELL threshold."""
        members = BASKETS[basket_of[symbol]]
        return ranked_sides(lambda series, ref: relative(series, LEVEL, symbol, members, **({"ref": ref} if ref else {})), REL_BUY, REL_SELL)

    def absolute_sides(symbol):
        """Filter 3, absolute(): the ranking among the same minute of its previous ABS_DAYS trading days, for pct_open, pct_vwap and iv (if USE_IV), each against its own
        ABS_BUY / ABS_SELL threshold."""
        return ranked_sides(lambda series, ref: absolute(series, LEVEL, symbol, ABS_DAYS, **({"ref": ref} if ref else {})), ABS_BUY, ABS_SELL)

    def rvol_ok(symbol):
        """Filter 4: today's cumulative volume so far must be at least RVOL_MIN_MULT times the AVERAGE of the same point in the session over the previous ABS_DAYS days.
        Not a ranking: True/False only (no side), so it never decides BUY vs SELL, only whether there is enough volume behind the move to trade it at all."""
        with exec_ctx:                                             # the call reads the candles table; a listener thread needs the terminal's query context for that
            result = absolute("volume", LEVEL, symbol, ABS_DAYS)
        if result[1] < 0:                                          # (the second item is -1 when it cannot be told)
            return False
        avg = float(np.mean(result[0][np.isfinite(result[0])]))
        return avg > 0 and result.now >= RVOL_MIN_MULT * avg

    def turnover_ok(symbol):
        """Filter 5: today's cumulative traded VALUE (price x volume, dollars) so far must be at least TURNOVER_MIN. A flat floor, not a ranking: `now` is set regardless of
        whether absolute() could rank it against earlier days, so this works even on a stock with no (or too few) saved days."""
        with exec_ctx:                                             # the call reads the candles table; a listener thread needs the terminal's query context for that
            result = absolute("turnover", LEVEL, symbol, max(ABS_DAYS, 1))
        return result.now >= TURNOVER_MIN

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
        """One CHASE_EVERY_S check of a resting order: re-price it (an "apply") if the spread is still ok AND the price moved since the last one sent; cancel (and, for an
        entry, unsubscribe) once CHASE_TRIES is used up. Exit-side chasing is symmetric here, but the engine's LimitOrder plugin only actually MODIFYs an ENTRY order --
        an exit's "apply" calls are harmless no-ops there (SquareUpEvent orders are never modified), kept only so an exit is cancelled the same way after CHASE_TRIES."""
        c = chase.get(pf)
        if c is None:
            return
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
        if not spread_ok(c["token"], c["side"]):
            return                                                # spread went bad again: wait, this check doesn't count as a try
        price = best_price(c["token"], c["side"])
        if price == c["price"]:
            return                                                # unchanged: wait, doesn't count as a try either
        send_modify(pf, c["qty"], price)
        c["price"] = price
        c["tries"] += 1

    def maybe_untrack(token):
        """Stop following a token's ticks once nothing still needs them: no pending entry/exit, no open position, no live chase."""
        if token in pending_entry or token in pending_exit or token in position:
            return
        if any(c["token"] == token for c in chase.values()):
            return
        tick_track(token, False)

    def fire_entry(token):
        """The signal's spread finally tightened (or was already tight): send the real entry now."""
        side = pending_entry.pop(token, {}).get("side")
        if side is None:
            return
        if len(live) >= MAX_PORTFOLIOS:
            if not at_limit_said:
                at_limit_said.append(True)
                print(f"{STRATEGY}: {len(live)} portfolios held (MAX_PORTFOLIOS {MAX_PORTFOLIOS}); no new entry until an exit has been unsubscribed")
            maybe_untrack(token)
            return
        if next_slot[token] >= 100:
            print(f"{symbol_of[token]}: all 100 portfolios of the day are used, no trade")
            maybe_untrack(token)
            return
        qty = QTY if ENTRY_VALUE is None else int(ENTRY_VALUE / last_px[token])         # by value: whole units for ENTRY_VALUE at the last close
        if qty < 1:
            print(f"{symbol_of[token]}: ENTRY_VALUE {ENTRY_VALUE:g} buys less than one unit at {last_px[token]:g}, no trade")
            maybe_untrack(token)
            return
        a = pf_assign(STRATEGY, token, token, name=symbol_of[token], slot=next_slot[token])       # the BOOK gives a NEW portfolio number (None while OFF or unticked)
        if not a:
            maybe_untrack(token)
            return
        next_slot[token] += 1
        pf = a["pf"]
        price = best_price(token, side)
        live.add(pf)
        pf_side[pf] = side
        entered.add(token)
        position[token] = {"side": side, "entry": last_px[token], "qty": qty, "pf": pf, "state": "entering", "trailing": False, "lock": None}
        chase[pf] = {"token": token, "side": side, "qty": qty, "price": price, "tries": 0, "kind": "entry"}
        print(f"{symbol_of[token]}: {'BUY' if side == 1 else 'SELL'} {qty} lot(s) at {price} (LimitOrder) on PF {pf}")
        send_place(pf, token, side, qty, price).add_done_callback(lambda f: settled(token, f))

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
        chase[pf] = {"token": token, "side": closing_side, "qty": qty, "price": price, "tries": 0, "kind": "exit"}
        print(f"{symbol_of[token]}: closing {qty} on NEW PF {pf} at {price}{' (forced, spread never tightened)' if forced else ''}")
        send_place(pf, token, closing_side, qty, price).add_done_callback(lambda f: settled_exit(token, pf, f))

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
        qty_open = filled.get(pos["pf"], 0)
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

    def settled_exit(token, pf, future):
        """The exit's own pf answered (called from the NATS thread)."""
        try:
            ok = bool(future.result().ok)
            why = "" if ok else future.result().status
        except Exception as e:
            ok, why = False, repr(e)
        if ok:
            position.pop(token, None)                              # ack, not fill (the same approximation already used for entries): treated as closed from here
            maybe_untrack(token)
            if UNSUB_AFTER_S is not None:
                timer = threading.Timer(UNSUB_AFTER_S, release, (pf, token))        # the slot is freed a little later: the plugin may still be filling it
                timer.daemon = True
                timer.start()
            else:
                live.discard(pf)
        else:
            live.discard(pf)
            chase.pop(pf, None)
            pos = position.get(token)
            if pos is not None:
                pos["state"] = "open"
            print(f"{symbol_of[token]}: exit FAILED ({why}); still open, will retry on the next signal")

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
                    arm_exit(token, why)
            return
        if token in pending_entry or not entry_ok or (ONE_ENTRY_PER_STOCK and token in entered):
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
            if RVOL_MIN_MULT is not None and ABS_DAYS > 0 and not rvol_ok(symbol_of[token]):      # 4. rvol: enough volume behind the move (not directional)
                count("refused by rvol")
                return
            if TURNOVER_MIN is not None and not turnover_ok(symbol_of[token]):                    # 5. turnover: a flat dollar floor on today's traded value (not directional)
                count("refused by turnover")
                return
            side = next(iter(sides))
        except Exception as e:
            ranking_failed(token, first, e)
            return
        last_cross[token] = bar
        arm_entry(token, side)
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
                ends_bar = (epoch_s(minute) + 60) % BAR_S == 0                              # this candle completes a bar of SIGNAL_BAR
                step(tok, not ACT_ON_BAR_END or ends_bar, ends_bar)
        for tok, t0 in due:
            if time.time() - t0 >= RETRY_RANKING_S:
                retry.pop(tok, None)                               # waited long enough: given up on this bar
            elif tok in retry:
                step(tok, True, True)
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
    shared["directional_status"], shared["directional_exit_all"], shared["directional_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_directional()
