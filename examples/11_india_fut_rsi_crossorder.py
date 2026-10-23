# INDIA STOCK FUTURES BASKETS + 5-MINUTE RSI + CrossOrder: 08 for the NSE. A plain script (functions, no class). Paste in the India server's terminal after the services are up (init.py).
#
#   The universe      every STOCK future in the master that has not expired (about 200; index futures are left out), taken at the NEAREST expiry. A basket entry is "SBIN FUT" = the nearest
#                     future that has not expired, on each day: the script trades today's nearest future, and every comparison with the past uses the nearest future of THAT day,
#                     so a stock is only ever compared with its own futures (never its cash price, never an option, never two contracts mixed inside one day).
#   The baskets       the sorted underlyings are dealt one by one into N_BASKETS (4) baskets, fut_1 ... fut_4, about 50 each (saved in baskets.txt too). A symbol is ranked against the OTHER
#                     symbols of its own basket only. ONLY = [...] limits the list, EXCLUDE = [...] removes some.
#   The filter        a symbol may be traded only while BOTH its % change from the OPEN and from the VWAP stand in the MIDDLE of its basket (percentile MID_ZONE = Q2 to Q3).
#   The own history   AND the future's own move (% from the OPEN and from the VWAP, now) must sit in Q2 to Q3 of the same minute on the nearest future of its previous ABS_DAYS (22) trading days:
#                     absolute() reads those days from the saved candle files, one future and one day at a time. With fewer than half of the days in the files there is no trade.
#   The signal        5-minute RSI, made from the 1-minute candles: it ENTERS oversold (< RSI_LOW) -> BUY, or overbought (> RSI_HIGH) -> SELL. One position per future at a time.
#   The history       the RSI does not start from nothing at the open: the 5-minute closes of the last HIST_DAYS saved days (candles_<day>.parquet in the market-data / EOD folders)
#                     come first, then today's candles, from each day's nearest future. On the day a stock rolls to the next expiry its price jumps by the spread between the two
#                     contracts, which can show once in the RSI that morning. No files = it says so and starts from today.
#   The exit          the script's own target and stop-loss on the price: +TARGET_PCT in its favour, or -STOP_PCT against it (checked on every 1-minute close).
#                     The engine's CrossOrder has no target or stop and reads its parameters ONCE per portfolio, so an exit is the portfolio's `squareup` command.
#   One entry         ONE_ENTRY_PER_STOCK = True: a stock is entered ONCE. After its take-profit, stop-loss or any other exit it is not entered again (until the script is pasted again).
#   The time exit     EXIT_AT (15:15 India time): the automation is stopped the way algo_stop does it: every portfolio is paused and squared up, a minute later unsubscribed (so the plugin
#                     can close first), and the script stops. No entry is made from then on. It is checked on every batch of 1-minute candles. It refuses while sending is OFF
#                     (it says so; then run india_fut_rsi_stop() by hand).
#   The order         a NEW portfolio for every trade: ONE `subscribe` that carries the fields, as for 07 (the engine knows the plugin as "CrossOrder"; SIDE 1 = buy, 2 = sell):
#                     {"strategyname": "CrossOrder", "TOKEN": <the future>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#                     The NATS service adds USERIDENTIFIER and user_id. The book gives the portfolio numbers; each future may use up to 100 a day.
#   Needs             the master and the candles running for the FUTURES (candles_start(kind="FUT") is enough, no options needed), and the saved candles_<day>.parquet files for the history.
#
# EVERYTHING is inside the function run_india_fut_rsi() (not named like the strategy: its console window is published under the strategy name): the terminal has ONE set of
# names shared by every script you paste. The only names left in the terminal start with india_fut_rsi:
#   india_fut_rsi_status()   india_fut_rsi_exit_all()   india_fut_rsi_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops; it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it (square it up from the console).

def run_india_fut_rsi():
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
    USER = 3
    STRATEGY = "india_fut_rsi"               # its name: recorded with every portfolio, the name of its sending switch and of its console window
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
    ]                               # optional: trade only these underlyings, e.g. ["SBIN", "TCS", "INFY"]; empty = every stock future
    EXCLUDE = []                             # optional: underlyings to leave out
    LEVEL = "intraday"                       # relative() level: today's open and VWAP
    MID_ZONE = (25.0, 75.0)                  # percentile of the symbol among its basket, for the % from open AND the % from VWAP: Q2 to Q3
    ABS_DAYS = 22                            # absolute(): compare the stock with itself at the same minute of its previous 22 trading days (saved files); 0 = off
    ABS_ZONE = MID_ZONE                      # the percentile band that comparison must fall in (Q2 to Q3)
    RSI_BAR = "5min"                         # the bar the RSI is computed on (made from the 1-minute candles)
    RSI_N, RSI_HIGH, RSI_LOW = 14, 70, 30
    RSI_LIVE = False                          # True: the bar still forming counts too (acts every minute); False: finished bars only
    ACT_ON_BAR_END = True                    # True: look for an ENTRY only on the 1-minute candle that ends an RSI_BAR bar (:05, :10, :15 ... for 5min). Take-profit and stop-loss are still checked on every
                                             # 1-minute close. False: look for an entry on every 1-minute candle
    HIST_DAYS = 3                            # how many earlier saved days seed the RSI (0 = none)
    TARGET_PCT, STOP_PCT = 1.5, 0.5
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
    if not (0 <= MID_ZONE[0] < MID_ZONE[1] <= 100 and TARGET_PCT > 0 and STOP_PCT > 0 and QTY >= 1 and HIST_DAYS >= 0 and BASKETS and all(len(v) >= 3 for v in BASKETS.values())
            and isinstance(ABS_DAYS, int) and 0 <= ABS_DAYS <= 400 and 0 <= ABS_ZONE[0] < ABS_ZONE[1] <= 100 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)):
        raise ValueError("MID_ZONE and ABS_ZONE must be inside 0 to 100, TARGET_PCT and STOP_PCT above 0, QTY at least 1, HIST_DAYS 0 or more, ABS_DAYS 0 to 400, EXIT_AT after the window opens, "
                         "and every basket needs at least 3 symbols")
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

    bars = {t: {} for t in symbol_of}                              # token -> {5-minute bar number: the last 1-minute close inside it}: what the RSI is computed from
    last_px = {}                                                   # token -> the latest 1-minute close
    position = {}                                                  # token -> {side, entry, qty, pf, state}   state: entering | open | exiting
    traded_in_zone = {}                                            # token -> the zone it was traded in: no second trade until the RSI has left it
    entered = set()                                                # tokens already entered by this script: with ONE_ENTRY_PER_STOCK they are not entered again
    last_fail = {}
    zone_side = {"OVERSOLD": 1, "OVERBOUGHT": 2}                   # 1 = buy, 2 = sell

    def epoch_s(minute):
        """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in India too (its offset, 5 h 30 min, is a whole number of 5-minute bars)."""
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
            print("history: pyarrow is not installed, the RSI starts from today's candles")
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
            print(f"history: no candles_<day>.parquet before {today} in {', '.join(folders) or 'any folder'}: the RSI starts from today's candles")
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

    def step(token, entry_ok=True):
        """One new 1-minute close of a traded stock: manage its position, or look for an entry (only when `entry_ok`: with ACT_ON_BAR_END that is the candle that ends a bar)."""
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
        if middle and enter(token, zone_side[zone]):
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
            if tok in bars and close > 0:
                add_close(tok, minute, close)
                last_px[tok] = float(close)
                step(tok, not ACT_ON_BAR_END or (epoch_s(minute) + 60) % BAR_S == 0)       # this candle ends a bar of RSI_BAR: an entry may be looked for
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
        ready = sum(1 for t in symbol_of if rsi(closes(t)) is not None)
        print(f"   RSI is ready for {ready} of {len(symbol_of)} stocks ({RSI_BAR} bars, {RSI_N + 1} needed)")

    tick_start()
    listener = watch("candles", on_candle, owner=STRATEGY)         # follows the table if candles are restarted; pasting again replaces this listener

    def stop():
        listener.stop()
        if tick_state:
            tick_state[1].stop()
            tick_jobs.put(None)
        algo_stop(STRATEGY)                                        # pause + square up + unsubscribe every portfolio, wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["india_fut_rsi_status"], shared["india_fut_rsi_exit_all"], shared["india_fut_rsi_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_india_fut_rsi()
