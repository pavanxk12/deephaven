# INDIA MULTI-UNDERLYING "OVERSOLD BOUNCE" CALL DEBIT SPREAD + CrossOrder: a plain script (functions, no class). Paste in the India server's
# terminal after the services are up (init.py) AND the straddle is running with the OPTIONS in the candles (straddle_start()).
#
#   The idea          every filter below is a sign of an underlying having just been sold off hard; the bet is a BOUNCE, taken with a CALL
#                     debit spread (defined risk, the most it can lose is the net premium paid): BUY OTM1_STEPS out-of-the-money CE, SELL
#                     OTM3_STEPS out-of-the-money CE, same expiry, same lot count.
#   The universe      UNDERLYINGS names each one to trade (its own nearest FUTURE for the window check's "synthetic", its own nearest
#                     OPTION expiry for the two CE legs and for the straddle V), with its own QTY (lots) and OTM1_STEPS / OTM3_STEPS.
#   The signal        on EVERY new straddle_series point (about once a minute) of an underlying with no open position, in the WINDOW
#                     (09:20 to 15:00 India time, checked every minute, not just on a bar close), ALL of these together:
#                       1. the straddle's Synthetic (the put-call-parity future) % change from today's open is a NEW running low of the
#                          session (<= every earlier value today) AND is negative.
#                       2. against the same minute of its previous ABS_DAYS (22) trading days, that same underlying's price % change from
#                          the open (absolute() has no series for the straddle's own Synthetic column, so its FUTURE's own price is used
#                          as the stand-in: the two track each other closely) ranks at or below the 50th percentile (Q1/Q2: a weak day).
#                       3. the straddle's V (the straddle cost) % change from today's open is negative.
#                       4. against the same minute of its previous ABS_DAYS trading days, V's % change from the open ranks at or below the
#                          50th percentile (Q1/Q2).
#                       5. V sits in the LOWER quarter of its own range SO FAR TODAY ((V - today's low) / (today's high - today's low) <= 0.25).
#                     Everything (1), (3) and (5) is tracked live in the script from the straddle_series table itself; (2) and (4) are
#                     absolute() reads against the saved straddle_<day>.parquet / candles_<day>.parquet files.
#   The exit          the SPREAD's own combined P&L, as a % of its net entry debit (long leg price - short leg price), checked on every
#                     price TICK of EITHER leg from the live feed (fo_market_data; the entry fill price itself also comes from there, not
#                     from candles -- the CE legs' prices are fo_market_data only, start to finish): -STOP_PCT against it, or TRAIL_TRIGGER_PCT /
#                     PROFIT_LOCK_PCT / TRAIL_STEP_PCT once it has moved far enough in its favour (the same trailing rule as 11/13/14). No fixed
#                     TARGET_PCT by default (TRAIL alone). Then `squareup` on BOTH legs' portfolios, each unsubscribed UNSUB_AFTER_S seconds later.
#   Re-entry          an underlying with NO open position may be entered; one WITH an open position never gets a second one (no separate
#                     portfolio juggling for two spreads on the same name at once). Once that position exits it is free to be entered again
#                     at the next signal (unlike 11/13's ONE_ENTRY_PER_STOCK, there is no permanent per-day ban here).
#   The time exit     EXIT_AT (15:10 India time): the automation is stopped the way algo_stop does it (pause, square up, a minute later
#                     unsubscribe every portfolio, then the script stops). No entry is made after it.
#   The order         TWO NEW portfolios per trade (one per leg), each ONE `subscribe` with the CrossOrder fields, as for 11/13:
#                     {"strategyname": "CrossOrder", "TOKEN": <the CE>, "SIDE": 1|2, "QTY": n lots, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
#   Needs             the master, the candles for the FUTURES (candles_start() -- the CE legs' own prices come from fo_market_data, not
#                     candles, so the options do not need to be in the candles for this script), and the straddle running (straddle_start();
#                     the script starts it itself if it is not already running) with straddle_<day>.parquet files saved for the ABS_DAYS
#                     history (absolute_warm() preloads what it can).
#
# EVERYTHING is inside the function run_dipbounce() (not named like the strategy: its console window is published under the strategy name):
# the terminal has ONE set of names shared by every script you paste. The only names left in the terminal start with dipbounce:
#   dipbounce_status()   dipbounce_exit_all()   dipbounce_stop()   (stop pauses, squares up and unsubscribes every portfolio, then stops;
#                                                                     it refuses while sending is OFF)
# Positions are kept in the script only: if you stop it or Deephaven restarts while a position is open, it no longer knows about it
# (square it up from the console).

def run_dipbounce():
    import queue
    import threading
    import time
    from contextlib import nullcontext
    from datetime import datetime, time as dtime
    from zoneinfo import ZoneInfo

    import numpy as np

    from algo import current
    from algo.comms.nats_service import NO_SERVICE

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
    # Every value below is read from params.ini in this folder, section [dipbounce].
    # Change a number there, not here; this block only names what the script needs.
    PARAMS_SECTION = "dipbounce"
    P = _params(PARAMS_SECTION)
    print(f"{P.section}: {len(P)} parameters from {P.path}")
    
    USER = P["USER"]
    STRATEGY = P["STRATEGY"]
    UNDERLYINGS = P["UNDERLYINGS"]
    LEVEL = P["LEVEL"]
    ABS_DAYS = P["ABS_DAYS"]
    PCTILE_MAX = P["PCTILE_MAX"]
    V_RANGE_MAX = P["V_RANGE_MAX"]
    WINDOW = (P.time("WINDOW_START"), P.time("WINDOW_END"))
    EXIT_AT = P.time("EXIT_AT")
    STOP_PCT = P["STOP_PCT"]
    TRAIL_TRIGGER_PCT = P["TRAIL_TRIGGER_PCT"]
    PROFIT_LOCK_PCT = P["PROFIT_LOCK_PCT"]
    TRAIL_STEP_PCT = P["TRAIL_STEP_PCT"]
    TARGET_PCT = P["TARGET_PCT"]
    STRADDLE_N = P["STRADDLE_N"]
    ENGINE = P["ENGINE"]
    MAX_PORTFOLIOS = P["MAX_PORTFOLIOS"]
    UNSUB_AFTER_S = P["UNSUB_AFTER_S"]
    TICK_EXITS = P["TICK_EXITS"]
    FEED_TABLE = P["FEED_TABLE"]
    FAIL_COOLDOWN_S = P["FAIL_COOLDOWN_S"]
    # ==========================================================================

    if not TICK_EXITS:
        raise ValueError("TICK_EXITS must be True: the CE legs' prices come from fo_market_data only (not candles), so it is the only way an open position's exit is ever checked")
    if not (ABS_DAYS == 0 or (isinstance(ABS_DAYS, int) and 1 <= ABS_DAYS <= 400)) or not (0 <= PCTILE_MAX <= 100) or not (0 <= V_RANGE_MAX <= 1):
        raise ValueError("ABS_DAYS must be 0 or a whole number from 1 to 400, PCTILE_MAX must be 0 to 100, V_RANGE_MAX must be 0 to 1")
    if not (STOP_PCT > 0 and WINDOW[0] < EXIT_AT and isinstance(MAX_PORTFOLIOS, int) and MAX_PORTFOLIOS >= 0 and (UNSUB_AFTER_S is None or UNSUB_AFTER_S >= 0)
            and ((TRAIL_TRIGGER_PCT is None) == (PROFIT_LOCK_PCT is None)) and (TRAIL_TRIGGER_PCT is None or TRAIL_TRIGGER_PCT > 0)
            and (TRAIL_STEP_PCT is None or (TRAIL_TRIGGER_PCT is not None and TRAIL_STEP_PCT > 0))):
        raise ValueError("STOP_PCT must be above 0, EXIT_AT after the window opens, TRAIL_TRIGGER_PCT and PROFIT_LOCK_PCT must both be None or both set "
                          "(and TRAIL_TRIGGER_PCT above 0), and TRAIL_STEP_PCT (if set) needs TRAIL_TRIGGER_PCT set too and must be above 0")
    for name, cfg in UNDERLYINGS.items():
        if not (cfg.get("QTY", 0) >= 1 and 1 <= cfg.get("OTM1_STEPS", 0) < cfg.get("OTM3_STEPS", 0)):
            raise ValueError(f"{name!r}: QTY must be at least 1 lot, and 0 < OTM1_STEPS < OTM3_STEPS (how many CE strikes out of the money each leg is)")

    try:
        from deephaven.execution_context import get_exec_ctx
        exec_ctx = get_exec_ctx()                                  # taken here, in the terminal: the table listener threads have none, and absolute() reads tables
    except Exception:                                              # (not in Deephaven, such as in a test)
        exec_ctx = nullcontext()

    nats_connect(user=USER, client=STRATEGY)                       # only checks the NATS service is up and this user may send
    registry_connect()                                             # ... and the portfolio registry. Neither starts anything.
    algo_start(USER, STRATEGY)                                     # its book (portfolio numbers) and its own sending switch, which starts OFF

    if current().get("straddle") is None:                          # the Synthetic/V filters come from the straddle series: start it here (it builds the whole day from the candles first)
        try:
            straddle_start(STRADDLE_N)
            print(f"{STRATEGY}: the straddle was not running: straddle_start({STRADDLE_N}) sent from here (it needs the candles WITH the options; it is stopped by the end-of-day step)")
        except Exception as e:
            print(f"{STRATEGY}: the straddle could not be started ({e}); nothing can be entered until it runs: straddle_start({STRADDLE_N}), with the options in the candles")
    else:
        print(f"{STRATEGY}: the straddle is already running, its Synthetic/V are used as they are")

    # ---- the nearest option expiry (fixed for the day: this is what straddle_series and `strikes()` are both read against) -------------
    def setup(name):
        exp = expiries(name, "OPT", show=False)
        if not exp:
            print(f"{name}: no option expiry found, left out of the trading")
            return None
        return {"key": (name.upper(), exp[0]), "qty": int(UNDERLYINGS[name]["QTY"])}

    def pick_legs(name):
        """OTM1 / OTM3 CE, picked FRESH off the CURRENT future price and CURRENT strike chain: called only when an entry actually fires,
        never cached from startup, so the strikes match where the underlying really is at that moment, not wherever it was hours earlier
        when the script was pasted. Called from the straddle_series listener thread, so (like the absolute() calls in check_entry) it
        needs the terminal's query context explicitly -- without it, `strikes()`/`last_price()` fail with "no ExecutionContext registered"
        and silently return None, which looked like "no price" rather than the real cause."""
        with exec_ctx:
            table = strikes(name, expiry=0, type="CE", show=False)
            if table is None or not len(table):
                return None
            table = table.sort_values("strike").reset_index(drop=True)
            price = last_price(name, show=False)
        if not price:
            return None
        atm = int(np.argmin(np.abs(table["strike"].to_numpy(dtype=float) - float(price))))
        cfg = UNDERLYINGS[name]
        i1, i3 = atm + cfg["OTM1_STEPS"], atm + cfg["OTM3_STEPS"]
        if i3 >= len(table):
            return None
        return {
            "long_token": int(table.iloc[i1]["token"]), "short_token": int(table.iloc[i3]["token"]),
            "long_strike": float(table.iloc[i1]["strike"]), "short_strike": float(table.iloc[i3]["strike"]),
        }

    state = {}                                                     # underlying name -> setup() dict, plus the live tracking fields below
    for name in UNDERLYINGS:
        info = setup(name)
        if info is None:
            continue
        info.update(day=None, synth_open=None, synth_pct_values=[], v_open=None, v_lo=None, v_hi=None,
                    last_synth_pct=None, last_v_pct=None, last_range_pos=None,
                    position=None)
        state[name] = info
        print(f"{name}: ready, nearest option expiry {info['key'][1]}, {info['qty']} lot(s); OTM1/OTM3 CE are picked fresh at each entry")
    if not state:
        raise ValueError("no underlying could be set up (see the messages above): nothing to trade")

    key_of = {s["key"]: name for name, s in state.items()}
    leg_tokens = {}                                                 # leg token -> underlying name: only the legs of a CURRENTLY open position (filled in by enter()/cleared on exit)
    next_slot = {}                                                  # leg token -> its next free portfolio slot (a token picked again later in the day resumes its own count)

    last_px = {}                                                    # leg token -> its latest known price (fo_market_data only: entry fill price, then every tick)
    last_fail = {}                                                  # underlying name -> when its last entry attempt failed (FAIL_COOLDOWN_S)
    counts = {}

    def count(key):
        counts[key] = counts.get(key, 0) + 1

    def in_window():
        now = datetime.now(ZoneInfo("Asia/Kolkata")).time()
        return WINDOW[0] <= now <= WINDOW[1] and now < EXIT_AT

    def past_exit():
        return datetime.now(ZoneInfo("Asia/Kolkata")).time() >= EXIT_AT

    # ---- the live tracking: Synthetic/V from every new straddle_series point -------------------------------------------------------------
    def update_tracking(name, synth, v):
        """One straddle_series point (Synthetic, V) for `name`: update its running-low / open / today's-range trackers. Used both for the
        backfill (today's points published before this script was pasted) and for every new live point; the backfill does not call
        check_entry, so it never fires a stale signal from earlier in the day."""
        st = state[name]
        today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
        if st["day"] != today:                                 # a new day: the running trackers restart from this first point
            st.update(day=today, synth_open=None, synth_pct_values=[], v_open=None, v_lo=None, v_hi=None)
        synth, v = float(synth), float(v)
        if not (synth == synth and synth > 0 and v == v):       # NaN or a non-positive Synthetic: cannot use this point
            return
        if st["synth_open"] is None:
            st["synth_open"] = synth
        if st["v_open"] is None:
            st["v_open"] = v
        synth_pct = (synth / st["synth_open"] - 1.0) * 100.0
        prev_values = st["synth_pct_values"]
        st["last_synth_pct"] = synth_pct
        st["new_low"] = bool(prev_values) and synth_pct <= min(prev_values)        # filter 1's "running low": needs at least one earlier point today
        prev_values.append(synth_pct)
        del prev_values[:-500]
        st["v_lo"] = v if st["v_lo"] is None else min(st["v_lo"], v)
        st["v_hi"] = v if st["v_hi"] is None else max(st["v_hi"], v)
        st["last_v_pct"] = (v / st["v_open"] - 1.0) * 100.0
        st["last_range_pos"] = ((v - st["v_lo"]) / (st["v_hi"] - st["v_lo"])) if st["v_hi"] > st["v_lo"] else None

    def backfill_straddle():
        """straddle_series' points published today BEFORE this script was pasted (the header requires the straddle to already be running):
        without this, `update_tracking`'s open / running-low / today's-range trackers would wrongly start from paste time instead of the
        session's actual open, since `watch()` below only sees NEW points from here on (replay=False)."""
        table = current().tables.get("straddle_series")
        if table is None:
            return
        from deephaven.pandas import to_pandas
        with exec_ctx:
            df = to_pandas(table.view(["Time", "Symbol", "Expiry", "Synthetic", "V"]))
        if df.empty:
            return
        df["__key"] = list(zip(df["Symbol"].astype(str), df["Expiry"].astype(str)))
        wanted = set(key_of)
        df = df[df["__key"].isin(wanted)].sort_values("Time")
        if df.empty:
            return
        for sym, exp, synth, v in zip(df["Symbol"], df["Expiry"], df["Synthetic"], df["V"]):
            name = key_of.get((str(sym), str(exp)))
            if name:
                update_tracking(name, synth, v)
        print(f"{STRATEGY}: backfilled {len(df)} straddle_series point(s) from earlier today")

    def on_straddle(update, is_replay):
        """Every new straddle_series point (about once a minute): update the tracking, look for an entry, and check the time exit here too
        (nothing else runs on a fixed schedule in this script, since the CE legs' prices come from fo_market_data, not candles)."""
        ch = update.added(["Time", "Symbol", "Expiry", "Synthetic", "V"])
        if ch is not None:
            for sym, exp, synth, v in zip(ch["Symbol"], ch["Expiry"], ch["Synthetic"], ch["V"]):
                name = key_of.get((str(sym), str(exp)))
                if name is None:
                    continue
                update_tracking(name, synth, v)
                check_entry(name)
        if past_exit() and not stopping:
            stopping.append(True)
            threading.Thread(target=time_stop, name=f"{STRATEGY}-time-exit", daemon=True).start()

    def check_entry(name):
        st = state[name]
        if st["position"] is not None or not in_window() or not algo_sending(STRATEGY):
            return
        if time.time() - last_fail.get(name, 0.0) < FAIL_COOLDOWN_S:
            return
        if not (st.get("new_low") and st["last_synth_pct"] is not None and st["last_synth_pct"] < 0):
            return
        if not (st["last_v_pct"] is not None and st["last_v_pct"] < 0):
            return
        if not (st["last_range_pos"] is not None and st["last_range_pos"] <= V_RANGE_MAX):
            return
        count("looked at")
        if ABS_DAYS > 0:
            try:
                with exec_ctx:                                     # absolute() reads the candles / straddle tables and the saved day files
                    r_price = absolute("pct_open", LEVEL, f"{name} FUT", ABS_DAYS)
                    r_v = absolute("v", LEVEL, name, ABS_DAYS, ref="open")
            except Exception as e:
                count("ranking errors")
                print(f"{name}: the 22-day ranking is not available ({e}); no trade this time")
                return
            if r_price[1] < 0 or not (r_price.pctile <= PCTILE_MAX):
                count("refused by own days (price)")
                return
            if r_v[1] < 0 or not (r_v.pctile <= PCTILE_MAX):
                count("refused by own days (V)")
                return
        if enter(name):
            count("entered")

    # ---- the orders ------------------------------------------------------------------------------------------------------------------
    def cross_order(token, side, qty):
        return {**ENGINE, "TOKEN": int(token), "SIDE": int(side), "QTY": int(qty)}

    def send_leg(pf, token, side, qty):
        svc = current().get("nats") or NO_SERVICE
        return svc.send(pf, "subscribe", cross_order(token, side, qty), user=USER, client=STRATEGY)       # honours this strategy's own switch

    def send_exit(pf):
        svc = current().get("nats") or NO_SERVICE
        return svc.squareup(pf, user=USER, client=STRATEGY)                              # the plugin's SquareUpEvent closes the position

    live = set()                                                   # portfolios this script holds in the engine: open legs, and closed ones waiting for their unsubscribe
    at_limit_said = []

    def release(pf, token):
        try:
            svc = current().get("nats") or NO_SERVICE
            fut = svc.unsubscribe(pf, token, user=USER, client=STRATEGY)
            fut.add_done_callback(lambda f: (live.discard(pf), at_limit_said.clear()))
        except Exception as e:
            print(f"{STRATEGY}: PF {pf} could not be unsubscribed ({e}); it keeps its slot until the stop")

    def exit_reason(pos, move):
        """Why `move` (the spread's % P&L against its net entry debit) should exit now, or None. Same trailing rule as 11/13/14."""
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
        elif move <= -STOP_PCT:
            return f"stop-loss {move:+.2f}%"
        return None

    def divisor_of(token):
        """The contract's own divisor from the master (100 for NSE options), else the market's default."""
        app = current()
        return app.master.divisor_of.get(int(token)) or app.market.price_div

    def feed_price(token):
        """A CE leg's current price straight from the live feed (fo_market_data), not candles: a one-off snapshot read, used only to get
        the entry fill price at the moment a leg is sent (after that, on_tick keeps last_px current for the same token)."""
        feed = current().tables.get(FEED_TABLE) or globals().get(FEED_TABLE)
        if feed is None:
            return None
        from deephaven.pandas import to_pandas
        with exec_ctx:
            df = to_pandas(feed.where(f"token == {int(token)}").view(["ltp"]))
        if df.empty:
            return None
        ltp = float(df["ltp"].iloc[-1])
        div = divisor_of(token)
        return ltp / div if ltp > 0 and div else None

    def manage_exit(name):
        """The spread's combined move against its net entry debit, from the two legs' latest known fo_market_data prices."""
        pos = state[name]["position"]
        if pos is None or pos["state"] != "open":
            return
        long_px, short_px = last_px.get(pos["long_token"]), last_px.get(pos["short_token"])
        if not long_px or not short_px or not pos["entry_net"]:
            return
        move = 100.0 * ((long_px - short_px) / pos["entry_net"] - 1.0)
        why = exit_reason(pos, move)
        if why is not None:
            leave(name, why)

    def untrack_legs(pos):
        """Drop a closed position's leg tokens from candle/tick routing (a later entry of the same underlying picks its own strikes again)."""
        for tok in (pos["long_token"], pos["short_token"]):
            leg_tokens.pop(tok, None)
            last_px.pop(tok, None)
        tick_track((pos["long_token"], pos["short_token"]), False)

    def settled(name, leg, what, future):
        try:
            ok = bool(future.result().ok)
            why = "" if ok else future.result().status
        except Exception as e:
            ok, why = False, repr(e)
        st = state[name]
        pos = st["position"]
        if pos is None:
            return
        if what == "enter":
            pos[f"{leg}_ok"] = ok
            if not ok:
                print(f"{name}: {leg} leg entry FAILED ({why})")
                other = "short" if leg == "long" else "long"
                if pos.get(f"{other}_ok"):                          # the other leg went through: close it, nothing is left open
                    leave(name, "the other leg's entry failed")
                else:
                    st["position"] = None
                    live.discard(pos["pf_long"]); live.discard(pos["pf_short"])
                    untrack_legs(pos)
                last_fail[name] = time.time()
            elif pos.get("long_ok") and pos.get("short_ok"):
                pos["state"] = "open"
        elif ok:
            pos[f"{leg}_closed"] = True
            if pos.get("long_closed") and pos.get("short_closed"):
                st["position"] = None
                untrack_legs(pos)
                if UNSUB_AFTER_S is not None:
                    for tok, pf in ((pos["long_token"], pos["pf_long"]), (pos["short_token"], pos["pf_short"])):
                        timer = threading.Timer(UNSUB_AFTER_S, release, (pf, tok))
                        timer.daemon = True
                        timer.start()
        else:
            last_fail[name] = time.time()
            print(f"{name}: {leg} leg exit FAILED ({why}); the position is still open, it is tried again")

    def enter(name):
        """Pick OTM1/OTM3 CE fresh off the current price (pick_legs), then send both legs. True when they were sent (not necessarily
        filled yet); False when they were not (the limit, no strikes available right now, no portfolio numbers)."""
        if len(live) + 2 > MAX_PORTFOLIOS:
            if not at_limit_said:
                at_limit_said.append(True)
                print(f"{STRATEGY}: {len(live)} portfolios held (MAX_PORTFOLIOS {MAX_PORTFOLIOS}); no new entry until an exit has been unsubscribed")
            return False
        legs = pick_legs(name)
        if legs is None:
            print(f"{name}: could not pick OTM1/OTM3 CE strikes right now (no price or not enough strikes above the ATM), no trade")
            return False
        s = state[name]
        long_token, short_token = legs["long_token"], legs["short_token"]
        if next_slot.get(long_token, 0) >= 100 or next_slot.get(short_token, 0) >= 100:
            print(f"{name}: all 100 portfolios of the day are used on one of the legs, no trade")
            return False
        a_long = pf_assign(STRATEGY, long_token, long_token, name=f"{name} OTM1 CE", slot=next_slot.get(long_token, 0))
        if not a_long:
            return False
        a_short = pf_assign(STRATEGY, short_token, short_token, name=f"{name} OTM3 CE", slot=next_slot.get(short_token, 0))
        if not a_short:
            return False
        next_slot[long_token] = next_slot.get(long_token, 0) + 1
        next_slot[short_token] = next_slot.get(short_token, 0) + 1
        live.add(a_long["pf"]); live.add(a_short["pf"])
        entry_long, entry_short = feed_price(long_token), feed_price(short_token)
        if not entry_long or not entry_short:
            print(f"{name}: no feed price yet for one of the legs, no trade")
            live.discard(a_long["pf"]); live.discard(a_short["pf"])
            return False
        leg_tokens[long_token], leg_tokens[short_token] = name, name
        last_px[long_token], last_px[short_token] = entry_long, entry_short
        s["position"] = pos = {"pf_long": a_long["pf"], "pf_short": a_short["pf"], "qty": s["qty"],
                                "long_token": long_token, "short_token": short_token,
                                "long_strike": legs["long_strike"], "short_strike": legs["short_strike"],
                                "entry_long": entry_long, "entry_short": entry_short, "entry_net": entry_long - entry_short,
                                "state": "entering", "trailing": False, "lock": None, "long_ok": None, "short_ok": None}
        tick_track((long_token, short_token), True)
        print(f"{name}: BUY {s['qty']} lot(s) {legs['long_strike']:g} CE at {entry_long:g}, SELL {s['qty']} lot(s) {legs['short_strike']:g} CE "
              f"at {entry_short:g} (net debit {pos['entry_net']:+.2f}), PF {a_long['pf']}/{a_short['pf']}")
        send_leg(a_long["pf"], long_token, 1, s["qty"]).add_done_callback(lambda f: settled(name, "long", "enter", f))
        send_leg(a_short["pf"], short_token, 2, s["qty"]).add_done_callback(lambda f: settled(name, "short", "enter", f))
        return True

    def leave(name, why):
        pos = state[name]["position"]
        if pos is None or pos["state"] != "open":
            return
        pos["state"] = "exiting"
        print(f"{name}: {why}, closing (net now {last_px.get(pos['long_token'], float('nan')):g} - "
              f"{last_px.get(pos['short_token'], float('nan')):g})")
        send_exit(pos["pf_long"]).add_done_callback(lambda f: settled(name, "long", "exit", f))
        send_exit(pos["pf_short"]).add_done_callback(lambda f: settled(name, "short", "exit", f))

    # ---- tick exits: the combined spread move, checked on every price tick of EITHER leg (the CE legs' only price source) ----------------
    tick_state = []                                                # [the table of open legs' tokens, the tick listener] once the tick exits are on

    def on_tick(update, is_replay):
        """Every price tick of a leg token with an open position (the engine filters the feed; the other ticks never reach Python).
        fo_market_data is a last_by("PF") table: a token's first tick arrives as an added row, every one after that as a
        modified row (same row, updated in place) -- both must be read or only the very first tick of each leg is seen."""
        touched = set()
        for ch in (update.added(["token", "ltp"]), update.modified(["token", "ltp"])):
            if ch is None:
                continue
            for tok, ltp in zip(ch["token"], ch["ltp"]):
                tok = int(tok)
                name = leg_tokens.get(tok)
                if name is None or ltp <= 0:
                    continue
                div = divisor_of(tok)
                if div:
                    last_px[tok] = float(ltp) / float(div)
                    touched.add(name)
        for name in touched:
            manage_exit(name)

    tick_jobs = queue.Queue()                                      # (token, add?) jobs for the one worker thread: Deephaven refuses an input-table edit made from inside a listener or a callback

    def tick_worker():
        """Adds / removes the leg tokens of open positions, one at a time and in order, outside any listener."""
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

    def tick_track(tokens, on):
        """Add / remove the given leg tokens to the set whose ticks are followed. Only queued here: the worker does it."""
        if tick_state:
            for tok in tokens:
                tick_jobs.put((int(tok), bool(on)))

    def tick_start():
        """There is no candle-close backup in this script (the CE legs' prices are fo_market_data only): if this cannot be set up, no
        open position would ever be checked for its exit, so a failure here is fatal, not just a logged warning."""
        feed = globals().get(FEED_TABLE)
        if feed is None:
            raise RuntimeError(f"no table {FEED_TABLE!r} in this session: start the feed before pasting this script")
        from deephaven import dtypes, input_table
        from deephaven.table_listener import listen
        keys = input_table(col_defs={"token": dtypes.int32}, key_cols="token")
        ticks = feed.where_in(keys, "token").where("ltp > 0").view(["token", "ltp"])           # in the engine: only the open legs' ticks come through
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
        print(f"{STRATEGY}: the spread's exit is checked on every price tick of {FEED_TABLE} (its only price source for the CE legs)")

    stopping = []

    def time_stop():
        print(f"{STRATEGY}: time exit {EXIT_AT:%H:%M}: pausing, squaring up and unsubscribing every portfolio, then stopping")
        try:
            stop()
        except Exception as e:
            print(f"{STRATEGY}: the time exit could not stop the automation ({e}); run {STRATEGY}_stop() by hand")

    # ---- by hand ---------------------------------------------------------------------------------------------------------------------
    def exit_all():
        for name in list(state):
            leave(name, "exit_all")

    def status():
        open_n = sum(1 for s in state.values() if s["position"] is not None)
        print(f"{STRATEGY}: sending {'ON' if algo_sending(STRATEGY) else 'OFF'}, {open_n} open position(s), {len(state)} underlying(s)")
        for name, s in state.items():
            pos = s["position"]
            if pos is not None:
                long_px, short_px = last_px.get(pos["long_token"], float("nan")), last_px.get(pos["short_token"], float("nan"))
                move = 100.0 * ((long_px - short_px) / pos["entry_net"] - 1.0) if pos["entry_net"] else float("nan")
                print(f"   {name:12} {pos['qty']} lot(s) {pos['long_strike']:g}/{pos['short_strike']:g} CE net entry {pos['entry_net']:+.2f} "
                      f"now {long_px - short_px:+.2f} ({move:+.2f}%) PF {pos['pf_long']}/{pos['pf_short']} {pos['state']}")
            else:
                print(f"   {name:12} flat   synth% {s['last_synth_pct']!r} v% {s['last_v_pct']!r} v-range {s['last_range_pos']!r}")
        print("   signals: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none yet"))

    if ABS_DAYS > 0:                                               # read the earlier days of every underlying NOW (one read per day file for all of them), not at the first signal
        try:
            for warm_ref in ("open",):
                absolute_warm("pct", LEVEL, sorted(f"{n} FUT" for n in state), ABS_DAYS, ref=warm_ref)
            absolute_warm("v", LEVEL, sorted(state), ABS_DAYS, ref="open")
        except Exception as e:
            print(f"absolute(): the earlier days could not be preloaded ({e}); they are read at the first signal instead")
    try:
        backfill_straddle()
    except Exception as e:
        print(f"{STRATEGY}: the straddle_series backfill failed ({e}); today's open/running-low/range tracking starts from the first live point instead")

    tick_start()
    straddle_listener = watch("straddle_series", on_straddle, owner=STRATEGY)

    def stop():
        straddle_listener.stop()
        tick_state[1].stop()
        tick_jobs.put(None)
        algo_stop(STRATEGY, pause=False)                           # square up + unsubscribe every portfolio (no pause step), wait for it, then stop (refuses while sending is OFF)

    shared = globals()
    shared["dipbounce_status"], shared["dipbounce_exit_all"], shared["dipbounce_stop"] = status, exit_all, stop
    nats_send(True, client=STRATEGY)                               # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)


run_dipbounce()
