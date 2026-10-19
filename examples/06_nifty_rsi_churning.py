# NIFTY churning on N strikes, driven by a 5-MINUTE RSI of the nearest NIFTY future: your old automation.py as a plain script. No class.
#   RSI > RSI_HIGH -> BUY PUTs on N_STRIKES strikes     RSI < RSI_LOW -> BUY CALLs on N_STRIKES strikes     (always the BUY side)
# Paste in the terminal after 00_services.py has started the services.
#
# The script decides three things:  WHICH options (pick), HOW MUCH (order_params), WHEN (on_zone, from the RSI it computes itself).
# The services do the rest:
#     pf_assign(...)    the BOOK picks the portfolio for each strike (new, the same, or a replacement) and takes the number
#     pf_activate(...)  the NATS service: [retire old] subscribe, pause, apply, unpause; stops and rolls back on any failure
# Each strike is a "slot" of the NIFTY future: slot 0 is the strike nearest the synthetic future, slot 1 the next, and so on.
#
# The 5-minute bars are made from the 1-minute candles: a bar's close is the last 1-minute close inside it. With RSI_LIVE = True the RSI also counts the
# bar that is still forming (its latest close), so it reacts every minute; RSI_LIVE = False uses finished bars only. The RSI needs RSI_N + 1 bars, so after
# a start at the open the first signal can come 50 minutes in (it starts from the candles already built today).
#
# Your old `blocked` set is kept: a slot that someone squares up or unsubscribes from the CONSOLE stays off until the RSI leaves its zone.
# Old files: the PF map of the NIFTY script (token:slot format) is not read; the book numbers its portfolios from PF_START on its own and the registry remembers them.
import math
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from algo import current
from algo.core.timeframe import parse_timeframe

# =============================== USER INPUT ===============================
USER = 4                         # USER_ID
STRATEGY = "nifty_rsi"           # its name: recorded with every portfolio, the name of its sending switch and of its console window
PF_START = 1933                  # PF_START
ADD_QTY = 3                      # ADD_QTY
UNDERLYING = "NIFTY"             # UNDERLYING
EXPIRY_INDEX = 0                 # OPT_EXPIRY_IDX: 0 = the nearest expiry that has not passed
N_STRIKES = 3                    # N_STRIKES
STRIKE_MODE = "OTM"              # STRIKE_MODE: "OTM" (away from the money), "ITM" (into it) or "BOTH" (0, +1, -1, +2, -2 ...)
STRIKE_GAP = 1                   # STRIKE_GAP: listed strikes between two slots
RSI_BAR = "5min"                 # the bar the RSI is computed on
RSI_N, RSI_HIGH, RSI_LOW = 9, 70, 30
RSI_LIVE = True                  # True: the forming bar counts too (acts every minute); False: finished bars only
FAIL_COOLDOWN_S = 30
WINDOW = (dtime(9, 20), dtime(15, 20))          # TRADE_START, TRADE_END (India time)
# what this script puts on subscribe / unsubscribe over the service's defaults (your SUB_PAYLOAD; the user ids are added by the service)
SUBSCRIBE = {"strategyname": "Churning", "maxloss": -400, "stoploss": -400, "max_unhedge": 3}
SIZING = dict(ival_pct=0.01, range_pct=0.2, trp_pct=0.01, jval_frac=0.75, sval_mult=5,
              jqty1=1, jqty2=1, target=1000, stoploss=-400, maxloss=-400, max_unhedge=3, PRNT=1)   # OPT_IVAL_PCT ... APPLY_PARAMS (read by order_params below)
# ==========================================================================
if not (N_STRIKES >= 1 and STRIKE_GAP >= 1 and STRIKE_MODE in ("OTM", "ITM", "BOTH") and N_STRIKES < 100):
    raise ValueError("N_STRIKES 1 to 99, STRIKE_GAP >= 1 and STRIKE_MODE in OTM / ITM / BOTH are required")
BAR_S = parse_timeframe(RSI_BAR)                                       # 300; refuses a size that does not line up with the clock

nats_connect(user=USER, client=STRATEGY)                               # CONNECT: only checks the NATS service is up and this user may send
registry_connect()                                                     # ... and that the portfolio registry is up. Neither starts anything.
algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=ADD_QTY, subscribe=SUBSCRIBE)      # registers it with the book. It starts OFF.
future = contract(UNDERLYING, show=False)["token"]
pf_track(STRATEGY, [future], slots=N_STRIKES)                          # find the portfolios it had before a restart
ltp = {}                                                               # token -> last close, filled by the listener below
bars = {}                                                              # bar number -> the last 1-minute close inside it: what the RSI is computed from
blocked = set()                                                        # slots stopped by hand: off until the RSI leaves its zone
ZONE_LEG = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}


def epoch_s(minute):
    """A candle's Minute (a numpy / pandas / Java time) as UTC epoch seconds. 5-minute bars line up with the clock in India and the US alike."""
    return int(pd.Timestamp(minute).timestamp())


def add_close(minute, close):
    bars[epoch_s(minute) // BAR_S] = float(close)
    for old in sorted(bars)[:-400]:
        del bars[old]


def closes():
    """The bar closes, oldest first. The newest bar is still forming (a candle of the next bar has not come), so it is left out when RSI_LIVE is False."""
    keys = sorted(bars)
    return [bars[k] for k in (keys if RSI_LIVE else keys[:-1])]


history = last_candles(UNDERLYING, 400, show=False)                    # start from the candles already built today (None if there are none yet)
if history is not None:
    for m, c in zip(history["Minute"], history["Close"]):
        add_close(m, c)


# ---- the indicator: Wilder's RSI of the closes ------------------------------------------------------------------------------------
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


# ---- HOW MUCH: the order parameters, from the option's price. This is strategy logic, so it lives in the script. -------------------
def floor_tick(x, tick):
    return round(math.floor(x / tick + 1e-9) * tick, 2) + 1e-6      # +1e-6: the bridge truncates (x100, int), so 2.3 must not land on 229


def ceil_tick(x, tick):
    return round(math.ceil(x / tick - 1e-9) * tick, 2) + 1e-6


def order_params(premium, tick, ival_pct=0.01, range_pct=0.05, trp_pct=0.01, jval_frac=0.75, sval_mult=3, **apply_fields):
    """The `apply` fields for BUYing an option: ival, jval, sval, trp and the ranges, every value on the tick. None if the price or tick is unusable."""
    if not (premium and premium > 0 and tick and tick >= 0.01):
        return None
    ival = max(floor_tick(premium * ival_pct, tick), tick)
    return {"ival": ival, "jval": max(floor_tick(ival * jval_frac, tick), tick), "sval": max(floor_tick(ival * sval_mult, tick), tick),
            "trp": ceil_tick(premium * (1 + trp_pct / 100), tick),
            "lowerrange": max(floor_tick(premium * (1 - range_pct), tick), tick), "upperrange": floor_tick(premium * (1 + range_pct), tick),
            "side": "BUY", **apply_fields}


# ---- 1. WHICH options ---------------------------------------------------------------------------------------------------------
def shift(k):
    """How many gaps slot k sits from the base strike, in the OTM direction (negative = ITM)."""
    if STRIKE_MODE == "ITM":
        return -k
    if STRIKE_MODE == "BOTH":
        return 0 if k == 0 else ((k + 1) // 2 if k % 2 else -(k // 2))
    return k


def synthetic(fut):
    """K + CE(K) - PE(K) at the strike nearest the future; None if either price is missing."""
    ce = strikes(UNDERLYING, EXPIRY_INDEX, "CE", show=False)
    pe = strikes(UNDERLYING, EXPIRY_INDEX, "PE", show=False)
    if ce is None or pe is None:
        return None
    i = int(np.abs(ce["strike"].to_numpy() - fut).argmin())
    k = ce["strike"].iloc[i]
    same = pe[pe["strike"] == k]
    c = ltp.get(int(ce["token"].iloc[i]))
    p = ltp.get(int(same["token"].iloc[0])) if len(same) else None
    return float(k + c - p) if c and p else None


def pick(leg):
    """{slot k: option token}. Slot 0 is the strike nearest the synthetic future; OTM is higher for calls, lower for puts."""
    fut = ltp.get(future)
    table = strikes(UNDERLYING, EXPIRY_INDEX, "CE" if leg == "CALL" else "PE", show=False)
    if fut is None or table is None:
        return {}
    ref = synthetic(fut)
    ref = fut if ref is None else ref                                   # no CE/PE price yet: use the future itself
    base = int(np.abs(table["strike"].to_numpy() - ref).argmin())
    sign = 1 if leg == "CALL" else -1
    out = {}
    for k in range(N_STRIKES):
        i = base + sign * shift(k) * STRIKE_GAP
        if 0 <= i < len(table):                                         # a strike off the end of the list is left out
            out[k] = int(table["token"].iloc[i])
    return out


# ---- 3. WHEN ---------------------------------------------------------------------------------------------------------------------
def in_window():
    return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]


def on_zone(zone, force=False):
    leg = ZONE_LEG[zone]
    if not in_window() or not algo_sending(STRATEGY):               # outside the window, or this automation is off: no portfolio is created
        return
    opts = pick(leg)
    if not opts:                                                    # no price yet: try again on the next update
        return
    if force:
        blocked.clear()                                             # buy() / sell() by hand starts the stopped slots again
    for k in range(N_STRIKES):
        option = opts.get(k)
        if k in blocked:
            continue                                                # stopped by hand: stays off until the RSI leaves its zone
        if option is None:
            pf_mark(STRATEGY, future, leg, slot=k)                  # no such strike in the chain: done for this leg, no retry
            continue
        if not pf_allowed(STRATEGY, future, k):                     # unticked on the console
            continue
        if pf_leg(STRATEGY, future, k) == leg or (not force and pf_cooling(STRATEGY, future, FAIL_COOLDOWN_S, k)):
            continue                                                # this slot already runs this leg, or failed a moment ago
        # ---- 2. HOW MUCH ----
        params = order_params(ltp.get(option), contract(option, show=False)["tick"], **SIZING)
        if params is None:
            print(f"on_zone {zone}: slot {k} (token {option}) skipped: no usable price or tick")
            continue
        a = pf_assign(STRATEGY, future, option, name=UNDERLYING, slot=k)      # the BOOK picks the portfolio number
        if a:
            pf_activate(a, leg, params)                                       # the NATS service does the rest


def check():
    """Look at the RSI now and act on its zone (also run once when the strategy is switched on). Returns the RSI."""
    value = rsi(closes())
    zone = zone_of(value)
    if zone is None:
        blocked.clear()                                             # out of the zone: a new entry may start the stopped slots again
    else:
        on_zone(zone)
    return value


def on_candle(update, is_replay):
    """Called with every batch of newly closed 1-minute candles: remember the prices, and when the future has a new candle, work out the RSI."""
    ch = update.added(["token", "Close", "Minute"])
    if ch is None:
        return
    new_bar = False
    for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
        if close > 0:
            ltp[int(tok)] = float(close)
            if int(tok) == future:
                add_close(minute, close)
                new_bar = True
    if new_bar:
        check()


# ---- the old `blocked` set: a slot squared up or unsubscribed from the console stays off ------------------------------------------
def slot_of(pf):
    ids = current().get(STRATEGY).book.state.current_pf_map()           # book id -> portfolio; slot 0 is the future itself, slot k is future * 100 + k
    for k in range(N_STRIKES):
        if ids.get(future if k == 0 else future * 100 + k) == pf:
            return k
    return None


def on_state(user, pf, st):
    if user == USER and st.cmd in ("squareup", "unsubscribe") and st.by != STRATEGY:
        k = slot_of(pf)
        if k is not None:
            blocked.add(k)
            print(f"{STRATEGY}: slot {k} (PF {pf}) was stopped by {st.by}: it stays off until the RSI leaves its zone, or buy() / sell()")


if "undo_state" in globals():                                          # pasted again: take the old listener off first
    undo_state()
undo_state = current().get("nats").states.on_change(on_state)     # what the confirmed commands did to each portfolio (shared with the console)


# ---- by hand ----------------------------------------------------------------------------------------------------------------------
def sell():
    on_zone("OVERBOUGHT", force=True)                               # BUY PUTs on all the strikes


def buy():
    on_zone("OVERSOLD", force=True)                                 # BUY CALLs on all the strikes


def exit_all():
    algo_exit(STRATEGY)                                             # pause + square up every portfolio
    blocked.update(range(N_STRIKES))                                # and no restart on the next candle


candle_listener = watch("candles", on_candle, owner=STRATEGY)       # follows the table if candles are restarted

nats_send(True, client=STRATEGY)                                    # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
check()                                                             # ACT_ON_START: if the RSI is already in a zone, start now
# To remove it:  candle_listener.stop(); undo_state(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)
