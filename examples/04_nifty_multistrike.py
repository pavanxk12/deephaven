# NIFTY churning on SEVERAL strikes at once, as a plain script: your USER INPUT block, a few functions and a listener. No class.
# Paste in the terminal after 00_services.py has started the services.
#
# The script decides three things:  WHICH options (pick), HOW MUCH (order_params), WHEN (on_zone, from an RSI it computes itself).
# The services do the rest:
#     pf_assign(...)    the BOOK picks the portfolio for each strike (new, the same, or a replacement) and takes the number
#     pf_activate(...)  the NATS service: [retire old] subscribe, pause, apply, unpause; stops and rolls back on any failure
# Each strike is a "slot" of the NIFTY future: slot 0 is the strike nearest the synthetic future, slot 1 the next, and so on.
#
# Not done here (a few lines each): your old `blocked` set (a slot squared up by hand stays off until the RSI leaves its zone), unsubscribe of one
# portfolio (pf_unsubscribe / algo_unsubscribe), and re-checking the RSI at the moment the automation is switched on.
import math
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np

# =============================== USER INPUT ===============================
USER = 4                         # USER_ID
STRATEGY = "nifty_multi"         # its name: recorded with every portfolio, and the name of its own sending switch
PF_START = 1933                  # PF_START
ADD_QTY = 3                      # ADD_QTY
UNDERLYING = "NIFTY"             # UNDERLYING
EXPIRY_INDEX = 0                 # OPT_EXPIRY_IDX: 0 = the nearest expiry that has not passed
N_STRIKES = 3                    # N_STRIKES
STRIKE_MODE = "OTM"              # STRIKE_MODE: "OTM" (away from the money), "ITM" (into it) or "BOTH" (0, +1, -1, +2, -2 ...)
STRIKE_GAP = 1                   # STRIKE_GAP: listed strikes between two slots
RSI_N, RSI_HIGH, RSI_LOW = 9, 70, 30
FAIL_COOLDOWN_S = 30
WINDOW = (dtime(9, 20), dtime(15, 20))          # TRADE_START, TRADE_END (India time)
SIZING = dict(ival_pct=0.01, range_pct=0.2, trp_pct=0.01, jval_frac=0.75, sval_mult=5,
              jqty1=1, jqty2=1, target=1000, stoploss=-400, maxloss=-400, max_unhedge=3, PRNT=1)   # OPT_IVAL_PCT ... APPLY_PARAMS (read by order_params below)
# ==========================================================================
if not (N_STRIKES >= 1 and STRIKE_GAP >= 1 and STRIKE_MODE in ("OTM", "ITM", "BOTH") and N_STRIKES < 100):
    raise ValueError("N_STRIKES 1 to 99, STRIKE_GAP >= 1 and STRIKE_MODE in OTM / ITM / BOTH are required")

nats_connect(user=USER, client=STRATEGY)                               # CONNECT: only checks the NATS service is up and this user may send
registry_connect()                                                     # ... and that the portfolio registry is up. Neither starts anything.
algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=ADD_QTY)       # registers it with the book. It starts OFF.
future = contract(UNDERLYING, show=False)["token"]
pf_track(STRATEGY, [future], slots=N_STRIKES)                          # find the portfolios it had before a restart
ltp = {}                                                               # token -> last close, filled by the listener below
closes = []                                                            # the future's closes, oldest first: what the RSI is computed from
history = last_candles(UNDERLYING, 200, show=False)                   # start from the candles already built today (None if there are none yet)
if history is not None:
    closes.extend(float(c) for c in history["Close"])
ZONE_LEG = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}


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


def on_zone(zone):
    leg = ZONE_LEG[zone]
    if not in_window() or not algo_sending(STRATEGY):               # outside the window, or this automation is off: no portfolio is created
        return
    opts = pick(leg)
    if not opts:                                                    # no price yet: try again on the next update
        return
    for k in range(N_STRIKES):
        option = opts.get(k)
        if option is None:
            pf_mark(STRATEGY, future, leg, slot=k)                  # no such strike in the chain: done for this leg, no retry
            continue
        if pf_leg(STRATEGY, future, k) == leg or pf_cooling(STRATEGY, future, FAIL_COOLDOWN_S, k):
            continue                                                # this slot already runs this leg, or failed a moment ago
        # ---- 2. HOW MUCH ----
        params = order_params(ltp.get(option), contract(option, show=False)["tick"], **SIZING)
        if params is None:
            continue
        a = pf_assign(STRATEGY, future, option, name=UNDERLYING, slot=k)      # the BOOK picks the portfolio number
        if a:
            pf_activate(a, leg, params)                                       # the NATS service does the rest


def on_candle(update, is_replay):
    """Called with every batch of newly closed candles: remember the prices, and when the future has a new candle, work out the RSI."""
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    new_bar = False
    for tok, close in zip(ch["token"], ch["Close"]):
        if close > 0:
            ltp[int(tok)] = float(close)
            if int(tok) == future:
                closes.append(float(close))
                del closes[:-300]
                new_bar = True
    value = rsi(closes) if new_bar else None
    if value is not None:
        if value > RSI_HIGH:
            on_zone("OVERBOUGHT")
        elif value < RSI_LOW:
            on_zone("OVERSOLD")


candle_listener = watch("candles", on_candle)                       # follows the table if candles are restarted

nats_send(True, client=STRATEGY)                                    # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
# To remove it:  candle_listener.stop(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)
