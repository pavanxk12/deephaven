# Multi-underlying churning on N strikes per underlying. No indicator: entries are by hand with buy() / sell().
#   buy()  -> BUY CALLs on N_STRIKES strikes of every underlying     sell() -> BUY PUTs on N_STRIKES strikes of every underlying
#   buy("TCS", "INFY") / sell("SBIN") act on only the symbols you name.
# Paste in the terminal after 00_services.py has started the services.
#
# The script decides: WHICH options (pick), HOW MUCH (order_params), WHEN (you call buy() / sell()).
# The services do the rest:
#     pf_assign(...)    the BOOK picks the portfolio for each strike (new, the same, or a replacement) and takes the number
#     pf_activate(...)  the NATS service: [retire old] subscribe, pause, apply, unpause; stops and rolls back on any failure
# Each strike is a "slot" of that underlying's future: slot 0 is the strike nearest the synthetic future, slot 1 the next, and so on.
#
# Your old `blocked` set is kept: a slot that someone squares up or unsubscribes from the CONSOLE stays off until buy() / sell() is called again.
import math
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np

from algo import current

# =============================== USER INPUT ===============================
USER = 4                         # USER_ID
STRATEGY = "multi_churn"         # its name: recorded with every portfolio, the name of its sending switch and of its console window
PF_START = 1933                  # PF_START
ADD_QTY = 3                      # ADD_QTY
UNDERLYINGS = ["NIFTY", "HDFCBANK", "RELIANCE", "SBIN", "ICICIBANK",
               "INFY", "TCS", "HCLTECH", "BSE", "AXISBANK"]
EXPIRY_INDEX = 0                 # OPT_EXPIRY_IDX: 0 = the nearest expiry that has not passed
N_STRIKES = 10                    # N_STRIKES per underlying
STRIKE_MODE = "OTM"              # STRIKE_MODE: "OTM" (away from the money), "ITM" (into it) or "BOTH" (0, +1, -1, +2, -2 ...)
STRIKE_GAP = 1                   # STRIKE_GAP: listed strikes between two slots
WINDOW = (dtime(9, 20), dtime(15, 20))          # TRADE_START, TRADE_END (India time)
SUBSCRIBE = {"strategyname": "Churning", "maxloss": -400, "stoploss": -400, "max_unhedge": 3}
SIZING = dict(ival_pct=0.01, range_pct=0.2, trp_pct=0.01, jval_frac=0.75, sval_mult=5,
              jqty1=1, jqty2=1, target=1000, stoploss=-400, maxloss=-400, max_unhedge=3, PRNT=1)
# ==========================================================================
if not (N_STRIKES >= 1 and STRIKE_GAP >= 1 and STRIKE_MODE in ("OTM", "ITM", "BOTH") and N_STRIKES < 100):
    raise ValueError("N_STRIKES 1 to 99, STRIKE_GAP >= 1 and STRIKE_MODE in OTM / ITM / BOTH are required")

nats_connect(user=USER, client=STRATEGY)
registry_connect()
algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=ADD_QTY, subscribe=SUBSCRIBE)      # registers it with the book. It starts OFF.

future_of = {sym: contract(sym, show=False)["token"] for sym in UNDERLYINGS}             # symbol -> its nearest future token
sym_of = {tok: sym for sym, tok in future_of.items()}
pf_track(STRATEGY, list(future_of.values()), slots=N_STRIKES)                            # find the portfolios it had before a restart
ltp = {}                                                                                 # token -> last close, filled by the listener below
blocked = set()                                                                          # (future token, slot) stopped by hand

for sym, fut in future_of.items():                                                       # seed the futures' prices from today's candles
    h = last_candles(sym, 1, show=False)
    if h is not None and len(h["Close"]) and h["Close"].iloc[-1] > 0:
        ltp[fut] = float(h["Close"].iloc[-1])


# ---- HOW MUCH: the order parameters, from the option's price ---------------------------------------------------------------------
def floor_tick(x, tick):
    return round(math.floor(x / tick + 1e-9) * tick, 2) + 1e-6      # +1e-6: the bridge truncates (x100, int), so 2.3 must not land on 229


def ceil_tick(x, tick):
    return round(math.ceil(x / tick - 1e-9) * tick, 2) + 1e-6


def order_params(premium, tick, ival_pct=0.01, range_pct=0.05, trp_pct=0.01, jval_frac=0.75, sval_mult=3, **apply_fields):
    """The `apply` fields for BUYing an option, every value on the tick. None if the price or tick is unusable."""
    if not (premium and premium > 0 and tick and tick >= 0.01):
        return None
    ival = max(floor_tick(premium * ival_pct, tick), tick)
    return {"ival": ival, "jval": max(floor_tick(ival * jval_frac, tick), tick), "sval": max(floor_tick(ival * sval_mult, tick), tick),
            "trp": ceil_tick(premium * (1 + trp_pct / 100), tick),
            "lowerrange": max(floor_tick(premium * (1 - range_pct), tick), tick), "upperrange": floor_tick(premium * (1 + range_pct), tick),
            "side": "BUY", **apply_fields}


# ---- WHICH options ---------------------------------------------------------------------------------------------------------------
def shift(k):
    """How many gaps slot k sits from the base strike, in the OTM direction (negative = ITM)."""
    if STRIKE_MODE == "ITM":
        return -k
    if STRIKE_MODE == "BOTH":
        return 0 if k == 0 else ((k + 1) // 2 if k % 2 else -(k // 2))
    return k


def synthetic(sym, fut):
    """K + CE(K) - PE(K) at the strike nearest the future; None if either price is missing."""
    ce = strikes(sym, EXPIRY_INDEX, "CE", show=False)
    pe = strikes(sym, EXPIRY_INDEX, "PE", show=False)
    if ce is None or pe is None:
        return None
    i = int(np.abs(ce["strike"].to_numpy() - fut).argmin())
    k = ce["strike"].iloc[i]
    same = pe[pe["strike"] == k]
    c = ltp.get(int(ce["token"].iloc[i]))
    p = ltp.get(int(same["token"].iloc[0])) if len(same) else None
    return float(k + c - p) if c and p else None


def pick(sym, leg):
    """{slot k: option token} for one underlying. Slot 0 is nearest the synthetic future; OTM is higher for calls, lower for puts."""
    fut = ltp.get(future_of[sym])
    table = strikes(sym, EXPIRY_INDEX, "CE" if leg == "CALL" else "PE", show=False)
    if fut is None or table is None:
        return {}
    ref = synthetic(sym, fut)
    ref = fut if ref is None else ref
    base = int(np.abs(table["strike"].to_numpy() - ref).argmin())
    sign = 1 if leg == "CALL" else -1
    out = {}
    for k in range(N_STRIKES):
        i = base + sign * shift(k) * STRIKE_GAP
        if 0 <= i < len(table):
            out[k] = int(table["token"].iloc[i])
    return out


# ---- WHEN: by hand ---------------------------------------------------------------------------------------------------------------
def in_window():
    return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]


def enter(sym, leg):
    future = future_of[sym]
    opts = pick(sym, leg)
    if not opts:
        print(f"{sym}: no price yet, skipped")
        return
    for k in range(N_STRIKES):
        blocked.discard((future, k))                                # buy() / sell() by hand starts the stopped slots again
        option = opts.get(k)
        if option is None:
            pf_mark(STRATEGY, future, leg, slot=k)                  # no such strike in the chain: done for this leg
            continue
        if not pf_allowed(STRATEGY, future, k):                     # unticked on the console
            continue
        if pf_leg(STRATEGY, future, k) == leg:                      # this slot already runs this leg
            continue
        params = order_params(ltp.get(option), contract(option, show=False)["tick"], **SIZING)
        if params is None:
            print(f"{sym}: slot {k} (token {option}) skipped: no usable price or tick")
            continue
        a = pf_assign(STRATEGY, future, option, name=sym, slot=k)   # the BOOK picks the portfolio number
        if a:
            pf_activate(a, leg, params)                             # the NATS service does the rest


def act(leg, syms):
    if not in_window() or not algo_sending(STRATEGY):               # outside the window, or this automation is off
        print("outside the trade window, or the automation is off")
        return
    for sym in (syms or UNDERLYINGS):
        if sym not in future_of:
            print(f"{sym}: not in UNDERLYINGS, skipped")
            continue
        enter(sym, leg)


def buy(*syms):
    act("CALL", syms)                                               # BUY CALLs on N_STRIKES strikes: all symbols, or the ones named


def sell(*syms):
    act("PUT", syms)                                                # BUY PUTs on N_STRIKES strikes: all symbols, or the ones named


def exit_all():
    algo_exit(STRATEGY)                                             # pause + square up every portfolio
    blocked.update((f, k) for f in future_of.values() for k in range(N_STRIKES))


# ---- prices ----------------------------------------------------------------------------------------------------------------------
def on_candle(update, is_replay):
    """Remember the last close of every token (futures and options)."""
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    for tok, close in zip(ch["token"], ch["Close"]):
        if close > 0:
            ltp[int(tok)] = float(close)


# ---- the `blocked` set: a slot squared up or unsubscribed from the console stays off ---------------------------------------------
def slot_of(pf):
    ids = current().get(STRATEGY).book.state.current_pf_map()           # book id -> portfolio; slot 0 is the future itself, slot k is future * 100 + k
    for fut in future_of.values():
        for k in range(N_STRIKES):
            if ids.get(fut if k == 0 else fut * 100 + k) == pf:
                return fut, k
    return None


def on_state(user, pf, st):
    if user == USER and st.cmd in ("squareup", "unsubscribe") and st.by != STRATEGY:
        found = slot_of(pf)
        if found is not None:
            blocked.add(found)
            print(f"{STRATEGY}: {sym_of[found[0]]} slot {found[1]} (PF {pf}) was stopped by {st.by}: it stays off until buy() / sell()")


if "undo_state" in globals():                                          # pasted again: take the old listener off first
    undo_state()
undo_state = current().get("nats").states.on_change(on_state)


candle_listener = watch("candles", on_candle, owner=STRATEGY)       # follows the table if candles are restarted

nats_send(True, client=STRATEGY)                                    # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
# To remove it:  candle_listener.stop(); undo_state(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)