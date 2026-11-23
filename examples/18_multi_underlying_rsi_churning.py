# Churning on SEVERAL underlyings at once, each driven by its OWN 5-minute RSI of its OWN nearest future: 06/08's one-underlying script,
# generalized. One console, one sending switch, one portfolio book (STRATEGY) for all of them -- pf_assign/pf_leg/pf_mark/pf_allowed/pf_cooling
# are all keyed by (strategy, instrument, slot), and `instrument` is each underlying's own future token, so the book already keeps every
# underlying's slots completely separate; nothing else needed to change for that part.
#   RSI > RSI_HIGH -> BUY PUTs on N_STRIKES strikes     RSI < RSI_LOW -> BUY CALLs on N_STRIKES strikes     (always the BUY side, per underlying)
# Paste in the terminal after 00_services.py has started the services.
#
# UNDERLYINGS: one entry per underlying, its own N_STRIKES / STRIKE_MODE / STRIKE_GAP / RSI_* / SIZING; everything else (WINDOW, FAIL_COOLDOWN_S,
# SUBSCRIBE, PF_START, ADD_QTY) is shared, exactly as pf_assign/algo_start expect (one book for the whole strategy).
# Each underlying keeps its own bars / blocked set; `ltp` is shared (one table of token -> last close) since tokens never collide across
# underlyings. on_candle routes a closed future candle to its own underlying by token; every other closed candle (an option) just updates ltp.
import math
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from algo import current
from algo.core.timeframe import parse_timeframe

# =============================== USER INPUT ===============================
USER = 4
STRATEGY = "multi_churning"       # its name: recorded with every portfolio, the name of its sending switch and of its console window
PF_START = 1300
ADD_QTY = 20
EXPIRY_INDEX = 0                  # OPT_EXPIRY_IDX: 0 = the nearest expiry that has not passed (shared by every underlying)
FAIL_COOLDOWN_S = 30
WINDOW = (dtime(9, 20), dtime(15, 20))          # TRADE_START, TRADE_END (India time)
SUBSCRIBE = {"strategyname": "Churning", "maxloss": -2000, "stoploss": -2000, "max_unhedge": 10, "gtw": 26286}

_COMMON = dict(
    N_STRIKES=1, STRIKE_MODE="OTM", STRIKE_GAP=1,
    RSI_BAR="5min", RSI_N=9, RSI_HIGH=70, RSI_LOW=30, RSI_LIVE=True,
    SIZING=dict(ival_pct=0.01, range_pct=0.2, trp_pct=0.01, jval_frac=0.75, sval_mult=5,
                jqty1=1, jqty2=1, target=6000, stoploss=-2000, maxloss=-2000, max_unhedge=10, PRNT=1),
)
NIFTY_50 = [                      # the 50 NIFTY 50 index constituents (stock futures; no index futures here)
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BEL", "BHARTIARTL", "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL",
    "JIOFIN", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SHRIRAMFIN", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TMPV", "TATASTEEL", "TECHM", "TITAN",
    "TRENT", "ULTRACEMCO", "WIPRO",
]
UNDERLYINGS = {name: dict(_COMMON) for name in NIFTY_50}      # add or remove underlyings here
# ==========================================================================
for name, cfg in UNDERLYINGS.items():
    if not (cfg["N_STRIKES"] >= 1 and cfg["STRIKE_GAP"] >= 1 and cfg["STRIKE_MODE"] in ("OTM", "ITM", "BOTH") and cfg["N_STRIKES"] < 100):
        raise ValueError(f"{name}: N_STRIKES 1 to 99, STRIKE_GAP >= 1 and STRIKE_MODE in OTM / ITM / BOTH are required")
if not UNDERLYINGS:
    raise ValueError("UNDERLYINGS is empty: add at least one")
ZONE_LEG = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}

nats_connect(user=USER, client=STRATEGY)                               # CONNECT: only checks the NATS service is up and this user may send
registry_connect()                                                     # ... and that the portfolio registry is up. Neither starts anything.
algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=ADD_QTY, subscribe=SUBSCRIBE)      # registers it with the book. It starts OFF.

ltp = {}                                                                # token -> last close, shared: tokens never collide across underlyings
state = {}                                                              # underlying name -> {future, bar_s, bars, blocked}

for name, cfg in UNDERLYINGS.items():
    future = contract(name, show=False)["token"]
    pf_track(STRATEGY, [future], slots=cfg["N_STRIKES"])                # find the portfolios it had before a restart, for THIS underlying's future
    state[name] = {"future": future, "bar_s": parse_timeframe(cfg["RSI_BAR"]), "bars": {}, "blocked": set()}
    history = last_candles(name, 400, show=False)                      # start from the candles already built today (None if there are none yet)
    if history is not None:
        bar_s = state[name]["bar_s"]
        bars = state[name]["bars"]
        for m, c in zip(history["Minute"], history["Close"]):
            bars[int(pd.Timestamp(m).timestamp()) // bar_s] = float(c)


def closes(name):
    """A underlying's bar closes, oldest first. The newest bar is still forming, so it is left out unless RSI_LIVE is True."""
    cfg, bars = UNDERLYINGS[name], state[name]["bars"]
    keys = sorted(bars)
    return [bars[k] for k in (keys if cfg["RSI_LIVE"] else keys[:-1])]


def add_close(name, minute, close):
    bar_s, bars = state[name]["bar_s"], state[name]["bars"]
    bars[int(pd.Timestamp(minute).timestamp()) // bar_s] = float(close)
    for old in sorted(bars)[:-400]:
        del bars[old]


# ---- the indicator: Wilder's RSI of the closes ------------------------------------------------------------------------------------
def rsi(values, n):
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


def zone_of(value, cfg):
    return None if value is None else "OVERBOUGHT" if value > cfg["RSI_HIGH"] else "OVERSOLD" if value < cfg["RSI_LOW"] else None


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
def shift(k, mode):
    """How many gaps slot k sits from the base strike, in the OTM direction (negative = ITM)."""
    if mode == "ITM":
        return -k
    if mode == "BOTH":
        return 0 if k == 0 else ((k + 1) // 2 if k % 2 else -(k // 2))
    return k


def synthetic(name, fut):
    """K + CE(K) - PE(K) at the strike nearest the future; None if either price is missing."""
    ce = strikes(name, EXPIRY_INDEX, "CE", show=False)
    pe = strikes(name, EXPIRY_INDEX, "PE", show=False)
    if ce is None or pe is None:
        return None
    i = int(np.abs(ce["strike"].to_numpy() - fut).argmin())
    k = ce["strike"].iloc[i]
    same = pe[pe["strike"] == k]
    c = ltp.get(int(ce["token"].iloc[i]))
    p = ltp.get(int(same["token"].iloc[0])) if len(same) else None
    return float(k + c - p) if c and p else None


def pick(name, leg):
    """{slot k: option token} for this underlying. Slot 0 is the strike nearest the synthetic future; OTM is higher for calls, lower for puts."""
    cfg = UNDERLYINGS[name]
    fut = ltp.get(state[name]["future"])
    table = strikes(name, EXPIRY_INDEX, "CE" if leg == "CALL" else "PE", show=False)
    if fut is None or table is None:
        return {}
    ref = synthetic(name, fut)
    ref = fut if ref is None else ref                                   # no CE/PE price yet: use the future itself
    base = int(np.abs(table["strike"].to_numpy() - ref).argmin())
    sign = 1 if leg == "CALL" else -1
    out = {}
    for k in range(cfg["N_STRIKES"]):
        i = base + sign * shift(k, cfg["STRIKE_MODE"]) * cfg["STRIKE_GAP"]
        if 0 <= i < len(table):                                         # a strike off the end of the list is left out
            out[k] = int(table["token"].iloc[i])
    return out


# ---- 3. WHEN ---------------------------------------------------------------------------------------------------------------------
def in_window():
    return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]


def on_zone(name, zone, force=False):
    cfg, future, blocked = UNDERLYINGS[name], state[name]["future"], state[name]["blocked"]
    leg = ZONE_LEG[zone]
    if not in_window() or not algo_sending(STRATEGY):               # outside the window, or this automation is off: no portfolio is created
        return
    opts = pick(name, leg)
    if not opts:                                                    # no price yet: try again on the next update
        return
    if force:
        blocked.clear()                                             # buy() / sell() by hand starts the stopped slots again
    for k in range(cfg["N_STRIKES"]):
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
        params = order_params(ltp.get(option), contract(option, show=False)["tick"], **cfg["SIZING"])
        if params is None:
            print(f"{name} on_zone {zone}: slot {k} (token {option}) skipped: no usable price or tick")
            continue
        a = pf_assign(STRATEGY, future, option, name=name, slot=k)      # the BOOK picks the portfolio number
        if a:
            pf_activate(a, leg, params)                                       # the NATS service does the rest


def check(name):
    """Look at one underlying's RSI now and act on its zone (also run once per underlying when the strategy is switched on). Returns the RSI."""
    cfg = UNDERLYINGS[name]
    value = rsi(closes(name), cfg["RSI_N"])
    zone = zone_of(value, cfg)
    if zone is None:
        state[name]["blocked"].clear()                              # out of the zone: a new entry may start the stopped slots again
    else:
        on_zone(name, zone)
    return value


future_to_name = {s["future"]: name for name, s in state.items()}


def on_candle(update, is_replay):
    """Called with every batch of newly closed 1-minute candles: remember the prices (shared across underlyings), and for whichever
    underlying's own future just got a new candle, work out that underlying's RSI."""
    ch = update.added(["token", "Close", "Minute"])
    if ch is None:
        return
    due = set()
    for tok, close, minute in zip(ch["token"], ch["Close"], ch["Minute"]):
        if close > 0:
            tok = int(tok)
            ltp[tok] = float(close)
            name = future_to_name.get(tok)
            if name is not None:
                add_close(name, minute, close)
                due.add(name)
    for name in due:
        check(name)


# ---- the old `blocked` set: a slot squared up or unsubscribed from the console stays off ------------------------------------------
def slot_of(name, pf):
    cfg, future = UNDERLYINGS[name], state[name]["future"]
    ids = current().get(STRATEGY).book.state.current_pf_map()           # book id -> portfolio; slot 0 is the future itself, slot k is future * 100 + k
    for k in range(cfg["N_STRIKES"]):
        if ids.get(future if k == 0 else future * 100 + k) == pf:
            return k
    return None


def on_state(user, pf, st):
    if user == USER and st.cmd in ("squareup", "unsubscribe") and st.by != STRATEGY:
        for name in state:
            k = slot_of(name, pf)
            if k is not None:
                state[name]["blocked"].add(k)
                print(f"{STRATEGY}: {name} slot {k} (PF {pf}) was stopped by {st.by}: it stays off until the RSI leaves its zone, or buy() / sell()")
                break


if "undo_state" in globals():                                          # pasted again: take the old listener off first
    undo_state()
undo_state = current().get("nats").states.on_change(on_state)     # what the confirmed commands did to each portfolio (shared with the console)


# ---- by hand ----------------------------------------------------------------------------------------------------------------------
def sell(name=None):
    """BUY PUTs on all the strikes of one underlying, or of every underlying if `name` is left out."""
    for n in ([name] if name else state):
        on_zone(n, "OVERBOUGHT", force=True)


def buy(name=None):
    """BUY CALLs on all the strikes of one underlying, or of every underlying if `name` is left out."""
    for n in ([name] if name else state):
        on_zone(n, "OVERSOLD", force=True)


def exit_all():
    algo_exit(STRATEGY)                                             # pause + square up every portfolio, of every underlying
    for name in state:
        state[name]["blocked"].update(range(UNDERLYINGS[name]["N_STRIKES"]))      # and no restart on the next candle


def status():
    for name in state:
        value = rsi(closes(name), UNDERLYINGS[name]["RSI_N"])
        print(f"{name}: RSI {'n/a' if value is None else round(value, 1)}, blocked slots {sorted(state[name]['blocked'])}")


candle_listener = watch("candles", on_candle, owner=STRATEGY)       # follows the table if candles are restarted

nats_send(True, client=STRATEGY)                                    # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
for name in state:                                                  # ACT_ON_START: if an underlying's RSI is already in a zone, start it now
    check(name)
# To remove it:  candle_listener.stop(); undo_state(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)
