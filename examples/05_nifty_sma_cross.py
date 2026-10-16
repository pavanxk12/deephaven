# NIFTY near-expiry options: churn the 5 strikes nearest the money when the fast SMA of the NIFTY future crosses the slow SMA.
#   fast crosses ABOVE slow -> BUY CALLs on the 5 nearest strikes        fast crosses BELOW slow -> BUY PUTs (the CALL portfolios retire)
# Plain script: paste it in the Deephaven terminal after the services are up (see below). No class.
#
# Before you paste (once a day, one person):
#   master_build("NSE");  candles_start("1min", symbols=["NIFTY"], expiries=1)      # candles for the NIFTY future AND its nearest-expiry options
#   registry_start();  nats_start("192.168.1.130", 4222, users=[3])                   # the shared services (a script never starts them)
# The script starts OFF, and switches itself on with its last line. Stop only this one:  nats_send(False, client="nifty_sma")
import math
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np

# =============================== SETTINGS ===============================
USER = 3                         # the user it trades as (one of the users given to nats_start)
STRATEGY = "nifty_sma"           # its name: recorded with every portfolio, the name of its sending switch and of its console window
UNDERLYING = "NIFTY"
EXPIRY_INDEX = 0                 # 0 = the nearest expiry that has not passed
N_STRIKES = 5                    # how many strikes nearest the money
FAST, SLOW = 5, 20               # SMA lengths, in candles of the future
ADD_QTY = 3                      # maxqty added on each order
FAIL_COOLDOWN_S = 30             # after a failed order, wait this long before trying that strike again
WINDOW = (dtime(9, 20), dtime(15, 20))                       # trade only inside this time (India time)
SIZING = dict(ival_pct=0.01, range_pct=0.05, trp_pct=0.01, target=1000, stoploss=-400, maxloss=-400, jqty1=1, jqty2=1, max_unhedge=1, PRNT=1)
# ========================================================================
if not (1 <= N_STRIKES < 100 and 1 <= FAST < SLOW):
    raise ValueError("N_STRIKES must be 1 to 99 and FAST must be smaller than SLOW")

nats_connect(user=USER, client=STRATEGY)                     # only checks the NATS service is up and this user may send
registry_connect()                                           # ... and that the portfolio registry is up. Neither starts anything.
algo_start(USER, STRATEGY, add_qty=ADD_QTY)                  # its own book, bridge and sending switch (OFF), and its console window
future = contract(UNDERLYING, show=False)["token"]           # the signal source: the nearest NIFTY future
pf_track(STRATEGY, [future], slots=N_STRIKES)                # find the portfolios it had before a restart (one per strike slot)
ltp = {}                                                     # token -> last close, filled by the listener below
closes = []                                                  # the future's closes, oldest first
history = last_candles(UNDERLYING, 200, show=False)         # start from the candles already built today (None if there are none yet)
if history is not None:
    closes.extend(float(c) for c in history["Close"])


# ---- the indicator: simple moving averages, and which one is on top ------------------------------------------------------------------
def sma(values, n):
    return None if len(values) < n else float(np.mean(values[-n:]))


def side():
    """+1 while the fast SMA is above the slow one, -1 below, 0 equal, None until there are SLOW candles."""
    fast, slow = sma(closes, FAST), sma(closes, SLOW)
    return None if fast is None or slow is None else (fast > slow) - (fast < slow)


state = {"side": side()}                                     # where the averages stand now, so a start never counts as a crossing


# ---- HOW MUCH: order parameters from the option's price (strategy logic, so it lives in the script) --------------------------------------
def floor_tick(x, tick):
    return round(math.floor(x / tick + 1e-9) * tick, 2) + 1e-6       # +1e-6: the bridge truncates (x100, int), so 2.3 must not land on 229


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


# ---- WHAT to trade and WHEN ------------------------------------------------------------------------------------------------------------
def in_window():
    return WINDOW[0] <= datetime.now(ZoneInfo("Asia/Kolkata")).time() <= WINDOW[1]


def churn(leg):
    """Move every strike slot to `leg`: the N_STRIKES strikes nearest the future, slot 0 the nearest. The BOOK gives each slot its portfolio
    (the same one if the option is unchanged, a new one if it moved or the leg flipped), the NATS service sends the commands."""
    if not in_window() or not algo_sending(STRATEGY) or ltp.get(future) is None:
        return                                               # outside the window, or this automation is off: no portfolio is created
    near = nearest_strikes(UNDERLYING, ltp[future], "CE" if leg == "CALL" else "PE", expiry=EXPIRY_INDEX, n=N_STRIKES, show=False)
    if near is None:
        return
    for k, option in enumerate(int(t) for t in near["token"]):
        if not pf_allowed(STRATEGY, future, k):              # unticked on the console: leave this slot alone
            continue
        if pf_leg(STRATEGY, future, k) == leg or pf_cooling(STRATEGY, future, FAIL_COOLDOWN_S, k):
            continue                                         # this slot already runs this leg, or failed a moment ago
        premium = ltp.get(option) or last_price(option, show=False)
        params = order_params(premium, contract(option, show=False)["tick"], **SIZING)
        if params is None:
            print(f"churn {leg}: slot {k} (token {option}) skipped: no usable price or tick (price {premium}). It is retried on the next crossing.")
            continue
        a = pf_assign(STRATEGY, future, option, name=UNDERLYING, slot=k)      # the BOOK picks the portfolio number
        if a:
            pf_activate(a, leg, params)                                       # the NATS service: [retire old] subscribe, pause, apply, unpause


def on_candle(update, is_replay):
    """Every batch of newly closed candles: remember the prices; when the future has a new candle, see whether the averages crossed."""
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
    if not new_bar:
        return
    now = side()
    if now:                                                  # None (not enough candles) and 0 (equal) change nothing
        if state["side"] and now != state["side"]:           # the order of the averages flipped: a crossing
            churn("CALL" if now > 0 else "PUT")
        state["side"] = now


candle_listener = watch("candles", on_candle, owner=STRATEGY)  # follows the table if candles are restarted; pasting the script again replaces it

nats_send(True, client=STRATEGY)                             # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
# To remove it:  candle_listener.stop(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)        Its window: console_nifty_sma
