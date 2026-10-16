# TEMPLATE FOR AN AUTOMATION: plain functions and a listener, like your old scripts. No class. Copy it, rename STRATEGY, change the marked parts.
# Paste in the terminal after 00_services.py has started the services. The services do the work:
#     pf_assign   the BOOK picks the portfolio (and takes the number)        pf_activate   the NATS service sends the commands
# The INDICATOR is yours: indicators are not a service, so the script computes its RSI from the candles it reads.
#
# It buys the nearest-strike option of one future when its RSI enters a zone: oversold -> a CALL, overbought -> a PUT.
import math

import numpy as np

# ---- 1. WHO and the settings (your old USER INPUT block) ----------------------------------------------------------------------
USER = 3                                     # the user it trades as (one of the users given to nats_start)
STRATEGY = "template"                        # its name: recorded with every portfolio, and the name of its own sending switch
PF_START = 5000                              # where its portfolio numbers begin
UNDERLYING = "NIFTY"
RSI_N, RSI_LOW, RSI_HIGH = 9, 30, 70
COOLDOWN_S = 30                              # after a failed order, wait this long before trying the same thing again

nats_connect(user=USER, client=STRATEGY)                     # CONNECT: only checks the NATS service is up and this user may send
registry_connect()                                           # ... and that the portfolio registry is up. Neither starts anything.
algo_start(USER, STRATEGY, pf_start=PF_START, add_qty=3)    # registers it with the book. It starts OFF.
future = contract(UNDERLYING, show=False)["token"]          # the signal source: the nearest future
pf_track(STRATEGY, [future])                                 # find the portfolio it had before a restart
ltp = {}                                                     # token -> last close, filled by the listener below
closes = []                                                  # the future's closes, oldest first: what the RSI is computed from
history = last_candles(UNDERLYING, 200, show=False)         # start from the candles already built today (None if there are none yet)
if history is not None:
    closes.extend(float(c) for c in history["Close"])


# ---- the indicator: Wilder's RSI of the closes (for a higher time frame, use every 5th close, or group the candles by their Minute) ----
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


# ---- 2. WHAT to buy: the strike nearest the future, in the nearest expiry -------------------------------------------------------
def choose(leg):
    price = ltp.get(future)
    if price is None:
        return None
    near = nearest_strikes(UNDERLYING, price, "CE" if leg == "CALL" else "PE", expiry=0, n=1, show=False)
    return None if near is None else int(near["token"].iloc[0])


def buy(leg):
    if not algo_sending(STRATEGY) or pf_leg(STRATEGY, future) == leg or pf_cooling(STRATEGY, future, COOLDOWN_S):
        return                                               # off, or already running this leg, or it failed a moment ago
    option = choose(leg)
    if option is None:
        return
    # ---- 3. HOW MUCH: the order parameters, from the option's price (None, with the reason printed, if the price or tick is unusable)
    params = order_params(ltp.get(option), contract(option, show=False)["tick"], ival_pct=0.01, target=1000, stoploss=-400, maxloss=-400,
                          jqty1=1, jqty2=1, max_unhedge=1, PRNT=1)
    if params is None:
        return
    a = pf_assign(STRATEGY, future, option, name=UNDERLYING)  # the BOOK: a portfolio for this (new, the same, or a replacement)
    if a:
        pf_activate(a, leg, params)                           # the NATS service: [retire the old one,] subscribe, pause, apply, unpause


# ---- the listener: what your script reacts to -------------------------------------------------------------------------------------
def on_candle(update, is_replay):
    """Called with every batch of newly closed candles: remember the prices, and when the future has a new candle, work out the RSI."""
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    new_bar = False
    for tok, close in zip(ch["token"], ch["Close"]):
        if close > 0:                                        # a missing price (null / NaN) is skipped
            ltp[int(tok)] = float(close)
            if int(tok) == future:
                closes.append(float(close))
                del closes[:-300]                            # keep the last 300 only
                new_bar = True
    value = rsi(closes) if new_bar else None
    if value is not None:
        if value < RSI_LOW:
            buy("CALL")
        elif value > RSI_HIGH:
            buy("PUT")


candle_listener = watch("candles", on_candle)               # follows the table if candles are restarted

nats_send(True, client=STRATEGY)                             # NOW it may send. Stop just this automation: nats_send(False, client=STRATEGY)
# To remove it:  candle_listener.stop(); algo_stop(STRATEGY)        Its portfolios:  algo_portfolios(STRATEGY)
