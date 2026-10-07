# churning_rsi.py -- RSI on TOKEN_1 1-min candles decides which pair runs.
#   RSI > 60 (overbought) -> buy put  + sell call
#   RSI < 40 (oversold)   -> buy call + sell put
#   only one pair is active at a time; when the signal flips, the other pair is paused first.
# self-contained: sends the HFT commands itself (NATS). Only needs `candles` (token, Close, RSI)
# in the same Deephaven session. RSI column is RSI x 100 (7125 = 71.25).
import asyncio
import json
import queue
import threading
import uuid
from datetime import datetime, timezone, timedelta, time as dtime
import pandas as pd
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.constants import NULL_LONG
from deephaven.table_listener import listen
from deephaven.pandas import to_pandas

# =============================== USER INPUT ===============================
DRY_RUN = False             # True = only print what would be sent. Set False to go live.
USER_ID = 3

NATS_URL = "nats://192.168.1.130:4222"
TIMEOUT_S = 5

TOKEN_1 = 48704            # underlying: its 1-min RSI is the signal, never traded

# four portfolios: (PF number, tradable token that portfolio runs -> used for its range)
PF_BUY_CALL,  TOKEN_BUY_CALL  = 1105, 44618
PF_BUY_PUT,   TOKEN_BUY_PUT   = 1106, 44619
PF_SELL_CALL, TOKEN_SELL_CALL = 1107, 44618
PF_SELL_PUT,  TOKEN_SELL_PUT  = 1108, 44619

RSI_HIGH = 60               # RSI above this -> buy put + sell call
RSI_LOW = 40                # RSI below this -> buy call + sell put

# True = if RSI is already inside a zone when the script starts, activate that pair immediately.
# WARNING: every re-paste with this True adds another ADD_QTY to maxqty. Set False after the first run.
ACT_ON_START = True

# qty ADDED to the portfolio's cumulative maxqty every time its pair is activated
ADD_QTY = 5

# range for each tradable token = its last candle Close -/+ these (in rupees)
RANGE_BELOW = 10
RANGE_ABOVE = 20
PRICE_DIV = 100            # candle Close is paise -> 100. Use 1 if already rupees.

SQUAREUP_ON_STOP = False   # False = only pause the pair being stopped. True = pause + squareup.
TRADE_START = dtime(9, 20)     # IST, signals outside this window are ignored
TRADE_END = dtime(15, 0)

# every other field of the Churning "apply" payload (maxqty, lowerrange, upperrange are set by the script)
APPLY_PARAMS = {
    # pullback set (churn_params, H = 4 Rs pullback depth): ival = H/2, jval = 0.85*ival, sval = H + ival
    "ival": 2, "jval": 1.7, "jqty1": 1, "jqty2": 1,
    "target": 30000, "stoploss": -30000, "maxloss": -30000,
    "max_unhedge": 3, "PRNT": 1, "sval": 6,
}
# ==========================================================================

for _n, _v in (("PF_BUY_CALL", PF_BUY_CALL), ("TOKEN_BUY_CALL", TOKEN_BUY_CALL),
               ("PF_BUY_PUT", PF_BUY_PUT), ("TOKEN_BUY_PUT", TOKEN_BUY_PUT),
               ("PF_SELL_CALL", PF_SELL_CALL), ("TOKEN_SELL_CALL", TOKEN_SELL_CALL),
               ("PF_SELL_PUT", PF_SELL_PUT), ("TOKEN_SELL_PUT", TOKEN_SELL_PUT)):
    if not _v:
        raise RuntimeError(f"set {_n} in the user input block")
if "candles" not in globals():
    raise RuntimeError("'candles' not found - run the indicator script first")

IST = timezone(timedelta(hours=5, minutes=30))

GROUPS = {
    "OVERBOUGHT": [(PF_BUY_PUT, TOKEN_BUY_PUT), (PF_SELL_CALL, TOKEN_SELL_CALL)],   # RSI > RSI_HIGH
    "OVERSOLD":   [(PF_BUY_CALL, TOKEN_BUY_CALL), (PF_SELL_PUT, TOKEN_SELL_PUT)],   # RSI < RSI_LOW
}
OTHER = {"OVERBOUGHT": "OVERSOLD", "OVERSOLD": "OVERBOUGHT"}
ALL_PF = [PF_BUY_CALL, PF_BUY_PUT, PF_SELL_CALL, PF_SELL_PUT]
TRADABLE = [TOKEN_BUY_CALL, TOKEN_BUY_PUT, TOKEN_SELL_CALL, TOKEN_SELL_PUT]

# re-run safe: stop old listener / worker / NATS client (also from the previous churning scripts)
if "h_sig" in globals():
    try:
        h_sig.stop()
    except Exception:
        pass
if "churn_q" in globals():
    churn_q.put(None)
if "hft" in globals() and hft is not None:
    try:
        hft.close()
    except Exception:
        pass

# ---- log table: one row per accepted command (kept across re-pastes) ----
if "cmd_writer" not in globals():
    cmd_writer = DynamicTableWriter({
        "PF": dht.int64,
        "Cmd": dht.string,
        "Payload": dht.string,
        "ReplyId": dht.string,
    })
    cmd_queue_automated = cmd_writer.table

# ---- cumulative maxqty sent per portfolio (kept across re-pastes) ----
# The strategy stops at TotalTradedEntryQty == MaxQty, so every activation sends  previous cap + ADD_QTY.
# For a portfolio that already traded, set it to the strategy's TotalTradedEntryQty, e.g. caps[1100] = 15
if "caps" not in globals():
    caps = {}
for _pf in ALL_PF:
    caps.setdefault(_pf, 0)


# ---- NATS command client (request/reply to cmd.hft.<pf>) ----
class HftClient:
    def __init__(self, url):
        import nats                       # pip install nats-py (only needed when live)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.nc = self._run(nats.connect(url, connect_timeout=3))

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(TIMEOUT_S + 5)

    def send(self, pf, cmd, payload):
        reply_id = str(uuid.uuid4())
        payload_str = json.dumps(payload)        # payload is a JSON string inside the JSON
        body = {"Cmd": cmd, "Payload": payload_str, "ReplyId": reply_id}
        subject = f"cmd.hft.{pf}"
        msg = self._run(self.nc.request(subject, json.dumps(body).encode(), timeout=TIMEOUT_S))
        reply = msg.data.decode()
        print(f"[{cmd}] {subject} -> {reply}")
        if json.loads(reply).get("Status") == "ok":
            cmd_writer.write_row(int(pf), cmd, payload_str, reply_id)
        return reply

    def close(self):
        if not self.loop.is_running():       # already closed (stop() then re-paste calls this twice)
            return
        self._run(self.nc.close())
        self.loop.call_soon_threadsafe(self.loop.stop)


hft = None if DRY_RUN else HftClient(NATS_URL)       # dry run never touches NATS

def _send(pf, cmd, payload):
    if hft is None:
        raise RuntimeError("DRY_RUN is True - set DRY_RUN = False and re-run to send commands")
    return hft.send(pf, cmd, payload)

def pause(pf, user_id):
    return _send(pf, "pause_unpause", {
        "Action": "Pause",
        "USERIDENTIFIER": user_id,
        "user_id": user_id,
    })

def unpause(pf, user_id):
    return _send(pf, "pause_unpause", {
        "Action": "UnPause",
        "USERIDENTIFIER": user_id,
        "user_id": user_id,
    })

def squareup(pf, user_id):
    return _send(pf, "squareup", {
        "USERIDENTIFIER": user_id,
        "user_id": user_id,
    })

def apply(pf, params, user_id):
    payload = dict(params)
    payload["USERIDENTIFIER"] = user_id
    payload["user_id"] = user_id
    return _send(pf, "apply", payload)

def apply_add(pf, params, add_qty, user_id):
    """apply with maxqty = previous cap + add_qty; the cap is only raised once the bridge says ok."""
    cap = caps[pf] + add_qty
    p = dict(params)
    p["maxqty"] = cap
    reply = apply(pf, p, user_id)
    if json.loads(reply).get("Status") == "ok":
        caps[pf] = cap
    else:
        print(f"[churning] apply to PF {pf} not ok, cap stays {caps[pf]}: {reply}")


# ---- worker thread: NATS requests block up to 5s, never do that inside a listener ----
churn_q = queue.Queue()

def _worker(q):
    while True:
        job = q.get()
        if job is None:
            return
        fn, args, kw = job
        try:
            fn(*args, **kw)
        except Exception as e:
            print(f"[churning] {fn.__name__}{args} FAILED: {e}")

threading.Thread(target=_worker, args=(churn_q,), daemon=True).start()

def _do(fn, *args, **kw):
    if DRY_RUN:
        print(f"[churning][DRY] {fn.__name__} {args} {kw}")
    else:
        churn_q.put((fn, args, kw))


# ---- state ----
churn_ltp = {}                          # tradable token -> last candle Close (raw)
sig = {"zone": None, "rsi": None}       # current RSI zone of TOKEN_1 and last RSI x100
live = {"group": None}                  # pair the script last started (info only)

def rsi_zone(rsi_x100):
    if rsi_x100 > RSI_HIGH * 100:
        return "OVERBOUGHT"
    if rsi_x100 < RSI_LOW * 100:
        return "OVERSOLD"
    return None

# seed prices of the four tradable tokens from candles already in the table
try:
    _snap = to_pandas(
        candles.where(" || ".join(f"token = {t}" for t in TRADABLE)).last_by("token")
    )
    for _t, _c in zip(_snap["token"], _snap["Close"]):
        churn_ltp[int(_t)] = float(_c)
except Exception as e:
    print(f"[churning] could not seed prices: {e}")

# seed the current RSI zone from history (no commands), so we only act on a fresh entry into a zone
try:
    _h = to_pandas(candles.where(f"token = {TOKEN_1}").view(["RSI"]))
    for _v in _h["RSI"]:
        if pd.isna(_v) or int(_v) == NULL_LONG:
            continue
        sig["rsi"] = int(_v)
        sig["zone"] = rsi_zone(int(_v))
except Exception as e:
    print(f"[churning] could not replay history: {e}")


def make_params(token):
    px = churn_ltp.get(token)
    if px is None:
        return None
    p = dict(APPLY_PARAMS)
    p["lowerrange"] = round(px / PRICE_DIV - RANGE_BELOW)
    p["upperrange"] = round(px / PRICE_DIV + RANGE_ABOVE)
    return p

def start_group(group):
    """pause -> apply (maxqty = cap + ADD_QTY) -> unpause, for both portfolios of the pair."""
    for pf, tok in GROUPS[group]:
        params = make_params(tok)
        _do(pause, pf, USER_ID)
        _do(apply_add, pf, params, ADD_QTY, USER_ID)
        _do(unpause, pf, USER_ID)
        print(f"[churning] {group}: PF {pf} token {tok} +{ADD_QTY} -> maxqty {caps[pf] + ADD_QTY} "
              f"range {params['lowerrange']}-{params['upperrange']}")
    live["group"] = group

def stop_group(group):
    for pf, tok in GROUPS[group]:
        _do(pause, pf, USER_ID)
        if SQUAREUP_ON_STOP:
            _do(squareup, pf, USER_ID)

def on_signal(group):
    """RSI entered a zone. Also callable by hand: on_signal("OVERBOUGHT")."""
    other = OTHER[group]
    if not (TRADE_START <= datetime.now(IST).time() <= TRADE_END):
        print(f"[churning] RSI -> {group} outside trade window, ignored")
        return
    for pf, tok in GROUPS[group]:
        if make_params(tok) is None:
            print(f"[churning] no candle close yet for token {tok}, {group} signal skipped")
            return
    print(f"[churning] RSI {sig['rsi'] / 100 if sig['rsi'] is not None else '?'} -> {group}, stopping {other}")
    stop_group(other)                                # always pause the other pair first
    start_group(group)

def exit_all():
    """Manual: pause + square up all four portfolios."""
    for pf in ALL_PF:
        _do(pause, pf, USER_ID)
        _do(squareup, pf, USER_ID)
    live["group"] = None

def status():
    print("rsi", sig, "| live", live)
    print("caps", caps)
    print("closes", {t: churn_ltp.get(t) for t in TRADABLE})

def stop():
    h_sig.stop()
    churn_q.put(None)
    if hft is not None:
        hft.close()

# ---- one listener on candles: prices for the four tokens, RSI of TOKEN_1 for the signal ----
def on_candle(update, is_replay):
    ch = update.added(["token", "Close", "RSI"])
    if ch is None:
        return
    for tok, close, rsi in zip(ch["token"], ch["Close"], ch["RSI"]):
        tok = int(tok)

        if tok in TRADABLE:
            if close != NULL_LONG and close > 0:
                churn_ltp[tok] = float(close)
            continue

        if tok != TOKEN_1 or rsi == NULL_LONG:
            continue
        rsi = int(rsi)
        zone = rsi_zone(rsi)
        prev = sig["zone"]
        sig["rsi"] = rsi
        sig["zone"] = zone
        if zone is not None and zone != prev:        # fires on entering a zone, not on every candle inside it
            on_signal(zone)

h_sig = listen(candles, on_candle, do_replay=False)
print(f"[churning] armed ({'DRY RUN' if DRY_RUN else 'LIVE'}), token {TOKEN_1}, "
      f"RSI >{RSI_HIGH} / <{RSI_LOW}, zone now {sig['zone']}, rsi now {sig['rsi']}")

# ---- already inside a zone when the script starts -> activate that pair now ----
if ACT_ON_START and sig["zone"] is not None:
    print(f"[churning] already in {sig['zone']} at start, activating")
    on_signal(sig["zone"])