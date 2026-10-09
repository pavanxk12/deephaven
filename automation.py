# churning_rsi_multi.py -- each portfolio trades its OWN token and is driven by that token's 1-min RSI.
#   RSI > RSI_HIGH (overbought) -> run SELL side
#   RSI < RSI_LOW  (oversold)   -> run BUY side
# On a flip: pause -> apply (maxqty = cap + ADD_QTY, new side, fresh range/ival/jval/sval) -> unpause.
# Needs `candles` (token, Close) and `indicators` (token, RSI_14, from indicators.py) in the same Deephaven session.
# RSI_14 is int64, RSI x 100.
import asyncio, json, math, queue, threading, uuid
from datetime import datetime, timezone, timedelta, time as dtime
import pandas as pd
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.constants import NULL_LONG
from deephaven.table_listener import listen
from deephaven.pandas import to_pandas

# =============================== USER INPUT ===============================
DRY_RUN = False              # True = only print what would be sent. Set False to go live.
USER_ID = 3
SENDER = "AUTOMATION"
NATS_URL = "nats://192.168.1.130:4222"
TIMEOUT_S = 5

# name: (PF, token)   -- each token is both the signal and the traded instrument
INSTR = {
    # "nifty":     (1129, 48704),
    "reliance":  (1130, 48987),
    "hdfcbank":  (1131, 48864),
    "icicibank": (1132, 48874),
    "icicipru":  (1133, 48876),
    "icicigi":   (1134, 48875),
    "ltm":       (1135, 48922),
    "suzlon":    (1136, 49005),
    "bhel":      (1137, 48764),
    "bel":       (1138, 48756),
    "pnb":       (1139, 48974),
    "dmart":     (1140, 48834),
    "kotakbank": (1141, 48906),
    "idea":      (1142, 48877),
    "infy":      (1143, 48886),
    "tcs":       (1144, 49013),
    "mcx":       (1145, 48937),
}

RSI_HIGH = 70               # RSI above this -> SELL
RSI_LOW = 30                # RSI below this -> BUY

# ASSUMPTION: how the apply payload tells the bridge which side to run. Change to match your bridge.
SIDE_FIELD = "side"
SIDE_VALUES = {"BUY": "BUY", "SELL": "SELL"}

# True = if RSI is already in a zone when the script starts, activate immediately.
# WARNING: every re-paste with this True adds another ADD_QTY to maxqty.
ACT_ON_START = True

ADD_QTY = 5                 # added to the cumulative maxqty on every activation / flip

# range = last Close -/+ these (rupees). NOTE: same for every stock; check it suits cheap names like suzlon/idea.
RANGE_BELOW = 100
RANGE_ABOVE = 200
PRICE_DIV = 100             # candle Close is paise -> 100. Use 1 if already rupees.

TICK = 0.1                # ival / jval / sval are floored to this
IVAL_PCT = 0.001            # ival  = 0.1% of close
JVAL_FRAC = 0.75            # jval  = 0.75 x ival
SVAL_MULT = 3               # sval  = 3 x ival

SQUAREUP_ON_STOP = False    # (unused on flip; only manual exit_all squares up)
TRADE_START = dtime(9, 20)  # IST, signals outside this window are ignored
TRADE_END = dtime(15, 0)

# other fields of the apply payload (maxqty, ranges, ival, jval, sval, side are set by the script)
APPLY_PARAMS = {
    "jqty1": 1, "jqty2": 1,
    "target": 30000, "stoploss": -30000, "maxloss": -30000,
    "max_unhedge": 2, "PRNT": 1,
}
# ==========================================================================

for _n in ("candles", "indicators"):
    if _n not in globals():
        raise RuntimeError(f"'{_n}' not found - run candles.py / indicators.py first")
RSI_COL = "RSI_14"          # column of `indicators` used as the signal

IST = timezone(timedelta(hours=5, minutes=30))
PF_OF = {tok: pf for pf, tok in INSTR.values()}
NAME_OF = {tok: n for n, (pf, tok) in INSTR.items()}
TOKENS = list(PF_OF)
ALL_PF = list(PF_OF.values())
ZONE_SIDE = {"OVERBOUGHT": "SELL", "OVERSOLD": "BUY"}

# re-run safe
for _h in ("h_sig", "h_px"):
    if _h in globals():
        try: globals()[_h].stop()
        except Exception: pass
if "churn_q" in globals():
    churn_q.put(None)
if "hft" in globals() and hft is not None:
    try: hft.close()
    except Exception: pass

if "cmd_writer" not in globals():
    cmd_writer = DynamicTableWriter({"PF": dht.int64, "Cmd": dht.string,
                                     "Payload": dht.string, "ReplyId": dht.string})
    cmd_queue_automated = cmd_writer.table

# cumulative maxqty per portfolio (kept across re-pastes). If a PF already traded, set caps[pf] = its TotalTradedEntryQty
if "caps" not in globals():
    caps = {}
for _pf in ALL_PF:
    caps.setdefault(_pf, 0)


class HftClient:
    def __init__(self, url):
        import nats
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.nc = self._run(nats.connect(url, connect_timeout=3))

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(TIMEOUT_S + 5)

    def send(self, pf, cmd, payload):
        reply_id = str(uuid.uuid4())
        payload_str = json.dumps(payload)
        body = {"Cmd": cmd, "Payload": payload_str, "ReplyId": reply_id}
        subject = f"cmd.hft.{pf}"
        msg = self._run(self.nc.request(subject, json.dumps(body).encode(), timeout=TIMEOUT_S))
        reply = msg.data.decode()
        print(f"[{cmd}] {subject} -> {reply}")
        if json.loads(reply).get("Status") == "ok":
            cmd_writer.write_row(int(pf), cmd, payload_str, reply_id)
        return reply

    def close(self):
        if not self.loop.is_running():
            return
        self._run(self.nc.close())
        self.loop.call_soon_threadsafe(self.loop.stop)


hft = None if DRY_RUN else HftClient(NATS_URL)

def _send(pf, cmd, payload):
    if hft is None:
        raise RuntimeError("DRY_RUN is True - set DRY_RUN = False and re-run to send commands")
    payload = dict(payload)
    payload["SENDER"] = SENDER
    return hft.send(pf, cmd, payload)

def pause(pf, user_id):
    return _send(pf, "pause_unpause", {"Action": "Pause", "USERIDENTIFIER": user_id, "user_id": user_id})

def unpause(pf, user_id):
    return _send(pf, "pause_unpause", {"Action": "UnPause", "USERIDENTIFIER": user_id, "user_id": user_id})

def squareup(pf, user_id):
    return _send(pf, "squareup", {"USERIDENTIFIER": user_id, "user_id": user_id})

def apply(pf, params, user_id):
    payload = dict(params)
    payload["USERIDENTIFIER"] = user_id
    payload["user_id"] = user_id
    return _send(pf, "apply", payload)

def apply_add(pf, params, add_qty, user_id):
    """apply with maxqty = previous cap + add_qty; cap only raised once the bridge says ok."""
    cap = caps[pf] + add_qty
    p = dict(params)
    p["maxqty"] = cap
    reply = apply(pf, p, user_id)
    if json.loads(reply).get("Status") == "ok":
        caps[pf] = cap
    else:
        print(f"[churning] apply to PF {pf} not ok, cap stays {caps[pf]}: {reply}")


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
churn_ltp = {}                                    # token -> last Close (raw)
zones = {t: None for t in TOKENS}                 # token -> current RSI zone
rsis = {t: None for t in TOKENS}                  # token -> last RSI x100
if "live" not in globals():                       # kept across re-pastes so a re-paste does not re-send the same side
    live = {}
for _t in TOKENS:
    live.setdefault(_t, None)                     # token -> side last started (BUY/SELL)

def rsi_zone(rsi_x100):
    if rsi_x100 > RSI_HIGH * 100: return "OVERBOUGHT"
    if rsi_x100 < RSI_LOW * 100:  return "OVERSOLD"
    return None

def floor_tick(x):
    return round(math.floor(x / TICK + 1e-9) * TICK, 2)

# seed prices and RSI zones from history (no commands)
try:
    _snap = to_pandas(candles.where(" || ".join(f"token = {t}" for t in TOKENS)).last_by("token"))
    for _t, _c in zip(_snap["token"], _snap["Close"]):
        churn_ltp[int(_t)] = float(_c)
except Exception as e:
    print(f"[churning] could not seed prices: {e}")

try:
    _h = to_pandas(indicators.where(" || ".join(f"token = {t}" for t in TOKENS)).view(["token", RSI_COL]))
    for _t, _v in zip(_h["token"], _h[RSI_COL]):
        if pd.isna(_v) or int(_v) == NULL_LONG:
            continue
        zones[int(_t)] = rsi_zone(int(_v))
        rsis[int(_t)] = int(_v)
except Exception as e:
    print(f"[churning] could not replay history: {e}")


def make_params(token, side):
    px = churn_ltp.get(token)
    if px is None:
        return None
    rs = px / PRICE_DIV                                    # close in rupees
    ival = max(floor_tick(rs * IVAL_PCT), TICK)
    jval = max(floor_tick(ival * JVAL_FRAC), TICK)
    sval = max(floor_tick(ival * SVAL_MULT), TICK)
    p = dict(APPLY_PARAMS)
    p.update({"ival": ival, "jval": jval, "sval": sval,
            "lowerrange": max(round(rs - RANGE_BELOW), 0), "upperrange": round(rs + RANGE_ABOVE),
            SIDE_FIELD: SIDE_VALUES[side]})
    return p

def activate(token, side):
    pf = PF_OF[token]
    params = make_params(token, side)
    _do(pause, pf, USER_ID)
    _do(apply_add, pf, params, ADD_QTY, USER_ID)
    _do(unpause, pf, USER_ID)
    live[token] = side
    print(f"[churning] {NAME_OF[token]} PF {pf} -> {side} maxqty {caps[pf] + ADD_QTY} "
          f"ival {params['ival']} jval {params['jval']} sval {params['sval']} "
          f"range {params['lowerrange']}-{params['upperrange']}")

def on_signal(token, zone, force=False):
    """RSI of `token` entered a zone. Manual: on_signal(48987, "OVERBOUGHT") or sell("reliance")."""
    if not (TRADE_START <= datetime.now(IST).time() <= TRADE_END):
        print(f"[churning] {NAME_OF[token]} -> {zone} outside trade window, ignored")
        return
    if token not in churn_ltp:
        print(f"[churning] no candle close yet for {NAME_OF[token]}, {zone} skipped")
        return
    if not force and live[token] == ZONE_SIDE[zone]:
        print(f"[churning] {NAME_OF[token]} -> {zone}: already running {live[token]}, not resent")
        return
    rsi = rsis[token]
    print(f"[churning] {NAME_OF[token]} RSI {rsi / 100 if rsi is not None else '?'} -> {zone}")
    activate(token, ZONE_SIDE[zone])

def sell(name): on_signal(INSTR[name][1], "OVERBOUGHT", force=True)
def buy(name):  on_signal(INSTR[name][1], "OVERSOLD", force=True)

def pause_all():
    for pf in ALL_PF: _do(pause, pf, USER_ID)

def exit_all():
    """Manual: pause + square up every portfolio."""
    for pf in ALL_PF:
        _do(pause, pf, USER_ID)
        _do(squareup, pf, USER_ID)
    for t in TOKENS: live[t] = None

def status():
    for t in TOKENS:
        r = rsis[t]
        print(f"{NAME_OF[t]:10} PF {PF_OF[t]} zone {zones[t]} rsi {r / 100 if r is not None else None} "
              f"side {live[t]} cap {caps[PF_OF[t]]} close {churn_ltp.get(t)}")

def stop():
    h_sig.stop()
    h_px.stop()
    churn_q.put(None)
    if hft is not None:
        hft.close()


LOG_RSI = True              # print symbol + RSI + close on every candle update

def on_price(update, is_replay):
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    for tok, close in zip(ch["token"], ch["Close"]):
        tok = int(tok)
        if tok in PF_OF and close != NULL_LONG and close > 0:
            churn_ltp[tok] = float(close)

def on_candle(update, is_replay):
    ch = update.added(["token", RSI_COL])
    if ch is None:
        return
    for tok, rsi in zip(ch["token"], ch[RSI_COL]):
        tok = int(tok)
        if tok not in PF_OF:
            continue
        if rsi == NULL_LONG:
            continue
        rsi = int(rsi)
        zone = rsi_zone(rsi)
        prev = zones[tok]
        rsis[tok] = rsi
        zones[tok] = zone
        if LOG_RSI:
            px = churn_ltp.get(tok)
            print(f"[rsi] {datetime.now(IST):%H:%M:%S} {NAME_OF[tok]:10} "
                  f"rsi {rsi / 100:.2f} close {px / PRICE_DIV if px else '?'} zone {zone}")
        if zone is not None and zone != prev:              # fires on entering a zone only
            on_signal(tok, zone)

h_px = listen(candles, on_price, do_replay=False)           # price first: candles ticks before the derived indicators
h_sig = listen(indicators, on_candle, do_replay=False)
print(f"[churning] armed ({'DRY RUN' if DRY_RUN else 'LIVE'}), {len(TOKENS)} instruments, "
      f"RSI >{RSI_HIGH} sell / <{RSI_LOW} buy")

if ACT_ON_START:
    for t in TOKENS:
        if zones[t] is not None:
            print(f"[churning] {NAME_OF[t]} already in {zones[t]} at start, activating")
            on_signal(t, zones[t])