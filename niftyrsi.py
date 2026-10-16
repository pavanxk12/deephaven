# automation.py -- ONE underlying (nearest NIFTY future RSI) -> BUY options on N_STRIKES strikes, always BUY side.
#   RSI > RSI_HIGH -> BUY PUTs   | RSI < RSI_LOW -> BUY CALLs
# Strikes: base = strike nearest to synthetic future (K + CE(K) - PE(K)); slot k = base shifted by STRIKE_MODE / STRIKE_GAP.
# Each slot (token, k) has its own PF, created lazily, never reused. One PF = one option contract for its whole life.
# On a signal, per slot:
#   no PF yet                -> NEW PF -> subscribe(option) -> pause -> apply -> unpause
#   PF on a different option -> old PF: pause -> squareup (opt) -> unsubscribe (opt), RETIRED -> NEW PF -> ...
#   PF already on that option-> pause -> apply -> unpause
# SEND SWITCH: send_off() / send_on().  PER-INSTRUMENT SWITCH: set_allow(x, False).  PER-PF: pause_pf, unpause_pf, squareup_pf.
# Needs `master`, `candles` (token, Close) and `indicators` (token, RSI) in the same Deephaven session.
# `candles` must contain the NIFTY option tokens for ALL strikes you want (base +/- N_STRIKES*STRIKE_GAP).
# RSI is int64 = RSI x 100.
import asyncio, json, math, time, uuid
import threading
from datetime import datetime, timezone, timedelta, time as dtime
import numpy as np
import pandas as pd
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.constants import NULL_LONG
from deephaven.table_listener import listen
from deephaven.pandas import to_pandas

# =============================== USER INPUT ===============================
DRY_RUN = False
SEND_ON = False
USER_ID = 4
SENDER = "AUTOMATION"
NATS_URL = "nats://192.168.1.104:4222"
TIMEOUT_S = 5

UNDERLYING = "NIFTY"
OPT_EXPIRY_IDX = 0
PF_MAP_FILE = "/data/stocks_pf.json"                 # "token:slot" -> PF (old format "token" = slot 0 is still read)
PF_SUB_FILE = "/data/stocks_pf_sub.json"
PF_START = 1933

# ---- NEW: multi-strike ----
N_STRIKES = 3               # number of strikes churning is started on (1 = only the strike nearest the synthetic future)
STRIKE_MODE = "OTM"         # "OTM" = away from the money, "ITM" = into the money, "BOTH" = 0, +1 OTM, 1 ITM, +2 OTM, 2 ITM ...
STRIKE_GAP = 1              # step between slots in LISTED strikes (1 = neighbouring strikes, 2 = every second strike)

SUB_PAYLOAD = {"T1": 0, "gtw": 32395, "PRNT": 1, "code": "", "ival": 1, "jval": 0.75, "side": "BUY", "sval": 3, "trp": 0,
               "jqty1": 1, "jqty2": 1, "openq": 0, "maxqty": 0, "target": 1000, "maxloss": -400, "stoploss": -400,
               "lowerrange": 0, "upperrange": 10000, "max_unhedge": 3, "strategyname": "Churning",
               "USERIDENTIFIER": 4, "user_id": 4}
RESET_ON_START = True
RSI_HIGH = 70
RSI_LOW = 30

SIDE_FIELD = "side"
SIDE_VALUES = {"PUT": "BUY", "CALL": "BUY"}

UNSUBSCRIBE_ON_RETIRE = True
SQUAREUP_ON_REPOINT = True

OPT_IVAL_PCT = 0.01
OPT_RANGE_BELOW_PCT = 0.2
OPT_RANGE_ABOVE_PCT = 0.2
TRP_PCT = 0.01
ACT_ON_START = True         # WARNING: every re-paste with this True adds another ADD_QTY to maxqty (per slot)

ADD_QTY = 3
PRICE_DIV = 100
TICK = 0.1
JVAL_FRAC = 0.75
SVAL_MULT = 5

FAIL_COOLDOWN_S = 30
MAX_PARALLEL = 0

TRADE_START = dtime(9, 20)
TRADE_END = dtime(15, 20)

APPLY_PARAMS = {
    "jqty1": 1, "jqty2": 1,
    "target": 1000, "stoploss": -400, "maxloss": -400,
    "max_unhedge": 3, "PRNT": 1,
}
# ==========================================================================

for _n in ("candles", "indicators", "master"):
    if _n not in globals():
        raise RuntimeError(f"'{_n}' not found - run the required scripts first (build_master.py for master)")
if N_STRIKES < 1 or STRIKE_GAP < 1 or STRIKE_MODE not in ("OTM", "ITM", "BOTH"):
    raise RuntimeError("N_STRIKES >= 1, STRIKE_GAP >= 1 and STRIKE_MODE in OTM/ITM/BOTH required")
RSI_COL = "RSI_9"

IST = timezone(timedelta(hours=5, minutes=30))
from deephaven.execution_context import get_exec_ctx
EXEC_CTX = get_exec_ctx()
ZONE_SIDE = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}

_f = master["futs"]
_o = master["opt"]

if "tick" not in _f.columns:
    raise RuntimeError("master has no tick column - re-run the updated build_master.py")
TICK_OF = {int(t): float(k) for df_, c in ((_f, "fut_token"), (_o, "Token"))
           for t, k in zip(df_[c], df_["tick"]) if pd.notna(k) and k > 0}
_fut_tick = _f.dropna(subset=["tick"]).drop_duplicates("TckrSymb").set_index("TckrSymb")["tick"]
for _tk, _sy in zip(master["eqs"]["eq_token"], master["eqs"]["TckrSymb"]):
    if _sy in _fut_tick.index and _fut_tick[_sy] > 0:
        TICK_OF[int(_tk)] = float(_fut_tick[_sy])
TOKEN_OF = {s.upper(): t for t, s in master["symbol_of"].items()}

OPT_IDX, EXPIRIES = {}, {}
for (_s, _e, _ot), _g in _o.groupby(["TckrSymb", "expiry", "OptionType"]):
    _g = _g.sort_values("strike")
    OPT_IDX[(_s, _e, _ot)] = (_g["strike"].to_numpy(dtype=float), _g["Token"].to_numpy())
    EXPIRIES.setdefault((_s, _ot), []).append(_e)
for _k in EXPIRIES:
    EXPIRIES[_k].sort()
FUT_INFO = {int(t): (s, e) for t, s, e in zip(_f["fut_token"], _f["TckrSymb"], _f["expiry"])}

def load_pf_map():
    """(token, slot) -> PF. File keys are "token:slot"; a plain "token" (old format) means slot 0."""
    try:
        with open(PF_MAP_FILE) as f:
            raw = json.load(f)
    except FileNotFoundError:
        return {}
    out = {}
    for k, v in raw.items():
        t, _, s = str(k).partition(":")
        out[(int(t), int(s) if s else 0)] = int(v)
    return out

def load_subs():
    try:
        with open(PF_SUB_FILE) as f:
            return {int(k): int(v) for k, v in json.load(f).items()}
    except FileNotFoundError:
        return {}

PF_MAP = load_pf_map()

_today = pd.Timestamp(datetime.now(IST).date())
_nf = _f[(_f["TckrSymb"] == UNDERLYING) & (_f["expiry"] >= _today)].sort_values("expiry")
if _nf.empty:
    raise RuntimeError(f"no {UNDERLYING} future found in master")
_tok = int(_nf["fut_token"].iloc[0])
if _tok not in TICK_OF:
    raise RuntimeError(f"{UNDERLYING} future {_tok} has no tick in master")
NAME_OF = {_tok: master["symbol_of"].get(_tok, UNDERLYING)}
print(f"[churning] signal source: {NAME_OF[_tok]} (token {_tok}), option expiry index {OPT_EXPIRY_IDX}, "
      f"{N_STRIKES} strike(s) {STRIKE_MODE} gap {STRIKE_GAP}")

TOKENS = list(NAME_OF)
PF_OF = {sk: pf for sk, pf in PF_MAP.items() if sk[0] in NAME_OF}      # (token, slot) -> newest PF (also slots above N_STRIKES from older runs)
ALL_PF = list(PF_OF.values())
print(f"[churning] tracking {NAME_OF[_tok]}; {len(PF_OF)} slot portfolio(s) already exist, the rest are created on the first signal")
print("[churning] future ticks:", {NAME_OF[t]: TICK_OF[t] for t in TOKENS})

def slots_of(t):
    """Slots of an underlying: 0..N_STRIKES-1 plus any older slot that still has a PF."""
    return sorted({(t, k) for k in range(N_STRIKES)} | {sk for sk in PF_OF if sk[0] == t})

# re-run safe
for _h in ("h_sig", "h_px"):
    if _h in globals():
        try: globals()[_h].stop()
        except Exception: pass
if "hft" in globals() and hft is not None:
    try: hft.close()
    except Exception: pass

if "cmd_writer" not in globals():
    cmd_writer = DynamicTableWriter({"PF": dht.int64, "Cmd": dht.string,
                                     "Payload": dht.string, "ReplyId": dht.string})
    cmd_queue_automated = cmd_writer.table

if "caps" not in globals():
    caps = {}
for _pf in ALL_PF:
    caps.setdefault(_pf, 0)

if "subscribed" not in globals():
    subscribed = load_subs()

def last_pf():
    found = list(PF_MAP.values()) + list(subscribed.keys())
    try:
        with open(PF_MAP_FILE) as f:
            found += [int(v) for v in json.load(f).values()]
    except (FileNotFoundError, ValueError):
        pass
    try:
        with open(PF_SUB_FILE) as f:
            found += [int(k) for k in json.load(f).keys()]
    except (FileNotFoundError, ValueError):
        pass
    return max(found) if found else None

_last = last_pf()
print(f"[churning] last portfolio number in the JSON files: {_last}, next new portfolio gets {_last + 1}" if _last is not None
      else f"[churning] no portfolio numbers in the JSON files, first new portfolio gets PF_START {PF_START}")

# slot (token, k) -> option token it currently trades
if "opt_of" not in globals():
    opt_of = {}

def save_subs():
    with open(PF_SUB_FILE, "w") as f:
        json.dump(subscribed, f, indent=1)

if "auto_allow" not in globals():
    auto_allow = {}
for _t in TOKENS:
    auto_allow.setdefault(_t, True)

if "paused_pf" not in globals():
    paused_pf = set()


class HftClient:
    def __init__(self, url):
        import nats
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.nc = self._run(nats.connect(url, connect_timeout=3))
        self.sem = None
        if MAX_PARALLEL and MAX_PARALLEL > 0:
            self.sem = self._run(self._make_sem(MAX_PARALLEL))

    async def _make_sem(self, n):
        return asyncio.Semaphore(n)

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(TIMEOUT_S + 5)

    async def _request(self, subject, data):
        return await self.nc.request(subject, data, timeout=TIMEOUT_S)

    async def arequest(self, pf, cmd, payload):
        reply_id = str(uuid.uuid4())
        payload_str = json.dumps(payload)
        body = {"Cmd": cmd, "Payload": payload_str, "ReplyId": reply_id}
        subject = f"cmd.hft.{pf}"
        data = json.dumps(body).encode()
        if self.sem is not None:
            async with self.sem:
                msg = await self._request(subject, data)
        else:
            msg = await self._request(subject, data)
        reply = msg.data.decode()
        print(f"[{cmd}] {subject} -> {reply}")
        r = json.loads(reply)
        if r.get("Status") == "ok":
            cmd_writer.write_row(int(pf), cmd, payload_str, reply_id)
            if cmd == "pause_unpause":
                if payload.get("Action") == "Pause":
                    paused_pf.add(int(pf))
                else:
                    paused_pf.discard(int(pf))
        return r

    def close(self):
        if not self.loop.is_running():
            return
        self._run(self.nc.close())
        self.loop.call_soon_threadsafe(self.loop.stop)


hft = None if DRY_RUN else HftClient(NATS_URL)
print("[churning] NATS: " + ("DRY RUN, no connection" if DRY_RUN else f"connected to {NATS_URL}"))


_pf_locks = {}

def _lock(pf):
    return _pf_locks.setdefault(pf, asyncio.Lock())

def _payload(d):
    d = dict(d)
    d.update({"USERIDENTIFIER": USER_ID, "user_id": USER_ID, "SENDER": SENDER})
    return d

def submit(coro):
    if not SEND_ON or hft is None:
        coro.close()
        return
    def _done(f):
        if not f.cancelled() and f.exception():
            print(f"[churning] job FAILED: {f.exception()!r}")
    asyncio.run_coroutine_threadsafe(coro, hft.loop).add_done_callback(_done)

last_fail = {}                       # slot (token, k) -> time.time() of the last failed swap

async def _retire_async(old_pf):
    async with _lock(old_pf):
        r = await hft.arequest(old_pf, "pause_unpause", _payload({"Action": "Pause"}))
        if r.get("Status") != "ok":
            raise RuntimeError(f"pause of old PF {old_pf} not ok, new PF skipped: {r}")
        if SQUAREUP_ON_REPOINT:
            r = await hft.arequest(old_pf, "squareup", _payload({}))
            if r.get("Status") != "ok":
                raise RuntimeError(f"squareup of old PF {old_pf} not ok, new PF skipped: {r}")
        if UNSUBSCRIBE_ON_RETIRE:
            r = await hft.arequest(old_pf, "unsubscribe", {**SUB_PAYLOAD, "T1": subscribed.get(old_pf, 0)})
            if r.get("Status") != "ok":
                print(f"[churning] unsubscribe of old PF {old_pf} not ok (left paused): {r}")

async def _swap_async(pf, sk, opt_tok, params, prev_leg, prev_opt, old_pf=None):
    """[retire old PF] -> [subscribe the option] -> pause -> apply (maxqty = cap + ADD_QTY) -> unpause. One slot."""
    try:
        if old_pf is not None:
            await _retire_async(old_pf)
        async with _lock(pf):
            if subscribed.get(pf) is None:
                r = await hft.arequest(pf, "subscribe", {**SUB_PAYLOAD, "T1": opt_tok})
                if r.get("Status") != "ok":
                    raise RuntimeError(f"subscribe not ok, apply skipped: {r}")
                subscribed[pf] = opt_tok
                save_subs()
            r = await hft.arequest(pf, "pause_unpause", _payload({"Action": "Pause"}))
            if r.get("Status") != "ok":
                raise RuntimeError(f"pause not ok, apply skipped: {r}")
            cap = caps[pf] + ADD_QTY
            r = await hft.arequest(pf, "apply", _payload({**params, "maxqty": cap}))
            if r.get("Status") != "ok":
                raise RuntimeError(f"apply not ok, left paused: {r}")
            caps[pf] = cap
            r = await hft.arequest(pf, "pause_unpause", _payload({"Action": "UnPause"}))
            if r.get("Status") != "ok":
                raise RuntimeError(f"unpause not ok, may still be paused: {r}")
    except Exception as e:
        if old_pf is not None and subscribed.get(pf) is None:
            PF_OF[sk] = old_pf
            PF_MAP[sk] = old_pf
            save_pf_map()
            if pf in ALL_PF:
                ALL_PF.remove(pf)
            caps.pop(pf, None)
        live[sk] = prev_leg
        opt_of[sk] = prev_opt
        last_fail[sk] = time.time()
        print(f"[churning] {NAME_OF[sk[0]]} slot {sk[1]} PF {pf} swap FAILED: {e!r}")

async def _pause_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "pause_unpause", _payload({"Action": "Pause"}))

async def _unpause_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "pause_unpause", _payload({"Action": "UnPause"}))

async def _squareup_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "squareup", _payload({}))

async def _exit_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "pause_unpause", _payload({"Action": "Pause"}))
        await hft.arequest(pf, "squareup", _payload({}))

async def _unsub_async(pf):
    async with _lock(pf):
        opt = subscribed.get(pf)
        if opt is None:
            return
        for cmd, pl in (("pause_unpause", _payload({"Action": "Pause"})),
                        ("squareup", _payload({})),
                        ("unsubscribe", {**SUB_PAYLOAD, "T1": opt})):
            r = await hft.arequest(pf, cmd, pl)
            if r.get("Status") != "ok":
                print(f"[churning] PF {pf} {cmd} not ok, stopped for this PF: {r}")
                return


# ---- state ----
churn_ltp = {}
zones = {t: None for t in TOKENS}
rsis = {t: None for t in TOKENS}
off_seen = {}                                     # underlying token -> leg already logged while OFF
if "live" not in globals():
    live = {}
for _t in TOKENS:
    for _sk in slots_of(_t):
        live.setdefault(_sk, None)                # slot -> leg last started (CALL/PUT)

# slots squared up by hand (square up / exit all / unsubscribe): they stay OFF until RSI leaves its zone, so the next
# candle does not start them again. Cleared when RSI goes back to neutral, or by a manual sell() / buy().
if "blocked" not in globals():
    blocked = set()

def all_live(token, leg):
    """True when every strike slot of this underlying runs `leg` (or was stopped by hand / has no strike in the chain)."""
    return all(live.get((token, k)) == leg or (token, k) in blocked for k in range(N_STRIKES))

def rsi_zone(rsi_x100):
    if rsi_x100 > RSI_HIGH * 100: return "OVERBOUGHT"
    if rsi_x100 < RSI_LOW * 100:  return "OVERSOLD"
    return None

def floor_tick(x, tick=TICK):
    n = math.floor(x / tick + 1e-9)
    return round(n * tick, 2) + 1e-6
def ceil_tick(x, tick=TICK):
    n = math.ceil(x / tick - 1e-9)
    return round(n * tick, 2) + 1e-6
def _on_tick(v, tick):
    return int(v * 100) % int(round(tick * 100)) == 0

def in_window():
    return TRADE_START <= datetime.now(IST).time() <= TRADE_END

if TOKENS:
    try:
        _snap = to_pandas(candles.where(f"token in {', '.join(map(str, TOKENS))}").last_by("token"))
        for _t, _c in zip(_snap["token"], _snap["Close"]):
            churn_ltp[int(_t)] = float(_c)
    except Exception as e:
        print(f"[churning] could not seed prices: {e}")

    try:
        _h = to_pandas(indicators.where(f"token in {', '.join(map(str, TOKENS))}")
                       .last_by("token").view(["token", RSI_COL]))
        for _t, _v in zip(_h["token"], _h[RSI_COL]):
            if pd.isna(_v) or int(_v) == NULL_LONG:
                continue
            zones[int(_t)] = rsi_zone(int(_v))
            rsis[int(_t)] = int(_v)
    except Exception as e:
        print(f"[churning] could not replay history: {e}")


# ---------------- option selection + sizing ----------------
def pick_expiry(sym, ot):
    today = pd.Timestamp(datetime.now(IST).date())
    later = [e for e in EXPIRIES.get((sym, ot), []) if e >= today]
    return later[OPT_EXPIRY_IDX] if len(later) > OPT_EXPIRY_IDX else None

def synthetic(sym, exp, spot):
    ce_strikes, ce_toks = OPT_IDX[(sym, exp, "CE")]
    pe_strikes, pe_toks = OPT_IDX[(sym, exp, "PE")]
    i = int(np.abs(ce_strikes - spot).argmin())
    k = ce_strikes[i]
    j = np.flatnonzero(pe_strikes == k)
    if len(j) == 0:
        return None
    ce, pe = opt_price(int(ce_toks[i])), opt_price(int(pe_toks[j[0]]))
    if ce is None or pe is None:
        return None
    return k + (ce - pe) / PRICE_DIV

def strike_shift(k):
    """How many (gap-sized) steps slot k sits away from the base strike, in the OTM direction (negative = ITM)."""
    if STRIKE_MODE == "ITM":
        return -k
    if STRIKE_MODE == "BOTH":
        return 0 if k == 0 else ((k + 1) // 2 if k % 2 else -(k // 2))    # 0, +1, -1, +2, -2 ...
    return k                                                                # OTM

def pick_options(token, leg, verbose=True):
    """{slot k: option token} for the N_STRIKES strikes. Slot 0 = strike nearest to the synthetic future.
    CALL -> CE (OTM = higher strike), PUT -> PE (OTM = lower strike). Slots falling off the strike list are left out."""
    sym, _ = FUT_INFO[token]
    px = churn_ltp.get(token)
    if px is None:
        return {}
    ot = "CE" if leg == "CALL" else "PE"
    exp = pick_expiry(sym, ot)
    if exp is None:
        return {}
    fut = px / PRICE_DIV
    ref = synthetic(sym, exp, fut)
    src = "synthetic"
    if ref is None:
        ref, src = fut, "future close (no CE/PE price at the nearest strike)"
    strikes, toks = OPT_IDX[(sym, exp, ot)]
    base = int(np.abs(strikes - ref).argmin())
    sign = 1 if ot == "CE" else -1
    out = {}
    for k in range(N_STRIKES):
        idx = base + sign * strike_shift(k) * STRIKE_GAP
        if 0 <= idx < len(strikes):
            out[k] = int(toks[idx])
    if verbose:
        print(f"[churning] {NAME_OF[token]} future {fut:.2f} {src} {ref:.2f} -> {sym} {exp:%d%b%y} {ot} strikes "
              + ", ".join(f"[{k}] {strikes[(base + sign * strike_shift(k) * STRIKE_GAP)]:g}" for k in out))
    return out

def opt_price(tok):
    if tok in churn_ltp:
        return churn_ltp[tok]
    try:
        with EXEC_CTX:
            s = to_pandas(candles.where(f"token == {tok}").last_by("token"))
        if len(s):
            churn_ltp[tok] = float(s["Close"].iloc[0])
            return churn_ltp[tok]
    except Exception as e:
        print(f"[churning] could not read price of option {tok}: {e}")
    return None

def make_params(opt_tok):
    px = opt_price(opt_tok)
    if px is None:
        return None
    tick = TICK_OF.get(opt_tok)
    if tick is None:
        print(f"[churning] option {opt_tok} has no tick in master, skipped")
        return None
    rs = px / PRICE_DIV
    ival = max(floor_tick(rs * OPT_IVAL_PCT, tick), tick)
    jval = max(floor_tick(ival * JVAL_FRAC, tick), tick)
    sval = max(floor_tick(ival * SVAL_MULT, tick), tick)
    trp = ceil_tick(rs * (1 + TRP_PCT / 100), tick)
    lower = max(floor_tick(rs * (1 - OPT_RANGE_BELOW_PCT), tick), tick)
    upper = floor_tick(rs * (1 + OPT_RANGE_ABOVE_PCT), tick)
    for name, v in (("ival", ival), ("jval", jval), ("sval", sval), ("trp", trp),
                    ("lowerrange", lower), ("upperrange", upper)):
        if not _on_tick(v, tick):
            print(f"[churning] option {opt_tok} {name} {v} is not a multiple of tick {tick}, NOT sent")
            return None
    p = dict(APPLY_PARAMS)
    p.update({"ival": ival, "jval": jval, "sval": sval, "trp": trp,
              "lowerrange": lower, "upperrange": upper,
              SIDE_FIELD: "BUY"})
    return p

def save_pf_map():
    with open(PF_MAP_FILE, "w") as f:
        json.dump({f"{t}:{k}": pf for (t, k), pf in PF_MAP.items()}, f, indent=1)

def new_pf(sk, why):
    last = last_pf()
    pf = PF_START if last is None else last + 1
    PF_MAP[sk] = pf
    save_pf_map()
    PF_OF[sk] = pf
    ALL_PF.append(pf)
    caps.setdefault(pf, 0)
    print(f"[churning] {NAME_OF[sk[0]]} slot {sk[1]} {why}: created PF {pf}")
    return pf

def activate(token, leg, force=False):
    opts = pick_options(token, leg)
    if not opts:
        print(f"[churning] {NAME_OF[token]} no option found for {leg}, not activated")
        return
    for k in range(N_STRIKES):
        sk = (token, k)
        opt = opts.get(k)
        if sk in blocked and not force:
            continue                                   # stopped by hand: stays off until RSI leaves the zone
        if opt is None:
            if live.get(sk) != leg:
                print(f"[churning] {NAME_OF[token]} slot {k}: strike is outside the option chain, skipped")
                live[sk] = leg                         # counts as done for this leg: no retry on every candle (a flip tries again)
            continue
        if live.get(sk) == leg:
            continue                                   # this slot already runs this leg
        if not force and FAIL_COOLDOWN_S and time.time() - last_fail.get(sk, 0) < FAIL_COOLDOWN_S:
            continue                                   # this slot failed recently
        params = make_params(opt)
        if params is None:
            print(f"[churning] {NAME_OF[token]} slot {k}: no option price or invalid params, not activated")
            continue
        prev_leg, prev_opt = live.get(sk), opt_of.get(sk)
        live[sk] = leg
        opt_of[sk] = opt
        nm = master["symbol_of"].get(opt, opt)
        cur_pf = PF_OF.get(sk)
        old_pf = cur_pf if cur_pf is not None and subscribed.get(cur_pf) not in (None, opt) else None
        if DRY_RUN:
            print(f"[churning][DRY] {NAME_OF[token]} slot {k} PF {'NEW (old ' + str(old_pf) + ' retired)' if old_pf else PF_OF.get(sk, 'NEW')} "
                  f"-> BUY {nm}: subscribe if needed, pause, apply {params}, unpause")
            continue
        if old_pf is not None:
            pf = new_pf(sk, f"moves to {nm}, PF {old_pf} retired")
        else:
            pf = cur_pf or new_pf(sk, "qualified and has no portfolio")
        print(f"[churning] {NAME_OF[token]} slot {k} PF {pf} -> BUY {nm} maxqty {caps[pf] + ADD_QTY} "
              f"ival {params['ival']} jval {params['jval']} sval {params['sval']} "
              f"range {params['lowerrange']}-{params['upperrange']}")
        submit(_swap_async(pf, sk, opt, params, prev_leg, prev_opt, old_pf))

def on_signal(token, zone, force=False):
    """RSI entered a zone. Manual: sell(x) = buy PUTs, buy(x) = buy CALLs (on all N_STRIKES strikes)."""
    if not in_window():
        print(f"[churning] {NAME_OF[token]} -> {zone} outside trade window, ignored")
        return
    if token not in churn_ltp:
        print(f"[churning] no candle close yet for {NAME_OF[token]}, {zone} skipped")
        return
    if not force and not auto_allow.get(token, True):
        return
    if force:
        blocked.difference_update({sk for sk in blocked if sk[0] == token})
    leg = ZONE_SIDE[zone]
    if not force and all_live(token, leg):
        return
    if not SEND_ON:
        if force or off_seen.get(token) != leg:
            off_seen[token] = leg
            opts = pick_options(token, leg, verbose=False)
            names = ", ".join(str(master["symbol_of"].get(o, "?")) for o in opts.values())
            print(f"[churning][OFF] {NAME_OF[token]} would BUY {names} ({leg}), NOT sent")
        return
    rsi = rsis[token]
    print(f"[churning] {NAME_OF[token]} RSI {rsi / 100 if rsi is not None else '?'} -> {zone}")
    activate(token, leg, force)

def _resolve(x):
    if isinstance(x, str):
        for tok, n in NAME_OF.items():
            if n.lower() == x.lower():
                return tok
        raise KeyError(f"unknown symbol {x}")
    if x in NAME_OF:
        return x
    for sk, pf in PF_OF.items():
        if pf == x:
            return sk[0]
    raise KeyError(f"unknown instrument {x}")

def reset(x=None):
    toks = TOKENS if x is None else [_resolve(x)]
    for t in toks:
        for sk in slots_of(t):
            live[sk] = None
            opt_of.pop(sk, None)
            if sk in PF_OF:
                caps[PF_OF[sk]] = 0
            last_fail.pop(sk, None)
            blocked.discard(sk)
        off_seen.pop(t, None)
    print(f"[churning] state reset for {len(toks)} instrument(s)")

def sell(x): on_signal(_resolve(x), "OVERBOUGHT", force=True)
def buy(x):  on_signal(_resolve(x), "OVERSOLD", force=True)

def _clear_live():
    for t in TOKENS:
        for sk in slots_of(t):
            live[sk] = None
            opt_of.pop(sk, None)

def pause_all():
    if not SEND_ON:
        print("[churning][OFF] pause_all NOT sent")
        return
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause {pf}")
        else: submit(_pause_async(pf))

def unpause_all():
    if not SEND_ON:
        print("[churning][OFF] unpause_all NOT sent")
        return
    for pf in PF_OF.values():
        if DRY_RUN: print(f"[churning][DRY] unpause {pf}")
        else: submit(_unpause_async(pf))

def exit_all():
    if not SEND_ON:
        print("[churning][OFF] exit_all NOT sent")
        return
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause + squareup {pf}")
        else: submit(_exit_async(pf))
    _clear_live()
    for t in TOKENS:
        blocked.update(slots_of(t))                  # no restart on the next candle; sell() / buy() or RSI leaving the zone re-arms

def set_allow(x, on):
    t = _resolve(x)
    auto_allow[t] = bool(on)
    print(f"[churning] {NAME_OF[t]} automation {'ALLOWED' if on else 'BLOCKED'}")

def pause_pf(pf):
    if not SEND_ON:
        print(f"[churning][OFF] pause PF {pf} NOT sent")
        return
    if DRY_RUN:
        print(f"[churning][DRY] pause {pf}")
        paused_pf.add(int(pf))
    else:
        submit(_pause_async(pf))

def unpause_pf(pf):
    if not SEND_ON:
        print(f"[churning][OFF] unpause PF {pf} NOT sent")
        return
    if DRY_RUN:
        print(f"[churning][DRY] unpause {pf}")
        paused_pf.discard(int(pf))
    else:
        submit(_unpause_async(pf))

def squareup_pf(pf):
    if not SEND_ON:
        print(f"[churning][OFF] squareup PF {pf} NOT sent")
        return
    if DRY_RUN: print(f"[churning][DRY] squareup {pf}")
    else: submit(_squareup_async(pf))
    for sk, p in PF_OF.items():
        if p == pf:
            live[sk] = None
            opt_of.pop(sk, None)
            blocked.add(sk)                  # stays off until RSI leaves its zone (no restart on the next candle)

def send_off():
    global SEND_ON
    SEND_ON = False
    print("[churning] sending OFF: computing only, no messages to NATS")

def send_on():
    global SEND_ON
    if hft is None:
        print("[churning] cannot turn sending ON: DRY_RUN is True so there is no NATS connection. "
              "Set DRY_RUN = False and re-paste.")
        return
    SEND_ON = True
    off_seen.clear()
    print("[churning] sending ON")
    for t in TOKENS:
        if zones[t] is not None:
            on_signal(t, zones[t])

def status():
    print(f"sending {'ON' if SEND_ON else 'OFF (compute only)'} | {N_STRIKES} strike(s) {STRIKE_MODE} gap {STRIKE_GAP}")
    for t in TOKENS:
        r = rsis[t]
        print(f"{NAME_OF[t]:24} zone {zones[t]} rsi {r / 100 if r is not None else None} "
              f"fut_close {churn_ltp.get(t)} allowed {auto_allow.get(t, True)}")
        for sk in slots_of(t):
            pf = PF_OF.get(sk)
            o = opt_of.get(sk)
            print(f"   slot {sk[1]} PF {pf if pf else '-'} leg {live.get(sk)} "
                  f"option {master['symbol_of'].get(o, '-') if o else '-'} "
                  f"subscribed {master['symbol_of'].get(subscribed.get(pf), '-') if pf else '-'} "
                  f"cap {caps.get(pf, 0)} paused {pf in paused_pf}{' BLOCKED (stopped by hand)' if sk in blocked else ''}")

def stop():
    h_sig.stop()
    h_px.stop()
    if hft is not None:
        hft.close()

def unsubscribe_all():
    """Stop signals, then pause + square up + unsubscribe every current portfolio (all slots)."""
    if not SEND_ON or hft is None:
        print("[churning] sending is OFF or DRY_RUN: run send_on() first (DRY_RUN must be False)")
        return
    h_sig.stop()
    h_px.stop()
    for pf in PF_OF.values():
        submit(_unsub_async(pf))
    _clear_live()
    for t in TOKENS:
        blocked.update(slots_of(t))
    print(f"[churning] unsubscribing {len(PF_OF)} portfolio(s), watch the log")


LOG_RSI = True

def on_price(update, is_replay):
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    for tok, close in zip(ch["token"], ch["Close"]):
        if close != NULL_LONG and close > 0:
            churn_ltp[int(tok)] = float(close)

def on_candle(update, is_replay):
    cols = ["token", RSI_COL]
    parts = [d for d in (update.added(cols), update.modified(cols)) if d is not None]
    if not parts:
        return
    for d in parts:
        for tok, rsi in zip(d["token"], d[RSI_COL]):
            tok = int(tok)
            if tok not in NAME_OF:
                continue
            if rsi == NULL_LONG:
                continue
            rsi = int(rsi)
            zone = rsi_zone(rsi)
            rsis[tok] = rsi
            zones[tok] = zone
            if zone is None:
                off_seen.pop(tok, None)
                blocked.difference_update({sk for sk in blocked if sk[0] == tok})     # a new zone entry may start the slots again
            if LOG_RSI:
                px = churn_ltp.get(tok)
                print(f"[rsi] {datetime.now(IST):%H:%M:%S} {NAME_OF[tok]:24} "
                      f"rsi {rsi / 100:.2f} "
                      f"close {px / PRICE_DIV if px else '?'} zone {zone}")

    if not in_window():
        return
    for t in TOKENS:
        z = zones[t]
        if z is not None and not all_live(t, ZONE_SIDE[z]):
            on_signal(t, z)

h_px = listen(candles, on_price, do_replay=False)
h_sig = listen(indicators, on_candle, do_replay=False)
print(f"[churning] armed ({'DRY RUN' if DRY_RUN else 'LIVE'}, sending {'ON' if SEND_ON else 'OFF'}), {len(TOKENS)} instruments, "
      f"{N_STRIKES} strike(s) each, RSI >{RSI_HIGH} buy PUT / <{RSI_LOW} buy CALL")
if RESET_ON_START:
    reset()
if ACT_ON_START:
    for t in TOKENS:
        if zones[t] is not None:
            print(f"[churning] {NAME_OF[t]} already in {zones[t]} at start, activating")
            on_signal(t, zones[t])

# ---------------- unsubscribe ONE portfolio ----------------
async def _unsub_one_async(pf):
    await _unsub_async(pf)                    # pause -> squareup -> unsubscribe (stops at the first failure)
    if pf in PF_OF.values():                  # a current PF: forget its option so a later signal subscribes it again
        subscribed.pop(pf, None)              # (the PF number stays in PF_MAP_FILE, never reused)
        save_subs()                           # a retired PF keeps its record in PF_SUB_FILE so its number is never reused

def unsubscribe_pf(pf):
    """Manual: pause + square up + unsubscribe ONE portfolio, e.g. unsubscribe_pf(1900). Signals keep running."""
    pf = int(pf)
    if not SEND_ON or hft is None:
        print("[churning] sending is OFF or DRY_RUN: run send_on() first (DRY_RUN must be False)")
        return
    if subscribed.get(pf) is None:
        print(f"[churning] PF {pf} is not subscribed to any option, nothing to unsubscribe")
        return
    for sk, p in PF_OF.items():
        if p == pf:
            live[sk] = None
            opt_of.pop(sk, None)
            blocked.add(sk)                   # stays off until RSI leaves its zone
    submit(_unsub_one_async(pf))
    print(f"[churning] unsubscribing PF {pf} (pause, squareup, unsubscribe), watch the log")