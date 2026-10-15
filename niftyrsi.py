# automation.py -- ONE portfolio follows the RSI of the nearest NIFTY future (from `indicators`) but TRADES A NIFTY OPTION, always BUY side.
#   RSI > RSI_HIGH (overbought) -> BUY the PUT  (PE) nearest to the synthetic future
#   RSI < RSI_LOW  (oversold)   -> BUY the CALL (CE) nearest to the synthetic future
# The signal source is the nearest NIFTY future (UNDERLYING, nearest non-expired expiry in master["futs"]).
# The option expiry is OPT_EXPIRY_IDX: 0 = nearest NIFTY option expiry (default), 1 = the next one, 2 = the one after, ...
# Strike: synthetic future = K + CE(K) - PE(K), K = the strike nearest to the future close; the traded strike is the one
# nearest to that synthetic future. If CE or PE of K has no price yet, the future close is used instead.
# A portfolio (PF) is created LAZILY: only when the first signal arrives (RSI zone, allowed...)
# and there is no PF yet, it gets the next number: the highest PF found in PF_MAP_FILE / PF_SUB_FILE + 1
# (PF_START if those files hold none). Numbers are never reused, remembered in PF_MAP_FILE.
# The option token each PF is subscribed to is remembered in PF_SUB_FILE.
# On a signal:
#   - stock has no PF yet            -> NEW PF -> subscribe(option) -> pause -> apply -> unpause
#   - PF on a different option (CE<->PE or other strike) -> old PF: pause -> squareup (optional) -> unsubscribe (optional) and it is RETIRED
#                                       (keeps its number and its option, never reused) -> NEW PF -> subscribe(new option) -> pause -> apply -> unpause
#   - PF already on that option      -> pause -> apply -> unpause
# One PF = one option contract for its whole life. PF_OF[stock] always points to the newest PF of that stock.
# Tick size per token comes from master (futs / opt). ival / jval / sval / ranges are whole multiples of the OPTION's tick.
# Commands run ASYNC on the NATS event loop: different portfolios overlap, each portfolio stays in order.
# SEND SWITCH: send_off() = keep computing but send NOTHING to NATS, send_on() = send again.
# PER-INSTRUMENT SWITCH (operator console checkbox): set_allow(x, False) = never send params for that instrument.
# PER-PF BUTTONS (operator console): pause_pf(pf), unpause_pf(pf), squareup_pf(pf).
# Needs `master`, `candles` (token, Close) and `indicators` (token, RSI) in the same Deephaven session.
# `candles` must ALSO contain the NIFTY option tokens (the strikes around the money, CE and PE), otherwise no price -> synthetic future falls back to the future close.
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
DRY_RUN = False              # True = only print what would be sent. Set False to go live.
SEND_ON = False              # True = send commands to NATS. False = compute only. Switch live: send_on() / send_off()
USER_ID = 3
SENDER = "AUTOMATION"
NATS_URL = "nats://192.168.1.130:4222"
TIMEOUT_S = 5

# What is traded
UNDERLYING = "NIFTY"                                 # signal = nearest future of this underlying, options of the same underlying
OPT_EXPIRY_IDX = 0                                   # option expiry: 0 = nearest, 1 = next, 2 = the one after, ...
PF_MAP_FILE = "/data/stocks_pf.json"                 # remembers underlying token -> PF (never reused)
PF_SUB_FILE = "/data/stocks_pf_sub.json"             # remembers PF -> option token it is subscribed to
PF_START = 1555                                      # first portfolio number, used ONLY when the two JSON files above hold no portfolio number yet
# sent as the subscribe payload when a PF is created / repointed; only T1 (the OPTION token) is replaced
SUB_PAYLOAD = {"T1": 0, "gtw": 32395, "PRNT": 1, "code": "", "ival": 1, "jval": 0.75, "side": "BUY", "sval": 3, "trp": 0,
               "jqty1": 1, "jqty2": 1, "openq": 0, "maxqty": 0, "target": 1000, "maxloss": -1000, "stoploss": -1000,
               "lowerrange": 0, "upperrange": 10000, "max_unhedge": 1, "strategyname": "Churning",
               "USERIDENTIFIER": 3, "user_id": 3}
RESET_ON_START = True       # forget live legs and caps on every paste (fresh start)
RSI_HIGH = 60               # RSI above this -> buy PUT
RSI_LOW = 40                # RSI below this -> buy CALL

# Overbought -> PUT, oversold -> CALL. The "side" sent to the bridge is ALWAYS BUY.
SIDE_FIELD = "side"
SIDE_VALUES = {"PUT": "BUY", "CALL": "BUY"}

# --- option selection ---
UNSUBSCRIBE_ON_RETIRE = True  # after the squareup, also unsubscribe the old PF from its option (payload = SUB_PAYLOAD with that T1; check it against your bridge)
SQUAREUP_ON_REPOINT = True  # when a stock moves to a different option (CE<->PE...), square up the OLD portfolio (it is paused either way)

# --- option sizing (percent of the OPTION premium, not of the stock price) ---
OPT_IVAL_PCT = 0.01         # ival = 0.2% of option premium (tune!)
OPT_RANGE_BELOW_PCT = 0.2   # lowerrange = premium * (1 - 0.5)
OPT_RANGE_ABOVE_PCT = 0.2   # upperrange = premium * (1 + 1.0)
TRP_PCT = 0.01   
# True = if RSI is already in a zone when the script starts, activate immediately.
# WARNING: every re-paste with this True adds another ADD_QTY to maxqty.
ACT_ON_START = True

ADD_QTY = 5                 # added to the cumulative maxqty on every activation / flip

PRICE_DIV = 100             # candle Close is paise -> 100. Use 1 if already rupees.

TICK = 0.1                  # only the default argument of floor_tick; real ticks come from TICK_OF
JVAL_FRAC = 0.75            # jval  = 0.75 x ival
SVAL_MULT = 3               # sval  = 3 x ival

FAIL_COOLDOWN_S = 30        # after a failed swap, the same portfolio is not retried for this long (0 = off)
MAX_PARALLEL = 0            # max simultaneous requests to the bridge (0 = unlimited, e.g. 10 to throttle)

TRADE_START = dtime(9, 20)  # IST, signals outside this window are ignored
TRADE_END = dtime(15, 0)

# other fields of the apply payload (maxqty, ranges, ival, jval, sval, side are set by the script)
APPLY_PARAMS = {
    "jqty1": 1, "jqty2": 1,
    "target": 5000, "stoploss": -5000, "maxloss": -5000,
    "max_unhedge": 1, "PRNT": 1,
}
# ==========================================================================

for _n in ("candles", "indicators", "master"):
    if _n not in globals():
        raise RuntimeError(f"'{_n}' not found - run the required scripts first (build_master.py for master)")
RSI_COL = "RSI_9"           # column of `indicators` used as the signal

IST = timezone(timedelta(hours=5, minutes=30))
from deephaven.execution_context import get_exec_ctx
EXEC_CTX = get_exec_ctx()                    # listeners run in threads without a context; queries there use `with EXEC_CTX:`
ZONE_SIDE = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}   # zone -> which option leg we BUY


# ---------------- nearest NIFTY future = the signal instrument ----------------
_f = master["futs"]                              # fut_token, TckrSymb, expiry, tick, lot
_o = master["opt"]                               # Token, TckrSymb, expiry, strike, OptionType, tick, lot ...

if "tick" not in _f.columns:
    raise RuntimeError("master has no tick column - re-run the updated build_master.py")
# token -> tick (rupees) for futures and options; an equity takes the tick of its own future (same TckrSymb)
TICK_OF = {int(t): float(k) for df_, c in ((_f, "fut_token"), (_o, "Token"))
           for t, k in zip(df_[c], df_["tick"]) if pd.notna(k) and k > 0}
_fut_tick = _f.dropna(subset=["tick"]).drop_duplicates("TckrSymb").set_index("TckrSymb")["tick"]   # equity tick = its future's tick
for _tk, _sy in zip(master["eqs"]["eq_token"], master["eqs"]["TckrSymb"]):
    if _sy in _fut_tick.index and _fut_tick[_sy] > 0:
        TICK_OF[int(_tk)] = float(_fut_tick[_sy])
TOKEN_OF = {s.upper(): t for t, s in master["symbol_of"].items()}   # exact symbol (equity / future / option) -> token

# ---------------- option lookup: (underlying, expiry, CE/PE) -> sorted strikes + tokens ----------------
OPT_IDX, EXPIRIES = {}, {}
for (_s, _e, _ot), _g in _o.groupby(["TckrSymb", "expiry", "OptionType"]):
    _g = _g.sort_values("strike")
    OPT_IDX[(_s, _e, _ot)] = (_g["strike"].to_numpy(dtype=float), _g["Token"].to_numpy())
    EXPIRIES.setdefault((_s, _ot), []).append(_e)
for _k in EXPIRIES:
    EXPIRIES[_k].sort()
FUT_INFO = {int(t): (s, e) for t, s, e in zip(_f["fut_token"], _f["TckrSymb"], _f["expiry"])}   # fut token -> (underlying, expiry)

def load_pf_map():
    """token -> PF of the portfolios created in earlier runs (PF_MAP_FILE); empty the first time."""
    try:
        with open(PF_MAP_FILE) as f:
            return {int(k): int(v) for k, v in json.load(f).items()}
    except FileNotFoundError:
        return {}

def load_subs():
    """pf -> option token it is subscribed to (PF_SUB_FILE); empty the first time."""
    try:
        with open(PF_SUB_FILE) as f:
            return {int(k): int(v) for k, v in json.load(f).items()}
    except FileNotFoundError:
        return {}

PF_MAP = load_pf_map()

_today = pd.Timestamp(datetime.now(IST).date())
_nf = _f[(_f["TckrSymb"] == UNDERLYING) & (_f["expiry"] >= _today)].sort_values("expiry")   # nearest non-expired NIFTY future
if _nf.empty:
    raise RuntimeError(f"no {UNDERLYING} future found in master")
_tok = int(_nf["fut_token"].iloc[0])
if _tok not in TICK_OF:
    raise RuntimeError(f"{UNDERLYING} future {_tok} has no tick in master")
NAME_OF = {_tok: master["symbol_of"].get(_tok, UNDERLYING)}      # token -> master symbol (one instrument only)
print(f"[churning] signal source: {NAME_OF[_tok]} (token {_tok}), option expiry index {OPT_EXPIRY_IDX}")

TOKENS = list(NAME_OF)                           # every tracked underlying (portfolio or not)
PF_OF = {t: PF_MAP[t] for t in TOKENS if t in PF_MAP}    # only stocks that already have a portfolio
ALL_PF = list(PF_OF.values())                    # portfolios that exist (grows as stocks qualify)
print(f"[churning] tracking {NAME_OF[_tok]}; {len(PF_OF)} portfolio(s) already exist, one is created on the first signal")

print("[churning] future ticks:", {NAME_OF[t]: TICK_OF[t] for t in TOKENS})

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

# cumulative maxqty per portfolio (kept across re-pastes). If a PF already traded, set caps[pf] = its TotalTradedEntryQty
if "caps" not in globals():
    caps = {}
for _pf in ALL_PF:
    caps.setdefault(_pf, 0)

# pf -> option token the portfolio is currently subscribed to (persisted in PF_SUB_FILE)
if "subscribed" not in globals():
    subscribed = load_subs()

def last_pf():
    """Highest portfolio number found in the JSON files (PF_MAP_FILE values, PF_SUB_FILE keys) and in memory.
    None when there is none, then the user given PF_START is used."""
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

# underlying token -> option token it currently trades
if "opt_of" not in globals():
    opt_of = {}

def save_subs():
    with open(PF_SUB_FILE, "w") as f:
        json.dump(subscribed, f, indent=1)

# per-instrument switch, set from the operator console checkbox: False = never send params for that token
if "auto_allow" not in globals():
    auto_allow = {}
for _t in TOKENS:
    auto_allow.setdefault(_t, True)

# portfolios the bridge has confirmed as paused (kept up to date from the replies in arequest);
# the operator console reads it to decide when Square up is allowed
if "paused_pf" not in globals():
    paused_pf = set()


class HftClient:
    """NATS client with its own asyncio loop (background thread). Requests are coroutines on that loop."""
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
        """Send one command and return the parsed reply dict. Raises on timeout."""
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


_pf_locks = {}                       # pf -> asyncio.Lock; keeps one PF's commands in order

def _lock(pf):
    return _pf_locks.setdefault(pf, asyncio.Lock())

def _payload(d):
    """Add the identity fields every command needs."""
    d = dict(d)
    d.update({"USERIDENTIFIER": USER_ID, "user_id": USER_ID, "SENDER": SENDER})
    return d

def submit(coro):
    """Schedule a coroutine on the NATS loop without waiting for it. Does nothing while sending is OFF."""
    if not SEND_ON or hft is None:
        coro.close()                 # nothing is sent; close so Python does not warn about an unawaited coroutine
        return
    def _done(f):
        if not f.cancelled() and f.exception():
            print(f"[churning] job FAILED: {f.exception()!r}")
    asyncio.run_coroutine_threadsafe(coro, hft.loop).add_done_callback(_done)

last_fail = {}                       # underlying token -> time.time() of the last failed swap

async def _retire_async(old_pf):
    """The stock moves to another option: stop the old portfolio (pause, optionally square up, optionally unsubscribe).
    It keeps its number, and subscribed[old_pf] keeps its option (the console and the numbering still use it);
    a new portfolio takes over the stock."""
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
            if r.get("Status") != "ok":                      # the old PF is already paused and squared up: do not block the new PF
                print(f"[churning] unsubscribe of old PF {old_pf} not ok (left paused): {r}")

async def _swap_async(pf, token, opt_tok, params, prev_leg, prev_opt, old_pf=None):
    """[retire old PF] -> [subscribe the option] -> pause -> apply (maxqty = cap + ADD_QTY) -> unpause.
    Stops and stays paused on any failure."""
    try:
        if old_pf is not None:
            await _retire_async(old_pf)
        async with _lock(pf):
            if subscribed.get(pf) is None:                   # new PF: subscribe it to the option
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
        if old_pf is not None and subscribed.get(pf) is None:    # the new PF never reached the bridge: go back to the old one
            PF_OF[token] = old_pf
            PF_MAP[token] = old_pf
            save_pf_map()
            if pf in ALL_PF:
                ALL_PF.remove(pf)
            caps.pop(pf, None)
        live[token] = prev_leg               # allow a retry on a later update
        opt_of[token] = prev_opt
        last_fail[token] = time.time()
        print(f"[churning] {NAME_OF[token]} PF {pf} swap FAILED: {e!r}")

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


# ---- state ----
churn_ltp = {}                                    # token (future AND options) -> last Close (raw)
zones = {t: None for t in TOKENS}                 # underlying token -> current RSI zone
rsis = {t: None for t in TOKENS}                  # underlying token -> last RSI x100
off_seen = {}                                     # underlying token -> leg already logged as "would BUY ..." while OFF
if "live" not in globals():                       # kept across re-pastes so a re-paste does not re-send the same leg
    live = {}
for _t in TOKENS:
    live.setdefault(_t, None)                     # underlying token -> leg last started (CALL/PUT)

def rsi_zone(rsi_x100):
    if rsi_x100 > RSI_HIGH * 100: return "OVERBOUGHT"
    if rsi_x100 < RSI_LOW * 100:  return "OVERSOLD"
    return None

def floor_tick(x, tick=TICK):
    n = math.floor(x / tick + 1e-9)
    return round(n * tick, 2) + 1e-6      # 2.3 -> 2.300001, so x100 gives 230.0001 and truncates to 230
def ceil_tick(x, tick=TICK):
    n = math.ceil(x / tick - 1e-9)
    return round(n * tick, 2) + 1e-6      # same +1e-6 trick as floor_tick so the bridge's x100 truncation lands on the tick
def _on_tick(v, tick):
    """True if the bridge, which converts rupees to paise by truncation, ends up on a whole tick."""
    return int(v * 100) % int(round(tick * 100)) == 0

def in_window():
    return TRADE_START <= datetime.now(IST).time() <= TRADE_END

# seed prices, RSI zones and Pct from history (no commands)
if TOKENS:                                        # nothing to seed while no instrument is subscribed yet
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
    """OPT_EXPIRY_IDX-th non-expired option expiry of the underlying (0 = nearest). None if there are not that many."""
    today = pd.Timestamp(datetime.now(IST).date())
    later = [e for e in EXPIRIES.get((sym, ot), []) if e >= today]
    return later[OPT_EXPIRY_IDX] if len(later) > OPT_EXPIRY_IDX else None

def synthetic(sym, exp, spot):
    """Synthetic future in rupees = K + CE(K) - PE(K), K = the strike nearest to the future close `spot`.
    None if CE or PE of that strike has no price yet."""
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

def pick_option(token, leg, verbose=True):
    """Option of the underlying with the strike nearest to the synthetic future: CALL -> CE, PUT -> PE. Returns the option token or None."""
    sym, _ = FUT_INFO[token]
    px = churn_ltp.get(token)
    if px is None:
        return None
    ot = "CE" if leg == "CALL" else "PE"
    exp = pick_expiry(sym, ot)
    if exp is None:
        return None
    fut = px / PRICE_DIV
    ref = synthetic(sym, exp, fut)
    src = "synthetic"
    if ref is None:
        ref, src = fut, "future close (no CE/PE price at the nearest strike)"
    strikes, toks = OPT_IDX[(sym, exp, ot)]
    i = int(np.abs(strikes - ref).argmin())
    if verbose:
        print(f"[churning] {NAME_OF[token]} future {fut:.2f} {src} {ref:.2f} -> {sym} {exp:%d%b%y} {strikes[i]:g} {ot}")
    return int(toks[i])

def opt_price(tok):
    """Last Close (raw) of an option; falls back to a query on `candles` if no tick was seen yet."""
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
    rs = px / PRICE_DIV                                # option premium in rupees
    ival = max(floor_tick(rs * OPT_IVAL_PCT, tick), tick)
    jval = max(floor_tick(ival * JVAL_FRAC, tick), tick)
    sval = max(floor_tick(ival * SVAL_MULT, tick), tick)
    trp = ceil_tick(rs * (1 + TRP_PCT / 100), tick)    # close + 0.1%
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
              SIDE_FIELD: "BUY"})                      # always BUY the option
    return p

def save_pf_map():
    with open(PF_MAP_FILE, "w") as f:
        json.dump(PF_MAP, f, indent=1)

def new_pf(token, why):
    """Give the stock the next free PF number and remember it in the file."""
    last = last_pf()
    pf = PF_START if last is None else last + 1      # highest number in the JSON files + 1, else the user given start
    PF_MAP[token] = pf
    save_pf_map()
    PF_OF[token] = pf
    ALL_PF.append(pf)
    caps.setdefault(pf, 0)
    print(f"[churning] {NAME_OF[token]} {why}: created PF {pf}")
    return pf

def activate(token, leg):
    opt = pick_option(token, leg)
    if opt is None:
        print(f"[churning] {NAME_OF[token]} no option found for {leg}, not activated")
        return
    params = make_params(opt)
    if params is None:
        print(f"[churning] {NAME_OF[token]} no option price or invalid params, not activated")
        return
    prev_leg, prev_opt = live[token], opt_of.get(token)
    live[token] = leg                        # mark now so an in-flight swap is not queued twice
    opt_of[token] = opt
    nm = master["symbol_of"].get(opt, opt)
    cur_pf = PF_OF.get(token)
    old_pf = cur_pf if cur_pf is not None and subscribed.get(cur_pf) not in (None, opt) else None   # stock moves to another option
    if DRY_RUN:                              # a dry run never creates a portfolio or writes the PF file
        print(f"[churning][DRY] {NAME_OF[token]} PF {'NEW (old ' + str(old_pf) + ' retired)' if old_pf else PF_OF.get(token, 'NEW')} "
              f"-> BUY {nm}: subscribe if needed, pause, apply {params}, unpause")
        return
    if old_pf is not None:
        pf = new_pf(token, f"moves to {nm}, PF {old_pf} retired")
    else:
        pf = cur_pf or new_pf(token, "qualified and has no portfolio")
    print(f"[churning] {NAME_OF[token]} PF {pf} -> BUY {nm} maxqty {caps[pf] + ADD_QTY} "
          f"ival {params['ival']} jval {params['jval']} sval {params['sval']} "
          f"range {params['lowerrange']}-{params['upperrange']}")
    submit(_swap_async(pf, token, opt, params, prev_leg, prev_opt, old_pf))

def on_signal(token, zone, force=False):
    """RSI of the underlying `token` entered a zone. Manual: sell(1147) / buy(48864) / sell("SYMBOL").
    sell = buy PUT, buy = buy CALL."""
    if not in_window():
        print(f"[churning] {NAME_OF[token]} -> {zone} outside trade window, ignored")
        return
    if token not in churn_ltp:
        print(f"[churning] no candle close yet for {NAME_OF[token]}, {zone} skipped")
        return
    if not force and not auto_allow.get(token, True):
        return                                # unticked in the operator console: no params are sent for this instrument
    if not force and live[token] == ZONE_SIDE[zone]:
        return                                # already running this leg
    if not force and FAIL_COOLDOWN_S and time.time() - last_fail.get(token, 0) < FAIL_COOLDOWN_S:
        return                                # recently failed, wait out the cooldown
    if not SEND_ON:
        # OFF: the signal is computed, nothing is sent and nothing is marked as done (live / caps stay untouched)
        leg = ZONE_SIDE[zone]
        if force or off_seen.get(token) != leg:
            off_seen[token] = leg             # log each new would-be leg once, not on every candle
            opt = pick_option(token, leg, verbose=False)
            print(f"[churning][OFF] {NAME_OF[token]} PF {PF_OF.get(token, 'NEW')} would BUY "
                  f"{master['symbol_of'].get(opt, '?')} ({leg}), NOT sent")
        return
    rsi = rsis[token]
    print(f"[churning] {NAME_OF[token]} RSI {rsi / 100 if rsi is not None else '?'} -> {zone}")
    activate(token, ZONE_SIDE[zone])

def _resolve(x):
    """Accept a master symbol (case-insensitive), an underlying token, or a PF number."""
    if isinstance(x, str):
        for tok, n in NAME_OF.items():
            if n.lower() == x.lower():
                return tok
        raise KeyError(f"unknown symbol {x}")
    if x in NAME_OF:
        return x
    for tok, pf in PF_OF.items():
        if pf == x:
            return tok
    raise KeyError(f"unknown instrument {x}")

def reset(x=None):
    """Forget what the script thinks it has done. x = symbol / token / PF, or None for all."""
    toks = TOKENS if x is None else [_resolve(x)]
    for t in toks:
        live[t] = None
        opt_of.pop(t, None)
        if t in PF_OF:
            caps[PF_OF[t]] = 0
        last_fail.pop(t, None)
        off_seen.pop(t, None)
    print(f"[churning] state reset for {len(toks)} instrument(s)")

def sell(x): on_signal(_resolve(x), "OVERBOUGHT", force=True)   # overbought -> BUY nearest PUT
def buy(x):  on_signal(_resolve(x), "OVERSOLD", force=True)     # oversold   -> BUY nearest CALL

def pause_all():
    if not SEND_ON:
        print("[churning][OFF] pause_all NOT sent")
        return
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause {pf}")
        else: submit(_pause_async(pf))

def unpause_all():
    """Console 'Resume all': unpause every portfolio."""
    if not SEND_ON:
        print("[churning][OFF] unpause_all NOT sent")
        return
    for pf in PF_OF.values():                # current portfolios only: a retired PF stays paused
        if DRY_RUN: print(f"[churning][DRY] unpause {pf}")
        else: submit(_unpause_async(pf))

def exit_all():
    """Manual: pause + square up every portfolio."""
    if not SEND_ON:
        print("[churning][OFF] exit_all NOT sent")
        return
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause + squareup {pf}")
        else: submit(_exit_async(pf))
    for t in TOKENS:
        live[t] = None
        opt_of.pop(t, None)

# ---- per-instrument / per-PF calls used by the operator console (the console does the rule checks) ----
def set_allow(x, on):
    """Console checkbox. on=True: automation may send params for this instrument. on=False: it never does.
    x = symbol / token / PF. A signal that arrives while blocked is dropped; ticking it again acts on the next update."""
    t = _resolve(x)
    auto_allow[t] = bool(on)
    print(f"[churning] {NAME_OF[t]} PF {PF_OF.get(t, 'NEW')} automation {'ALLOWED' if on else 'BLOCKED'}")

def pause_pf(pf):
    """Console button: pause one portfolio."""
    if not SEND_ON:
        print(f"[churning][OFF] pause PF {pf} NOT sent")
        return
    if DRY_RUN:
        print(f"[churning][DRY] pause {pf}")
        paused_pf.add(int(pf))
    else:
        submit(_pause_async(pf))

def unpause_pf(pf):
    """Console button: unpause one portfolio."""
    if not SEND_ON:
        print(f"[churning][OFF] unpause PF {pf} NOT sent")
        return
    if DRY_RUN:
        print(f"[churning][DRY] unpause {pf}")
        paused_pf.discard(int(pf))
    else:
        submit(_unpause_async(pf))

def squareup_pf(pf):
    """Console button: square up one portfolio (the console only allows it while the PF is paused)."""
    if not SEND_ON:
        print(f"[churning][OFF] squareup PF {pf} NOT sent")
        return
    if DRY_RUN: print(f"[churning][DRY] squareup {pf}")
    else: submit(_squareup_async(pf))
    for t in TOKENS:
        if PF_OF.get(t) == pf:
            live[t] = None                   # same as exit_all: a later signal may start this portfolio again
            opt_of.pop(t, None)

def send_off():
    """Keep computing signals, but send NOTHING to NATS. A swap already in flight is allowed to finish."""
    global SEND_ON
    SEND_ON = False
    print("[churning] sending OFF: computing only, no messages to NATS")

def send_on():
    """Send to NATS again. Tokens currently sitting in an RSI zone are acted on right away."""
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
    print(f"sending {'ON' if SEND_ON else 'OFF (compute only)'}")
    for t in TOKENS:
        r = rsis[t]
        pf = PF_OF.get(t)
        o = opt_of.get(t)
        print(f"{NAME_OF[t]:24} PF {pf if pf else '-'} zone {zones[t]} rsi {r / 100 if r is not None else None} "
              f"leg {live[t]} option {master['symbol_of'].get(o, '-') if o else '-'} "
              f"subscribed {master['symbol_of'].get(subscribed.get(pf), '-') if pf else '-'} "
              f"cap {caps.get(pf, 0)} fut_close {churn_ltp.get(t)} "
              f"allowed {auto_allow.get(t, True)} paused {pf in paused_pf}")

def stop():
    h_sig.stop()
    h_px.stop()
    if hft is not None:
        hft.close()


LOG_RSI = True              # print symbol + RSI + pct + close on every candle update (set False once happy)

def on_price(update, is_replay):
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    for tok, close in zip(ch["token"], ch["Close"]):
        if close != NULL_LONG and close > 0:
            churn_ltp[int(tok)] = float(close)      # futures AND options (options are needed for sizing)

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
                off_seen.pop(tok, None)       # left its zone: a later re-entry is logged again while OFF
            if LOG_RSI:
                px = churn_ltp.get(tok)
                print(f"[rsi] {datetime.now(IST):%H:%M:%S} {NAME_OF[tok]:24} "
                      f"rsi {rsi / 100:.2f} "
                      f"close {px / PRICE_DIV if px else '?'} zone {zone}")

    # ranks shift on every update, so re-check every stock sitting in a zone
    if not in_window():
        return
    for t in TOKENS:
        z = zones[t]
        if z is not None and live[t] != ZONE_SIDE[z]:
            on_signal(t, z)

h_px = listen(candles, on_price, do_replay=False)           # price first: candles ticks before the derived indicators
h_sig = listen(indicators, on_candle, do_replay=False)
print(f"[churning] armed ({'DRY RUN' if DRY_RUN else 'LIVE'}, sending {'ON' if SEND_ON else 'OFF'}), {len(TOKENS)} instruments, "
      f"RSI >{RSI_HIGH} buy PUT / <{RSI_LOW} buy CALL")
if RESET_ON_START:
    reset()
if ACT_ON_START:
    for t in TOKENS:
        if zones[t] is not None:
            print(f"[churning] {NAME_OF[t]} already in {zones[t]} at start, activating")
            on_signal(t, zones[t])
            # ---------------- unsubscribe all ----------------
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

def unsubscribe_all():
    """Stop signals, then pause + square up + unsubscribe every current portfolio."""
    if not SEND_ON or hft is None:
        print("[churning] sending is OFF or DRY_RUN: run send_on() first (DRY_RUN must be False)")
        return
    h_sig.stop()                              # no new signals while we shut down
    h_px.stop()
    for pf in PF_OF.values():                 # current PFs only; retired ones were handled when retired
        submit(_unsub_async(pf))
    for t in TOKENS:
        live[t] = None
        opt_of.pop(t, None)
    print(f"[churning] unsubscribing {len(PF_OF)} portfolio(s), watch the log")