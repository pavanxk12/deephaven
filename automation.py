# automation.py -- each portfolio trades its OWN token and is driven by that token's RSI (from `indicators`).
#   RSI > RSI_HIGH (overbought) -> run SELL side
#   RSI < RSI_LOW  (oversold)   -> run BUY side
# Instruments (PF, token) are read from the `cmd_queue` table: rows with Cmd == "subscribe",
# PF column = portfolio number, Payload["T1"] = token. Distinct (PF, token) pairs are used.
# Symbol names come from the `Symbol` column of the `candles` table.
# Tick size per token comes from master["futs"]["tick"] (build_master.py, BidIntrvl paise -> rupees).
# ival / jval / sval / ranges are always whole multiples of the token's tick.
# Extra filter: stocks are ranked by Pct change from the day's open; Q1 (lowest 25%) and Q4 (highest 25%)
# are ignored, only the middle 50% (Q2+Q3) may activate / flip.
# On a flip: pause -> apply (maxqty = cap + ADD_QTY, new side, fresh range/ival/jval/sval) -> unpause.
# Commands run ASYNC on the NATS event loop: different portfolios overlap, each portfolio stays in order.
# Needs `master`, `cmd_queue` (PF, Cmd, Payload), `candles` (token, Symbol, Close) and
# `indicators` (token, RSI, Pct) in the same Deephaven session.
# RSI is int64 = RSI x 100. Pct is int64 = percent-from-open x 100.
import asyncio, json, math, time, uuid
import threading
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

# Where the instruments come from
CMD_TABLE = "cmd_queue"      # table in the Deephaven session holding the subscribe rows
SUB_CMD = "subscribe"        # Cmd value to pick
STRATEGY_FILTER = "Churning" # only payloads with this strategyname; None = accept every subscribe row
SYMBOL_COL = "Symbol"        # column of `candles` holding the symbol name
RESET_ON_START = True       # forget live sides and caps on every paste (fresh start)
RSI_HIGH = 65               # RSI above this -> SELL
RSI_LOW = 35                # RSI below this -> BUY

MID_FILTER = True           # only trade stocks in the middle 50% (Q2+Q3) of the Pct-from-open ranking

# ASSUMPTION: how the apply payload tells the bridge which side to run. Change to match your bridge.
SIDE_FIELD = "side"
SIDE_VALUES = {"BUY": "BUY", "SELL": "SELL"}

# True = if RSI is already in a zone when the script starts, activate immediately.
# WARNING: every re-paste with this True adds another ADD_QTY to maxqty.
ACT_ON_START = True

ADD_QTY = 5                 # added to the cumulative maxqty on every activation / flip

# range = last Close -/+ these (rupees). NOTE: same for every stock; check it suits cheap names.
RANGE_BELOW = 100
RANGE_ABOVE = 200
PRICE_DIV = 100             # candle Close is paise -> 100. Use 1 if already rupees.

TICK = 0.1                  # only the default argument of floor_tick; real ticks come from TICK_OF
IVAL_PCT = 0.002            # ival  = 0.2% of close
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
    "max_unhedge": 2, "PRNT": 1,
}
# ==========================================================================

for _n in (CMD_TABLE, "candles", "indicators", "master"):
    if _n not in globals():
        raise RuntimeError(f"'{_n}' not found - run the required scripts first (build_master.py for master)")
RSI_COL = "RSI_9"           # column of `indicators` used as the signal
PCT_COL = "Pct"             # column of `indicators`: % change from the day's open, x100

IST = timezone(timedelta(hours=5, minutes=30))
ZONE_SIDE = {"OVERBOUGHT": "SELL", "OVERSOLD": "BUY"}


# ---------------- load instruments from cmd_queue + names from candles ----------------
def load_names(tokens):
    """token -> Symbol, taken from the `candles` table. Empty dict if it cannot be read."""
    names = {}
    try:
        flt = " || ".join(f"token = {t}" for t in tokens)
        df = to_pandas(candles.where(flt).last_by("token").view(["token", SYMBOL_COL]))
        for t, s in zip(df["token"], df[SYMBOL_COL]):
            if s is not None and not pd.isna(s) and str(s).strip():
                names[int(t)] = str(s).strip()
    except Exception as e:
        print(f"[churning] could not load {SYMBOL_COL} from candles: {e} (using t<token> names)")
    return names

def load_instruments():
    """Net subscription state: replay subscribe/unsubscribe rows in table order.
    subscribe -> add (PF, token); unsubscribe -> remove it. Only pairs still subscribed at the end are kept."""
    tbl = globals()[CMD_TABLE]
    df = to_pandas(tbl.where(f"Cmd in `{SUB_CMD}`, `unsubscribe`"))
    live_pairs = {}                              # (pf, token) -> event index of its latest subscribe
    for i, (pf, cmd, payload) in enumerate(zip(df["PF"], df["Cmd"], df["Payload"])):
        try:
            p = json.loads(payload)
        except Exception:
            continue
        tok = p.get("T1")
        if tok is None or pd.isna(pf):
            continue
        key = (int(pf), int(tok))
        if cmd == "unsubscribe":
            live_pairs.pop(key, None)            # removed
        else:
            if STRATEGY_FILTER and p.get("strategyname") != STRATEGY_FILTER:
                continue
            live_pairs[key] = i                  # added (or re-added)

    # a token can only be on one PF, and a PF on one token: the most recent subscribe wins
    keep, seen_tok, seen_pf = [], {}, {}
    for (pf, tok), i in sorted(live_pairs.items(), key=lambda kv: -kv[1]):   # newest first
        if tok in seen_tok:
            print(f"[churning] token {tok} also live on PF {pf}, using newer PF {seen_tok[tok]}")
            continue
        if pf in seen_pf:
            print(f"[churning] PF {pf} also live on token {tok}, using newer token {seen_pf[pf]}")
            continue
        seen_tok[tok], seen_pf[pf] = pf, tok
        keep.append((pf, tok))
    keep.sort()

    names = load_names([t for _, t in keep]) if keep else {}
    out = {}
    for pf, tok in keep:
        nm = names.get(tok, f"t{tok}")
        if nm in out:
            nm = f"{nm}_{tok}"
        out[nm] = (pf, tok)
    return out

INSTR = load_instruments()
if not INSTR:
    raise RuntimeError(f"no '{SUB_CMD}' rows found in {CMD_TABLE}"
                       + (f" with strategyname={STRATEGY_FILTER}" if STRATEGY_FILTER else ""))

PF_OF = {tok: pf for pf, tok in INSTR.values()}
NAME_OF = {tok: n for n, (pf, tok) in INSTR.items()}
TOKENS = list(PF_OF)
ALL_PF = list(PF_OF.values())
print(f"[churning] loaded {len(INSTR)} instruments from {CMD_TABLE}: "
      + ", ".join(f"{n}=PF{pf}/{t}" for n, (pf, t) in INSTR.items()))

# ---- token-wise tick (rupees), from master["futs"]["tick"] built by build_master.py ----
_f = master["futs"]
if "tick" not in _f.columns:
    raise RuntimeError("master has no tick column - re-run the updated build_master.py")
TICK_OF = {int(t): float(k) for t, k in zip(_f["fut_token"], _f["tick"]) if pd.notna(k) and k > 0}
_missing = [t for t in TOKENS if t not in TICK_OF]
if _missing:
    raise RuntimeError(f"no tick in master for tokens {_missing}")
print("[churning] ticks:", {NAME_OF[t]: TICK_OF[t] for t in TOKENS})

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
        return r

    def close(self):
        if not self.loop.is_running():
            return
        self._run(self.nc.close())
        self.loop.call_soon_threadsafe(self.loop.stop)


hft = None if DRY_RUN else HftClient(NATS_URL)

_pf_locks = {}                       # pf -> asyncio.Lock; keeps one PF's commands in order

def _lock(pf):
    return _pf_locks.setdefault(pf, asyncio.Lock())

def _payload(d):
    """Add the identity fields every command needs."""
    d = dict(d)
    d.update({"USERIDENTIFIER": USER_ID, "user_id": USER_ID, "SENDER": SENDER})
    return d

def submit(coro):
    """Schedule a coroutine on the NATS loop without waiting for it."""
    def _done(f):
        if not f.cancelled() and f.exception():
            print(f"[churning] job FAILED: {f.exception()!r}")
    asyncio.run_coroutine_threadsafe(coro, hft.loop).add_done_callback(_done)

last_fail = {}                       # token -> time.time() of the last failed swap

async def _swap_async(pf, token, params, prev_side):
    """pause -> apply (maxqty = cap + ADD_QTY) -> unpause. Stops and stays paused on any failure."""
    async with _lock(pf):
        try:
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
            live[token] = prev_side          # allow a retry on a later update
            last_fail[token] = time.time()
            print(f"[churning] {NAME_OF[token]} PF {pf} swap FAILED: {e!r}")

async def _pause_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "pause_unpause", _payload({"Action": "Pause"}))

async def _exit_async(pf):
    async with _lock(pf):
        await hft.arequest(pf, "pause_unpause", _payload({"Action": "Pause"}))
        await hft.arequest(pf, "squareup", _payload({}))


# ---- state ----
churn_ltp = {}                                    # token -> last Close (raw)
zones = {t: None for t in TOKENS}                 # token -> current RSI zone
rsis = {t: None for t in TOKENS}                  # token -> last RSI x100
pcts = {t: None for t in TOKENS}                  # token -> last Pct-from-open x100
if "live" not in globals():                       # kept across re-pastes so a re-paste does not re-send the same side
    live = {}
for _t in TOKENS:
    live.setdefault(_t, None)                     # token -> side last started (BUY/SELL)

def rsi_zone(rsi_x100):
    if rsi_x100 > RSI_HIGH * 100: return "OVERBOUGHT"
    if rsi_x100 < RSI_LOW * 100:  return "OVERSOLD"
    return None

def floor_tick(x, tick=TICK):
    n = math.floor(x / tick + 1e-9)
    return round(n * tick, 2) + 1e-6      # 2.3 -> 2.300001, so x100 gives 230.0001 and truncates to 230

def _on_tick(v, tick):
    """True if the bridge, which converts rupees to paise by truncation, ends up on a whole tick."""
    return int(v * 100) % int(round(tick * 100)) == 0

def middle_set():
    """Q2+Q3 by Pct from open: drop the lowest and highest quarter of stocks."""
    ranked = sorted((p, t) for t, p in pcts.items() if p is not None)
    n = len(ranked)
    if n < 4:
        return set()
    k = n // 4                                    # size of each outer quartile
    return {t for _, t in ranked[k:n - k]}

def eligible(token):
    return (not MID_FILTER) or token in middle_set()

def in_window():
    return TRADE_START <= datetime.now(IST).time() <= TRADE_END

# seed prices, RSI zones and Pct from history (no commands)
try:
    _snap = to_pandas(candles.where(" || ".join(f"token = {t}" for t in TOKENS)).last_by("token"))
    for _t, _c in zip(_snap["token"], _snap["Close"]):
        churn_ltp[int(_t)] = float(_c)
except Exception as e:
    print(f"[churning] could not seed prices: {e}")

try:
    _h = to_pandas(indicators.where(" || ".join(f"token = {t}" for t in TOKENS))
                   .last_by("token").view(["token", RSI_COL, PCT_COL]))
    for _t, _v, _pc in zip(_h["token"], _h[RSI_COL], _h[PCT_COL]):
        if not pd.isna(_pc) and int(_pc) != NULL_LONG:
            pcts[int(_t)] = int(_pc)
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
    tick = TICK_OF[token]                              # no fallback: a missing tick stops the script at startup
    rs = px / PRICE_DIV                                # close in rupees
    ival = max(floor_tick(rs * IVAL_PCT, tick), tick)
    jval = max(floor_tick(ival * JVAL_FRAC, tick), tick)
    sval = max(floor_tick(ival * SVAL_MULT, tick), tick)
    lower = max(floor_tick(rs - RANGE_BELOW, tick), 0)
    upper = floor_tick(rs + RANGE_ABOVE, tick)
    for name, v in (("ival", ival), ("jval", jval), ("sval", sval), ("lowerrange", lower), ("upperrange", upper)):
        if not _on_tick(v, tick):
            print(f"[churning] {NAME_OF[token]} {name} {v} is not a multiple of tick {tick}, NOT sent")
            return None
    p = dict(APPLY_PARAMS)
    p.update({"ival": ival, "jval": jval, "sval": sval,
              "lowerrange": lower, "upperrange": upper,
              SIDE_FIELD: SIDE_VALUES[side]})
    return p

def activate(token, side):
    pf = PF_OF[token]
    params = make_params(token, side)
    if params is None:
        print(f"[churning] {NAME_OF[token]} no price or invalid params, not activated")
        return
    prev = live[token]
    live[token] = side                       # mark now so an in-flight swap is not queued twice
    print(f"[churning] {NAME_OF[token]} PF {pf} -> {side} maxqty {caps[pf] + ADD_QTY} "
          f"ival {params['ival']} jval {params['jval']} sval {params['sval']} "
          f"range {params['lowerrange']}-{params['upperrange']}")
    if DRY_RUN:
        print(f"[churning][DRY] pause, apply {params}, unpause on PF {pf}")
        return
    submit(_swap_async(pf, token, params, prev))

def on_signal(token, zone, force=False):
    """RSI of `token` entered a zone. Manual: sell(1147) / buy(48864) / sell("SYMBOL")."""
    if not in_window():
        print(f"[churning] {NAME_OF[token]} -> {zone} outside trade window, ignored")
        return
    if token not in churn_ltp:
        print(f"[churning] no candle close yet for {NAME_OF[token]}, {zone} skipped")
        return
    if not force and not eligible(token):
        print(f"[churning] {NAME_OF[token]} -> {zone}: not in middle Pct band (Q2+Q3), skipped")
        return
    if not force and live[token] == ZONE_SIDE[zone]:
        return                                # already running this side
    if not force and FAIL_COOLDOWN_S and time.time() - last_fail.get(token, 0) < FAIL_COOLDOWN_S:
        return                                # recently failed, wait out the cooldown
    rsi = rsis[token]
    print(f"[churning] {NAME_OF[token]} RSI {rsi / 100 if rsi is not None else '?'} -> {zone}")
    activate(token, ZONE_SIDE[zone])

def _resolve(x):
    """Accept a Symbol (case-insensitive), a token, or a PF number."""
    if isinstance(x, str):
        for n, (pf, tok) in INSTR.items():
            if n.lower() == x.lower():
                return tok
        raise KeyError(f"unknown symbol {x}")
    if x in PF_OF:
        return x
    for pf, tok in INSTR.values():
        if pf == x:
            return tok
    raise KeyError(f"unknown instrument {x}")

def reset(x=None):
    """Forget what the script thinks it has done. x = symbol / token / PF, or None for all."""
    toks = TOKENS if x is None else [_resolve(x)]
    for t in toks:
        live[t] = None
        caps[PF_OF[t]] = 0
        last_fail.pop(t, None)
    print(f"[churning] state reset for {len(toks)} instrument(s)")

def sell(x): on_signal(_resolve(x), "OVERBOUGHT", force=True)
def buy(x):  on_signal(_resolve(x), "OVERSOLD", force=True)

def pause_all():
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause {pf}")
        else: submit(_pause_async(pf))

def exit_all():
    """Manual: pause + square up every portfolio."""
    for pf in ALL_PF:
        if DRY_RUN: print(f"[churning][DRY] pause + squareup {pf}")
        else: submit(_exit_async(pf))
    for t in TOKENS: live[t] = None

def status():
    mid = middle_set()
    for t in TOKENS:
        r = rsis[t]
        p = pcts[t]
        print(f"{NAME_OF[t]:24} PF {PF_OF[t]} tick {TICK_OF[t]} zone {zones[t]} rsi {r / 100 if r is not None else None} "
              f"pct {p / 100 if p is not None else None} mid {t in mid} "
              f"side {live[t]} cap {caps[PF_OF[t]]} close {churn_ltp.get(t)}")

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
        tok = int(tok)
        if tok in PF_OF and close != NULL_LONG and close > 0:
            churn_ltp[tok] = float(close)

def on_candle(update, is_replay):
    cols = ["token", RSI_COL, PCT_COL]
    parts = [d for d in (update.added(cols), update.modified(cols)) if d is not None]
    if not parts:
        return
    for d in parts:
        for tok, rsi, pct in zip(d["token"], d[RSI_COL], d[PCT_COL]):
            tok = int(tok)
            if tok not in PF_OF:
                continue
            if pct != NULL_LONG:
                pcts[tok] = int(pct)
            if rsi == NULL_LONG:
                continue
            rsi = int(rsi)
            zone = rsi_zone(rsi)
            rsis[tok] = rsi
            zones[tok] = zone
            if LOG_RSI:
                px = churn_ltp.get(tok)
                print(f"[rsi] {datetime.now(IST):%H:%M:%S} {NAME_OF[tok]:24} "
                      f"rsi {rsi / 100:.2f} pct {pcts[tok] / 100 if pcts[tok] is not None else '?'} "
                      f"close {px / PRICE_DIV if px else '?'} zone {zone}")

    # ranks shift on every update, so re-check every stock sitting in a zone
    if not in_window():
        return
    for t in TOKENS:
        z = zones[t]
        if z is not None and live[t] != ZONE_SIDE[z] and eligible(t):
            on_signal(t, z)

h_px = listen(candles, on_price, do_replay=False)           # price first: candles ticks before the derived indicators
h_sig = listen(indicators, on_candle, do_replay=False)
print(f"[churning] armed ({'DRY RUN' if DRY_RUN else 'LIVE'}), {len(TOKENS)} instruments, "
      f"RSI >{RSI_HIGH} sell / <{RSI_LOW} buy, "
      f"mid-quartile filter {'ON' if MID_FILTER else 'OFF'}")
if RESET_ON_START:
    reset()
if ACT_ON_START:
    for t in TOKENS:
        if zones[t] is not None:
            print(f"[churning] {NAME_OF[t]} already in {zones[t]} at start, activating")
            on_signal(t, zones[t])