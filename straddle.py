# straddle_series.py -- V / D straddle time series (same logic as str.cpp), 1 point per minute
# supports all symbols (SYMBOLS = None) + stale price guard
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta, time as dtime
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.constants import NULL_LONG
from deephaven.table_listener import listen
from deephaven.pandas import to_pandas

# =============================== USER INPUT ===============================
SYMBOLS = None                    # None = every symbol with future + options; or e.g. ["NIFTY", "BANKNIFTY"]
EXPIRY_INDEX = 0                  # 0 = nearest option expiry, 1 = next, ...
EXPIRY_OVERRIDE = {}              # e.g. {"NIFTY": "2026-10-13"} to force an expiry per symbol
WINDOW = 5                        # +/- strikes around ATM (C++ WINDOW)
SHIFT_THRESHOLD = 3               # re-centre window when ATM drifts more than this
MIN_SEARCH = 2                    # search +/-2 strikes around the previous minimum
PRICE_DIV = 100                   # candle Close is paise -> 100. Use 1 if already rupees.
MAX_AGE_S = 180                   # ignore a price older than this (seconds). None = no guard
OFFSET_S = 2                      # seconds after each minute boundary (lets the candle close)
START = dtime(9, 15)
END = dtime(15, 30)
RESET_STATE = False               # True = wipe today's series state on re-paste
# ==========================================================================

for _n in ("candles", "master"):
    if _n not in globals():
        raise RuntimeError(f"'{_n}' not found - run the indicator script / build_master.py first")

IST = timezone(timedelta(hours=5, minutes=30))
today = pd.Timestamp(datetime.now(IST).date())

# ---- stop previous run on re-paste ----
if "ss_stop" in globals():
    ss_stop.set()
if "h_px" in globals():
    try:
        h_px.stop()
    except Exception:
        pass
ss_stop = threading.Event()

# ---- 1) series definitions from cached master (no CSV read) ----
futs = master["futs"]
chain = master["chain"]
futs = futs[futs["expiry"] >= today].sort_values("expiry")
chain = chain[chain["expiry"] >= today]

all_syms = sorted(set(futs["TckrSymb"]) & set(chain["TckrSymb"]))
if SYMBOLS is None:
    use_syms = all_syms
else:
    use_syms = [s.upper() for s in SYMBOLS if s.upper() in all_syms]
    for s in SYMBOLS:
        if s.upper() not in all_syms:
            print(f"[series] {s}: no future or no options in master, skipped")

near_fut = futs.groupby("TckrSymb")["fut_token"].first()      # nearest future per symbol
by_sym = {s: g for s, g in chain.groupby("TckrSymb")}         # group once, not N filters

defs = {}
for s in use_syms:
    c = by_sym[s]
    exps = sorted(pd.Timestamp(e) for e in c["expiry"].unique())
    if s in EXPIRY_OVERRIDE:
        exp = pd.Timestamp(EXPIRY_OVERRIDE[s])
        if exp not in exps:
            print(f"[series] {s}: expiry {exp.date()} not in file, skipped")
            continue
    else:
        exp = exps[min(EXPIRY_INDEX, len(exps) - 1)]
    cc = c[c["expiry"] == exp].sort_values("strike")
    if len(cc) < 2 * WINDOW + 1:
        continue
    defs[(s, exp.strftime("%Y-%m-%d"))] = {
        "fut": int(near_fut[s]),
        "strikes": cc["strike"].to_numpy(dtype=float),
        "ce": [int(x) for x in cc["ce_token"]],
        "pe": [int(x) for x in cc["pe_token"]],
    }

needed = set()
for d in defs.values():
    needed.add(d["fut"]); needed.update(d["ce"]); needed.update(d["pe"])
print(f"[series] {len(defs)} series of {len(all_syms)} symbols with future+options, "
      f"{len(needed)} tokens needed in candles")

# ---- 2) live prices: token -> (price, timestamp) ----
px = {}
try:
    _snap = to_pandas(candles.last_by("token").view(["token", "Close", "Ts = epochSeconds(Minute) + 60"]))
    for _t, _c, _ts in zip(_snap["token"], _snap["Close"], _snap["Ts"]):
        if int(_t) in needed and _c != NULL_LONG and _c > 0:
            px[int(_t)] = (float(_c) / PRICE_DIV, float(_ts))   # candle close time
except Exception as e:
    print(f"[series] could not seed prices: {e}")

def on_px(update, is_replay):
    ch = update.added(["token", "Close"])
    if ch is None:
        return
    now_ts = time.time()
    for tok, close in zip(ch["token"], ch["Close"]):
        tok = int(tok)
        if tok in needed and close != NULL_LONG and close > 0:
            px[tok] = (float(close) / PRICE_DIV, now_ts)

h_px = listen(candles, on_px, do_replay=False)

# ---- 3) output table + per-series state (kept across re-pastes) ----
if "straddle_writer" not in globals():
    straddle_writer = DynamicTableWriter({
        "Time": dht.string, "Symbol": dht.string, "Expiry": dht.string,
        "Point": dht.int64, "Fut": dht.double,
        "ATMStrike": dht.double, "ATMStraddle": dht.double,
        "MinStrike": dht.double, "V": dht.double,
        "Cost": dht.double, "CumCost": dht.double, "D": dht.double,
        "AltCost": dht.double, "AltCumCost": dht.double, "AltD": dht.double,
        "Synthetic": dht.double, "Drift": dht.int64,
    })
    straddle_series = straddle_writer.table
    straddle_latest = straddle_series.last_by(["Symbol", "Expiry"])

if "ss_state" not in globals() or RESET_STATE:
    ss_state = {}

def _fresh_state(day):
    return {"date": day, "center": None, "min": None, "alt": None,
            "cum": 0.0, "altcum": 0.0, "n": 0}

# ---- 4) one sample = one C++ onTimer push ----
def sample_one(key, d, st, now_str, now_ts):
    def get(tok):
        v = px.get(tok)
        if v is None:
            return None
        if MAX_AGE_S is not None and now_ts - v[1] > MAX_AGE_S:
            return None                              # stale -> treat as missing
        return v[0]

    fut = get(d["fut"])
    if not fut:
        return False
    strikes = d["strikes"]
    n = len(strikes)

    def strad(i):
        c = get(d["ce"][i]); p = get(d["pe"][i])
        return (c + p) if (c and p) else 0.0

    atm = int(np.abs(strikes - fut).argmin())
    if st["center"] is None:
        st["center"] = atm
        st["min"] = atm                              # C++: currentMinPoolIdx_ = windowCenterIdx_
    drift = atm - st["center"]
    if abs(drift) > SHIFT_THRESHOLD and atm - WINDOW >= 0 and atm + WINDOW < n:
        st["center"] = atm                           # re-centre window
    lo, hi = st["center"] - WINDOW, st["center"] + WINDOW
    if lo < 0 or hi >= n:
        return False

    # lowestActiveStraddle: full window scan if no previous min, else +/-MIN_SEARCH around it
    prev = st["min"]
    if prev is None:
        cand = range(lo, hi + 1)
    else:
        cand = range(max(lo, prev - MIN_SEARCH), min(hi, prev + MIN_SEARCH) + 1)
    best_v, best_i = 0.0, None
    for i in cand:
        v = strad(i)
        if v <= 0:
            continue
        if best_v == 0 or v < best_v:
            best_v, best_i = v, i
    st["min"] = best_i                               # None -> full scan next time (as in C++)
    if best_i is None:
        return False
    new_v = best_v

    # cost: how much the straddle value jumped because the minimum strike moved
    cost = 0.0
    if prev is not None:
        pv = strad(prev)
        if pv > 0:
            cost = abs(new_v - pv)
    st["cum"] += cost

    # alt series: only counts when the minimum moves more than 1 strike from the alt reference
    alt_cost = 0.0
    if st["alt"] is None:
        st["alt"] = best_i
    elif abs(best_i - st["alt"]) > 1:
        rv = strad(st["alt"])
        if rv > 0:
            alt_cost = abs(new_v - rv)
        st["alt"] = best_i
    st["altcum"] += alt_cost

    ce = get(d["ce"][best_i]); pe = get(d["pe"][best_i])
    synth = ce - pe + float(strikes[best_i])
    st["n"] += 1
    straddle_writer.write_row(
        now_str, key[0], key[1], st["n"] - 1, float(fut),
        float(strikes[atm]), float(strad(atm)),
        float(strikes[best_i]), float(new_v),
        float(cost), float(st["cum"]), float(new_v + st["cum"]),
        float(alt_cost), float(st["altcum"]), float(new_v + st["altcum"]),
        float(synth), int(drift),
    )
    return True

def sample_all():
    now = datetime.now(IST)
    if not (START <= now.time() <= END):
        return
    day = now.date()
    now_str = now.strftime("%H:%M:%S")
    now_ts = time.time()
    ok = 0
    for key, d in defs.items():
        st = ss_state.get(key)
        if st is None or st["date"] != day:          # new trading day -> fresh series
            st = ss_state[key] = _fresh_state(day)
        try:
            if sample_one(key, d, st, now_str, now_ts):
                ok += 1
        except Exception as e:
            print(f"[series] {key} failed: {e}")
    print(f"[series] {now_str} sampled {ok}/{len(defs)} series")

# ---- 5) timer thread aligned to minute boundary + OFFSET_S ----
def _loop(stop):
    while not stop.is_set():
        now = datetime.now(IST)
        nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=OFFSET_S)
        if stop.wait(max(0.5, (nxt - now).total_seconds())):
            return
        sample_all()

threading.Thread(target=_loop, args=(ss_stop,), daemon=True).start()
print("[series] running: tables `straddle_series` (history) and `straddle_latest` (latest row per series)")