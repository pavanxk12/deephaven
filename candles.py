import time
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.table_listener import listen

# =============================== USER INPUT ===============================
MARKET = "NSE"        # "NSE" or "US"

CONFIGS = {
    "NSE": dict(
        ltt_tz="Asia/Kolkata",           # timezone the feed's ltt string is written in
        ltt_fmt="%d-%m-%Y %H:%M",        # ltt = "dd-mm-yyyy HH:MM:SS"
        feed="fo_market_data",           # live feed table name
        int_prices=True,                 # True = round OHLC to int64 (as before), False = double
    ),
    "US": dict(
        ltt_tz="Asia/Kolkata",           # feed stamps IST (confirmed from the row)
        ltt_fmt="%Y-%m-%dT%H:%M",        # ltt = "yyyy-mm-ddTHH:MM:SS.mmm"
        feed="fo_market_data",
        int_prices=True,                 # keeping int as you said
    ),
}
# ==========================================================================

cfg = CONFIGS[MARKET]
LTT_TZ = ZoneInfo(cfg["ltt_tz"])
FMT_MIN = cfg["ltt_fmt"]
INT_PRICES = cfg["int_prices"]
PRICE_T = dht.int64 if INT_PRICES else dht.double

# stop the previous listener if this script is re-run
if "h_data" in globals():
    h_data.stop()

if "master" not in globals():
    raise RuntimeError("'master' not found - run build_master.py first")

START_MIN = int(time.time() // 60 * 60) + 60   # first full minute we build

# ---- token -> symbol lookup (built once from the in-memory master) ----
_cndl_sym = {}

def _fexp(x):
    return "nan" if pd.isna(x) else x.strftime("%d%b%y").upper()

_ch = master["chain"]
for _tcol, _tp in (("ce_token", "CE"), ("pe_token", "PE")):
    for _t, _s, _e, _k in zip(_ch[_tcol].astype("int64").tolist(),
                              _ch["TckrSymb"].tolist(),
                              _ch["expiry"].tolist(),
                              _ch["strike"].tolist()):
        _cndl_sym[_t] = f"{_s} {_fexp(_e)} {_k:g} {_tp}"

_fu = master["futs"]
for _t, _s, _e in zip(_fu["fut_token"].astype("int64").tolist(),
                      _fu["TckrSymb"].tolist(),
                      _fu["expiry"].tolist()):
    _cndl_sym[_t] = f"{_s} {_fexp(_e)} FUT"
del _ch, _fu

# equities (only present if master was built by the newer build_master.py)
for _r in master.get("eqs", pd.DataFrame(columns=["eq_token", "TckrSymb"])).itertuples(index=False):
    _cndl_sym[int(_r.eq_token)] = _r.TckrSymb

_cndl_mc = {}   # "<date> HH:MM" -> epoch seconds of that minute (one entry per minute)

def _cndl_ltt(s):
    # strptime runs once per minute; every other tick is a slice + dict lookup + int()
    s = s.strip()
    if len(s) < 5 or s[-3] != ":":   # must end with ":SS"
        return None
    key = s[:-3]
    base = _cndl_mc.get(key)
    if base is None:
        try:
            base = int(datetime.strptime(key, FMT_MIN).replace(tzinfo=LTT_TZ).timestamp())
        except Exception:
            return None
        _cndl_mc[key] = base
    try:
        return base + int(s[-2:])
    except Exception:
        return None

# ---- output: one row per token per closed minute ----
cw = DynamicTableWriter({
    "MinuteEpoch": dht.int64,
    "Symbol": dht.string,
    "token": dht.int64,
    "Open": PRICE_T,
    "High": PRICE_T,
    "Low": PRICE_T,
    "Close": PRICE_T,
    "Volume": dht.int64,
})
candles = cw.table.view([
    "Minute = epochSecondsToInstant(MinuteEpoch)",
    "Symbol", "token", "Open", "High", "Low", "Close", "Volume",
])

# ---- state (one entry per token) ----
_cndl_cur = {}         # token -> [minute, o, h, l, c, cum_vol, first_vol]
_cndl_prev = {}    # token -> cumulative vol at end of last closed candle
_cndl_seen = {}   # token -> (ltt string, cumulative vol) of last trade seen

def _cndl_px(v):
    return round(v) if INT_PRICES else float(v)

def _cndl_close(tok, c):
    minute, o, h, l, cl, cum, first_vol = c
    base = _cndl_prev.get(tok, first_vol)
    cw.write_row(minute, _cndl_sym.get(tok), tok,
                 _cndl_px(o), _cndl_px(h), _cndl_px(l), _cndl_px(cl),
                 max(cum - base, 0))
    _cndl_prev[tok] = cum

def _cndl_flush(cutoff_min):
    for tok in [t for t, c in _cndl_cur.items() if c[0] < cutoff_min]:
        _cndl_close(tok, _cndl_cur.pop(tok))

def _cndl_tick(tok, ltp, vol, ltt_sec):
    minute = int(ltt_sec // 60 * 60)
    if minute < START_MIN:
        _cndl_prev[tok] = vol          # track cumulative volume, build no candle
        return
    c = _cndl_cur.get(tok)
    if c is not None and minute > c[0]:
        _cndl_close(tok, _cndl_cur.pop(tok))
        c = None
    if c is None:
        _cndl_cur[tok] = [minute, ltp, ltp, ltp, ltp, vol, vol]
    elif minute == c[0]:
        if ltp > c[2]: c[2] = ltp
        if ltp < c[3]: c[3] = ltp
        c[4] = ltp
        c[5] = vol
    # minute < c[0]: late/out-of-order tick, ignored

COLS = ["token", "ltp", "vol", "ltt"]

def _cndl_update(update, is_replay):
    latest = 0
    for ch in (update.added(COLS), update.modified(COLS)):
        if ch is None:
            continue
        for tok, ltp, vol, lt in zip(ch["token"], ch["ltp"], ch["vol"], ch["ltt"]):
            if lt is None or vol < 0 or not (ltp == ltp and ltp > 0):
                continue
            tok = int(tok)
            key = (lt, int(vol))
            if _cndl_seen.get(tok) == key:        # no new trade
                continue
            try:
                sec = _cndl_ltt(lt)
                if sec is None:                      # bad/empty string
                    continue
                _cndl_seen[tok] = key
                _cndl_tick(tok, float(ltp), int(vol), sec)
            except Exception as e:                   # one bad tick must never kill the listener
                print(f"[candles] tick skipped token={tok}: {e!r}")
                continue
            if sec > latest:
                latest = sec
    if latest:
        _cndl_flush(int((latest - 10) // 60 * 60))     # close finished minutes, 10s grace

h_data = listen(globals()[cfg["feed"]], _cndl_update, do_replay=True)
print(f"[candles] market={MARKET} ltt_tz={cfg['ltt_tz']} fmt={FMT_MIN} feed={cfg['feed']} "
      f"tokens mapped={len(_cndl_sym)}")