import time
from datetime import datetime, timezone, timedelta

from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.table_listener import listen

# stop the previous listener if this script is re-run
if "h_data" in globals():
    h_data.stop()

if "master" not in globals():
    raise RuntimeError("'master' not found - run build_master.py first")

IST = timezone(timedelta(hours=5, minutes=30))
FMT_MIN = "%d-%m-%Y %H:%M"          # ltt is fixed width: "dd-mm-yyyy HH:MM:SS"

START_MIN = int(time.time() // 60 * 60) + 60   # first full minute we build

# ---- token -> symbol lookup (built once from the in-memory master) ----
# symbol = full contract, e.g. "NIFTY 27OCT26 25000 CE" / "NIFTY 27OCT26 FUT"
symbol_of = {}

_ch = master["chain"]
_exp = _ch["expiry"].dt.strftime("%d%b%y").str.upper()
_strk = _ch["strike"].map("{:g}".format)
for _tcol, _tp in (("ce_token", "CE"), ("pe_token", "PE")):
    _names = (_ch["TckrSymb"] + " " + _exp + " " + _strk + " " + _tp).tolist()
    symbol_of.update(zip(_ch[_tcol].astype("int64").tolist(), _names))

_fu = master["futs"]
_names = (_fu["TckrSymb"] + " " + _fu["expiry"].dt.strftime("%d%b%y").str.upper() + " FUT").tolist()
symbol_of.update(zip(_fu["fut_token"].astype("int64").tolist(), _names))
del _ch, _fu, _exp, _strk, _names

minute_cache = {}   # "dd-mm-yyyy HH:MM" -> epoch seconds of that minute (one entry per minute)

def parse_ltt(s):
    # strptime runs once per minute; every other tick is a slice + dict lookup + int()
    s = s.strip()
    if len(s) != 19:                 # not "dd-mm-yyyy HH:MM:SS"
        return None
    key = s[:16]
    base = minute_cache.get(key)
    if base is None:
        try:
            base = int(datetime.strptime(key, FMT_MIN).replace(tzinfo=IST).timestamp())
        except Exception:
            return None
        minute_cache[key] = base
    try:
        return base + int(s[17:19])
    except Exception:
        return None

# ---- output: one row per token per closed minute ----
cw = DynamicTableWriter({
    "MinuteEpoch": dht.int64,
    "Symbol": dht.string,
    "token": dht.int64,
    "Open": dht.int64,
    "High": dht.int64,
    "Low": dht.int64,
    "Close": dht.int64,
    "Volume": dht.int64,
})
candles = cw.table.view([
    "Minute = epochSecondsToInstant(MinuteEpoch)",
    "Symbol", "token", "Open", "High", "Low", "Close", "Volume",
])

# ---- state (one entry per token) ----
cur = {}         # token -> [minute, o, h, l, c, cum_vol, first_vol]
prev_cum = {}    # token -> cumulative vol at end of last closed candle
last_seen = {}   # token -> (ltt string, cumulative vol) of last trade seen

def _close(tok, c):
    minute, o, h, l, cl, cum, first_vol = c
    base = prev_cum.get(tok, first_vol)
    cw.write_row(minute, symbol_of.get(tok), tok,
                 round(o), round(h), round(l), round(cl),
                 max(cum - base, 0))
    prev_cum[tok] = cum

def flush(cutoff_min):
    for tok in [t for t, c in cur.items() if c[0] < cutoff_min]:
        _close(tok, cur.pop(tok))

def on_tick(tok, ltp, vol, ltt_sec):
    minute = int(ltt_sec // 60 * 60)
    if minute < START_MIN:
        prev_cum[tok] = vol          # track cumulative volume, build no candle
        return
    c = cur.get(tok)
    if c is not None and minute > c[0]:
        _close(tok, cur.pop(tok))
        c = None
    if c is None:
        cur[tok] = [minute, ltp, ltp, ltp, ltp, vol, vol]
    elif minute == c[0]:
        if ltp > c[2]: c[2] = ltp
        if ltp < c[3]: c[3] = ltp
        c[4] = ltp
        c[5] = vol
    # minute < c[0]: late/out-of-order tick, ignored

COLS = ["token", "ltp", "vol", "ltt"]

def on_update(update, is_replay):
    latest = 0
    for ch in (update.added(COLS), update.modified(COLS)):
        if ch is None:
            continue
        for tok, ltp, vol, lt in zip(ch["token"], ch["ltp"], ch["vol"], ch["ltt"]):
            if lt is None or vol < 0 or not (ltp == ltp and ltp > 0):
                continue
            tok = int(tok)
            key = (lt, int(vol))
            if last_seen.get(tok) == key:        # no new trade
                continue
            sec = parse_ltt(lt)
            if sec is None:                      # bad/empty string
                continue
            last_seen[tok] = key
            on_tick(tok, float(ltp), int(vol), sec)
            if sec > latest:
                latest = sec
    if latest:
        flush(int((latest - 10) // 60 * 60))     # close finished minutes, 10s grace

h_data = listen(fo_market_data, on_update, do_replay=True)