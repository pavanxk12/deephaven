# rsi_scanner.py -- live scan of all tokens in `candles`: RSI > 75 or < 25 (RSI column is RSI x 100)
from datetime import datetime, timezone, timedelta
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven.constants import NULL_LONG
from deephaven.table_listener import listen

SCAN_HIGH = 75
SCAN_LOW = 25
ALERT_ON_ENTRY_ONLY = True   # True = log a token only when it enters the zone; False = log every candle it is inside

IST = timezone(timedelta(hours=5, minutes=30))

if "candles" not in globals():
    raise RuntimeError("'candles' not found - run the indicator script first")

# stop previous listener on re-paste
if "h_scan" in globals():
    try:
        h_scan.stop()
    except Exception:
        pass

# ---- 1) live table: latest candle per token, only those in an extreme zone ----
rsi_scan = (
    candles.last_by("token")
    .where(["!isNull(RSI)", f"RSI > {SCAN_HIGH * 100} || RSI < {SCAN_LOW * 100}"])
    .update_view([
        "RSI_val = RSI / 100.0",
        f"Zone = RSI > {SCAN_HIGH * 100} ? `OVERBOUGHT` : `OVERSOLD`",
    ])
    .view(["token", "Close", "RSI_val", "Zone"])
    .sort_descending("RSI_val")
)

# ---- 2) history log: one row each time a token is flagged ----
if "scan_writer" not in globals():
    scan_writer = DynamicTableWriter({
        "Time": dht.string,
        "token": dht.int64,
        "Close": dht.double,
        "RSI_val": dht.double,
        "Zone": dht.string,
    })
    rsi_scan_log = scan_writer.table

if "scan_state" not in globals():
    scan_state = {}          # token -> last zone

def _zone(r):
    if r > SCAN_HIGH * 100:
        return "OVERBOUGHT"
    if r < SCAN_LOW * 100:
        return "OVERSOLD"
    return None

def on_scan(update, is_replay):
    ch = update.added(["token", "Close", "RSI"])
    if ch is None:
        return
    for tok, close, rsi in zip(ch["token"], ch["Close"], ch["RSI"]):
        if rsi == NULL_LONG:
            continue
        tok = int(tok)
        z = _zone(int(rsi))
        prev = scan_state.get(tok)
        scan_state[tok] = z
        if z is None:
            continue
        if ALERT_ON_ENTRY_ONLY and z == prev:
            continue
        now = datetime.now(IST).strftime("%H:%M:%S")
        scan_writer.write_row(now, tok, float(close), int(rsi) / 100.0, z)
        print(f"[scan] {now} token {tok} RSI {int(rsi) / 100:.2f} {z}")

h_scan = listen(candles, on_scan, do_replay=False)
print(f"[scan] armed: RSI >{SCAN_HIGH} / <{SCAN_LOW}")