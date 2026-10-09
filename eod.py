# eod_save.py -- save straddle_series and candles to /data/fo_market_data at end of day
import os
import threading
from datetime import datetime, timezone, timedelta, time as dtime
from deephaven import parquet
from deephaven import csv as dhcsv

CACHE_DIR = "/data/fo_market_data"
FORMAT = "parquet"              # "parquet" or "csv"
AUTO_AT = dtime(15, 55)         # IST, auto-save time. None = manual only

# table variable name in the Deephaven session -> file prefix
# file = <prefix><yyyy-mm-dd>.pq
TABLES = {
    "straddle_series": "straddle_",
    "candles": "candles_",
}

IST = timezone(timedelta(hours=5, minutes=30))

def _save_one(name, prefix, day):
    tbl = globals().get(name)
    if tbl is None:
        print(f"[eod] {name} not found, skipped")
        return
    t = tbl.snapshot()                                   # frozen copy, safe while it is still ticking
    if t.size == 0:
        print(f"[eod] {name} is empty, nothing saved")
        return
    t = t.update(f"Date = `{day}`")                      # Time/Minute has no date, so add one
    if FORMAT == "csv":
        path = f"{CACHE_DIR}/{prefix}{day}.csv"
        dhcsv.write(t, path)
    else:
        path = f"{CACHE_DIR}/{prefix}{day}.pq"
        parquet.write(t, path)
    print(f"[eod] saved {name}: {t.size} rows -> {path}")

def save_eod():
    day = datetime.now(IST).strftime("%Y-%m-%d")
    os.makedirs(CACHE_DIR, exist_ok=True)
    for name, prefix in TABLES.items():
        try:
            _save_one(name, prefix, day)
        except Exception as e:
            print(f"[eod] {name} save failed: {e}")

# ---- optional: save automatically at AUTO_AT ----
if "eod_stop" in globals():
    eod_stop.set()
eod_stop = threading.Event()

def _eod_loop(stop):
    while not stop.is_set():
        now = datetime.now(IST)
        nxt = now.replace(hour=AUTO_AT.hour, minute=AUTO_AT.minute, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        if stop.wait((nxt - now).total_seconds()):
            return
        save_eod()

if AUTO_AT is not None:
    threading.Thread(target=_eod_loop, args=(eod_stop,), daemon=True).start()
    print(f"[eod] will auto-save at {AUTO_AT} IST. Manual: save_eod()")

# host command (not part of the script), copies both files out of the container:
# docker cp <container_name>:/data/fo_market_data/straddle_2026-10-09.pq /home/pavan/Documents/dev-setup/deephaven/fo_market_data/
# docker cp <container_name>:/data/fo_market_data/candles_2026-10-09.pq /home/pavan/Documents/dev-setup/deephaven/fo_market_data/