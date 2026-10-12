# eod_save.py -- manual end-of-day save of straddle_series and candles
# Usage: run this script once, then call save_eod() whenever you want to save.
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from deephaven import parquet
from deephaven import csv as dhcsv

CACHE_DIR = "/data/deephaven/fo_market_data"
FORMAT = "parquet"              # "parquet" or "csv"
TZ = ZoneInfo("Asia/Kolkata")   # use "America/New_York" for the US session

# table variable name in the Deephaven session -> file prefix
# file = <prefix><yyyy-mm-dd>.<ext>
TABLES = {
    "straddle_series": "straddle_",
    "candles": "candles_",
}

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
        path = f"{CACHE_DIR}/{prefix}{day}.parquet"
        parquet.write(t, path)
    print(f"[eod] saved {name}: {t.size} rows -> {path}")

def save_eod():
    day = datetime.now(TZ).strftime("%Y-%m-%d")
    os.makedirs(CACHE_DIR, exist_ok=True)
    for name, prefix in TABLES.items():
        try:
            _save_one(name, prefix, day)
        except Exception as e:
            print(f"[eod] {name} save failed: {e}")

print("[eod] ready. Call save_eod() to save.")

# host command (not part of the script), copies both files out of the container:
# docker cp algofusion-deephaven:/data/deephaven/fo_market_data/straddle_2026-10-12.parquet /home/pavan/Documents/dev-setup/deephaven/fo_market_data/
# docker cp algofusion-deephaven:/data/deephaven/fo_market_data/candles_2026-10-12.parquet /home/pavan/Documents/dev-setup/deephaven/fo_market_data/