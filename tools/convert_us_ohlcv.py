"""Convert daily 1-minute OHLCV Parquet files (ts_event, symbol, open, high, low, close, volume, instrument_id) into the files `eod_save()` writes
for the `candles` table, so `eod_load("2026-05-15")` reads them: `candles_<day>.parquet` (+ `candles_<day>.meta.json`).

    python tools/convert_us_ohlcv.py  <input folder>  <output folder>  [--symbols AAPL,SPY | --symbols-file list.txt]
                                      [--session 09:30-16:00]  [--zstd-level 9]  [--tar out.tar]  [--force]

Columns written (the same names and types as the candles table):  Minute (timestamp, UTC), Symbol, token (int64), Open, High, Low, Close,
Volume (int64), Date (text, the trading day in New York).  `token` is the vendor's instrument_id: it is NOT a token of the contract master, so
join to the master by Symbol.  Rows are sorted by Symbol then Minute and written with ZSTD, which is about 28% smaller than the time-ordered file the
vendor ships. A parquet file is already compressed, so a tar of it saves nothing; `--tar` only gathers the files into one file to copy.
Holiday markers (`<day>.empty`) are listed in the manifest and produce no file. Nothing is overwritten unless --force; each file is written under a
temporary name and renamed when complete, and checked (rows, volume, first and last minute) against its source before it counts.
Needs pyarrow (it is not part of the wheel; this is a stand-alone tool).
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import sys
import tarfile
import time
from datetime import datetime, time as dtime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

NEW_YORK = "America/New_York"
REQUIRED = ("ts_event", "symbol", "open", "high", "low", "close", "volume", "instrument_id")
COLUMNS = ["Minute", "Symbol", "token", "Open", "High", "Low", "Close", "Volume", "Date"]


def parse_session(text: str | None) -> tuple[dtime, dtime] | None:
    """"09:30-16:00" -> (09:30, 16:00), in New York time; bars from the first minute up to and including the last."""
    if not text:
        return None
    try:
        a, b = text.split("-")
        lo, hi = datetime.strptime(a.strip(), "%H:%M").time(), datetime.strptime(b.strip(), "%H:%M").time()
    except ValueError:
        raise ValueError(f"session must look like 09:30-16:00, got {text!r}") from None
    if lo > hi:
        raise ValueError(f"session start {a} is after its end {b}")
    return lo, hi


def read_symbols(arg: str | None, path: str | None) -> set[str] | None:
    names: set[str] = set()
    if arg:
        names |= {s.strip().upper() for s in arg.split(",") if s.strip()}
    if path:
        with open(path, encoding="utf-8") as f:
            names |= {ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")}
    return names or None


def day_of(path: str) -> str:
    name = os.path.basename(path).split(".")[0]
    try:
        datetime.strptime(name, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"{os.path.basename(path)}: the file name must be <yyyy-mm-dd>.parquet") from None
    return name


def to_candles(table: pa.Table, day: str, symbols: set[str] | None = None, session: tuple[dtime, dtime] | None = None) -> pa.Table:
    """The vendor table -> the candles table (filtered, renamed, typed, sorted by Symbol then Minute)."""
    missing = [c for c in REQUIRED if c not in table.column_names]
    if missing:
        raise ValueError(f"missing columns {missing}; found {table.column_names}")
    if symbols is not None:
        table = table.filter(pc.is_in(table["symbol"], value_set=pa.array(sorted(symbols), pa.string())))
    if session is not None:
        # New York minute of the day from the UTC time. One offset serves the whole file: the clocks change at 02:00 and the bars start at 04:00.
        noon = datetime.strptime(day, "%Y-%m-%d").replace(hour=12, tzinfo=ZoneInfo(NEW_YORK))
        offset = int(noon.utcoffset().total_seconds() // 60)
        minutes = pc.divide(pc.cast(table["ts_event"], pa.int64()), 60_000_000_000)          # whole minutes since 1970 (UTC); plain integer
        utc_minute = pc.subtract(minutes, pc.multiply(pc.divide(minutes, 1440), 1440))        # arithmetic: no time-zone database is needed
        shifted = pc.add(utc_minute, offset)
        minute_of_day = pc.if_else(pc.less(shifted, 0), pc.add(shifted, 1440), shifted)
        lo, hi = session[0].hour * 60 + session[0].minute, session[1].hour * 60 + session[1].minute
        table = table.filter(pc.and_(pc.greater_equal(minute_of_day, lo), pc.less_equal(minute_of_day, hi)))
    ts = table["ts_event"]
    if not (pa.types.is_timestamp(ts.type) and ts.type.tz):
        raise ValueError(f"ts_event must be a timestamp with a time zone, it is {ts.type}")
    out = pa.table({
        "Minute": pc.cast(ts, pa.timestamp("ns", "UTC")),
        "Symbol": pc.cast(table["symbol"], pa.string()),
        "token": pc.cast(table["instrument_id"], pa.int64()),
        "Open": pc.cast(table["open"], pa.float64()), "High": pc.cast(table["high"], pa.float64()),
        "Low": pc.cast(table["low"], pa.float64()), "Close": pc.cast(table["close"], pa.float64()),
        "Volume": pc.cast(table["volume"], pa.int64()),
        "Date": pa.array([day] * table.num_rows, pa.string()),
    })
    return out.sort_by([("Symbol", "ascending"), ("Minute", "ascending")])


def rename(src: str, dst: str, tries: int = 40, wait: float = 0.25) -> None:
    """Rename over `dst`, retried for a few seconds: on Windows a virus scanner or indexer may hold a file it has just seen written."""
    for i in range(tries):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i == tries - 1:
                raise
            time.sleep(wait)


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def convert_file(src: str, out_dir: str, symbols: set[str] | None = None, session: tuple[dtime, dtime] | None = None, level: int = 9,
                 force: bool = False, read=None, dayfn=None, label: str = "vendor 1-minute OHLCV") -> dict:
    """Convert one day. Returns {day, status, rows, ...}; status is converted, exists, empty or (an exception) failed.
    `read(path)` returns the vendor table (default: a Parquet file) and `dayfn(path)` the day of a file name, for other kinds of vendor file."""
    day = (dayfn or day_of)(src)
    final = os.path.join(out_dir, f"candles_{day}.parquet")
    if os.path.exists(final) and not force:
        return {"day": day, "status": "exists", "file": os.path.basename(final), "rows": pq.ParquetFile(final).metadata.num_rows,
                "bytes": os.path.getsize(final)}
    source = (read or pq.read_table)(src)
    result = to_candles(source, day, symbols, session)
    if result.num_rows == 0:
        return {"day": day, "status": "empty", "rows": 0}
    os.makedirs(out_dir, exist_ok=True)
    tmp = final[:-len(".parquet")] + ".tmp.parquet"
    try:
        pq.write_table(result, tmp, compression="zstd", compression_level=level, row_group_size=1_000_000)
        back = pq.read_table(tmp, memory_map=False)                          # (a memory map would keep the file open, and Windows cannot rename it then)                                           # check what was written, not what was meant to be
        if back.num_rows != result.num_rows or back.column_names != COLUMNS:
            raise RuntimeError(f"{day}: the written file does not read back the same ({back.num_rows} rows, {back.column_names})")
        if pc.sum(back["Volume"]).as_py() != pc.sum(result["Volume"]).as_py():
            raise RuntimeError(f"{day}: volume changed while writing")
        del back
        rename(tmp, final)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    mm = pc.min_max(result["Minute"])
    lo, hi = mm["min"].as_py(), mm["max"].as_py()
    meta = {"table": "candles", "day": day, "market": "US", "timezone": NEW_YORK, "rows": result.num_rows, "file": os.path.basename(final),
            "columns": COLUMNS, "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "built_by": f"converted from {label}; token = the vendor's instrument_id (not the contract master's token), sorted by Symbol, Minute",
            "source": os.path.basename(src), "source_rows": source.num_rows, "symbols": int(pc.count_distinct(result["Symbol"]).as_py()),
            "first_minute": str(lo), "last_minute": str(hi)}
    with open(final[:-len(".parquet")] + ".meta.json.tmp", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1)
    rename(final[:-len(".parquet")] + ".meta.json.tmp", final[:-len(".parquet")] + ".meta.json")
    return {"day": day, "status": "converted", "file": os.path.basename(final), "rows": result.num_rows, "source_rows": source.num_rows,
            "bytes": os.path.getsize(final)}


def folder_manifest(out_dir: str, src_dir: str, failed: list, holidays: list) -> dict:
    """The manifest of EVERYTHING in the output folder (not only what this run touched), so adding days to a folder that already holds others keeps all of them listed."""
    files = sorted(glob.glob(os.path.join(out_dir, "candles_*.parquet")))
    files = [f for f in files if ".tmp." not in f]
    old = {}
    path = os.path.join(out_dir, "manifest.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                old = json.load(f)
        except (OSError, ValueError):
            old = {}
    names = [os.path.basename(f) for f in files]
    sources: dict = {}                                                       # which feed each day came from (the .meta.json of each file says so)
    for f in files:
        try:
            with open(f[:-len(".parquet")] + ".meta.json", encoding="utf-8") as m:
                src = json.load(m).get("built_by", "unknown").split(";")[0]
        except (OSError, ValueError):
            src = "unknown"
        sources.setdefault(src, []).append(os.path.basename(f)[8:18])
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    manifest = {"made_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source_folder": os.path.abspath(src_dir),
                "files": len(files), "rows": rows, "bytes": sum(os.path.getsize(f) for f in files), "failed": failed,
                "holidays": sorted(set(old.get("holidays", [])) | set(holidays)),
                "first_day": names[0][8:18] if names else None, "last_day": names[-1][8:18] if names else None,
                "sources": {k: {"days": len(v), "first": v[0], "last": v[-1]} for k, v in sources.items()},
                "mixed_sources": len(sources) > 1,                            # True when the folder holds days from different feeds: prices, volumes and bar counts differ between them
                "sha256": {os.path.basename(f): sha256(f) for f in files}}
    return manifest


def convert_folder(src_dir: str, out_dir: str, symbols=None, session=None, level: int = 9, force: bool = False, log=print,
                   pattern: str = "*.parquet", read=None, dayfn=None, label: str = "vendor 1-minute OHLCV") -> dict:
    files = sorted(glob.glob(os.path.join(src_dir, pattern)))
    holidays = sorted(os.path.basename(p)[:-len(".empty")] for p in glob.glob(os.path.join(src_dir, "*.empty")))
    if not files:
        raise FileNotFoundError(f"no {pattern} in {src_dir}")
    results = []
    for src in files:
        try:
            r = convert_file(src, out_dir, symbols, session, level, force, read, dayfn, label)
        except Exception as e:                                               # one bad file must not stop the rest
            try:
                shown = (dayfn or day_of)(src)
            except ValueError:
                shown = os.path.basename(src)
            r = {"day": shown, "status": "failed", "error": f"{type(e).__name__}: {e}"}
        results.append(r)
        log(f"{r['day']}  {r['status']:<9} {r.get('rows', ''):>10}  {r.get('error', '')}")
    os.makedirs(out_dir, exist_ok=True)
    manifest = folder_manifest(out_dir, src_dir, [r for r in results if r["status"] == "failed"], holidays)
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
    manifest["this_run"] = {"converted": [r["day"] for r in results if r["status"] == "converted"], "already_there": [r["day"] for r in results if r["status"] == "exists"],
                            "empty": [r["day"] for r in results if r["status"] == "empty"]}
    return manifest


def make_tar(out_dir: str, tar_path: str) -> int:
    """All the candles files of a folder in ONE plain tar (no compression: parquet is compressed already)."""
    n = 0
    with tarfile.open(tar_path, "w") as tf:
        for p in sorted(glob.glob(os.path.join(out_dir, "candles_*"))) + [os.path.join(out_dir, n) for n in ("manifest.json", "tickers.json")]:
            if os.path.exists(p) and ".tmp." not in p:
                tf.add(p, arcname=os.path.basename(p))
                n += 1
    return n


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--symbols")
    ap.add_argument("--symbols-file")
    ap.add_argument("--session", help="New York time, for example 09:30-16:00 (default: every bar in the file)")
    ap.add_argument("--zstd-level", type=int, default=9)
    ap.add_argument("--tar")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    manifest = convert_folder(a.src, a.out, read_symbols(a.symbols, a.symbols_file), parse_session(a.session), a.zstd_level, a.force)
    print(f"\n{manifest['files']} files, {manifest['rows']:,} rows, {manifest['bytes'] / 1e6:,.0f} MB written, {len(manifest['failed'])} failed, "
          f"{len(manifest['holidays'])} holidays")
    if a.tar:
        print(f"{make_tar(a.out, a.tar)} files in {a.tar}")
    return 1 if manifest["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
