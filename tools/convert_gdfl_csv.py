"""Convert GDFL cash-market 1-minute CSV files (one per day: Ticker, Date, Time, Open, High, Low, Close, Volume, Open Interest) into the files
`eod_save()` writes for the `candles` table, so `eod_load("2026-03-12")` reads them: `candles_<day>.parquet` (+ `candles_<day>.meta.json`).

    python tools/convert_gdfl_csv.py  <folder with the CSV files, any depth>  <output folder>  [--symbols RELIANCE,TCS | --symbols-file list.txt]
                                      [--session 09:15-15:30]  [--zstd-level 9]  [--tar out.tar]  [--force]

What the CSV means: Date is dd/mm/yyyy and Time is hh:mm:ss, both India time; every Time ends in :59 and the first bar of the day is 09:15:59, so a bar's
minute is its Time with the seconds dropped (09:15:59 is the 09:15 bar). Ticker is `SYMBOL.NSE`, or `SYMBOL.SERIES.NSE` for other series (BE, SM, bonds ...).

Columns written (the names and types of the candles table):  Minute (timestamp, UTC, the START of the bar), Symbol (the ticker without `.NSE`,
so `RELIANCE`, or `ABC.BE` for another series), token (int64), Open, High, Low, Close, Volume (int64), Date (text, the trading day).  Open Interest is dropped
(it is 0 for cash).  The CSV has no instrument id, so `token` is a stable made-up number: one per ticker, starting at 10,000,000 (above every real token), kept in
`tickers.json` in the output folder and reused by every later run. It is NOT a token of the contract master: join to the master by Symbol.
Rows are sorted by Symbol then Minute and written with ZSTD. Each day is checked (rows, volume, one date, the date in the file name) before it counts; nothing is
overwritten without --force; a bad file is reported and the rest carry on.  Needs pyarrow (not part of the wheel).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import namedtuple
from datetime import datetime, time as dtime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from convert_us_ohlcv import COLUMNS, make_tar, read_symbols, rename, sha256  # noqa: E402

IST_MINUTES = 330                                            # India time is UTC + 5:30 all year (no daylight saving)
FIRST_TOKEN = 10_000_000
NAME = re.compile(r"^GFDLCM_STOCK_(\d{2})(\d{2})(\d{4})\.csv$", re.IGNORECASE)
CSV_TYPES = {"Ticker": pa.string(), "Date": pa.string(), "Time": pa.string(), "Open": pa.float64(), "High": pa.float64(), "Low": pa.float64(),
             "Close": pa.float64(), "Volume": pa.int64(), "Open Interest": pa.int64()}


def parse_session(text: str | None) -> tuple[dtime, dtime] | None:
    """"09:15-15:30" -> (09:15, 15:30), India time; bars from the first minute up to and including the last."""
    if not text:
        return None
    try:
        a, b = text.split("-")
        lo, hi = datetime.strptime(a.strip(), "%H:%M").time(), datetime.strptime(b.strip(), "%H:%M").time()
    except ValueError:
        raise ValueError(f"session must look like 09:15-15:30, got {text!r}") from None
    if lo > hi:
        raise ValueError(f"session start {a} is after its end {b}")
    return lo, hi


def day_of(path: str) -> str:
    """GFDLCM_STOCK_12032026.csv -> 2026-03-12"""
    m = NAME.match(os.path.basename(path))
    if not m:
        raise ValueError(f"{os.path.basename(path)}: the file name must look like GFDLCM_STOCK_ddmmyyyy.csv")
    d, mo, y = m.groups()
    try:
        return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"{os.path.basename(path)}: {d}/{mo}/{y} is not a date") from None


def find_files(root: str) -> list[str]:
    """Every GFDLCM_STOCK_ddmmyyyy file under `root` (upper or lower case .csv), oldest day first."""
    found = [os.path.join(dp, f) for dp, _, fs in os.walk(root) for f in fs if NAME.match(f)]
    return sorted(found, key=day_of)


def load_tokens(out_dir: str) -> dict:
    path = os.path.join(out_dir, "tickers.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return {str(k): int(v) for k, v in json.load(f).items()}


def save_tokens(out_dir: str, tokens: dict) -> None:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "tickers.json")
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=0, sort_keys=True)
    rename(path + ".tmp", path)


def to_candles(raw: pa.Table, day: str, tokens: dict, symbols: set[str] | None = None, session: tuple[dtime, dtime] | None = None) -> pa.Table:
    """The CSV table -> the candles table. `tokens` (ticker -> number) is extended in place with any new ticker."""
    missing = [c for c in CSV_TYPES if c not in raw.column_names]
    if missing:
        raise ValueError(f"missing columns {missing}; found {raw.column_names}")
    dates = pc.unique(raw["Date"]).to_pylist()
    if len(dates) != 1 or datetime.strptime(dates[0], "%d/%m/%Y").strftime("%Y-%m-%d") != day:
        raise ValueError(f"the file name says {day} but the Date column holds {dates[:3]}")
    if raw.num_rows and (pc.min(raw["Open"]).as_py() <= 0 or pc.min(raw["Low"]).as_py() <= 0 or pc.min(raw["Close"]).as_py() <= 0):
        raise ValueError(f"{day}: a price is zero or negative")
    local = pc.strptime(pc.binary_join_element_wise(raw["Date"], raw["Time"], " "), format="%d/%m/%Y %H:%M:%S", unit="ns")     # India time, no zone
    minute = pc.floor_temporal(local, unit="minute")
    table = raw.append_column("_minute", minute)
    symbol = pc.replace_substring_regex(table["Ticker"], pattern=r"\.NSE$", replacement="")
    table = table.append_column("_symbol", symbol)
    if symbols is not None:
        table = table.filter(pc.is_in(table["_symbol"], value_set=pa.array(sorted(symbols), pa.string())))
    if session is not None:
        mod = pc.add(pc.multiply(pc.hour(table["_minute"]), 60), pc.minute(table["_minute"]))
        lo, hi = session[0].hour * 60 + session[0].minute, session[1].hour * 60 + session[1].minute
        table = table.filter(pc.and_(pc.greater_equal(mod, lo), pc.less_equal(mod, hi)))
    uniq = pc.unique(table["_symbol"])
    for name in sorted(n for n in uniq.to_pylist() if n not in tokens):          # a new ticker gets the next number
        tokens[name] = FIRST_TOKEN + len(tokens)
    ids = pa.array([tokens[n] for n in uniq.to_pylist()], pa.int64())
    token = pc.take(ids, pc.index_in(table["_symbol"], value_set=uniq))
    utc = pc.subtract(table["_minute"], pa.scalar(IST_MINUTES * 60 * 10**9, pa.duration("ns")))                                # the bar start in UTC
    out = pa.table({
        "Minute": pc.cast(utc, pa.timestamp("ns", "UTC")), "Symbol": table["_symbol"], "token": token,
        "Open": table["Open"], "High": table["High"], "Low": table["Low"], "Close": table["Close"], "Volume": table["Volume"],
        "Date": pa.array([day] * table.num_rows, pa.string()),
    })
    return out.sort_by([("Symbol", "ascending"), ("Minute", "ascending")])


def read_csv(path: str) -> pa.Table:
    return pacsv.read_csv(path, convert_options=pacsv.ConvertOptions(column_types=CSV_TYPES))


# What differs between the GDFL files: how a file name gives its day, how the files are found, how one is read (and its tickers made into symbols), and a label.
Format = namedtuple("Format", "day_of find_files read label")


CASH = Format(day_of, find_files, read_csv, "GDFL cash 1-minute CSV")


def convert_file(src: str, out_dir: str, tokens: dict, symbols=None, session=None, level: int = 9, force: bool = False, fmt: Format = CASH) -> dict:
    """Convert one day. Returns {day, status, rows ...}; status is converted, exists or empty (an error raises)."""
    day = fmt.day_of(src)
    final = os.path.join(out_dir, f"candles_{day}.parquet")
    if os.path.exists(final) and not force:
        return {"day": day, "status": "exists", "file": os.path.basename(final), "rows": pq.ParquetFile(final).metadata.num_rows,
                "bytes": os.path.getsize(final)}
    raw = fmt.read(src)
    result = to_candles(raw, day, tokens, symbols, session)
    if result.num_rows == 0:
        return {"day": day, "status": "empty", "rows": 0}
    os.makedirs(out_dir, exist_ok=True)
    tmp = final[:-len(".parquet")] + ".tmp.parquet"
    try:
        pq.write_table(result, tmp, compression="zstd", compression_level=level, row_group_size=1_000_000)
        back = pq.read_table(tmp, memory_map=False)                          # (a memory map would keep the file open, and Windows cannot rename it then)
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
    meta = {"table": "candles", "day": day, "market": "NSE", "timezone": "Asia/Kolkata", "rows": result.num_rows, "file": os.path.basename(final),
            "columns": COLUMNS, "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "built_by": f"converted from {fmt.label}; Minute = bar start (UTC); token = a made-up number per ticker (tickers.json), not the contract master's",
            "source": os.path.basename(src), "source_rows": raw.num_rows, "symbols": int(pc.count_distinct(result["Symbol"]).as_py()),
            "first_minute": str(mm["min"].as_py()), "last_minute": str(mm["max"].as_py())}
    with open(final[:-len(".parquet")] + ".meta.json.tmp", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1)
    rename(final[:-len(".parquet")] + ".meta.json.tmp", final[:-len(".parquet")] + ".meta.json")
    return {"day": day, "status": "converted", "file": os.path.basename(final), "rows": result.num_rows, "source_rows": raw.num_rows,
            "bytes": os.path.getsize(final)}


def convert_folder(src_dir: str, out_dir: str, symbols=None, session=None, level: int = 9, force: bool = False, log=print, fmt: Format = CASH) -> dict:
    files = fmt.find_files(src_dir)
    if not files:
        raise FileNotFoundError(f"no files of this kind under {src_dir}")
    tokens = load_tokens(out_dir)
    results, seen = [], {}
    for src in files:
        day = fmt.day_of(src)
        if day in seen:                                                      # the same day twice (a re-download): the first is used, the rest said so
            results.append({"day": day, "status": "failed", "error": f"a second file for this day: {src} (using {seen[day]})"})
            log(f"{day}  failed     {results[-1]['error']}")
            continue
        seen[day] = src
        before = dict(tokens)
        try:
            r = convert_file(src, out_dir, tokens, symbols, session, level, force, fmt)
            if tokens != before:
                save_tokens(out_dir, tokens)                                 # saved after every day that added tickers, so a stop loses nothing
        except Exception as e:                                               # one bad file must not stop the rest
            tokens.clear()
            tokens.update(before)
            r = {"day": day, "status": "failed", "error": f"{type(e).__name__}: {e}"}
        results.append(r)
        log(f"{r['day']}  {r['status']:<9} {r.get('rows', ''):>10}  {r.get('error', '')}")
    done = [r for r in results if r["status"] in ("converted", "exists")]
    manifest = {"made_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source_folder": os.path.abspath(src_dir),
                "files": len(done), "rows": sum(r.get("rows", 0) for r in done), "bytes": sum(r.get("bytes", 0) for r in done),
                "failed": [r for r in results if r["status"] == "failed"], "tickers": len(tokens),
                "first_day": done[0]["day"] if done else None, "last_day": done[-1]["day"] if done else None,
                "sha256": {r["file"]: sha256(os.path.join(out_dir, r["file"])) for r in done}}
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
    return manifest


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--symbols")
    ap.add_argument("--symbols-file")
    ap.add_argument("--session", help="India time, for example 09:15-15:30 (default: every bar in the file)")
    ap.add_argument("--zstd-level", type=int, default=9)
    ap.add_argument("--tar")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    manifest = convert_folder(a.src, a.out, read_symbols(a.symbols, a.symbols_file), parse_session(a.session), a.zstd_level, a.force)
    print(f"\n{manifest['files']} files, {manifest['rows']:,} rows, {manifest['bytes'] / 1e6:,.0f} MB written, {manifest['tickers']:,} tickers, "
          f"{len(manifest['failed'])} failed")
    if a.tar:
        print(f"{make_tar(a.out, a.tar)} files in {a.tar}")
    return 1 if manifest["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
