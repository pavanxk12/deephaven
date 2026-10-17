"""Convert the FUTURES in GDFL NSE F&O 1-minute files (GFDLNFO_CONTRACT_ddmmyyyy.csv or .zip) into `candles_<day>.parquet` files in the format eod_save() writes,
so `absolute(..., "NIFTY FUT", ...)` and `eod_load` can read them.

    python tools/convert_gdfl_futures.py  <root folder>  <output folder>  [--year 2026]  [--symbols NIFTY,BANKNIFTY | --symbols-file f.txt]
                                          [--session 09:15-15:30]  [--zstd-level 9]  [--tar out.tar]  [--force]

Only the futures are taken (tickers like `NIFTY27JAN26FUT.NFO`); the symbol written is the master's own text, `NIFTY 27JAN26 FUT`, so a named contract works and
"NIFTY FUT" can roll to the nearest unexpired future of each day. Everything else is as in convert_gdfl_csv.py: Minute = bar start in UTC, token = a made-up number per
symbol kept in tickers.json (not the contract master's token), Open Interest dropped, sorted by Symbol then Minute, ZSTD, each day checked, resumable, nothing
overwritten without --force. Needs pyarrow; run it from the repository root (it imports the algo package through build_straddle_history).
"""
from __future__ import annotations

import argparse
import os
import sys

import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_straddle_history import day_of, find_files, read_day  # noqa: E402
from convert_gdfl_csv import Format, convert_folder, parse_session  # noqa: E402
from convert_us_ohlcv import make_tar, read_symbols  # noqa: E402

FUTURE = r"^(.+?)(\d{2}[A-Z]{3}\d{2})FUT\.NFO$"


def read_futures(path: str):
    """The futures bars of one day with the Ticker already in the master's text (`NIFTY 27JAN26 FUT`)."""
    raw = read_day(path)
    futures = raw.filter(pc.match_substring_regex(raw["Ticker"], pattern=r"FUT\.NFO$"))
    text = pc.replace_substring_regex(futures["Ticker"], pattern=FUTURE, replacement=r"\1 \2 FUT")
    return futures.set_column(futures.column_names.index("Ticker"), "Ticker", text)


def futures_format(year: int | None) -> Format:
    return Format(day_of=day_of, find_files=lambda root: find_files(root, year)[0], read=read_futures, label="GDFL NSE F&O 1-minute files (futures only)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--symbols")
    ap.add_argument("--symbols-file")
    ap.add_argument("--session", help="India time, for example 09:15-15:30 (default: every bar in the file)")
    ap.add_argument("--zstd-level", type=int, default=9)
    ap.add_argument("--tar")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    manifest = convert_folder(a.src, a.out, read_symbols(a.symbols, a.symbols_file), parse_session(a.session), a.zstd_level, a.force, fmt=futures_format(a.year))
    print(f"\n{manifest['files']} files, {manifest['rows']:,} rows, {manifest['bytes'] / 1e6:,.0f} MB written, {manifest['tickers']:,} symbols, "
          f"{len(manifest['failed'])} failed")
    if a.tar:
        print(f"{make_tar(a.out, a.tar)} files in {a.tar}")
    return 1 if manifest["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
