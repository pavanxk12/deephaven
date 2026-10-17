"""Convert Databento batch downloads (`xnas-itch-<yyyymmdd>.ohlcv-1m.dbn.zst`, one file per day, schema ohlcv-1m) into the candles files `eod_save()` writes
(`candles_<day>.parquet` + `.meta.json`), and ADD them to a folder that already holds other days.

    python tools/convert_databento_dbn.py  <folder with the .dbn.zst files>  <output folder>  [--symbols AAPL,SPY | --symbols-file list.txt]
                                           [--session 09:30-16:00]  [--zstd-level 9]  [--tar out.tar]  [--force]

The ticker of each bar is taken from the symbol mappings stored inside each DBN file (the download has no `symbol` column of its own: it was made with map_symbols off and
stype_out instrument_id), and `token` is the vendor's instrument_id, as in the other US files. A day that is already in the output folder is left alone (use --force to redo
it), so a download that overlaps what you converted before only adds its new days. The manifest and the tar always list every file of the output folder. Bars whose price is
not a finite number are dropped and counted. Needs databento and pyarrow (not part of the wheel). Columns and conventions: see convert_us_ohlcv.py.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from convert_us_ohlcv import convert_folder, make_tar, parse_session, read_symbols  # noqa: E402

NAME = re.compile(r"-(\d{4})(\d{2})(\d{2})\.ohlcv-1m\.dbn(?:\.zst)?$")
LABEL = "Databento XNAS.ITCH 1-minute OHLCV (.dbn.zst)"


def day_of(path: str) -> str:
    m = NAME.search(os.path.basename(path))
    if not m:
        raise ValueError(f"{os.path.basename(path)}: the name must end in -yyyymmdd.ohlcv-1m.dbn.zst")
    return "-".join(m.groups())


def read_dbn(path: str) -> pa.Table:
    """One day as the vendor table the US converter expects: ts_event (UTC), symbol, open, high, low, close, volume, instrument_id."""
    import databento as db
    store = db.DBNStore.from_file(path)
    df = store.to_df(price_type="float", pretty_ts=True, map_symbols=True)
    missing = [c for c in ("symbol", "open", "high", "low", "close", "volume", "instrument_id") if c not in df.columns]
    if missing:
        raise ValueError(f"{os.path.basename(path)}: missing {missing}; found {list(df.columns)}")
    df = df.reset_index().rename(columns={df.index.name or "ts_event": "ts_event"})
    t = pa.table({"ts_event": pa.array(df["ts_event"].to_numpy(), type=pa.timestamp("ns", "UTC")) if df["ts_event"].dt.tz is not None
                  else pa.array(df["ts_event"].dt.tz_localize("UTC").to_numpy(), type=pa.timestamp("ns", "UTC")),
                  "symbol": pa.array(df["symbol"].astype(str).to_numpy(), pa.large_string()),
                  "open": df["open"].to_numpy(), "high": df["high"].to_numpy(), "low": df["low"].to_numpy(), "close": df["close"].to_numpy(),
                  "volume": pa.array(df["volume"].to_numpy().astype("int64")), "instrument_id": pa.array(df["instrument_id"].to_numpy().astype("uint32"))})
    ok = pc.and_(pc.and_(pc.is_finite(t["open"]), pc.is_finite(t["high"])), pc.and_(pc.is_finite(t["low"]), pc.is_finite(t["close"])))
    bad = t.num_rows - pc.sum(ok.cast(pa.int64())).as_py()
    if bad:
        print(f"{os.path.basename(path)}: {bad} bar(s) with an undefined price were dropped")
        t = t.filter(ok)
    return t


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
    m = convert_folder(a.src, a.out, read_symbols(a.symbols, a.symbols_file), parse_session(a.session), a.zstd_level, a.force, pattern="*.ohlcv-1m.dbn*",
                       read=read_dbn, dayfn=day_of, label=LABEL)
    run = m["this_run"]
    print(f"\nthis run: {len(run['converted'])} converted, {len(run['already_there'])} already there, {len(run['empty'])} empty, {len(m['failed'])} failed")
    print(f"the folder now: {m['files']} days ({m['first_day']} to {m['last_day']}), {m['rows']:,} rows, {m['bytes'] / 1e6:,.0f} MB")
    if a.tar:
        print(f"{make_tar(a.out, a.tar)} files in {a.tar}")
    return 1 if m["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
