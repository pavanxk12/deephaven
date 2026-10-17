"""Build `straddle_series` history from GDFL NSE F&O 1-minute files, in the format `eod_save()` writes, so `eod_load("2026-03-12", tables=["straddle_series"])`
reads it. It runs the SAME engine as `straddle_start()` (algo.series.straddle: StraddleEngine and replay), so every column, including IV, is what the live table
would have held for that day.

    python tools/build_straddle_history.py  <root folder>  <output folder>  [--year 2026]  [--expiries 0]  [--symbols NIFTY,BANKNIFTY | --symbols-file f.txt]
                                            [--window 5] [--tar out.tar] [--force]

Input: `GFDLNFO_CONTRACT_ddmmyyyy.csv` (or `.zip` holding the csv) anywhere under <root>, columns `Ticker, Date, Time, Open, High, Low, Close, Volume, Open Interest`;
Ticker is `SYMBOL` + `DDMONYY` + `FUT` or `STRIKE` + `CE`/`PE` + `.NFO`. Times are India time and end in :59: a bar's minute is the time without the seconds, and its close is
used at the end of that minute, as the live candles are.
`--expiries n` is the same argument as `straddle_start(n)`: 0 = the nearest option expiry only (the default), 1 = the two nearest, ...

How the contract list for a day is built (there is no contract master in these files): a contract is known on a day if it traded on that day or on one of the 5 days before it
and has not expired. A listed strike that has not traded yet is therefore missing from that day's chain (it has no price to use anyway); nothing from the future is used.
Output per day: `straddle_<day>.parquet` (+ `.meta.json`): Time, Symbol, Expiry, Fut, ATMStrike, ATMStraddle, MinStrike, V, Cost, CumCost, D, AltCost, AltCumCost, AltD,
Synthetic, IV, Date.  Sorted as produced (time, then symbol). Written with ZSTD under a temporary name and renamed when done; checked by reading back. Needs pyarrow.
Run it with the algo package importable (from the repository root, or with the wheel installed).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import zipfile
from collections import deque
from dataclasses import replace
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from convert_us_ohlcv import read_symbols, rename, sha256  # noqa: E402

from algo.core.config import NSE  # noqa: E402
from algo.data.master import MasterData  # noqa: E402
from algo.series.straddle import StraddleConfig, StraddleEngine, build_series, replay  # noqa: E402

IST_MINUTES = 330
KNOWN_DAYS = 5                                                # a contract that traded in the last 5 days is still listed
NAME = re.compile(r"^GFDLNFO_CONTRACT_(\d{2})(\d{2})(\d{4})\.(csv|zip)$", re.IGNORECASE)
TICKER = re.compile(r"^(?P<sym>.+?)(?P<dd>\d{2})(?P<mon>[A-Z]{3})(?P<yy>\d{2})(?:(?P<fut>FUT)|(?P<strike>\d+(?:\.\d+)?)(?P<typ>CE|PE))\.NFO$")
CSV_TYPES = {"Ticker": pa.string(), "Date": pa.string(), "Time": pa.string(), "Open": pa.float64(), "High": pa.float64(), "Low": pa.float64(),
             "Close": pa.float64(), "Volume": pa.int64(), "Open Interest": pa.int64()}
STRING_COLUMNS = ("Time", "Symbol", "Expiry")
COLUMNS = [*StraddleEngine.COLUMNS, "Date"]


def day_of(path: str) -> str:
    m = NAME.match(os.path.basename(path))
    if not m:
        raise ValueError(f"{os.path.basename(path)}: the file name must look like GFDLNFO_CONTRACT_ddmmyyyy.csv")
    d, mo, y = m.group(1, 2, 3)
    try:
        return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"{os.path.basename(path)}: {d}/{mo}/{y} is not a date") from None


def find_files(root: str, year: int | None = None) -> tuple[list[str], list[str]]:
    """(one file per day, oldest first; the extra copies of a day that were left out). A csv is preferred to a zip of the same day."""
    by_day: dict[str, list[str]] = {}
    for dp, _, fs in os.walk(root):
        for f in fs:
            if NAME.match(f):
                day = day_of(f)
                if year is None or day.startswith(f"{year}-"):
                    by_day.setdefault(day, []).append(os.path.join(dp, f))
    chosen, extra = [], []
    for day in sorted(by_day):
        copies = sorted(by_day[day], key=lambda p: (not p.lower().endswith(".csv"), p))
        chosen.append(copies[0])
        extra += copies[1:]
    return chosen, extra


def read_day(path: str) -> pa.Table:
    """The day's bars as a table; a .zip is read straight from the archive."""
    opts = pacsv.ConvertOptions(column_types=CSV_TYPES)
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(names) != 1:
                raise ValueError(f"{os.path.basename(path)}: expected one csv in the zip, found {names}")
            with z.open(names[0]) as f:
                return pacsv.read_csv(f, convert_options=opts)
    return pacsv.read_csv(path, convert_options=opts)


class Contracts:
    """The contracts known on a day: those that traded today or in the last KNOWN_DAYS days."""

    def __init__(self):
        self._parsed: dict[str, tuple | None] = {}
        self._recent: deque = deque(maxlen=KNOWN_DAYS + 1)

    def parse(self, ticker: str):
        if ticker not in self._parsed:
            m = TICKER.match(ticker)
            if not m:
                self._parsed[ticker] = None
            else:
                try:
                    expiry = datetime.strptime(m["dd"] + m["mon"] + m["yy"], "%d%b%y")
                except ValueError:
                    self._parsed[ticker] = None
                    return None
                self._parsed[ticker] = (m["sym"], pd.Timestamp(expiry), None if m["fut"] else float(m["strike"]), "FUT" if m["fut"] else m["typ"])
        return self._parsed[ticker]

    def add_day(self, tickers) -> list[str]:
        """Today's tickers join the recent days; returns every known ticker (sorted, so a token is its position)."""
        self._recent.append(set(tickers))
        return sorted(set().union(*self._recent))

    def master(self, known: list[str], tokens: dict[str, int]) -> MasterData:
        """A stand-in for the contract master with the pieces `build_series` reads: futures, the option chain (a strike needs both its CE and PE)."""
        futs, ce, pe = [], {}, {}
        for t in known:
            p = self.parse(t)
            if p is None:
                continue
            sym, exp, strike, typ = p
            if typ == "FUT":
                futs.append((tokens[t], sym, exp))
            elif typ == "CE":
                ce[(sym, exp, strike)] = tokens[t]
            else:
                pe[(sym, exp, strike)] = tokens[t]
        chain = pd.DataFrame([(ce[k], pe[k], k[0], k[1], k[2]) for k in sorted(set(ce) & set(pe))],
                             columns=["ce_token", "pe_token", "TckrSymb", "expiry", "strike"])
        fut_frame = pd.DataFrame(futs, columns=["fut_token", "TckrSymb", "expiry"])
        empty = pd.DataFrame
        return MasterData(chain=chain, opt=empty(), futs=fut_frame, eqs=pd.DataFrame({"eq_token": pd.Series(dtype="int64"), "TckrSymb": pd.Series(dtype="object")}),
                          symbol_of={})


def bar_arrays(raw: pa.Table, known: list[str], needed: set[int]):
    """(bar start epoch seconds UTC, token, close) of the bars of the contracts the series use. A token is the ticker's position in `known`."""
    idx = pc.index_in(raw["Ticker"], value_set=pa.array(known, pa.string()))
    token = idx.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)
    mask = np.zeros(len(known) + 1, dtype=bool)
    mask[sorted(needed)] = True
    keep = (token >= 0) & mask[np.where(token >= 0, token, len(known))]
    local = pc.strptime(pc.binary_join_element_wise(raw["Date"], raw["Time"], " "), format="%d/%m/%Y %H:%M:%S", unit="s")        # India time, no zone
    minute = pc.floor_temporal(local, unit="minute")
    epoch = pc.subtract(pc.cast(minute, pa.int64()), IST_MINUTES * 60)                                                         # the bar start in UTC
    ts = epoch.to_numpy(zero_copy_only=False).astype(np.int64)
    close = raw["Close"].to_numpy(zero_copy_only=False).astype(np.float64)
    return ts[keep], token[keep], close[keep]


def day_table(rows: list[tuple], day: str) -> pa.Table:
    cols = list(zip(*rows)) if rows else [()] * len(StraddleEngine.COLUMNS)
    data = {name: pa.array(col, pa.string() if name in STRING_COLUMNS else pa.float64()) for name, col in zip(StraddleEngine.COLUMNS, cols)}
    data["Date"] = pa.array([day] * len(rows), pa.string())
    return pa.table(data)


def build_day(path: str, contracts: Contracts, cfg: StraddleConfig, day: str) -> tuple[list[tuple], dict]:
    raw = read_day(path)
    missing = [c for c in CSV_TYPES if c not in raw.column_names]
    if missing:
        raise ValueError(f"missing columns {missing}; found {raw.column_names}")
    dates = pc.unique(raw["Date"]).to_pylist()
    if len(dates) != 1 or datetime.strptime(dates[0], "%d/%m/%Y").strftime("%Y-%m-%d") != day:
        raise ValueError(f"the file name says {day} but the Date column holds {dates[:3]}")
    known = contracts.add_day(pc.unique(raw["Ticker"]).to_pylist())
    tokens = {t: i for i, t in enumerate(known)}
    master = contracts.master(known, tokens)
    defs = build_series(master, cfg, pd.Timestamp(day))
    eng = StraddleEngine(defs, cfg)
    rows: list[tuple] = []
    if defs:
        ts, token, close = bar_arrays(raw, known, eng.needed)
        if cfg.max_age_s is not None:
            eng.cfg = replace(cfg, max_age_s=max(cfg.max_age_s, 180))                                                              # as the live plugin (3 sample periods)
        replay(eng, ts, token, close, 60, 60, cfg.offset_s, NSE.session, NSE.zone, rows.extend)
    return rows, {"series": len(defs), "bars_used": int(len(ts)) if defs else 0, "source_rows": raw.num_rows, "known_contracts": len(known)}


def convert_file(src: str, out_dir: str, contracts: Contracts, cfg: StraddleConfig, level: int = 9, force: bool = False) -> dict:
    day = day_of(src)
    final = os.path.join(out_dir, f"straddle_{day}.parquet")
    if os.path.exists(final) and not force:
        # the day must still pass through the contract list, so tomorrow knows today's contracts
        contracts.add_day(pc.unique(read_day(src)["Ticker"]).to_pylist())
        return {"day": day, "status": "exists", "file": os.path.basename(final), "rows": pq.ParquetFile(final).metadata.num_rows, "bytes": os.path.getsize(final)}
    rows, info = build_day(src, contracts, cfg, day)
    if not rows:
        return {"day": day, "status": "empty", "rows": 0, **info}
    table = day_table(rows, day)
    os.makedirs(out_dir, exist_ok=True)
    tmp = final[:-len(".parquet")] + ".tmp.parquet"
    try:
        pq.write_table(table, tmp, compression="zstd", compression_level=level)
        back = pq.read_table(tmp, memory_map=False)
        if back.num_rows != table.num_rows or back.column_names != COLUMNS:
            raise RuntimeError(f"{day}: the written file does not read back the same ({back.num_rows} rows, {back.column_names})")
        del back
        rename(tmp, final)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    meta = {"table": "straddle_series", "day": day, "market": "NSE", "timezone": "Asia/Kolkata", "rows": table.num_rows, "file": os.path.basename(final),
            "columns": COLUMNS, "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "built_by": f"build_straddle_history.py from GDFL F&O 1-minute bars with the straddle_start engine; expiries={cfg.expiries}, window={cfg.window}",
            "source": os.path.basename(src), **info}
    with open(final[:-len(".parquet")] + ".meta.json.tmp", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1)
    rename(final[:-len(".parquet")] + ".meta.json.tmp", final[:-len(".parquet")] + ".meta.json")
    return {"day": day, "status": "converted", "file": os.path.basename(final), "rows": table.num_rows, "bytes": os.path.getsize(final), **info}


def build_folder(src_dir: str, out_dir: str, cfg: StraddleConfig, year: int | None = None, level: int = 9, force: bool = False, log=print) -> dict:
    files, extra = find_files(src_dir, year)
    if not files:
        raise FileNotFoundError(f"no GFDLNFO_CONTRACT_ddmmyyyy files{f' of {year}' if year else ''} under {src_dir}")
    contracts, results = Contracts(), []
    for src in files:
        day = day_of(src)
        try:
            r = convert_file(src, out_dir, contracts, cfg, level, force)
        except Exception as e:                                               # one bad file must not stop the rest
            r = {"day": day, "status": "failed", "error": f"{type(e).__name__}: {e}"}
        results.append(r)
        log(f"{r['day']}  {r['status']:<9} {r.get('rows', ''):>8}  series {r.get('series', ''):>4}  {r.get('error', '')}")
    done = [r for r in results if r["status"] in ("converted", "exists")]
    manifest = {"made_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source_folder": os.path.abspath(src_dir), "expiries": cfg.expiries,
                "files": len(done), "rows": sum(r.get("rows", 0) for r in done), "bytes": sum(r.get("bytes", 0) for r in done),
                "failed": [r for r in results if r["status"] == "failed"], "empty_days": [r["day"] for r in results if r["status"] == "empty"],
                "extra_copies_left_out": extra, "first_day": done[0]["day"] if done else None, "last_day": done[-1]["day"] if done else None,
                "sha256": {r["file"]: sha256(os.path.join(out_dir, r["file"])) for r in done}}
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
    return manifest


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--expiries", type=int, default=0, help="as straddle_start(n): 0 = the nearest option expiry only")
    ap.add_argument("--symbols")
    ap.add_argument("--symbols-file")
    ap.add_argument("--window", type=int, default=5)
    ap.add_argument("--zstd-level", type=int, default=9)
    ap.add_argument("--tar")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    syms = read_symbols(a.symbols, a.symbols_file)
    cfg = StraddleConfig(symbols=tuple(sorted(syms)) if syms else None, expiries=a.expiries + 1, window=a.window)
    manifest = build_folder(a.src, a.out, cfg, a.year, a.zstd_level, a.force)
    print(f"\n{manifest['files']} days, {manifest['rows']:,} rows, {manifest['bytes'] / 1e6:,.0f} MB, {len(manifest['failed'])} failed, "
          f"{len(manifest['empty_days'])} empty days")
    if a.tar:
        print(f"{make_tar_straddle(a.out, a.tar)} files in {a.tar}")
    return 1 if manifest["failed"] else 0


def make_tar_straddle(out_dir: str, tar_path: str) -> int:
    import glob
    import tarfile
    n = 0
    with tarfile.open(tar_path, "w") as tf:
        for p in sorted(glob.glob(os.path.join(out_dir, "straddle_*"))) + [os.path.join(out_dir, "manifest.json")]:
            if os.path.exists(p) and ".tmp." not in p:
                tf.add(p, arcname=os.path.basename(p))
                n += 1
    return n


if __name__ == "__main__":
    sys.exit(main())
