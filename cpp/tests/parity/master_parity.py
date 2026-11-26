"""Parity: the C++ master builder must classify a real contract file exactly like algo.data.master.

Both sides load the SAME .db3 file (a real contract dump, not synthetic rows -- the master's logic is dominated by
exact-format parsing of the vendor's own file, which is worth checking against the real thing) and produce a
canonical "token,display,kind,tick,divisor" dump, sorted by token. Every row must match.

    python3 cpp/tests/parity/master_parity.py --bin cpp/build/master_dump --db3 cpp/tests/parity/data/20261120DB.db3 [--market NSE]
"""
from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(ROOT, "cpp", "tests", "parity", "reference"))   # the Python reference (see reference/README.md)
from algo.core.config import NSE, US               # noqa: E402
from algo.data.master import classify, normalise, read_contracts  # noqa: E402

MARKETS = {"NSE": (NSE, os.path.join(ROOT, "cpp", "config", "markets", "NSE.ini")),
          "US": (US, os.path.join(ROOT, "cpp", "config", "markets", "US.ini"))}


def python_dump(db3_path: str, market) -> list[tuple[int, str, str, float, float]]:
    raw, _ = read_contracts(db3_path, market)
    df = normalise(raw, market)
    data = classify(df)
    kind_of = {}
    for _, row in data.eqs.iterrows():  kind_of[int(row["eq_token"])] = "EQ"
    for _, row in data.futs.iterrows(): kind_of[int(row["fut_token"])] = "FUT"
    for _, row in data.opt.iterrows():  kind_of[int(row["Token"])] = "OPT"
    rows = []
    for tok, display in data.symbol_of.items():
        tick = data.tick_of.get(tok, float("nan"))
        divisor = data.divisor_of.get(tok, float("nan"))
        rows.append((int(tok), display, kind_of.get(int(tok), "?"), float(tick), float(divisor)))
    rows.sort(key=lambda r: r[0])
    return rows


def cpp_dump(binary: str, market_ini: str, db3_path: str, out_path: str) -> list[tuple[int, str, str, float, float]]:
    r = subprocess.run([binary, "--market", market_ini, "--db3", db3_path, out_path], capture_output=True, text=True)
    if r.returncode:
        raise SystemExit(f"master_dump failed: {r.stderr}")
    rows = []
    with open(out_path) as f:
        f.readline()                  # "# chain,opt,futs,eqs,tokens"
        counts = f.readline()
        f.readline()                  # "token,display,kind,tick,divisor"
        for line in f:
            tok, display, kind, tick, divisor = line.rstrip("\n").split(",")
            rows.append((int(tok), display, kind, float(tick), float(divisor)))
    return rows, counts.strip()


def close(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.path.join(ROOT, "cpp", "build", "master_dump"))
    ap.add_argument("--db3", default=os.path.join(ROOT, "cpp", "tests", "parity", "data", "20261120DB.db3"))
    ap.add_argument("--market", default="NSE", choices=sorted(MARKETS))
    a = ap.parse_args()
    if not os.path.exists(a.db3):
        print(f"skipping: no contract file at {a.db3} (pull a real <yyyymmdd>DB.db3 from a server into cpp/tests/parity/data/ to run this)")
        return
    market, market_ini = MARKETS[a.market]

    want = python_dump(a.db3, market)
    with tempfile.TemporaryDirectory() as tmp:
        got, counts = cpp_dump(a.bin, market_ini, a.db3, os.path.join(tmp, "out.csv"))

    want_by_tok = {r[0]: r for r in want}
    got_by_tok = {r[0]: r for r in got}
    mismatches = []
    for tok in sorted(set(want_by_tok) | set(got_by_tok)):
        w, g = want_by_tok.get(tok), got_by_tok.get(tok)
        if w is None: mismatches.append(f"token {tok}: only in C++: {g}"); continue
        if g is None: mismatches.append(f"token {tok}: only in python: {w}"); continue
        if w[1] != g[1] or w[2] != g[2] or not close(w[3], g[3]) or not close(w[4], g[4]):
            mismatches.append(f"token {tok}: python={w} cpp={g}")

    print(f"python tokens={len(want)}  cpp tokens={len(got)} (counts line: {counts})")
    for m in mismatches[:30]:
        print("  MISMATCH:", m)
    if len(mismatches) > 30:
        print(f"  ... and {len(mismatches) - 30} more")
    print("ALL IDENTICAL" if not mismatches else f"{len(mismatches)} MISMATCHED TOKENS")
    sys.exit(1 if mismatches else 0)


if __name__ == "__main__":
    main()
