"""Parity: the C++ candle builder must produce exactly the bars of the Python Candles plugin.

The same randomly generated, deliberately dirty feed batches go through
  * Python: algo.data.candles.Candles._on_update (the real code, with a stand-in for the Deephaven update object), and
  * C++:    cpp/build/candles_replay (TokenIndex + LttParser + CandleBuilder),
and every closed bar (token, start, open, high, low, close, volume, cvd) must be identical.

    python3 cpp/tests/parity/candles_parity.py --bin cpp/build/candles_replay [--seeds 8] [--rows 40000]

Dirt in the feed: NaN / inf / zero / negative prices, negative and NaN volumes, null and unparseable ltt texts, ltt with
.mmm fractions and surrounding spaces, exact repeats (same ltt and volume), late ticks, unchanged prices, missing ltq,
tokens the master does not know, the same token several times in one batch and in both parts of a batch.
"""
from __future__ import annotations

import argparse
import math
import os
import random
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, ROOT)
from algo.data.candles import CandleAggregator, Candles, LttParser  # noqa: E402

MARKETS = {   # name: (python minute format, C++ full format, zone, session start local)
    "NSE": ("%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S", "Asia/Kolkata", (9, 15)),
    "US":  ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "UTC", (13, 30)),
}
COLS = ["token", "ltp", "vol", "ltt", "ltq"]


class FakeUpdate:
    def __init__(self, added, modified):
        self._a, self._m = added, modified

    @staticmethod
    def _frame(rows):
        if not rows:
            return None
        return {"token": np.array([r[0] for r in rows], dtype=np.int64),
                "ltp": np.array([r[1] for r in rows], dtype=np.float64),
                "vol": np.array([r[2] for r in rows], dtype=np.float64),
                "ltt": np.array([r[3] for r in rows], dtype=object),
                "ltq": np.array([r[4] for r in rows], dtype=np.float64)}

    def added(self, cols):
        return self._frame(self._a)

    def modified(self, cols):
        return self._frame(self._m)


def generate(seed: int, market: str, n_rows: int):
    rnd = random.Random(seed)
    _, _, zone, (h, m) = MARKETS[market]
    tz = ZoneInfo(zone)
    day = datetime(2026, 10, 15, h, m, tzinfo=tz)
    clock = day - timedelta(seconds=75)                     # start before the first bar: those ticks build no candle
    master = [1000 + i * 7 for i in range(30)]
    unknown = [987654321, 55, 4242424]                      # not in the master: still get candles (no symbol)
    tokens = master + unknown
    state = {t: {"px": rnd.randint(2000, 500000), "vol": rnd.randint(0, 5000), "last": None} for t in tokens}
    fmt = MARKETS[market][1]
    batches, rows_left, b = [], n_rows, 0

    def text(dt):
        s = dt.astimezone(tz).strftime(fmt)
        r = rnd.random()
        if r < 0.05:
            s += ".%03d" % rnd.randint(0, 999)
        elif r < 0.07:
            s = "  " + s + " "
        return s

    while rows_left > 0:
        clock += timedelta(milliseconds=rnd.choice([200, 500, 1000, 1000, 1500, 3000]))
        if rnd.random() < 0.01:
            clock += timedelta(seconds=rnd.randint(30, 200))   # quiet stretch: whole bars with no ticks
        added, modified = [], []
        for _ in range(rnd.randint(1, 25)):
            t = rnd.choice(tokens)
            st = state[t]
            kind = rnd.random()
            if st["last"] is not None and kind < 0.08:          # exact repeat of the last row
                row = st["last"]
            else:
                if rnd.random() < 0.7:
                    st["px"] = max(1, st["px"] + rnd.choice([-3, -2, -1, 0, 0, 1, 2, 3]) * rnd.choice([1, 5, 10]))
                if rnd.random() < 0.8:
                    st["vol"] += rnd.randint(1, 300)
                when = clock - timedelta(seconds=rnd.randint(0, 90)) if rnd.random() < 0.04 else clock   # late tick
                ltt = text(when)
                ltq = float(rnd.randint(1, 50)) if rnd.random() < 0.85 else rnd.choice([0.0, float("nan"), -3.0])
                row = (t, float(st["px"]), float(st["vol"]), ltt, ltq)
                d = rnd.random()                                 # dirty variants
                if d < 0.02: row = (t, float("nan"), row[2], row[3], row[4])
                elif d < 0.03: row = (t, rnd.choice([0.0, -5.0, float("inf")]), row[2], row[3], row[4])
                elif d < 0.04: row = (t, row[1], rnd.choice([-1.0, float("nan"), float("inf")]), row[3], row[4])
                elif d < 0.05: row = (t, row[1], row[2], None, row[4])
                elif d < 0.06: row = (t, row[1], row[2], rnd.choice(["", "garbage", "15-10-2026", "99-99-2026 10:00:00", "abc:de"]), row[4])
                elif d < 0.80: st["last"] = row
            (added if rnd.random() < 0.4 else modified).append(row)
        batches.append((added, modified))
        rows_left -= len(added) + len(modified)
        b += 1
    start = int(day.timestamp())
    return batches, master, start


def python_bars(batches, market, bucket, start):
    minute_fmt, _, zone, _ = MARKETS[market]
    out = []
    c = Candles("30s" if bucket == 30 else f"{bucket // 60}min")
    c.scale, c.scale_of = 0.01, {}
    c._build_lookups(10 ** 6)                                  # tokens above this size get the default scale too
    c.agg = CandleAggregator(lambda tok, mn, o, h, l, cl, v, cvd: out.append(
        (int(tok), int(mn), round(o * 100), round(h * 100), round(l * 100), round(cl * 100), int(v), int(round(cvd)))), start, bucket)
    c._parse = LttParser(minute_fmt, ZoneInfo(zone))
    for added, modified in batches:
        c._on_update(FakeUpdate(added, modified), False)
    return out


def write_input(path, batches):
    with open(path, "w") as f:
        f.write("batch,part,token,ltp,vol,ltt,ltq\n")
        for b, (added, modified) in enumerate(batches):
            for part, rows in ((0, added), (1, modified)):
                for t, ltp, vol, ltt, ltq in rows:
                    f.write(f"{b},{part},{t},{ltp!r},{vol!r},{'<null>' if ltt is None else ltt},{ltq!r}\n")


def cpp_bars(binary, path_in, path_out, market, bucket, start, master_path):
    _, fmt, zone, _ = MARKETS[market]
    r = subprocess.run([binary, "--format", fmt, "--tz", zone, "--bucket", str(bucket), "--start", str(start),
                        "--master", master_path, path_in, path_out], capture_output=True, text=True)
    if r.returncode:
        raise SystemExit(f"candles_replay failed: {r.stderr}")
    out = []
    with open(path_out) as f:
        next(f)
        for line in f:
            out.append(tuple(int(x) for x in line.split(",")))
    return out, r.stderr.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.path.join(ROOT, "cpp", "build", "candles_replay"))
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--rows", type=int, default=40000)
    a = ap.parse_args()
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        for seed in range(a.seeds):
            for market in ("NSE", "US"):
                for bucket in (30, 60, 300):
                    batches, master, start = generate(seed, market, a.rows)
                    pin, pout, pm = (os.path.join(tmp, n) for n in ("in.csv", "out.csv", "master.txt"))
                    write_input(pin, batches)
                    with open(pm, "w") as f:
                        f.write("\n".join(map(str, master)))
                    want = sorted(python_bars(batches, market, bucket, start))
                    got, info = cpp_bars(a.bin, pin, pout, market, bucket, start, pm)
                    got.sort()
                    ok = want == got
                    failures += not ok
                    print(f"seed={seed} {market:3} bucket={bucket:3}s  bars py={len(want):5} cpp={len(got):5}  {'OK' if ok else 'MISMATCH'}   [{info}]")
                    if not ok:
                        ws, gs = set(want), set(got)
                        for x in sorted(ws - gs)[:5]:
                            print("   only python:", x)
                        for x in sorted(gs - ws)[:5]:
                            print("   only c++:   ", x)
    print("ALL IDENTICAL" if not failures else f"{failures} MISMATCHED RUNS")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
