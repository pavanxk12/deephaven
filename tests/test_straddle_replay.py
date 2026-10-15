"""The array-based backfill must give exactly the rows the old bar-by-bar logic gave."""
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from algo.series.straddle import SeriesDef, StraddleConfig, StraddleEngine, replay

IST = ZoneInfo("Asia/Kolkata")
SESSION = (dtime(9, 15), dtime(15, 30))
OPEN = int(datetime(2030, 1, 10, 9, 10, tzinfo=IST).timestamp())
N = 31                                               # 31 strikes with the spot in the middle: drift stays inside the window


def defs():
    strikes = np.arange(23500.0, 23500.0 + 100 * N, 100.0)
    return {("NIFTY", "2030-01-30"): SeriesDef(ref=1, strikes=strikes, ce=tuple(range(100, 100 + N)), pe=tuple(range(200, 200 + N))),
            ("BANK", "2030-01-30"): SeriesDef(ref=2, strikes=strikes + 20000, ce=tuple(range(300, 300 + N)), pe=tuple(range(400, 400 + N)),
                                              ref_kind="SPOT")}


def candles(seed, minutes=45, step=60, gaps=True, junk=True):
    rng = np.random.default_rng(seed)
    ts, tok, close = [], [], []
    legs = [1, *range(100, 100 + N), *range(200, 200 + N), 2, *range(300, 300 + N), *range(400, 400 + N), 999]    # 999 is not a leg
    base = {t: 25000.0 if t == 1 else 45000.0 if t == 2 else 120.0 for t in legs}
    for m in range(minutes):
        for t in legs:
            if gaps and rng.random() < 0.1:
                continue                                   # no trade this bar
            base[t] = max(1.0, base[t] * (1 + rng.normal(0, 0.002)))
            c = base[t]
            if junk and rng.random() < 0.03:
                c = [float("nan"), 0.0, -4.0, float("inf")][int(rng.integers(0, 4))]
            ts.append(OPEN + m * step)
            tok.append(t)
            close.append(c)
    return np.array(ts), np.array(tok), np.array(close)


def old_way(engine, ts, token, close, bucket, cadence, offset):
    """The previous implementation: pandas filter / sort / groupby, one set_price call per row."""
    df = pd.DataFrame({"Ts": ts, "token": token, "Close": close})
    df = df[df["token"].isin(engine.needed) & df["Close"].notna() & (df["Close"] > 0)].sort_values("Ts")
    out = []
    for t, g in df.groupby("Ts"):
        c = float(t) + bucket
        for tk, cl in zip(g["token"], g["Close"]):
            engine.set_price(int(tk), cl, c)
        if c % cadence:
            continue
        t_s = c + offset
        when = datetime.fromtimestamp(t_s, IST)
        if SESSION[0] <= when.time() <= SESSION[1]:
            out += engine.sample(when, t_s)
    return out


def new_way(engine, ts, token, close, bucket, cadence, offset):
    out = []
    replay(engine, ts, token, close, bucket, cadence, offset, SESSION, IST, out.extend)
    return out


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("bucket,cadence", [(60, 60), (60, 300), (300, 300)])
def test_same_rows_as_the_old_logic(seed, bucket, cadence):
    ts, tok, close = candles(seed, step=bucket)
    a = old_way(StraddleEngine(defs(), StraddleConfig()), ts, tok, close, bucket, cadence, 2)
    eng = StraddleEngine(defs(), StraddleConfig())
    b = new_way(eng, ts, tok, close, bucket, cadence, 2)
    assert a == b and len(a) > 0


def test_the_prices_left_in_the_engine_are_identical_too():
    ts, tok, close = candles(3)
    e1, e2 = StraddleEngine(defs(), StraddleConfig()), StraddleEngine(defs(), StraddleConfig())
    old_way(e1, ts, tok, close, 60, 60, 2)
    new_way(e2, ts, tok, close, 60, 60, 2)
    assert e1.px == e2.px and 999 not in e2.px


def test_unsorted_candles_give_the_same_result():
    ts, tok, close = candles(4)
    order = np.random.default_rng(0).permutation(len(ts))
    sorted_rows = new_way(StraddleEngine(defs(), StraddleConfig()), ts, tok, close, 60, 60, 2)
    shuffled = new_way(StraddleEngine(defs(), StraddleConfig()), ts[order], tok[order], close[order], 60, 60, 2)
    assert sorted_rows == shuffled


def test_nothing_comes_out_before_the_session_or_for_empty_input():
    ts, tok, close = candles(1, minutes=4)                       # 09:10 - 09:13 bars: samples fall before 09:15
    assert new_way(StraddleEngine(defs(), StraddleConfig()), ts, tok, close, 60, 60, 2) == []
    assert new_way(StraddleEngine(defs(), StraddleConfig()), [], [], [], 60, 60, 2) == []


def test_set_prices_ignores_what_set_price_ignores():
    e = StraddleEngine(defs(), StraddleConfig())
    e.set_prices([1, 1, 2, 5000, -3, 100, 101], [25000.0, float("nan"), 0.0, 10.0, 10.0, float("inf"), 120.5], 7.0)
    assert e.px == {1: (25000.0, 7.0), 101: (120.5, 7.0)}


def test_a_leg_token_beyond_the_lookup_range_cannot_index_out_of_bounds():
    e = StraddleEngine(defs(), StraddleConfig())
    e.set_prices([10 ** 12], [5.0], 1.0)
    assert e.px == {}
