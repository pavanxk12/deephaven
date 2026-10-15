from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from algo.series.straddle import SeriesDef, StraddleConfig, StraddleEngine, build_series

IST = ZoneInfo("Asia/Kolkata")
WHEN = datetime(2030, 1, 10, 10, 0, 0, tzinfo=IST)
NOW = WHEN.timestamp()


def series(n=11):
    strikes = np.arange(24500.0, 24500.0 + 100 * n, 100.0)
    return SeriesDef(fut=1, strikes=strikes, ce=tuple(range(100, 100 + n)), pe=tuple(range(200, 200 + n)))


def engine(cfg=StraddleConfig(), n=11):
    return StraddleEngine({("NIFTY", "2030-01-30"): series(n)}, cfg)


def feed(eng, fut, straddles):
    """straddles: {strike index: (ce, pe)}"""
    eng.set_price(1, fut, NOW)
    d = eng.defs[("NIFTY", "2030-01-30")]
    for i, (c, p) in straddles.items():
        eng.set_price(d.ce[i], c, NOW)
        eng.set_price(d.pe[i], p, NOW)


def test_picks_minimum_straddle_in_window():
    e = engine()
    feed(e, 25000.0, {4: (120, 130), 5: (100, 90), 6: (140, 150)})
    (row,) = e.sample(WHEN, NOW)
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert r["MinStrike"] == 25000.0 and r["V"] == 190.0 and r["ATMStrike"] == 25000.0
    assert r["Synthetic"] == 25000.0 + 100 - 90
    assert r["Cost"] == 0.0 and r["D"] == 190.0


def test_cost_accumulates_when_the_minimum_moves():
    e = engine()
    feed(e, 25000.0, {4: (120, 130), 5: (100, 90), 6: (140, 150)})
    e.sample(WHEN, NOW)
    feed(e, 25000.0, {4: (80, 90), 5: (100, 90), 6: (140, 150)})        # strike 4 is now the cheapest: 170 vs 190
    (row,) = e.sample(WHEN, NOW)
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert r["MinStrike"] == 24900.0 and r["V"] == 170.0
    assert r["Cost"] == 20.0 and r["CumCost"] == 20.0 and r["D"] == 190.0


def test_stale_prices_are_ignored():
    e = engine(StraddleConfig(max_age_s=60))
    feed(e, 25000.0, {5: (100, 90)})
    assert e.sample(WHEN, NOW + 120) == []                              # everything older than 60 s


def test_no_row_without_future_price():
    e = engine()
    assert e.sample(WHEN, NOW) == []


def test_window_outside_the_chain_gives_no_row():
    e = engine(n=11)
    feed(e, 24500.0, {0: (100, 90)})                                    # ATM at the edge: window would leave the chain
    assert e.sample(WHEN, NOW) == []


def test_state_resets_each_day():
    e = engine()
    feed(e, 25000.0, {5: (100, 90)})
    e.sample(WHEN, NOW)
    nxt = datetime(2030, 1, 11, 10, 0, 0, tzinfo=IST)
    feed(e, 25000.0, {5: (100, 90)})
    e.px = {k: (v[0], nxt.timestamp()) for k, v in e.px.items()}
    (row,) = e.sample(nxt, nxt.timestamp())
    assert dict(zip(StraddleEngine.COLUMNS, row))["CumCost"] == 0.0


def test_build_series_uses_nearest_expiry_and_needs_enough_strikes(master):
    today = pd.Timestamp("2030-01-01")
    assert build_series(master, StraddleConfig(window=1), today) .keys() == {("NIFTY", "2030-01-30"), ("RELIANCE", "2030-01-30")}
    assert build_series(master, StraddleConfig(window=5), today) == {}                 # 3 strikes < 2*5+1
    only = build_series(master, StraddleConfig(window=1, symbols=("nifty", "nope")), today)
    assert list(only) == [("NIFTY", "2030-01-30")]
    d = only[("NIFTY", "2030-01-30")]
    assert d.fut == 1 and list(d.strikes) == [24900.0, 25000.0, 25100.0] and len(d.ce) == len(d.pe) == 3
