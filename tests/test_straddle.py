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


def keys(defs):
    return sorted(defs)


def test_expiries_count_weeklies_and_use_the_matching_future(master_weekly):
    today = pd.Timestamp("2030-01-01")
    one = build_series(master_weekly, StraddleConfig(window=1, expiries=1, symbols=("NIFTY",)), today)
    assert keys(one) == [("NIFTY", "2030-01-09")]
    assert one[("NIFTY", "2030-01-09")].fut == 1                    # weekly option -> the monthly future (Jan)

    five = build_series(master_weekly, StraddleConfig(window=1, expiries=5, symbols=("NIFTY",)), today)
    assert keys(five) == [("NIFTY", "2030-01-09"), ("NIFTY", "2030-01-16"), ("NIFTY", "2030-01-23"),
                          ("NIFTY", "2030-01-30"), ("NIFTY", "2030-02-27")]
    assert [five[k].fut for k in keys(five)] == [1, 1, 1, 1, 2]    # Feb options -> the Feb future
    assert five[("NIFTY", "2030-02-27")].ce != five[("NIFTY", "2030-01-30")].ce


def test_more_expiries_than_exist_just_gives_what_there_is(master_weekly):
    today = pd.Timestamp("2030-01-01")
    d = build_series(master_weekly, StraddleConfig(window=1, expiries=50, symbols=("RELIANCE",)), today)
    assert keys(d) == [("RELIANCE", "2030-01-30")]


def test_expired_expiries_are_not_counted(master_weekly):
    d = build_series(master_weekly, StraddleConfig(window=1, expiries=1, symbols=("NIFTY",)), pd.Timestamp("2030-01-10"))
    assert keys(d) == [("NIFTY", "2030-01-16")]


def test_reference_future_choice():
    from algo.series.straddle import reference_future
    e = [pd.Timestamp("2030-01-30"), pd.Timestamp("2030-02-27")]
    assert reference_future(e, pd.Timestamp("2030-01-09")) == 0
    assert reference_future(e, pd.Timestamp("2030-01-30")) == 0
    assert reference_future(e, pd.Timestamp("2030-02-05")) == 1
    assert reference_future(e, pd.Timestamp("2030-04-01")) == 1                    # beyond the last future: the last one
