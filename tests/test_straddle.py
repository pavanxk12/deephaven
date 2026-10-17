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
    return SeriesDef(ref=1, strikes=strikes, ce=tuple(range(100, 100 + n)), pe=tuple(range(200, 200 + n)))


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
    assert d.ref == 1 and list(d.strikes) == [24900.0, 25000.0, 25100.0] and len(d.ce) == len(d.pe) == 3


def keys(defs):
    return sorted(defs)


def test_expiries_count_weeklies_and_use_the_matching_future(master_weekly):
    today = pd.Timestamp("2030-01-01")
    one = build_series(master_weekly, StraddleConfig(window=1, expiries=1, symbols=("NIFTY",)), today)
    assert keys(one) == [("NIFTY", "2030-01-09")]
    assert one[("NIFTY", "2030-01-09")].ref == 1                    # weekly option -> the monthly future (Jan)

    five = build_series(master_weekly, StraddleConfig(window=1, expiries=5, symbols=("NIFTY",)), today)
    assert keys(five) == [("NIFTY", "2030-01-09"), ("NIFTY", "2030-01-16"), ("NIFTY", "2030-01-23"),
                          ("NIFTY", "2030-01-30"), ("NIFTY", "2030-02-27")]
    assert [five[k].ref for k in keys(five)] == [1, 1, 1, 1, 2]    # Feb options -> the Feb future
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


# ---- no future: fall back to the spot ----------------------------------------------------------------------------
def spot_master():
    """FUTS has a future and a spot (the future wins), SPOTONLY has options and a spot, BARE has only options."""
    import pandas as pd
    from algo.core.config import NSE
    from algo.data.master import classify, normalise
    from tests.conftest import secs
    e = secs("2030-01-30")
    rows, tok = [], 1000
    for sym in ("FUTS", "SPOTONLY", "BARE"):
        for strike in (900, 1000, 1100):
            for ot in ("CE", "PE"):
                rows.append(dict(Token=tok, Symbol=sym, ExpiryDate=e, InstType="OPTSTK", OptionType=ot,
                                 StrikePrice=strike * 100, LotSize=1, TickSize=5))
                tok += 1
    rows.append(dict(Token=1, Symbol="FUTS", ExpiryDate=e, InstType="FUTSTK", OptionType="XX", StrikePrice=0, LotSize=1, TickSize=5))
    rows.append(dict(Token=2, Symbol="FUTS", ExpiryDate=None, InstType="EQ", OptionType="XX", StrikePrice=0, LotSize=1, TickSize=5))
    rows.append(dict(Token=3, Symbol="SPOTONLY", ExpiryDate=None, InstType="EQ", OptionType="XX", StrikePrice=0, LotSize=1, TickSize=5))
    return classify(normalise(pd.DataFrame(rows), NSE))


def test_a_symbol_without_a_future_uses_its_spot_and_a_symbol_with_one_keeps_the_future():
    m = spot_master()
    defs = build_series(m, StraddleConfig(window=1), pd.Timestamp("2030-01-01"))
    assert sorted(defs) == [("FUTS", "2030-01-30"), ("SPOTONLY", "2030-01-30")]
    assert (defs[("FUTS", "2030-01-30")].ref, defs[("FUTS", "2030-01-30")].ref_kind) == (1, "FUT")      # the future wins
    assert (defs[("SPOTONLY", "2030-01-30")].ref, defs[("SPOTONLY", "2030-01-30")].ref_kind) == (3, "SPOT")


def test_a_symbol_with_neither_is_skipped_and_named_in_the_log(caplog):
    import logging
    m = spot_master()
    with caplog.at_level(logging.INFO, logger="algo.straddle"):
        build_series(m, StraddleConfig(window=1), pd.Timestamp("2030-01-01"), logging.getLogger("algo.straddle"))
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "BARE" in text and "1 of 2 series use the spot" in text
    build_series(m, StraddleConfig(window=1, symbols=("BARE", "SPOTONLY")), pd.Timestamp("2030-01-01"))   # asking for it by name does not raise


def test_after_the_future_has_expired_the_spot_takes_over():
    m = spot_master()
    defs = build_series(m, StraddleConfig(window=1, symbols=("FUTS",)), pd.Timestamp("2030-01-30"))
    assert defs[("FUTS", "2030-01-30")].ref_kind == "FUT"              # expires today: still counts
    m.futs = m.futs.iloc[0:0]                                          # no future at all
    m.__dict__.pop("fut_info", None)
    defs = build_series(m, StraddleConfig(window=1, symbols=("FUTS",)), pd.Timestamp("2030-01-01"))
    assert (defs[("FUTS", "2030-01-30")].ref, defs[("FUTS", "2030-01-30")].ref_kind) == (2, "SPOT")


def test_the_engine_prices_the_atm_strike_from_the_spot():
    m = spot_master()
    defs = build_series(m, StraddleConfig(window=1, symbols=("SPOTONLY",)), pd.Timestamp("2030-01-01"))
    d = defs[("SPOTONLY", "2030-01-30")]
    e = StraddleEngine(defs, StraddleConfig(window=1))
    assert 3 in e.needed and set(d.ce) <= e.needed                       # the spot is subscribed like any other leg
    now = NOW
    e.set_price(3, 1000.0, now)                                         # spot at 1000 -> ATM strike 1000
    for i in range(3):
        e.set_price(d.ce[i], 20.0, now)
        e.set_price(d.pe[i], 25.0, now)
    (row,) = e.sample(WHEN, now)
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert r["Fut"] == 1000.0 and r["ATMStrike"] == 1000.0 and r["V"] == 45.0


# ---- IV = V / Synthetic * sqrt(max(dte, 1)) * 100 ------------------------------------------------------------------------------
def test_iv_is_v_over_synthetic_times_root_of_the_days_left_times_100():
    import math
    e = engine()
    feed(e, 25000.0, {4: (120, 130), 5: (100, 90), 6: (140, 150)})
    (row,) = e.sample(WHEN, NOW)                                                        # 2030-01-10, expiry 2030-01-30: 20 days
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert StraddleEngine.COLUMNS[-1] == "IV" and len(row) == len(StraddleEngine.COLUMNS)
    assert r["IV"] == 190.0 / 25010.0 * math.sqrt(20) * 100


def test_on_the_expiry_day_the_days_left_count_as_one():
    from algo.series.straddle import implied_vol
    assert implied_vol(190.0, 25010.0, 0) == implied_vol(190.0, 25010.0, 1) == 190.0 / 25010.0 * 100
    assert implied_vol(190.0, 25010.0, 4) == 190.0 / 25010.0 * 2 * 100
    assert implied_vol(190.0, 25010.0, -3) == 190.0 / 25010.0 * 100                    # never below one day


def test_iv_is_nan_when_the_synthetic_price_is_not_positive():
    from algo.series.straddle import implied_vol
    assert np.isnan(implied_vol(10.0, 0.0, 5)) and np.isnan(implied_vol(10.0, -5.0, 5)) and np.isnan(implied_vol(10.0, float("nan"), 5))


def test_the_days_left_follow_the_sample_day_not_a_fixed_date():
    e = engine()
    feed(e, 25000.0, {4: (120, 130), 5: (100, 90), 6: (140, 150)})
    first = dict(zip(StraddleEngine.COLUMNS, e.sample(WHEN, NOW)[0]))["IV"]
    later = datetime(2030, 1, 29, 10, 0, 0, tzinfo=IST)                                  # one day before the expiry
    for tok in list(e.px):
        e.px[tok] = (e.px[tok][0], later.timestamp())
    last = dict(zip(StraddleEngine.COLUMNS, e.sample(later, later.timestamp())[0]))["IV"]
    assert last < first and abs(last - 190.0 / 25010.0 * 100) < 1e-9
