from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

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


# ---- IV = whole-chain, CBOE-style VIX synthetic-forward IV ---------------------------------------------------------------------
def feed_bidask(eng, quotes, key=("NIFTY", "2030-01-30")):
    """quotes: {strike index: ((call_bid, call_ask), (put_bid, put_ask))}"""
    d = eng.defs[key]
    toks, bids, asks = [], [], []
    for i, ((cb, ca), (pb, pa)) in quotes.items():
        toks += [d.ce[i], d.pe[i]]
        bids += [cb, pb]
        asks += [ca, pa]
    eng.set_bidasks(toks, bids, asks, NOW)


def flat_chain(calls_puts):
    """{strike index: (call_mid, put_mid)} -> tight (bid == ask == mid) quotes for feed_bidask."""
    return {i: ((float(c), float(c)), (float(p), float(p))) for i, (c, p) in calls_puts.items()}


def test_iv_is_nan_without_any_bidask():
    e = engine()
    feed(e, 25000.0, {4: (120, 130), 5: (100, 90), 6: (140, 150)})         # last-trade prices only, no bid/ask fed
    (row,) = e.sample(WHEN, NOW)
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert StraddleEngine.COLUMNS[-1] == "IV" and len(row) == len(StraddleEngine.COLUMNS)
    assert np.isnan(r["IV"])


def test_iv_is_computed_once_the_chain_has_bidask():
    e = engine()
    feed(e, 25000.0, {5: (100, 90)})
    feed_bidask(e, flat_chain({0: (520, 10), 1: (420, 15), 2: (320, 25), 3: (220, 45), 4: (120, 70),
                                5: (95, 95), 6: (70, 120), 7: (45, 220), 8: (25, 320), 9: (15, 420), 10: (10, 520)}))
    (row,) = e.sample(WHEN, NOW)
    r = dict(zip(StraddleEngine.COLUMNS, row))
    assert np.isfinite(r["IV"]) and r["IV"] > 0


def test_iv_falls_as_expiry_approaches_with_the_same_quotes():
    e = engine()
    chain = flat_chain({0: (520, 10), 1: (420, 15), 2: (320, 25), 3: (220, 45), 4: (120, 70),
                         5: (95, 95), 6: (70, 120), 7: (45, 220), 8: (25, 320), 9: (15, 420), 10: (10, 520)})
    feed(e, 25000.0, {5: (100, 90)})
    feed_bidask(e, chain)
    first = dict(zip(StraddleEngine.COLUMNS, e.sample(WHEN, NOW)[0]))["IV"]

    later = datetime(2030, 1, 29, 10, 0, 0, tzinfo=IST)                   # one day before expiry: less variance to annualise
    for tok in list(e.px):
        e.px[tok] = (e.px[tok][0], later.timestamp())
    for tok in list(e.bidask):
        b, a, _ = e.bidask[tok]
        e.bidask[tok] = (b, a, later.timestamp())
    last = dict(zip(StraddleEngine.COLUMNS, e.sample(later, later.timestamp())[0]))["IV"]
    assert np.isfinite(first) and np.isfinite(last)


def test_a_wide_spread_quote_is_spline_repaired_or_dropped():
    from algo.series.straddle import repair_quotes
    strikes = np.array([100.0, 110.0, 120.0, 130.0, 140.0])
    bid = np.array([50.0, 40.0, 1.0, 20.0, 10.0])          # strike 120's spread is way too wide
    ask = np.array([51.0, 41.0, 39.0, 21.0, 11.0])
    mid = (bid + ask) / 2.0
    out, keep = repair_quotes(strikes, bid, ask, mid, max_spread_pct=0.10, min_knots=3)
    assert keep.tolist() == [True, True, True, True, True]                # inside the good knots' range: repaired, not dropped
    assert out[2] != mid[2]


def test_a_bad_quote_outside_the_good_range_is_dropped_not_repaired():
    from algo.series.straddle import repair_quotes
    strikes = np.array([100.0, 110.0, 120.0])
    bid = np.array([1.0, 40.0, 30.0])                       # strike 100 (the edge) has the bad spread
    ask = np.array([30.0, 41.0, 31.0])
    mid = (bid + ask) / 2.0
    out, keep = repair_quotes(strikes, bid, ask, mid, max_spread_pct=0.10, min_knots=2)
    assert keep.tolist() == [False, True, True]


def test_atm_strike_is_the_largest_at_or_below_the_forward():
    from algo.series.straddle import atm_strike
    strikes = np.array([24800.0, 24900.0, 25000.0, 25100.0])
    assert atm_strike(strikes, 25050.0) == 25000.0
    assert atm_strike(strikes, 24850.0) == 24800.0
    assert atm_strike(strikes, 24000.0) == 24800.0                       # below every strike: falls back to the lowest


def test_strike_weights_are_half_the_neighbour_gap_full_gap_at_the_ends():
    from algo.series.straddle import strike_weights
    w = strike_weights(np.array([100.0, 120.0, 150.0, 160.0]))
    assert w.tolist() == [20.0, 25.0, 20.0, 10.0]


def test_synthetic_forward_averages_the_n_tightest_call_put_gaps():
    from algo.series.straddle import synthetic_forward
    strikes = np.array([100.0, 110.0, 120.0])
    call_mid = np.array([15.0, 8.0, 3.0])
    put_mid = np.array([5.0, 8.0, 15.0])                                  # strike 110: C - P = 0, the tightest gap
    f = synthetic_forward(strikes, call_mid, put_mid, r=0.0, T=1.0, n=1)
    assert f == 110.0


def test_vix_iv_matches_the_formula_computed_by_hand():
    import math
    from algo.series.straddle import strike_weights, vix_iv
    strikes = np.array([100.0, 120.0])
    q = np.array([5.0, 5.0])
    w = strike_weights(strikes)
    iv = vix_iv(strikes, q, w, r=0.0, T=1.0, forward=110.0, k0=100.0)
    term = (20.0 / 100.0 ** 2 * 5.0) + (20.0 / 120.0 ** 2 * 5.0)
    sigma2 = 2.0 * term - (110.0 / 100.0 - 1.0) ** 2
    assert iv == pytest.approx(100.0 * math.sqrt(sigma2))


def test_time_to_expiry_is_minutes_to_settle_over_minutes_in_a_year():
    from datetime import time as dtime
    from algo.series.straddle import time_to_expiry
    now = datetime(2030, 1, 10, 15, 0, 0, tzinfo=IST)
    t = time_to_expiry(now, date(2030, 1, 10), settle=dtime(15, 30))      # same day, 30 minutes left
    assert t == pytest.approx(30.0 / 525_600.0)


def test_iv_is_nan_when_the_synthetic_price_is_not_positive():
    from algo.series.straddle import implied_vol
    assert np.isnan(implied_vol(10.0, 0.0, 5)) and np.isnan(implied_vol(10.0, -5.0, 5)) and np.isnan(implied_vol(10.0, float("nan"), 5))
