"""Bad numbers, bad rows and races must never get through or take a listener down."""
import asyncio
import threading

import pytest

from algo.core.nulls import NULL_DOUBLE, NULL_LONG, as_float, good_price, good_volume, is_null
from algo.data.candles import CandleAggregator, Candles, LttParser
from algo.series.straddle import StraddleConfig, StraddleEngine
from algo.strategy.book import Portfolios
from algo.strategy.bridge import Bridge
from algo.strategy.config import BridgeConfig, RsiOptionsConfig
from algo.strategy.picker import OptionPicker
from algo.strategy.sizing import on_tick, option_params
from tests.test_bridge import make
from zoneinfo import ZoneInfo


# ---- what counts as a number ------------------------------------------------------------------------
@pytest.mark.parametrize("x", [None, float("nan"), float("inf"), float("-inf"), NULL_DOUBLE, NULL_LONG, "abc", "", [], object()])
def test_everything_that_is_not_a_number_is_null(x):
    assert is_null(x) and not good_price(x) and not good_volume(x) and as_float(x) is None


@pytest.mark.parametrize("x", [0, 1, 245.67, 1e9, "12.5"])
def test_real_numbers_pass(x):
    assert not is_null(x) and as_float(x) == float(x)


def test_price_must_be_above_zero_volume_may_be_zero():
    assert not good_price(0) and not good_price(-1) and good_price(0.05)
    assert good_volume(0) and not good_volume(-1)


# ---- candles: no feed row can raise or be stored wrongly ------------------------------------------
def candles_plugin(multiplier=None, **kw):
    c = Candles(multiplier=multiplier, build_from=0, **kw)
    rows = []
    c.scale = multiplier if multiplier is not None else 0.01
    c._parse = LttParser("%d-%m-%Y %H:%M", ZoneInfo("Asia/Kolkata"))
    agg = CandleAggregator(lambda *r: rows.append(r), 0, 60)
    return c, agg, rows


GOOD = "14-10-2030 10:15:05"


@pytest.mark.parametrize("tok,ltp,vol,lt", [
    (1, float("nan"), 5, GOOD), (1, float("inf"), 5, GOOD), (1, None, 5, GOOD), (1, "abc", 5, GOOD),
    (1, 0, 5, GOOD), (1, -5, 5, GOOD), (1, NULL_DOUBLE, 5, GOOD), (1, 100, float("nan"), GOOD), (1, 100, -1, GOOD),
    (1, 100, None, GOOD), (1, 100, "x", GOOD), (1, 100, NULL_LONG, GOOD), (1, 100, 5, None), (1, 100, 5, ""),
    (1, 100, 5, "garbage"), (1, 100, 5, 12345), (None, 100, 5, GOOD), ("x", 100, 5, GOOD), (float("nan"), 100, 5, GOOD),
])
def test_a_bad_feed_row_is_dropped_and_never_raises(tok, ltp, vol, lt):
    c, agg, rows = candles_plugin()
    assert c._tick(agg, tok, ltp, vol, lt) is None
    agg.flush(10 ** 12)
    assert rows == []


def test_a_good_row_passes_and_a_repeat_of_the_same_trade_does_not():
    c, agg, rows = candles_plugin()
    assert c._tick(agg, 1, 24567, 5, GOOD) is not None
    assert c._tick(agg, 1, 24567, 5, GOOD) is None                     # same trade again
    agg.flush(10 ** 12)
    assert len(rows) == 1


def test_the_multiplier_gives_proper_rates_without_float_noise():
    c, agg, rows = candles_plugin(multiplier=0.01)
    c._tick(agg, 1, 24567, 5, GOOD)
    c._tick(agg, 1, 24570, 6, "14-10-2030 10:15:06")
    agg.flush(10 ** 12)
    (token, minute, o, h, l, close, vol) = rows[0]
    assert (o, h, l, close) == (245.67, 245.7, 245.67, 245.7)          # not 245.67000000000002
    assert repr(o) == "245.67"


def test_multiplier_that_would_round_a_price_to_zero_drops_the_tick():
    c, agg, rows = candles_plugin(multiplier=1e-9)
    c.decimals = 6
    assert c._tick(agg, 1, 5, 5, GOOD) is None


@pytest.mark.parametrize("bad", [0, -0.01, float("nan"), float("inf"), "0.01", [], True])
def test_a_bad_multiplier_is_refused_up_front(bad):
    with pytest.raises(ValueError):
        Candles(multiplier=bad)


@pytest.mark.parametrize("bad", [-1, 13, 2.5, True, "6"])
def test_a_bad_decimals_is_refused(bad):
    with pytest.raises(ValueError):
        Candles(decimals=bad)


def test_a_failing_row_is_counted_not_raised():
    c, agg, rows = candles_plugin()
    c._parse = lambda s: (_ for _ in ()).throw(RuntimeError("boom"))
    assert c._tick(agg, 1, 100, 5, GOOD) is None and c.skipped == 1


# ---- division by zero and truncation in the order sizing ------------------------------------------
@pytest.mark.parametrize("premium,tick", [(100, 0), (100, float("nan")), (100, -0.05), (100, 0.001), (0, 0.05),
                                          (-5, 0.05), (float("nan"), 0.05), (None, 0.05), (float("inf"), 0.05)])
def test_unusable_premium_or_tick_gives_no_order(premium, tick):
    params, why = option_params(premium, tick, RsiOptionsConfig.stocks())
    assert params is None and why


def test_on_tick_never_divides_by_zero():
    assert on_tick(1.0, 0.0) is False and on_tick(1.0, 0.004) is False and on_tick(1.05, 0.05) is True


def test_sizing_values_sit_on_the_tick_after_the_bridges_truncation():
    cfg = RsiOptionsConfig.stocks()
    for premium in (0.35, 7.45, 99.95, 1234.56, 24999.9):
        p, why = option_params(premium, 0.05, cfg)
        assert p is not None, why
        for k in ("ival", "jval", "sval", "trp", "lowerrange", "upperrange"):
            assert int(p[k] * 100) % 5 == 0


# ---- picker: a NaN price must not silently pick strike 0 ---------------------------------------------
@pytest.mark.parametrize("price", [float("nan"), 0, -1, None, float("inf")])
def test_picker_refuses_an_unusable_future_price(master, price):
    import pandas as pd
    p = OptionPicker(master, RsiOptionsConfig.stocks(), lambda t: None)
    tok, why = p.pick("NIFTY", pd.Timestamp("2030-01-30"), price, "CALL", pd.Timestamp("2030-01-01"))
    assert tok is None and "usable" in why


def test_synthetic_ignores_a_nonsense_option_price(master):
    import pandas as pd
    ce, pe = 102, 103
    p = OptionPicker(master, RsiOptionsConfig.nifty(), {ce: float("nan"), pe: 100.0}.get)
    tok, why = p.pick("NIFTY", pd.Timestamp("2030-01-30"), 25000.0, "CALL", pd.Timestamp("2030-01-01"))
    assert tok is not None and "no CE/PE price" in why                 # fell back to the future close


# ---- straddle ------------------------------------------------------------------------------------------
@pytest.mark.parametrize("price", [float("nan"), float("inf"), 0, -1, None, NULL_DOUBLE, "x"])
def test_straddle_ignores_bad_prices(price):
    e = StraddleEngine({}, StraddleConfig())
    e.set_price(7, price, 1.0)
    assert 7 not in e.px
    e.set_price(7, 12.5, 1.0)
    assert e.px[7] == (12.5, 1.0)


# ---- threads --------------------------------------------------------------------------------------------
def test_portfolio_numbers_stay_unique_when_many_threads_create_at_once(tmp_path):
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    tokens = list(range(1, 201))
    book.track(tokens)
    errors = []

    def work(chunk):
        try:
            for t in chunk:
                book.create(t)
        except Exception as e:                                           # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=work, args=(tokens[i::8],)) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    pfs = sorted(book.pf_map.values())
    assert not errors and pfs == list(range(1000, 1200))


def test_state_files_are_never_left_half_written(tmp_path):
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1)
    book.track([1])
    book.create(1)
    assert not list(tmp_path.glob("*.tmp"))
    assert Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json")).pf_map == {1: 1}


def test_a_listener_that_fails_cannot_break_the_book(tmp_path):
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1)
    seen = []
    book.on_change(lambda: 1 / 0)
    book.on_change(lambda: seen.append(1))
    book.track([1])
    assert seen == [1]


def test_submit_on_a_closed_loop_returns_false_instead_of_raising(tmp_path):
    b, book, fake = make(tmp_path)
    dead = asyncio.new_event_loop()
    dead.close()
    real, b.loop = b.loop, dead

    async def noop():
        return 1
    assert b.submit(noop()) is False
    b.loop = real
    b.on_stop()


def test_bridge_stop_twice_is_harmless(tmp_path):
    b, _, _ = make(tmp_path)
    b.on_stop()
    b.on_stop()
    assert b.loop is None and b.submit(asyncio.sleep(0)) is False


def test_dry_run_bridge_has_no_loop_and_never_sends():
    b = Bridge(BridgeConfig(dry_run=True))
    assert b.loop is None and b.transport is None
