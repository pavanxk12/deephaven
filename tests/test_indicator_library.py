"""SMA / EMA / RSI / MACD / ATR: names, step order and the query they build, against a recording stand-in for
Deephaven's update_by (the real arithmetic runs in the engine). Every indicator makes exactly one column."""
import math
import sys
import types

import pytest

from algo.data import indicators as ind
from algo.data.indicators import (PCT, ATR, EMA, MACD, MACD_HIST, MACD_SIGNAL, RSI, SMA, build_indicator_table,
                                  decay_ticks)


class Rec:
    """Records the query calls made on a table."""

    def __init__(self, calls=None):
        self.calls = [] if calls is None else calls

    def _next(self, *call):
        self.calls.append(call)
        return Rec(self.calls)

    def update_by(self, ops, by=None):
        return self._next("update_by", [o[0] + ":" + ",".join(o[1]) if isinstance(o, tuple) else o for o in ops], by)

    def update(self, exprs):
        return self._next("update", [exprs] if isinstance(exprs, str) else list(exprs))

    def update_view(self, exprs):
        return self._next("update_view", exprs)

    def view(self, cols):
        return self._next("view", list(cols))

    def agg_by(self, aggs, by=None):
        return self._next("agg_by", aggs, by)

    def natural_join(self, other, on=None, joins=None):
        return self._next("natural_join", on, joins)

    def aj(self, other, on=None, joins=None):
        return self._next("aj", on, joins)


@pytest.fixture
def fake_uby(monkeypatch):
    uby = types.ModuleType("deephaven.updateby")
    uby.cum_sum = lambda cols: ("cum_sum", cols)
    uby.delta = lambda cols: ("delta", cols)
    uby.forward_fill = lambda cols: ("forward_fill", cols)
    uby.ema_tick = lambda decay, cols: ("ema_tick", cols)
    uby.rolling_avg_tick = lambda cols, rev_ticks: ("rolling_avg_tick:%d" % rev_ticks, cols)
    agg = types.ModuleType("deephaven.agg")
    for name in ("first", "last", "max_", "min_"):
        setattr(agg, name, lambda col, _n=name: (_n, col))
    dh = types.ModuleType("deephaven")
    dh.updateby, dh.agg = uby, agg
    monkeypatch.setitem(sys.modules, "deephaven", dh)
    monkeypatch.setitem(sys.modules, "deephaven.updateby", uby)
    monkeypatch.setitem(sys.modules, "deephaven.agg", agg)


# ---- names --------------------------------------------------------------------------------------------
def test_one_column_per_indicator_with_the_bar_size_in_the_name():
    made = [SMA(20), EMA(9), RSI(14), ATR(), MACD(), MACD_SIGNAL(), MACD_HIST()]
    assert [i.column for i in made] == ["SMA_20", "EMA_9", "RSI_14", "ATR_14", "MACD_12_26",
                                        "MACD_SIGNAL_12_26_9", "MACD_HIST_12_26_9"]
    assert all(isinstance(i.final(), str) and i.final().startswith(i.column + " =") for i in made)
    assert SMA(20, "5min").column == "SMA_20_5min" and ATR(14, "15 MIN").column == "ATR_14_15min"
    assert MACD_SIGNAL(5, 10, 3, "5min").column == "MACD_SIGNAL_5_10_3_5min"


@pytest.mark.parametrize("make", [lambda: SMA(0), lambda: EMA(0), lambda: RSI(1), lambda: ATR(0), lambda: MACD(26, 12),
                                  lambda: MACD_SIGNAL(12, 26, 0), lambda: SMA(2.5), lambda: EMA(True), lambda: MACD(0, 5),
                                  lambda: MACD_HIST(12, 26, 9, "7x")])
def test_bad_arguments_are_refused(make):
    with pytest.raises(ValueError):
        make()


def test_library_exposes_every_indicator_as_a_function():
    assert list(ind.LIBRARY) == ["SMA", "EMA", "RSI", "MACD", "MACD_SIGNAL", "MACD_HIST", "ATR", "PCT"]


def test_decay_ticks_reproduces_the_smoothing_factor():
    for alpha in (0.5, 0.2, 2 / 27, 1 / 14):
        assert 1 - math.exp(-1 / decay_ticks(alpha)) == pytest.approx(alpha)


# ---- steps ------------------------------------------------------------------------------------------
def test_step_shapes(fake_uby):
    assert [k for k, _ in SMA(5).steps()] == ["ops"]
    assert [k for k, _ in EMA(5).steps()] == ["ops"]
    assert [k for k, _ in ATR().steps()] == ["update", "ops"]
    assert [k for k, _ in MACD().steps()] == ["ops"]                       # the line needs no smoothing of its own
    assert [k for k, _ in MACD_SIGNAL().steps()] == ["ops", "update", "ops"]
    assert SMA(5).steps()[0][1][0] == ("rolling_avg_tick:5", ["SMA_5_raw = Px"])


def test_atr_true_range_uses_previous_close(fake_uby):
    (_, (tr,)), _ = ATR(14).steps()
    assert tr.startswith("ATR_14_tr = isNull(D) ? Hi - Lo :") and "Px - D" in tr and "Math.abs" in tr


def test_macd_lines_and_their_warm_up(fake_uby):
    assert "Cnt < 26" in MACD().final() and "_f - " in MACD().final()
    sig, hist = MACD_SIGNAL(3, 6, 2), MACD_HIST(3, 6, 2)
    assert "Cnt < 7" in sig.final() and "Cnt < 7" in hist.final() and "_m - " in hist.final()
    assert sig.steps()[1][1] == ["MACD_SIGNAL_3_6_2_m = MACD_SIGNAL_3_6_2_f - MACD_SIGNAL_3_6_2_s"]
    assert sig.steps()[2][1][0] == ("ema_tick", ["MACD_SIGNAL_3_6_2_g = MACD_SIGNAL_3_6_2_m"])


def test_helper_columns_do_not_clash_between_lines_or_bar_sizes(fake_uby):
    a, b, c = MACD_SIGNAL(3, 6, 2), MACD_HIST(3, 6, 2), MACD_SIGNAL(3, 6, 2, "5min")
    names = [h for i in (a, b, c) for _, p in i.steps() for h in map(str, p)]
    assert "MACD_HIST_3_6_2_g" in " ".join(names) and "MACD_SIGNAL_3_6_2_5min_g" in " ".join(names)
    assert len({i.column for i in (a, b, c)}) == 3


# ---- the query ---------------------------------------------------------------------------------------
def run(indicators, candle=60, default=60):
    rec = Rec()
    build_indicator_table(rec, candle, default, indicators)
    return rec.calls


def test_steps_run_in_order_across_indicators(fake_uby):
    calls = run([MACD_SIGNAL(3, 6, 2), ATR(5), EMA(4)])
    texts = [str(c[1]) for c in calls]
    pos = lambda needle: next(i for i, t in enumerate(texts) if needle in t)          # noqa: E731
    assert pos("ATR_5_tr =") < pos("ATR_5_atr")
    assert pos("MACD_SIGNAL_3_6_2_f") < pos("MACD_SIGNAL_3_6_2_m =") < pos("MACD_SIGNAL_3_6_2_g")
    assert pos("EMA_4_raw") < pos("MACD_SIGNAL_3_6_2_m =")                 # independent smoothings share the first pass
    assert "delta" in str(calls[1])                                         # ATR asked for the price change
    assert "Gain" not in " ".join(texts)                                    # no RSI -> no gain / loss columns


def test_output_columns_are_one_per_indicator_in_order(fake_uby):
    view = run([SMA(3), MACD(3, 6), MACD_HIST(3, 6, 2)])[-1]
    assert view[0] == "view" and view[1][:3] == ["Minute", "Symbol", "token"]
    assert [c.split(" =")[0] for c in view[1][3:]] == ["SMA_3", "MACD_3_6", "MACD_HIST_3_6_2"]       # no hidden extra column


def test_rsi_alone_adds_gain_and_loss(fake_uby):
    assert "Gain" in str(run([RSI(5)])) and "Loss" in str(run([RSI(5)]))
    assert "Gain" not in str(run([SMA(5), EMA(5), MACD(), ATR()]))


def test_high_and_low_come_along_when_candles_are_rolled_up(fake_uby):
    calls = run([ATR(5, "5min")], candle=60, default=60)
    agg = next(c for c in calls if c[0] == "agg_by")
    assert sorted(a[0] for a in agg[1]) == ["first", "last", "max_", "min_"]
    assert "High" in str(calls[0]) and "Low" in str(calls[0])


def test_mixed_bar_sizes_join_the_coarser_group_on_the_bar_start(fake_uby):
    calls = run([EMA(9), EMA(9, "5min"), RSI(5, "15min")])
    joins = [c for c in calls if c[0] == "natural_join"]
    assert [j[1] for j in joins] == [["token", "Bin_300 = Minute"], ["token", "Bin_900 = Minute"]]
    assert joins[0][2] == ["EMA_9_5min"] and joins[1][2] == ["RSI_5_15min"]
    assert calls[-1][1] == ["Minute", "Symbol", "token", "EMA_9", "EMA_9_5min", "RSI_5_15min"]


def test_a_table_does_not_carry_columns_no_indicator_uses(fake_uby):
    """EMA needs PxSum, RSI needs D / Gain / Loss, ATR needs Hi / Lo: a table without them must not carry them."""
    sma = str(run([SMA(5)]))
    assert "PxSum" not in sma and "Gain" not in sma and "Hi =" not in sma and "delta" not in sma
    assert "PxSum" in str(run([EMA(5)])) and "Hi =" not in str(run([EMA(5)]))
    rsi = str(run([RSI(5)]))
    assert "Gain" in rsi and "delta" in rsi and "PxSum" not in rsi and "Hi =" not in rsi
    atr = str(run([ATR(5)]))
    assert "Hi = High" in atr and "delta" in atr and "Gain" not in atr and "PxSum" not in atr


def test_plain_formulas_are_lazy_so_they_take_no_memory_per_row(fake_uby):
    calls = run([RSI(5), ATR(5), MACD_SIGNAL(3, 6, 2)])
    assert "update" not in {c[0] for c in calls}                    # no stored update(): only update_view and update_by
    assert {"update_view", "update_by"} <= {c[0] for c in calls}


def test_pct_is_built_from_the_right_pieces(fake_uby):
    open_ = str(run([PCT()]))
    assert "OpenFirst_PCT_OPEN" in open_ and "DayOpen_PCT_OPEN" in open_ and "aj" not in {c[0] for c in run([PCT()])}
    timed = run([PCT("5min")])
    assert "aj" in {c[0] for c in timed} and "Lag_PCT_5min = minus(Minute, 300 * SECOND)" in str(timed)
    bars = run([PCT(5)])
    assert "natural_join" in {c[0] for c in bars} and "CntAgo_PCT_5 = Cnt - 5" in str(bars)
    assert "PxSum" not in str(timed) and "Gain" not in str(timed) and "delta" not in str(timed)       # nothing it does not use


def test_pct_names_and_arguments():
    assert [p.column for p in (PCT(), PCT("5min"), PCT("90s"), PCT("2h"), PCT(5), PCT("10 MIN", timeframe="5min"))] == \
        ["PCT_OPEN", "PCT_5min", "PCT_90s", "PCT_120min", "PCT_5", "PCT_10min_5min"]
    for bad in (0, -3, True, 2.5, "5", "five minutes", "0s", "7h", ["5min"]):
        with pytest.raises(ValueError):
            PCT(bad)
