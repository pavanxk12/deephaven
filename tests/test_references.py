"""The reference point is a choice: open, vwap, prev_close, N minutes ago, or a fixed time of the day, for absolute() and relative(), on prices and on IV."""
import math

import numpy as np
import pytest

from algo.series.absolute import AbsoluteService, Baskets, Clock, parse_ref, ref_label
from tests.test_absolute import DAYS, IST, NSE_SESSION, TODAY, MemorySource, epoch, flat, service
from tests.test_relative import Reader, arrays

CLOCK = Clock(IST, NSE_SESSION)                                   # session start 09:15, 375 slots


# ---- reading the reference ---------------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("ref, want", [("open", ("open", None)), ("VWAP", ("vwap", None)), (" prev_close ", ("prev_close", None)), ("10min", ("ago", 10)),
                                       ("5 min", ("ago", 5)), ("1m", ("ago", 1)), ("15 minutes", ("ago", 15)), (7, ("ago", 7)), ("09:15", ("at", 0)),
                                       ("09:30", ("at", 15)), ("9:45", ("at", 30)), ("15:29", ("at", 374))])
def test_the_reference_forms(ref, want):
    assert parse_ref(ref, CLOCK) == want


@pytest.mark.parametrize("ref", ["tomorrow", "10", "0min", "375min", "400min", "09:00", "15:30", "25:00", "", True, 0, -3, "-5min"])
def test_a_reference_that_makes_no_sense_is_refused_with_the_choices(ref):
    with pytest.raises(ValueError, match="ref"):
        parse_ref(ref, CLOCK)


def test_labels_read_back_as_written():
    assert [ref_label(parse_ref(r, CLOCK), CLOCK) for r in ("open", "vwap", "prev_close", "10min", "09:30")] == ["open", "vwap", "prev_close", "10min", "09:30"]
    assert ref_label(None, CLOCK) is None


# ---- absolute with a reference ----------------------------------------------------------------------------------------------------------------------
def three_bars(src, closes, name="ABC"):
    """Each day: open 100 at slot 0, c5 at slot 5, c10 at slot 10; the day's last bar is the slot-10 one."""
    for d, (c5, c10) in zip(DAYS, closes):
        src.add(d, name, {0: flat(100), 5: flat(c5), 10: flat(c10)})


CLOSES = [(101, 102), (102, 104), (100, 105), (104, 103), (105, 108)]


def test_minutes_ago_measures_from_the_price_that_many_minutes_before():
    src = MemorySource()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 11))                                              # the latest closed bar is minute 10
    r = s.absolute("pct", "intraday", "ABC", 5, ref="5min", now=0.0)
    assert r[0].tolist() == pytest.approx([(c10 - c5) / c5 * 100 for c5, c10 in CLOSES]) and r.ref == "5min" and r.minute == 10
    r = s.absolute("pct", "intraday", "ABC", 5, ref=10, now=0.0)                                # a bare number is minutes ago: back to the open's price at minute 0
    assert r[0].tolist() == pytest.approx([(c10 - 100) / 100 * 100 for _, c10 in CLOSES])


def test_before_the_session_has_a_price_that_many_minutes_back_the_value_is_nan():
    src = MemorySource()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 4))                                               # minute 3
    r = s.absolute("pct", "intraday", "ABC", 5, ref="5min", now=1.0)
    assert np.isnan(r[0]).all() and r[1] == -1                                                  # minute -2 does not exist


def test_a_fixed_time_is_the_price_at_that_minute_and_nan_until_it_has_come():
    src = MemorySource()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 11))
    r = s.absolute("pct", "intraday", "ABC", 5, ref="09:20", now=0.0)                           # 09:20 is minute 5
    assert r[0].tolist() == pytest.approx([(c10 - c5) / c5 * 100 for c5, c10 in CLOSES]) and r.ref == "09:20"
    early = service(src, now=lambda: epoch(TODAY, 4))                                           # at minute 3 it is not 09:20 yet
    assert np.isnan(early.absolute("pct", "intraday", "ABC", 5, ref="09:20", now=0.0)[0]).all()
    assert early.absolute("pct", "intraday", "ABC", 5, ref="09:15", now=0.0)[0].tolist() == pytest.approx([0.0] * 5)                  # 09:15 is minute 0, already past: the price carried since then


def test_prev_close_is_the_last_price_of_the_previous_trading_day():
    src = MemorySource()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 11))
    r = s.absolute("pct", "intraday", "ABC", 5, ref="prev_close", now=0.0)
    prev_last = [None] + [c10 for _, c10 in CLOSES[:-1]]
    want = [math.nan if p is None else (c10 - p) / p * 100 for p, (_, c10) in zip(prev_last, CLOSES)]
    assert np.allclose(r[0], want, equal_nan=True) and np.isnan(r[0][0]) and r.n_valid == 4        # the first saved day has no day before it
    assert r.today == TODAY and r.dates == DAYS


def test_the_open_and_the_vwap_are_the_same_as_the_older_series_names():
    src = MemorySource()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 11))
    for old, ref in (("pct_open", "open"), ("pct_vwap", "vwap")):
        a, b = s.absolute(old, "intraday", "ABC", 5, now=0.0), s.absolute("pct", "intraday", "ABC", 5, ref=ref, now=0.0)
        assert a[0].tolist() == b[0].tolist() and a.ref == b.ref == ref
    assert s.absolute("pct", "intraday", "ABC", 5, now=0.0).ref == "open"                       # the default reference
    assert s.absolute("pct_open", "intraday", "ABC", 5, ref="open", now=0.0).ref == "open"
    with pytest.raises(ValueError, match="measures from the open"):
        s.absolute("pct_open", "intraday", "ABC", 5, ref="10min")


def test_today_is_measured_from_the_same_reference_as_the_past_days():
    src = MemorySource()
    three_bars(src, CLOSES)
    today = lambda spec, day: arrays((0, 100, 100, 100, 100, 5), (5, 110, 110, 110, 110, 5), (10, 121, 121, 121, 121, 5))   # noqa: E731
    s = service(src, now=lambda: epoch(TODAY, 11), today_bars=today)
    r = s.absolute("pct", "intraday", "ABC", 5, ref="5min")
    assert r.now == pytest.approx((121 - 110) / 110 * 100) and r[1] == 5                         # 10% is above every past day's change
    assert s.absolute("pct", "intraday", "ABC", 5, ref="prev_close").now == pytest.approx((121 - 108) / 108 * 100)      # yesterday (the last saved day) closed at 108


def test_a_weekly_level_with_a_minutes_ago_reference_compares_the_same_weekday_and_minute():
    src = MemorySource()
    for d, c in (("2026-03-04", 103), ("2026-03-11", 110)):
        src.add(d, "ABC", {0: flat(100), 5: flat(100), 10: flat(c)})
    for d in ("2026-03-05", "2026-03-10"):
        src.add(d, "ABC", {0: flat(100), 5: flat(100), 10: flat(500)})                          # other weekdays: not compared
    s = service(src, now=lambda: epoch("2026-03-18", 11))                                       # a Wednesday
    r = s.absolute("pct", "intraweek", "ABC", 2, ref="5min", now=0.0)
    assert r.dates == ["2026-03-04", "2026-03-11"] and r[0].tolist() == pytest.approx([3.0, 10.0])


def test_the_history_a_reference_needs_is_known_in_advance_so_warming_reads_it_once():
    class Counting(MemorySource):
        def __init__(self):
            super().__init__()
            self.many, self.single = [], 0

        def bars(self, day, spec):
            self.single += 1
            return super().bars(day, spec)

        def bars_many(self, day, specs):
            self.many.append(day)
            return {sp: super(Counting, self).bars(day, sp) for sp in specs if super(Counting, self).bars(day, sp) is not None}

    src = Counting()
    three_bars(src, CLOSES)
    s = service(src, now=lambda: epoch(TODAY, 11))
    assert s.warm("pct", "intraday", ["ABC"], 3, ref="prev_close") == 1
    assert src.many == ["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13"]                 # the 3 days compared and the day before the first of them
    s.absolute("pct", "intraday", "ABC", 3, ref="prev_close", now=0.0)
    assert src.single == 0
    src.many.clear()
    s2 = service(src, now=lambda: epoch(TODAY, 11))
    s2.warm("pct", "intraday", ["ABC"], 3, ref="5min")
    assert src.many == ["2026-03-11", "2026-03-12", "2026-03-13"]                               # minutes ago needs only the days compared


# ---- iv with a reference -----------------------------------------------------------------------------------------------------------------------------------
def iv_history(src):
    for i, d in enumerate(DAYS):
        src.add_iv(d, "NIFTY", {0: 2.0 + i, 5: 3.0 + i, 10: 4.0 + 2 * i})
        src.add(d, "NIFTY", {0: flat(100)})


def test_the_iv_itself_or_its_change_from_a_reference():
    src = MemorySource()
    iv_history(src)
    s = service(src, now=lambda: epoch(TODAY, 11), iv_max_age=0)
    assert s.absolute("iv", "intraday", "NIFTY", 5, now=1.0)[0].tolist() == [4.0, 6.0, 8.0, 10.0, 12.0] and s.absolute("iv", "intraday", "NIFTY", 5, now=1.0).ref is None
    opened = s.absolute("iv", "intraday", "NIFTY", 5, ref="open", now=0.0)                      # (iv at minute 10 - the day's first iv) / the first iv
    assert opened[0].tolist() == pytest.approx([(4.0 + 2 * i - (2.0 + i)) / (2.0 + i) * 100 for i in range(5)])
    ago = s.absolute("iv", "intraday", "NIFTY", 5, ref="5min", now=0.0)
    assert ago[0].tolist() == pytest.approx([(4.0 + 2 * i - (3.0 + i)) / (3.0 + i) * 100 for i in range(5)])
    prev = s.absolute("iv", "intraday", "NIFTY", 5, ref="prev_close", now=0.0)
    assert np.isnan(prev[0][0]) and prev[0][1] == pytest.approx((6.0 - 4.0) / 4.0 * 100)        # yesterday's last iv was 4.0 + 2 * 0
    with pytest.raises(ValueError, match="no VWAP"):
        s.absolute("iv", "intraday", "NIFTY", 5, ref="vwap")


# ---- relative with a reference ------------------------------------------------------------------------------------------------------------------------------
def test_relative_measures_every_symbol_from_the_same_reference():
    reader = Reader({n: arrays((0, 100, 100, 100, 100, 5), (5, c5, c5, c5, c5, 5), (10, c10, c10, c10, c10, 5))
                     for n, c5, c10 in (("S", 100, 110), ("A", 100, 105), ("B", 200, 190), ("C", 50, 60))})
    s = AbsoluteService(CLOCK, MemorySource(), now=lambda: epoch(TODAY, 11), today_bars_many=reader, baskets=Baskets())
    r = s.relative("pct", "intraday", "S", ["A", "B", "C"], ref="5min")
    assert r[0].tolist() == pytest.approx([5.0, -5.0, 20.0]) and r.now == pytest.approx(10.0) and r[1] == 2 and r.ref == "5min"
    r = s.relative("pct", "intraday", "S", ["A", "B", "C"], ref="09:20")                        # the same here: minute 5 is 09:20
    assert r[0].tolist() == pytest.approx([5.0, -5.0, 20.0])
    r = s.relative("pct", "intraday", "S", ["A", "B", "C"])                                     # the default is the open (every symbol opened at 100 here)
    assert r[0].tolist() == pytest.approx([5.0, 90.0, -40.0]) and r.now == pytest.approx(10.0) and r[1] == 2 and r.ref == "open"


def test_relative_prev_close_reads_the_previous_day_once_for_the_whole_basket():
    class Counting(MemorySource):
        def __init__(self):
            super().__init__()
            self.many = []

        def bars_many(self, day, specs):
            self.many.append((day, len(specs)))
            return {sp: super(Counting, self).bars(day, sp) for sp in specs if super(Counting, self).bars(day, sp) is not None}

    src = Counting()
    for name, last in (("S", 100), ("A", 100), ("B", 200)):
        src.add("2026-03-13", name, {0: flat(last), 10: flat(last)})
    reader = Reader({"S": arrays((10, 110, 110, 110, 110, 5)), "A": arrays((10, 105, 105, 105, 105, 5)), "B": arrays((10, 190, 190, 190, 190, 5))})
    s = AbsoluteService(CLOCK, src, now=lambda: epoch(TODAY, 11), today_bars_many=reader, baskets=Baskets())
    r = s.relative("pct", "intraday", "S", ["A", "B"], ref="prev_close")
    assert src.many == [("2026-03-13", 3)]                                                      # ONE read of the previous day for the symbol and the two others
    assert r[0].tolist() == pytest.approx([5.0, -5.0]) and r.now == pytest.approx(10.0) and r[1] == 2


def test_relative_iv_change_from_a_reference():
    class IvReader:
        def __call__(self, unders, day):
            return {u: (np.array([555, 555 + 5, 555 + 10]), np.array(v)) for u, v in {"NIFTY": [2.0, 2.0, 3.0], "BANKNIFTY": [4.0, 4.0, 5.0], "FINNIFTY": [1.0, 1.0, 0.5]}.items() if u in unders}

    s = AbsoluteService(CLOCK, MemorySource(), now=lambda: epoch(TODAY, 11), today_iv_many=IvReader(), baskets=Baskets(), iv_max_age=0)
    r = s.relative("iv", "intraday", "NIFTY", ["BANKNIFTY", "FINNIFTY"], ref="5min")
    assert r.now == pytest.approx(50.0) and r[0].tolist() == pytest.approx([25.0, -50.0]) and r[1] == 2
    assert s.relative("iv", "intraday", "NIFTY", ["BANKNIFTY", "FINNIFTY"])[0].tolist() == [5.0, 0.5]       # no reference: the IV itself


def test_a_bad_reference_is_refused_by_every_entry_point():
    s = service(MemorySource(), now=lambda: epoch(TODAY, 11))
    for call in (lambda: s.absolute("pct", "intraday", "A", 5, ref="soon"), lambda: s.relative("pct", "intraday", "A", ["B"], ref="soon"),
                 lambda: s.warm("pct", "intraday", ["A"], 5, ref="soon")):
        with pytest.raises(ValueError, match="not understood"):
            call()
