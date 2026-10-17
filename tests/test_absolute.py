"""absolute(series, level, symbol, n): the value now against the same point in time of the previous n periods (algo/series/absolute.py)."""
import math
import threading
from datetime import date, datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from algo.series import absolute as ab
from algo.series.absolute import AbsoluteService, Clock, build_day, carry, parse_symbol

IST = ZoneInfo("Asia/Kolkata")
NY = ZoneInfo("America/New_York")
NSE_SESSION = (dtime(9, 15), dtime(15, 30))
US_SESSION = (dtime(9, 30), dtime(16, 0))
nan = float("nan")


def epoch(day: str, slot: int, zone=IST, session=NSE_SESSION) -> int:
    """UTC epoch seconds of the bar that starts `slot` minutes after the session opens on `day`."""
    h, m = divmod(session[0].hour * 60 + session[0].minute + slot, 60)
    d = date.fromisoformat(day)
    return int(datetime(d.year, d.month, d.day, h, m, tzinfo=zone).timestamp())


class MemorySource:
    """History in memory: bars[(kind, name, day)] = {slot: (o, h, l, c, v)}; iv[(und, day)] = {slot: iv}. Counts what was asked."""

    def __init__(self, zone=IST, session=NSE_SESSION):
        self.zone, self.session = zone, session
        self.bar_data, self.iv_data, self.calls = {}, {}, []
        self._lock = threading.Lock()

    def add(self, day, name, bars, kind="cash"):
        self.bar_data[(kind, name, day)] = bars

    def add_iv(self, day, und, ivs):
        self.iv_data[(und, day)] = ivs

    def days(self, kind):
        keys = {d for (_, _, d) in self.bar_data} if kind == "candles" else {d for (_, d) in self.iv_data}
        return sorted(keys)

    def bars(self, day, spec):
        with self._lock:
            self.calls.append((day, spec))
        data = self.bar_data.get((spec[0], spec[1], day))
        if not data:
            return None
        slots = sorted(data)
        cols = list(zip(*(data[s] for s in slots)))
        return (np.array([epoch(day, s, self.zone, self.session) for s in slots], dtype=np.int64), *(np.array(c, dtype=np.float64) for c in cols))

    def iv(self, day, und):
        data = self.iv_data.get((und, day))
        if not data:
            return None
        slots = sorted(data)
        start = self.session[0].hour * 60 + self.session[0].minute
        return np.array([start + s for s in slots], dtype=np.int64), np.array([data[s] for s in slots])


def px(o, c=None, h=None, l=None, v=100):
    c = o if c is None else c
    return (o, c if h is None else h, c if l is None else l, c, v)


def flat(price, v=100):
    return (price, price, price, price, v)


def service(src, clock=None, **kw):
    clock = clock or Clock(src.zone, src.session)
    return AbsoluteService(clock, src, **kw)


DAYS = ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13"]
TODAY = "2026-03-16"


def five_days(src, name="ABC", closes=(101, 102, 103, 104, 105), at=10):
    """Each day opens at 100 (slot 0) and closes at the given price at slot `at`."""
    for d, c in zip(DAYS, closes):
        src.add(d, name, {0: flat(100), at: flat(c)})


# ---- the clock: session minutes, not UTC ----------------------------------------------------------------------------------------------
def test_the_us_clock_change_does_not_move_the_session_minute():
    clock = Clock(NY, US_SESSION)
    winter = epoch("2026-03-06", 0, NY, US_SESSION)                       # EST: 09:30 is 14:30 UTC
    summer = epoch("2026-03-09", 0, NY, US_SESSION)                       # the Monday after the clock change: EDT, 09:30 is 13:30 UTC
    assert datetime.utcfromtimestamp(winter).strftime("%H:%M") == "14:30" and datetime.utcfromtimestamp(summer).strftime("%H:%M") == "13:30"
    _, slot = clock.slots_of([winter, summer, winter + 60 * 30, summer + 60 * 389, summer + 60 * 390])
    assert slot.tolist() == [0, 0, 30, 389, 390] and clock.slots == 390          # 390 is already outside the session


def test_the_india_clock_has_375_slots_and_no_shift():
    clock = Clock(IST, NSE_SESSION)
    _, slot = clock.slots_of([epoch("2026-01-05", 0), epoch("2026-07-06", 374)])
    assert slot.tolist() == [0, 374] and clock.slots == 375


# ---- one day ----------------------------------------------------------------------------------------------------------------------------
def test_a_price_is_carried_forward_only_for_max_age_minutes():
    raw = np.array([nan, 10.0, nan, nan, nan, nan, 20.0, nan])
    assert np.allclose(carry(raw, 2), [nan, 10, 10, 10, nan, nan, 20, 20], equal_nan=True)


def test_a_day_holds_the_close_the_running_vwap_sums_and_the_open():
    d = build_day([0, 1, 5], [10, 11, 15], [12, 13, 16], [9, 10, 14], [11, 12, 15], [100, 200, 300], 8, 2)
    assert d.open == 10 and np.allclose(d.close, [11, 12, 12, 12, nan, 15, 15, 15], equal_nan=True)
    typ = [(12 + 9 + 11) / 3, (13 + 10 + 12) / 3, (16 + 14 + 15) / 3]
    assert np.allclose(d.cum_v, [100, 300, 300, 300, 300, 600, 600, 600]) and np.isclose(d.pv, 100 * typ[0] + 200 * typ[1] + 300 * typ[2], rtol=1e-6)
    assert build_day([-3, 999], [1, 1], [1, 1], [1, 1], [1, 1], [1, 1], 8, 2) is None             # nothing inside the session


# ---- intraday ---------------------------------------------------------------------------------------------------------------------------
def test_intraday_pct_open_returns_the_past_values_oldest_first_and_where_now_stands():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 11))
    hist, idx = s.absolute("pct_open", "intraday", "abc", 5, now=2.5)                         # (lower case is fine)
    assert hist.tolist() == [1.0, 2.0, 3.0, 4.0, 5.0] and idx == 2                         # 1 and 2 are below 2.5
    r = s.absolute("pct_open", "intraday", "ABC", 3, now=2.5)
    assert r[0].tolist() == [3.0, 4.0, 5.0] and r[1] == 0 and r.n_valid == 3 and r.pctile == 0.0 and r.minute == 10
    assert r.dates == ["2026-03-11", "2026-03-12", "2026-03-13"] and r.today == TODAY
    assert s.absolute("pct_open", "intraday", "ABC", 5, now=9.0)[1] == 5 and s.absolute("pct_open", "intraday", "ABC", 5, now=-1.0)[1] == 0


def test_pct_vwap_uses_the_volume_weighted_price_since_the_open():
    src = MemorySource()
    for d, (p1, v1, p2, v2) in zip(DAYS[:3], [(100, 100, 110, 300), (100, 100, 100, 100), (100, 400, 120, 100)]):
        src.add(d, "ABC", {0: flat(p1, v1), 10: flat(p2, v2)})
    s = service(src, now=lambda: epoch(TODAY, 11))
    hist, _ = s.absolute("pct_vwap", "intraday", "ABC", 3, now=0.0)
    vwap0 = (100 * 100 + 110 * 300) / 400
    assert hist[0] == pytest.approx((110 / vwap0 - 1) * 100, rel=1e-5) and hist[1] == 0.0
    assert hist[2] == pytest.approx((120 / ((100 * 400 + 120 * 100) / 500) - 1) * 100, rel=1e-5)


def test_a_day_with_no_bars_or_no_price_at_that_minute_is_nan_and_too_few_valid_days_give_no_position():
    src = MemorySource()
    five_days(src)
    del src.bar_data[("cash", "ABC", "2026-03-11")]
    src.add("2026-03-11", "OTHER", {0: flat(1)})                                                  # that day exists, but not for ABC
    src.bar_data[("cash", "ABC", "2026-03-12")] = {0: flat(100)}                                  # a day whose only bar is at slot 0: nothing near minute 10
    s = service(src, now=lambda: epoch(TODAY, 11))
    r = s.absolute("pct_open", "intraday", "ABC", 5, now=2.0)
    assert np.isnan(r[0]).tolist() == [False, False, True, True, False] and r.n_valid == 3 and r[1] == 1
    s2 = service(src, now=lambda: epoch(TODAY, 11), min_valid=4)
    assert s2.absolute("pct_open", "intraday", "ABC", 5, now=2.0)[1] == -1                       # 3 valid days are fewer than the 4 asked for
    assert s.absolute("pct_open", "intraday", "ABC", 8, now=2.0).n_valid == 3 and np.isnan(s.absolute("pct_open", "intraday", "ABC", 8, now=2.0)[0][:3]).all()


def test_the_days_before_today_only_and_fewer_days_than_n_are_padded_with_nan():
    src = MemorySource()
    five_days(src)
    src.add(TODAY, "ABC", {0: flat(100), 10: flat(150)})                                          # today's own file is already saved: it is not its own history
    s = service(src, now=lambda: epoch(TODAY, 11))
    hist, _ = s.absolute("pct_open", "intraday", "ABC", 7, now=0.0)
    assert np.isnan(hist[:2]).all() and hist[2:] == pytest.approx([1, 2, 3, 4, 5])


# ---- the US clock change across the history ---------------------------------------------------------------------------------------------
def test_us_history_across_the_clock_change_lines_up_on_the_session_minute():
    src = MemorySource(NY, US_SESSION)
    days = ["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-09", "2026-03-10"]           # the 8th was the change
    for i, d in enumerate(days):
        src.add(d, "AAPL", {0: flat(200), 20: flat(200 + i + 1)})
    s = service(src, now=lambda: epoch("2026-03-11", 21, NY, US_SESSION))
    hist, idx = s.absolute("pct_open", "intraday", "AAPL", 5, now=1.2)
    assert hist == pytest.approx([0.5, 1.0, 1.5, 2.0, 2.5]) and idx == 2
    assert {datetime.utcfromtimestamp(epoch(d, 20, NY, US_SESSION)).strftime("%H:%M") for d in days} == {"14:50", "13:50"}      # UTC does shift; the slot did not


# ---- intraweek ----------------------------------------------------------------------------------------------------------------------------
def weeks(src, mondays, closes, holiday_monday=None):
    """Mon-Fri of each week: the week opens at 100 on its first day; the Wednesday closes at the given price at slot 10, other days at 100."""
    for mon, c in zip(mondays, closes):
        d0 = date.fromisoformat(mon)
        for k in range(5):
            d = (d0 + timedelta(days=k)).isoformat()
            if d == holiday_monday:
                continue
            src.add(d, "ABC", {0: flat(100), 10: flat(c if k == 2 else 100)})


def test_intraweek_compares_the_same_weekday_and_minute_of_the_previous_weeks_against_the_week_open():
    src = MemorySource()
    weeks(src, ["2026-02-16", "2026-02-23", "2026-03-02", "2026-03-09"], [101, 102, 103, 104])
    src.add("2026-03-16", "ABC", {0: flat(100)})                                                  # this week: Monday only so far
    s = service(src, now=lambda: epoch("2026-03-18", 11))                                         # now is a Wednesday
    r = s.absolute("pct_open", "intraweek", "ABC", 4, now=2.5)
    assert r[0].tolist() == [1.0, 2.0, 3.0, 4.0] and r[1] == 2 and r.dates == ["2026-02-18", "2026-02-25", "2026-03-04", "2026-03-11"]


def test_a_week_that_opens_on_tuesday_uses_tuesdays_open_and_a_missing_wednesday_is_nan():
    src = MemorySource()
    weeks(src, ["2026-03-02", "2026-03-09"], [103, 104], holiday_monday="2026-03-09")
    src.add("2026-03-10", "ABC", {0: flat(200), 10: flat(200)})                                    # the week of the 9th opens (Tuesday) at 200
    src.add("2026-03-11", "ABC", {0: flat(200), 10: flat(208)})
    del src.bar_data[("cash", "ABC", "2026-03-04")]
    s = service(src, now=lambda: epoch("2026-03-18", 11))
    hist, _ = s.absolute("pct_open", "intraweek", "ABC", 2, now=0.0)
    assert math.isnan(hist[0]) and hist[1] == pytest.approx(4.0)                                  # 208 against the week's first open, Tuesday's 200


def test_intraweek_vwap_runs_from_the_first_day_of_the_week():
    src = MemorySource()
    src.add("2026-03-09", "ABC", {0: flat(100, 100), 10: flat(100, 100)})                           # Monday: 200 shares at 100
    src.add("2026-03-10", "ABC", {0: flat(100, 100), 10: flat(100, 100)})                           # Tuesday: 200 more at 100
    src.add("2026-03-11", "ABC", {0: flat(100, 100), 10: flat(130, 600)})                           # Wednesday slot 10: 600 at 130
    s = service(src, now=lambda: epoch("2026-03-18", 11))
    hist, _ = s.absolute("pct_vwap", "intraweek", "ABC", 1, now=0.0)
    cum_pv, cum_v = 200 * 100 + 200 * 100 + 100 * 100 + 600 * 130, 200 + 200 + 100 + 600
    assert hist[0] == pytest.approx((130 / (cum_pv / cum_v) - 1) * 100, rel=1e-5)


# ---- intramonth -----------------------------------------------------------------------------------------------------------------------------
def test_intramonth_compares_the_same_trading_day_of_the_month_in_the_previous_months():
    src = MemorySource()
    for month, base in (("2026-01", 101), ("2026-02", 102)):
        y, mo = map(int, month.split("-"))
        n = 0
        for dd in range(1, 29):
            d = date(y, mo, dd)
            if d.weekday() < 5:
                n += 1
                src.add(d.isoformat(), "ABC", {0: flat(100), 10: flat(base + (n == 3))})        # the third trading day closes one higher
    for d in ("2026-03-02", "2026-03-03"):
        src.add(d, "ABC", {0: flat(100), 10: flat(100)})                                          # March: two trading days so far; today is the third
    s = service(src, now=lambda: epoch("2026-03-04", 11))
    r = s.absolute("pct_open", "intraday", "ABC", 1, now=0)                                       # (for contrast)
    assert r.dates == ["2026-03-03"]
    r = s.absolute("pct_open", "intramonth", "ABC", 2, now=0.0)
    assert r.dates == ["2026-01-05", "2026-02-04"]                                                # the 3rd trading day of January was the 5th (1st is a holiday gap here), of February the 4th
    assert r[0].tolist() == pytest.approx([2.0, 3.0])                                             # 102 / 100 and 103 / 100 against each month's own first open


def test_a_month_without_enough_trading_days_gives_nan():
    src = MemorySource()
    src.add("2026-02-02", "ABC", {0: flat(100), 10: flat(101)})
    for d in ("2026-03-02", "2026-03-03", "2026-03-04"):
        src.add(d, "ABC", {0: flat(100), 10: flat(100)})
    s = service(src, now=lambda: epoch("2026-03-05", 11))                                         # the 4th trading day of March; February has only one
    r = s.absolute("pct_open", "intramonth", "ABC", 1, now=0.0)
    assert r.dates == [None] and math.isnan(r[0][0]) and r[1] == -1


# ---- iv -----------------------------------------------------------------------------------------------------------------------------------------
def test_iv_compares_the_straddle_iv_of_the_same_minute_with_a_short_carry():
    src = MemorySource()
    for i, d in enumerate(DAYS):
        src.add_iv(d, "NIFTY", {10: 1.0 + i, 11: 9.0, 14: 7.0})                                    # a value at minutes 10, 11 and 14; nothing between
        src.add(d, "NIFTY", {0: flat(100)})
    s = service(src, now=lambda: epoch(TODAY, 11), iv_max_age=2)
    r = s.absolute("iv", "intraday", "NIFTY", 5, now=2.5)
    assert r[0].tolist() == [1, 2, 3, 4, 5] and r[1] == 2
    s2 = service(src, now=lambda: epoch(TODAY, 14), iv_max_age=2)
    assert s2.absolute("iv", "intraday", "NIFTY", 5, now=2.5)[0].tolist() == [9.0] * 5             # minute 13: carried from 11 (age 2)
    s3 = service(src, now=lambda: epoch(TODAY, 14), iv_max_age=1)
    assert np.isnan(s3.absolute("iv", "intraday", "NIFTY", 5, now=2.5)[0]).all() and s3.absolute("iv", "intraday", "NIFTY", 5, now=2.5)[1] == -1


# ---- names, checks, the result -----------------------------------------------------------------------------------------------------------------
def test_symbol_names():
    assert parse_symbol("reliance") == ("cash", "RELIANCE") and parse_symbol("nifty fut") == ("fut", "NIFTY")
    assert parse_symbol("NIFTY 27OCT26 FUT") == ("exact", "NIFTY 27OCT26 FUT") and ab.underlying_of(parse_symbol("NIFTY 27OCT26 FUT")) == "NIFTY"
    with pytest.raises(ValueError):
        parse_symbol("  ")


def test_wrong_arguments_are_refused_with_the_choices():
    s = service(MemorySource(), now=lambda: epoch(TODAY, 11))
    for args in (("rsi", "intraday", "A", 5), ("iv", "daily", "A", 5), ("iv", "intraday", "A", 0), ("iv", "intraday", "A", 2.5), ("iv", "intraday", "A", True),
                 ("iv", "intraday", "A", 1000)):
        with pytest.raises(ValueError):
            s.absolute(*args)


def test_the_result_unpacks_into_the_array_and_the_position_and_prints_readably():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 11))
    res = s.absolute("pct_open", "intraday", "ABC", 5, now=2.5)
    hist, idx = res
    assert isinstance(res, tuple) and len(res) == 2 and idx == 2 and hist.dtype == np.float64 and res.pctile == 40.0
    assert "pct_open intraday ABC" in repr(res) and "idx=2 of 5 valid" in repr(res)


# ---- memory and many callers ---------------------------------------------------------------------------------------------------------------------
def test_a_tiny_cache_budget_gives_the_same_answers_and_never_grows_past_it():
    src = MemorySource()
    five_days(src)
    big = service(src, now=lambda: epoch(TODAY, 11))
    tiny = service(src, now=lambda: epoch(TODAY, 11), memory_mb=0.005)                             # room for about one day
    a = big.absolute("pct_open", "intraday", "ABC", 5, now=2.5)
    b = tiny.absolute("pct_open", "intraday", "ABC", 5, now=2.5)
    assert a[0].tolist() == b[0].tolist() and a[1] == b[1]
    assert tiny.cache.nbytes <= 0.005 * 1e6 + 5000 and len(tiny.cache) < len(big.cache)


def test_many_callers_at_once_load_each_day_once():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 11))
    out = []

    def work():
        out.append(s.absolute("pct_open", "intraday", "ABC", 5, now=2.5)[0].tolist())

    ts = [threading.Thread(target=work) for _ in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(o == [1, 2, 3, 4, 5] for o in out) and len(src.calls) == 5                         # one read per day, not sixteen
    assert s.stats()["loads"] == 5 and s.stats()["hits"] >= 11 * 5 // 5


# ---- the Parquet files -----------------------------------------------------------------------------------------------------------------------------
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from algo.data.store import Store  # noqa: E402
from algo.series.absolute import ParquetSource  # noqa: E402


def write_candles(folder, day, rows):
    """rows: (symbol, slot, close, volume) -> candles_<day>.parquet in the format eod_save and the converters write."""
    t = pa.table({"Minute": pa.array([epoch(day, s) * 1_000_000_000 for _, s, _, _ in rows], pa.timestamp("ns", "UTC")),
                  "Symbol": [r[0] for r in rows], "token": pa.array(range(len(rows)), pa.int64()),
                  "Open": [float(r[2]) for r in rows], "High": [float(r[2]) for r in rows], "Low": [float(r[2]) for r in rows], "Close": [float(r[2]) for r in rows],
                  "Volume": pa.array([r[3] for r in rows], pa.int64()), "Date": [day] * len(rows)})
    pq.write_table(t, str(folder / f"candles_{day}.parquet"), compression="zstd", row_group_size=3)


def write_straddle(folder, day, rows):
    """rows: (symbol, expiry, 'HH:MM:SS', iv)"""
    t = pa.table({"Time": [r[2] for r in rows], "Symbol": [r[0] for r in rows], "Expiry": [r[1] for r in rows], "IV": [float(r[3]) for r in rows],
                  "Date": [day] * len(rows)})
    pq.write_table(t, str(folder / f"straddle_{day}.parquet"), compression="zstd")


@pytest.fixture
def folder(tmp_path):
    for i, d in enumerate(DAYS):
        write_candles(tmp_path, d, [("ABC", 0, 100, 10), ("ABC", 10, 101 + i, 10), ("XYZ", 0, 5, 1),
                                    ("NIFTY 27MAR26 FUT", 0, 25000, 1), ("NIFTY 27MAR26 FUT", 10, 25000 + 25 * (i + 1), 1),
                                    ("NIFTY 28APR26 FUT", 0, 26000, 1), ("NIFTY 28APR26 FUT", 10, 26000 + 260 * (i + 1), 1)])
        write_straddle(tmp_path, d, [("NIFTY", "2026-03-17", "09:25:02", 1.0 + i), ("NIFTY", "2026-03-24", "09:25:02", 50.0), ("BANKNIFTY", "2026-03-17", "09:25:02", 7.0)])
    return tmp_path


def test_the_parquet_source_reads_one_symbol_of_one_day(folder):
    src = ParquetSource(Store(str(folder)))
    assert src.days("candles") == DAYS and src.days("straddle") == DAYS
    ts, o, h, l, c, v = src.bars("2026-03-09", ("cash", "ABC"))
    assert c.tolist() == [100.0, 101.0] and v.tolist() == [10.0, 10.0] and ts[0] == epoch("2026-03-09", 0) and ts[1] - ts[0] == 600
    assert src.bars("2026-03-09", ("cash", "NOPE")) is None and src.bars("2026-04-01", ("cash", "ABC")) is None


def test_the_nearest_future_is_chosen_for_each_day_so_the_series_rolls(folder):
    write_candles(folder, "2026-03-30", [("NIFTY 27MAR26 FUT", 0, 1, 1), ("NIFTY 28APR26 FUT", 0, 2, 1), ("NIFTY 28APR26 FUT", 5, 3, 1)])      # the March future has expired
    src = ParquetSource(Store(str(folder)))
    assert src.bars("2026-03-09", ("fut", "NIFTY"))[4].tolist() == [25000.0, 25025.0]                    # March was the nearest
    assert src.bars("2026-03-30", ("fut", "NIFTY"))[4].tolist() == [2.0, 3.0]                            # April is
    assert src.bars("2026-03-09", ("exact", "NIFTY 28APR26 FUT"))[4].tolist() == [26000.0, 26260.0]     # a named contract does not roll
    assert src.bars("2026-03-09", ("fut", "NOSUCH")) is None


def test_the_iv_of_the_nearest_expiry_is_read(folder):
    src = ParquetSource(Store(str(folder)))
    minute, iv = src.iv("2026-03-10", "NIFTY")
    assert minute.tolist() == [9 * 60 + 25] and iv.tolist() == [2.0] and src.iv("2026-03-10", "NOPE") is None


def test_the_service_gives_the_same_answers_from_the_files_as_from_memory(folder):
    s = AbsoluteService(Clock(IST, NSE_SESSION), ParquetSource(Store(str(folder))), now=lambda: epoch(TODAY, 11))
    hist, idx = s.absolute("pct_open", "intraday", "ABC", 5, now=2.5)
    assert hist == pytest.approx([1, 2, 3, 4, 5]) and idx == 2
    hist, idx = s.absolute("pct_open", "intraday", "NIFTY FUT", 5, now=0.15)                              # 25025/25000 ... on the rolling future: 0.1 .. 0.5 %
    assert hist == pytest.approx([0.1, 0.2, 0.3, 0.4, 0.5]) and idx == 1
    hist, idx = s.absolute("iv", "intraday", "NIFTY", 5, now=2.5)                                         # 09:25:02 is session minute 10: the nearest expiry only (the 50.0 is the later one)
    assert hist == pytest.approx([1, 2, 3, 4, 5]) and idx == 2


# ---- warming many symbols reads each day once ------------------------------------------------------------------------------------------------------
class BatchSource(MemorySource):
    """The same history, but it can answer for many symbols at once; counts the reads."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.many_calls, self.single_calls = [], 0

    def bars(self, day, spec):
        self.single_calls += 1
        return super().bars(day, spec)

    def bars_many(self, day, specs):
        self.many_calls.append((day, tuple(specs)))
        return {sp: super(BatchSource, self).bars(day, sp) for sp in specs if super(BatchSource, self).bars(day, sp) is not None}


def test_warming_many_symbols_reads_each_day_file_once_and_gives_the_same_answers():
    names = [f"S{i}" for i in range(30)]
    src, plain = BatchSource(), MemorySource()
    for d, c in zip(DAYS, (101, 102, 103, 104, 105)):
        for i, nm in enumerate(names):
            for t in (src, plain):
                t.add(d, nm, {0: flat(100), 10: flat(c + i)})
    warm = service(src, now=lambda: epoch(TODAY, 11))
    assert warm.warm("pct_open", "intraday", names, 5) == 30
    assert len(src.many_calls) == 5 and src.single_calls == 0 and all(len(sp) == 30 for _, sp in src.many_calls)       # 5 reads, not 150
    cold = service(plain, now=lambda: epoch(TODAY, 11))
    for nm in ("S0", "S17", "S29"):
        a, b = warm.absolute("pct_open", "intraday", nm, 5, now=2.5), cold.absolute("pct_open", "intraday", nm, 5, now=2.5)
        assert a[0].tolist() == b[0].tolist() and a[1] == b[1]
    assert len(src.many_calls) == 5 and src.single_calls == 0                                                          # asking afterwards reads nothing more


def test_warming_a_weekly_level_also_loads_the_earlier_days_of_each_week():
    src = BatchSource()
    weeks(src, ["2026-03-02", "2026-03-09"], [103, 104])
    s = service(src, now=lambda: epoch("2026-03-18", 11))
    s.warm("pct_open", "intraweek", ["ABC"], 2)
    assert {d for d, _ in src.many_calls} == {"2026-03-02", "2026-03-03", "2026-03-04", "2026-03-09", "2026-03-10", "2026-03-11"}
    assert s.absolute("pct_open", "intraweek", "ABC", 2, now=0.0)[0].tolist() == pytest.approx([3.0, 4.0]) and src.single_calls == 0


def test_the_parquet_batch_reader_gives_what_the_single_reader_gives(folder):
    src = ParquetSource(Store(str(folder)))
    many = src.bars_many("2026-03-10", [("cash", "ABC"), ("cash", "XYZ"), ("fut", "NIFTY"), ("exact", "NIFTY 28APR26 FUT"), ("cash", "NOPE")])
    assert set(many) == {("cash", "ABC"), ("cash", "XYZ"), ("fut", "NIFTY"), ("exact", "NIFTY 28APR26 FUT")}
    for sp in many:
        one = src.bars("2026-03-10", sp)
        assert all(np.array_equal(x, y) for x, y in zip(many[sp], one))
    assert many[("fut", "NIFTY")][4].tolist() == [25000.0, 25050.0] and src.bars_many("2026-04-01", [("cash", "ABC")]) == {}
    ivs = src.iv_many("2026-03-10", ["NIFTY", "BANKNIFTY", "NOPE"])
    assert set(ivs) == {"NIFTY", "BANKNIFTY"} and ivs["NIFTY"][1].tolist() == [2.0] and ivs["BANKNIFTY"][1].tolist() == [7.0]


def test_warming_iv_for_many_underlyings(folder):
    s = AbsoluteService(Clock(IST, NSE_SESSION), ParquetSource(Store(str(folder))), now=lambda: epoch(TODAY, 11))
    assert s.warm("iv", "intraday", ["NIFTY", "BANKNIFTY"], 5) == 2 and s.stats()["loads"] == 10
    assert s.absolute("iv", "intraday", "NIFTY", 5, now=2.5)[0].tolist() == pytest.approx([1, 2, 3, 4, 5]) and s.stats()["loads"] == 10
