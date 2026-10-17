"""absolute() is a plain function: today so far is read at the moment of the call (one symbol), nothing runs in the background."""
import math
import sys
import types
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from algo.series.absolute import deephaven_readers
from tests.test_absolute import DAYS, TODAY, MemorySource, epoch, five_days, flat, service, weeks


def bars_of(*rows):
    """(slot, o, h, l, c, v) rows of TODAY -> the arrays a reader returns."""
    cols = list(zip(*rows))
    return (np.array([epoch(TODAY, s) for s in cols[0]], dtype=np.int64), *(np.array(c, dtype=np.float64) for c in cols[1:]))


def test_the_value_now_is_computed_from_todays_bars_read_at_the_call():
    src = MemorySource()
    five_days(src)
    reads = []

    def today_bars(spec, day):
        reads.append((spec, day))
        return bars_of((0, 100, 100, 100, 100, 50), (10, 102.5, 102.5, 102.5, 102.5, 50))

    s = service(src, now=lambda: epoch(TODAY, 11), today_bars=today_bars)                       # 09:26: the bar of minute 10 has just closed
    r = s.absolute("pct_open", "intraday", "abc", 5)
    assert r.now == pytest.approx(2.5) and r[1] == 2 and r[0].tolist() == pytest.approx([1, 2, 3, 4, 5]) and reads == [(("cash", "ABC"), TODAY)]
    s.absolute("pct_open", "intraday", "abc", 5)
    assert len(reads) == 2                                                                        # today is read at every call, never cached
    assert s.stats()["entries" if False else "cache_entries"] == 5                                # only the five past days are in the cache


def test_the_price_carries_over_quiet_minutes_in_the_history_and_in_today_alike():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 14), today_bars=lambda spec, day: bars_of((0, 100, 100, 100, 100, 5), (10, 104, 104, 104, 104, 5)))   # now is minute 13
    r = s.absolute("pct_open", "intraday", "ABC", 5)
    assert r.minute == 13 and r.now == pytest.approx(4.0) and r[0].tolist() == pytest.approx([1, 2, 3, 4, 5]) and r[1] == 3                      # 3 days below 4.0


def test_a_symbol_with_no_rows_today_gives_the_history_and_no_position():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 11), today_bars=lambda spec, day: None)
    hist, idx = s.absolute("pct_open", "intraday", "ABC", 5)
    assert hist.tolist() == pytest.approx([1, 2, 3, 4, 5]) and idx == -1
    assert math.isnan(s.absolute("pct_open", "intraday", "ABC", 5).now)


def test_a_price_older_than_max_age_is_nan_and_gives_no_position():
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 31), max_age=5, today_bars=lambda spec, day: bars_of((0, 100, 100, 100, 100, 5)))                   # the last bar was 30 minutes ago
    r = s.absolute("pct_open", "intraday", "ABC", 5)
    assert math.isnan(r.now) and r[1] == -1


def test_today_s_saved_file_is_used_when_there_is_no_live_table():
    src = MemorySource()
    five_days(src)
    src.add(TODAY, "ABC", {0: flat(100), 10: flat(103.5)})                                       # after the close, the file of today exists
    s = service(src, now=lambda: epoch(TODAY, 11))
    r = s.absolute("pct_open", "intraday", "ABC", 5)
    assert r.now == pytest.approx(3.5) and r[1] == 3 and r.dates == DAYS                          # the file of today is the value now, not part of the history


def test_at_evaluates_as_of_another_moment_in_exchange_time():
    src = MemorySource()
    five_days(src)
    src.add("2026-03-12", "ABC", {0: flat(100), 3: flat(100.5), 10: flat(104)})
    s = service(src, now=lambda: epoch(TODAY, 300))
    r = s.absolute("pct_open", "intraday", "ABC", 3, at="2026-03-12 09:19")                      # 09:19 is session minute 4: the bar of minute 3 is the latest
    assert r.today == "2026-03-12" and r.minute == 3 and r.now == pytest.approx(0.5) and r.dates == ["2026-03-09", "2026-03-10", "2026-03-11"]
    assert s.absolute("pct_open", "intraday", "ABC", 3, at=epoch("2026-03-12", 11)).now == pytest.approx(4.0)                              # epoch seconds
    assert s.absolute("pct_open", "intraday", "ABC", 3, at=datetime(2026, 3, 12, 9, 26)).now == pytest.approx(4.0)                         # a datetime (exchange time)
    assert s.absolute("pct_open", "intraday", "ABC", 3, at="2026-03-12 09:26:00").minute == 10
    with pytest.raises(ValueError, match="at must be"):
        s.absolute("pct_open", "intraday", "ABC", 3, at="tomorrow")


def test_before_the_open_and_after_the_close_the_first_and_last_minute_are_used():
    src = MemorySource()
    for d in DAYS:
        src.add(d, "ABC", {0: flat(100), 374: flat(101)})
    s = service(src, now=lambda: epoch(TODAY, 0))
    assert s.absolute("pct_open", "intraday", "ABC", 5, at="2026-03-16 08:00").minute == 0
    assert s.absolute("pct_open", "intraday", "ABC", 5, at="2026-03-16 17:00").minute == 374


def test_the_weekly_anchor_of_now_comes_from_the_earlier_days_of_this_week():
    src = MemorySource()
    weeks(src, ["2026-03-09"], [104])
    src.add("2026-03-16", "ABC", {0: flat(120)})                                                  # this Monday opened at 120
    s = service(src, now=lambda: epoch("2026-03-18", 11),
                today_bars=lambda spec, day: (np.array([epoch(day, 0), epoch(day, 10)]), *(np.array([x, x]) for x in (130.0, 130.0, 130.0, 132.0)), np.array([5.0, 5.0])))
    r = s.absolute("pct_open", "intraweek", "ABC", 1)
    assert r.now == pytest.approx((132 - 120) / 120 * 100)                                        # against Monday's open, not today's 130
    assert s.absolute("pct_open", "intraday", "ABC", 1).now == pytest.approx((132 - 130) / 130 * 100)


def test_iv_now_is_todays_latest_iv_of_the_nearest_expiry_carried_a_few_minutes():
    src = MemorySource()
    for i, d in enumerate(DAYS):
        src.add_iv(d, "NIFTY", {10: 1.0 + i})
        src.add(d, "NIFTY", {0: flat(100)})
    s = service(src, now=lambda: epoch(TODAY, 11), today_iv=lambda und, day: (np.array([555 + 10, 555 + 4]), np.array([2.5, 9.0])) if und == "NIFTY" else None)
    r = s.absolute("iv", "intraday", "NIFTY", 5)
    assert r.now == 2.5 and r[1] == 2
    s2 = service(src, now=lambda: epoch(TODAY, 20), iv_max_age=3, today_iv=s.today_iv)                 # 9 minutes later with no new IV
    assert math.isnan(s2.absolute("iv", "intraday", "NIFTY", 5).now)


def test_nothing_is_kept_between_calls_but_the_cache_and_there_is_no_thread_or_listener():
    import threading
    before = threading.active_count()
    src = MemorySource()
    five_days(src)
    s = service(src, now=lambda: epoch(TODAY, 11), today_bars=lambda spec, day: bars_of((0, 100, 100, 100, 100, 5), (10, 101, 101, 101, 101, 5)))
    for _ in range(3):
        s.absolute("pct_open", "intraday", "ABC", 5)
    assert threading.active_count() == before and set(vars(s)) <= {"clock", "source", "cache", "max_age", "iv_max_age", "min_valid", "today_bars", "today_iv", "_now", "_days_cache", "baskets", "today_bars_many", "today_iv_many"}


# ---- the Deephaven readers (the tables are faked) -----------------------------------------------------------------------------------------------------
class Table:
    """Remembers the filter and the view it was given, and answers with a ready DataFrame."""

    def __init__(self, df):
        self.df, self.where_expr, self.view_cols = df, None, None

    def where(self, expr):
        self.where_expr = expr
        return self

    def view(self, cols):
        self.view_cols = cols
        return self


@pytest.fixture
def readers(monkeypatch):
    futs = pd.DataFrame({"fut_token": [11, 12], "TckrSymb": ["NIFTY", "NIFTY"], "expiry": [pd.Timestamp("2026-03-17"), pd.Timestamp("2026-04-28")]})
    master = types.SimpleNamespace(spot_of={"ABC": 7}, token_of={"NIFTY 28APR26 FUT": 12}, futs=futs)
    candles = Table(pd.DataFrame({"Ts": [epoch(TODAY, 0), epoch(TODAY, 1)], "Open": [100.0, 101.0], "High": [101.0, 102.0], "Low": [99.0, 100.0],
                                  "Close": [100.5, 101.5], "Volume": [10, 20]}))
    straddle = Table(pd.DataFrame({"Expiry": ["2026-03-24", "2026-03-17", "2026-03-17"], "Time": ["09:26:02", "09:25:02", "09:26:02"], "IV": [9.0, 2.0, 2.5]}))
    app = types.SimpleNamespace(master=master, tables={"candles": candles, "straddle_series": straddle})
    dh = types.ModuleType("deephaven")
    dhp = types.ModuleType("deephaven.pandas")
    dhp.to_pandas = lambda t: t.df
    monkeypatch.setitem(sys.modules, "deephaven", dh)
    monkeypatch.setitem(sys.modules, "deephaven.pandas", dhp)
    return app, candles, straddle


def test_todays_bars_are_read_for_the_one_token_of_the_symbol(readers):
    app, candles, _ = readers
    today_bars, _, _, _ = deephaven_readers(app)
    ts, o, h, l, c, v = today_bars(("cash", "ABC"), TODAY)
    assert candles.where_expr == "token = 7" and candles.view_cols[0] == "Ts = epochSeconds(Minute)"
    assert c.tolist() == [100.5, 101.5] and v.tolist() == [10.0, 20.0] and ts.dtype == np.int64
    assert today_bars(("cash", "NOPE"), TODAY) is None and today_bars(("exact", "NIFTY 28APR26 FUT"), TODAY) is not None and candles.where_expr == "token = 12"
    today_bars(("fut", "NIFTY"), "2026-03-10")
    assert candles.where_expr == "token = 11"                                                      # the nearest future that has not expired
    today_bars(("fut", "NIFTY"), "2026-03-18")
    assert candles.where_expr == "token = 12" and today_bars(("fut", "NOSUCH"), TODAY) is None


def test_todays_iv_is_the_nearest_expiry_as_minute_of_day_and_value(readers):
    app, _, straddle = readers
    _, today_iv, _, _ = deephaven_readers(app)
    minute, iv = today_iv("NIFTY", TODAY)
    assert straddle.where_expr == "Symbol = `NIFTY`" and minute.tolist() == [9 * 60 + 25, 9 * 60 + 26] and iv.tolist() == [2.0, 2.5]
    assert today_iv("NIFTY`; drop", TODAY) is None                                                  # only plain names go into a query


def test_a_missing_table_gives_none_so_the_saved_file_is_used(readers):
    app, _, _ = readers
    app.tables.clear()
    today_bars, today_iv, _, _ = deephaven_readers(app)
    assert today_bars(("cash", "ABC"), TODAY) is None and today_iv("NIFTY", TODAY) is None


def test_the_functions_in_the_console_share_one_service_and_settings_rebuild_it(tmp_path, monkeypatch, readers):
    import algo
    from algo.core import app as app_module
    from algo.core.config import Paths
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    app = algo.install(ns, "NSE", paths=Paths(market_data=str(tmp_path), work=str(tmp_path)))
    assert {"absolute", "absolute_warm", "absolute_settings"} <= set(ns) and "absolute_start" not in ns                        # no start, no stop: just calls
    app.plugins["master"] = types.SimpleNamespace(data=readers[0].master)
    first = ns["absolute_settings"]()
    svc = app.shared["absolute_service"]
    assert svc.cache.max_bytes == 200_000_000 and ns["absolute_settings"]() is not None and app.shared["absolute_service"] is svc          # reading changes nothing
    ns["absolute_settings"](memory_mb=50)
    assert app.shared["absolute_service"] is not svc and app.shared["absolute_service"].cache.max_bytes == 50_000_000 and first["cache_entries"] == 0
    r = ns["absolute"]("pct_open", "intraday", "ABC", 5)                                                                           # no saved days at all: still answers
    assert r[1] == -1 and r.n_valid == 0 and np.isnan(r[0]).all()
    assert app.plugins.keys() == {"master"}                                                                                        # nothing was started
