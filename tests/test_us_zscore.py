"""examples/10_us_zscore_crossorder.py: 08 without the RSI and without the own-history filter, driven by a z-score of the % from the VWAP (same minute, last 90 CALENDAR days):
BUY at z <= -3, SELL at z >= +3, a BUY is closed when z > 0, a SELL when z < 0, and everything is squared up at the time exit.
Real registry, book, bridge and NATS service; fake wire and fake Deephaven listener; absolute() and relative() are fakes that return the real types."""
import re
import time
from datetime import date, timedelta

import numpy as np
import pytest

from algo.series.absolute import Absolute, Relative
from tests.test_examples import EXAMPLES, FUT, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)
from tests.test_us_basket_rsi import ALWAYS_OPEN, MY_BASKET, basket_block, feed

SCRIPT = (EXAMPLES / "10_us_zscore_crossorder.py").read_text(encoding="utf-8")
OPEN_WINDOW = re.search(r"WINDOW = [(]dtime[(]\d+, \d+[)], dtime[(]\d+, \d+[)][)]", SCRIPT).group(0)
TODAY = date(2026, 10, 21)


def history(n_recent=30, n_old=20, old_value=1000.0):
    """Earlier days of the % from the VWAP at one minute: `n_recent` days within 90 calendar days (values 0.0 .. 0.58) and `n_old` days older than that (a huge value that would wreck the mean
    and the deviation if it were counted). Returns (hist, dates), oldest first."""
    recent = np.linspace(0.0, 0.58, n_recent)
    old_dates = [(TODAY - timedelta(days=100 + 5 * i)).isoformat() for i in range(n_old)][::-1]
    new_dates = [(TODAY - timedelta(days=2 + 2 * i)).isoformat() for i in range(n_recent)][::-1]
    return np.array([old_value] * n_old + list(recent)), old_dates + new_dates


def mean_sd(hist_dates=None):
    hist, dates = hist_dates if hist_dates is not None else history()
    recent = [h for h, d in zip(hist, dates) if (TODAY - date.fromisoformat(d)).days <= 90]
    return float(np.mean(recent)), float(np.std(recent, ddof=1)), len(recent)


def start(tmp_path, monkeypatch, pct, hist_dates=None, window=ALWAYS_OPEN, **replace):
    """Paste 10 with a fake absolute() whose `now` (the % from the VWAP now) is read from the one-item list `pct` at every call."""
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, "ok")
    hist, dates = hist_dates if hist_dates is not None else history()
    calls = []

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        return Relative(np.array([1.0, 2.0, 3.0]), 3, now=0.5, n_valid=7, pctile=50.0, symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        calls.append((series, level, symbol, n, ref))
        return Absolute(hist, 5, now=pct[0], n_valid=len(hist), pctile=50.0, series=series, level=level, symbol=symbol, dates=dates, today=TODAY.isoformat())

    ns["absolute_warm"] = lambda *a, **k: calls.append(("warm", a, k))
    ns["relative"] = relative
    ns["absolute"] = absolute
    ns["basket"] = lambda *a, **k: None
    _, b0, b1 = basket_block(SCRIPT)
    src = (SCRIPT[:b0] + MY_BASKET + SCRIPT[b1:]).replace(OPEN_WINDOW, window)
    src = src.replace("EXIT_AT = dtime(15, 55)", "EXIT_AT = dtime(23, 59, 59)")                       # the real clock never matters here
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    exec(compile(src, "10_us_zscore_crossorder.py", "exec"), ns)
    return ns, app, svc, wire, calls


def candle(fake_dh, i=0, px=100.0, minute=None):
    """One 1-minute candle. By default the one that ENDS the i-th 5-minute bar (minute 4, 9, 14 ... after the :00 start), which is when the rules look at z; `minute=` picks another minute."""
    feed(fake_dh, [px], first=(4 + 5 * i) if minute is None else minute)


def test_it_has_no_rsi_and_no_absolute_filter_and_the_z_inputs():
    for gone in ("RSI", "rsi(", "HIST_DAYS", "ABS_DAYS", "ABS_ZONE", "in_own_history", "load_history"):
        assert gone not in SCRIPT.replace("no RSI", "").replace("without the RSI", ""), gone
    assert "Z_DAYS = 90" in SCRIPT and "Z_MIN_DAYS = 20" in SCRIPT and "Z_ENTRY = 3.0" in SCRIPT and 'Z_BAR = "5min"' in SCRIPT and "Z_BAR_LIVE = False" in SCRIPT
    assert "TARGET_PCT, STOP_PCT = None, None" in SCRIPT and "EXIT_AT = dtime(15, 55)" in SCRIPT
    assert "def run_us_zscore()" in SCRIPT and "us_zscore_stop" in SCRIPT and "run_us_basket_rsi" not in SCRIPT


def test_a_buy_at_z_minus_3_and_nothing_inside_the_band_and_the_days_older_than_90_are_ignored(tmp_path, fake_dh, monkeypatch):
    mean, sd, n = mean_sd()
    assert n == 30
    pct = [mean - 2.9 * sd]                                                                           # z = -2.9: inside the band, no trade
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, pct)
    candle(fake_dh, 0)
    time.sleep(0.3)
    assert wire.sent == []
    pct[0] = mean - 3.2 * sd                                                                          # z = -3.2, but in the MIDDLE of a 5-minute bar: nothing yet
    candle(fake_dh, minute=6)
    time.sleep(0.3)
    assert wire.sent == []
    candle(fake_dh, 1)                                                                                # the minute that ends the bar: BUY
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe" and wire.sent[0][2]["SIDE"] == 1
    assert ("pct", "intraday", "NIFTY", 70, "vwap") in calls                                          # % from the VWAP; the trading days that fit in 90 calendar days
    assert any(c[0] == "warm" for c in calls)                                                         # the history is read ahead of the first signal
    svc.on_stop()


def test_a_sell_at_z_plus_3(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [mean + 3.05 * sd])
    candle(fake_dh)
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][2]["SIDE"] == 2
    svc.on_stop()


def test_a_buy_is_closed_when_z_turns_above_zero_and_not_before_and_no_second_buy_until_z_is_back_inside(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    pct = [mean - 3.5 * sd]
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, pct)
    candle(fake_dh, 0)
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(1.0)                                                                                   # the entry is confirmed: the position is open
    pct[0] = mean - 0.2 * sd                                                                          # z = -0.2: below 0 still
    candle(fake_dh, 1, px=105.0)                                                                      # even a +5% move does not close it: no target is set
    time.sleep(0.3)
    assert len(wire.sent) == 1
    pct[0] = mean + 0.1 * sd                                                                          # z = +0.1: above 0 (counting the 100-day-old values in would make z hugely negative here)
    candle(fake_dh, 2, px=105.0)
    assert wait_for(lambda: len(wire.sent) == 2) and wire.sent[1][:2] == ("cmd.hft.1001", "squareup")
    time.sleep(1.0)                                                                                   # closed: the position is gone
    pct[0] = mean - 3.2 * sd                                                                          # z far below again: it trades again (z was back inside the band first)
    candle(fake_dh, 3)
    assert wait_for(lambda: len(wire.sent) == 3) and wire.sent[2][1] == "subscribe" and wire.sent[2][0] == "cmd.hft.1002"      # a NEW portfolio for the new trade
    svc.on_stop()


def test_a_sell_is_closed_when_z_turns_below_zero_and_not_before(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    pct = [mean + 3.5 * sd]
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, pct)
    candle(fake_dh, 0)
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][2]["SIDE"] == 2
    time.sleep(1.0)
    pct[0] = mean + 0.3 * sd                                                                          # z still above 0: a SELL stays open
    candle(fake_dh, 1)
    time.sleep(0.3)
    assert len(wire.sent) == 1
    pct[0] = mean - 0.1 * sd
    candle(fake_dh, 2)
    assert wait_for(lambda: len(wire.sent) == 2) and wire.sent[1][1] == "squareup"
    svc.on_stop()


def test_no_trade_without_enough_earlier_days_and_a_flat_history_gives_no_z(tmp_path, fake_dh, monkeypatch):
    few = history(n_recent=10, n_old=0)                                                                # 10 earlier days: fewer than Z_MIN_DAYS = 20
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [-50.0], hist_dates=few)
    candle(fake_dh)
    time.sleep(0.4)
    assert wire.sent == []
    svc.on_stop()
    fake_dh.clear()
    flat = (np.zeros(30), history()[1][20:])                                                           # every earlier day the same: the deviation is 0, z cannot be told
    ns, app, svc, wire, calls = start(tmp_path / "flat", monkeypatch, [5.0], hist_dates=flat)
    candle(fake_dh)
    time.sleep(0.4)
    assert wire.sent == []
    svc.on_stop()


def test_the_basket_filter_still_applies_and_a_target_and_a_stop_can_be_added(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [mean + 3.5 * sd])
    ns["relative"] = lambda series, level, symbol, basket=None, ref=None, **kw: Relative(
        np.array([1.0, 2.0, 3.0]), 3, now=0.5, n_valid=7, pctile=99.0, symbols=list(basket), series=series, level=level, symbol=symbol)       # far from the middle of its basket
    candle(fake_dh)
    time.sleep(0.4)
    assert wire.sent == []                                                                             # z says SELL, the basket says no
    svc.on_stop()
    fake_dh.clear()
    pct = [mean - 3.5 * sd]
    ns, app, svc, wire, calls = start(tmp_path / "ts", monkeypatch, pct, **{"TARGET_PCT, STOP_PCT = None, None": "TARGET_PCT, STOP_PCT = 1.5, 0.5"})
    candle(fake_dh, 0)
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(1.0)
    pct[0] = mean - 3.4 * sd                                                                           # z still far below 0
    candle(fake_dh, 1, px=102.0)                                                                       # +2% for a long: the target closes it
    assert wait_for(lambda: len(wire.sent) == 2) and wire.sent[1][1] == "squareup"
    svc.on_stop()


def test_no_entry_once_the_time_exit_has_come(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [mean - 4.0 * sd], **{"EXIT_AT = dtime(23, 59, 59)": "EXIT_AT = dtime(0, 0, 1)"})
    candle(fake_dh)
    time.sleep(0.3)
    assert wire.sent == []
    svc.on_stop()


def test_the_status_shows_the_z_of_each_stock_and_bad_inputs_are_refused(tmp_path, fake_dh, monkeypatch, capsys):
    mean, sd, _ = mean_sd()
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [mean + 1.0 * sd])
    capsys.readouterr()
    ns["us_zscore_status"]()
    out = capsys.readouterr().out
    assert "z-score ready for 1 of 1 stocks" in out and "NIFTY +1.00" in out
    leaked = sorted(n for n in set(ns) if n.startswith("us_zscore"))
    assert leaked == ["us_zscore_exit_all", "us_zscore_status", "us_zscore_stop"]
    svc.on_stop()
    for old, new in (("Z_DAYS = 90", "Z_DAYS = 2"), ("Z_MIN_DAYS = 20", "Z_MIN_DAYS = 1"), ("Z_ENTRY = 3.0", "Z_ENTRY = 0")):
        with pytest.raises(ValueError, match="Z_DAYS 5 to 560, Z_MIN_DAYS at least 3, Z_ENTRY above 0"):
            start(tmp_path / old.split()[0], monkeypatch, [0.0], **{old: new})


def test_z_bar_live_looks_at_z_on_every_minute_instead_of_only_when_a_bar_ends(tmp_path, fake_dh, monkeypatch):
    mean, sd, _ = mean_sd()
    ns, app, svc, wire, calls = start(tmp_path, monkeypatch, [mean - 3.5 * sd], **{"Z_BAR_LIVE = False": "Z_BAR_LIVE = True"})
    candle(fake_dh, minute=1)                                                                          # the 2nd minute of a bar: with Z_BAR_LIVE it already acts
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    svc.on_stop()
