"""examples/14_us_ema_cross_crossorder.py: the US version of 13. Entry on the EMA 5/10 cross of the 5-minute closes, looked at once per finished bar, only when the stock is EXTREME
(percentile below 25 or above 75) against its basket and against its own days, for the % from the open and the % from the VWAP. USE_IV is False (the US has no IV series)."""
import re
import time

import numpy as np
import pytest

from algo.series.absolute import Absolute, Relative
from tests.test_examples import EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)
from tests.test_us_basket_rsi import ALWAYS_OPEN, DAY0, MY_BASKET, basket_block, write_history

pytest.importorskip("pyarrow")

SCRIPT = (EXAMPLES / "14_us_ema_cross_crossorder.py").read_text(encoding="utf-8")
OPEN_WINDOW = re.search(r"WINDOW = [(]dtime[(]\d+, \d+[)], dtime[(]\d+, \d+[)][)]", SCRIPT).group(0)


def start(tmp_path, monkeypatch, pct=50.0, iv_cannot=False, **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, "ok")
    calls = []

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        calls.append(("relative", series, symbol))
        return Relative(np.array([1.0, 2.0, 3.0]), -1 if (series == "iv" and iv_cannot) else 3, now=0.5, n_valid=7, pctile=pct, symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        calls.append(("absolute", series, symbol))
        return Absolute(np.arange(float(n)), -1 if (series == "iv" and iv_cannot) else 11, now=0.5, n_valid=n, pctile=pct, series=series, level=level, symbol=symbol)

    ns["absolute_warm"] = lambda *a, **k: None
    ns["relative"], ns["absolute"], ns["basket"] = relative, absolute, (lambda *a, **k: None)
    _, b0, b1 = basket_block(SCRIPT)
    src = (SCRIPT[:b0] + MY_BASKET + SCRIPT[b1:]).replace(OPEN_WINDOW, ALWAYS_OPEN)
    assert "EXIT_AT = dtime(15, 55)" in src
    src = src.replace("EXIT_AT = dtime(15, 55)", "EXIT_AT = dtime(23, 59, 59)")                           # late: the real clock never matters
    src = re.sub(r"HIST_DAYS = \d+", "HIST_DAYS = 3", src)                                                 # three falling days = 36 five-minute bars, more than the 20 the EMAs need
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    exec(compile(src, "14_us_ema_cross_crossorder.py", "exec"), ns)
    return ns, svc, wire, calls


def run(tmp_path, fake_dh, monkeypatch, rising_history, pct=50.0, **kw):
    """Three days of a falling (or rising) stock, then live candles that REVERSE sharply. Returns (svc, wire, entries, calls): entries = the live minute at which each subscribe appeared."""
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25 if rising_history else -0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=pct, **kw)
    entries = []
    for i in range(40):                                                                                # 8 bars of one candle per minute
        price = last + (-10.0 if rising_history else +10.0) * (i // 5 + 1) + (i % 5) * 0.01
        before = len(wire.sent)
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
        if len(wire.sent) > before:
            entries.append(i)
    return svc, wire, entries, calls


def test_the_script_is_the_us_one_with_the_iv_off():
    block = basket_block(SCRIPT)[0]
    tickers = re.findall(r"\"([A-Z]+)\"", block.split(":", 1)[1])
    assert re.findall(r"\"([a-z_]+)\":", block) == ["us_stocks"] and len(tickers) >= 3 and len(set(tickers)) == len(tickers)       # ONE basket of US tickers, none twice
    assert "America/New_York" in SCRIPT and "Asia/Kolkata" not in SCRIPT and "straddle_start" not in SCRIPT
    assert "USE_IV = False" in SCRIPT and "EMA_FAST, EMA_SLOW = 5, 10" in SCRIPT and 'EMA_BAR = "5min"' in SCRIPT and "ACT_ON_BAR_END = True" in SCRIPT
    assert "def cross_of(values):" in SCRIPT and "before <= 0 < now" in SCRIPT and "before >= 0 > now" in SCRIPT and "RSI" not in SCRIPT


def test_a_bullish_cross_with_a_strong_stock_buys_once_and_only_on_the_candle_that_ends_a_bar(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=90.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and subs[0]["TOKEN"] == FUT                          # one BUY, on the cross
    assert entries and entries[0] % 5 == 4                                                              # the candle that ends a 5-minute bar, not a minute in the middle of one
    assert not [c for c in calls if c[1] == "iv"]                                                       # USE_IV = False: the IV is never asked
    svc.on_stop()


def test_a_cross_with_a_middle_stock_does_not_trade(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=50.0)
    assert wire.sent == []
    svc.on_stop()


def test_a_bearish_cross_with_a_weak_stock_sells(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, rising_history=True, pct=10.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 2 and entries[0] % 5 == 4
    svc.on_stop()


def test_a_bullish_cross_with_a_weak_stock_is_refused_when_the_direction_must_match_and_taken_when_it_need_not(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=10.0)
    assert wire.sent == []
    svc.on_stop()
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    svc, wire, entries, calls = run(tmp_path / "b", fake_dh, monkeypatch, rising_history=False, pct=10.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]
    svc.on_stop()


def test_with_the_iv_on_an_iv_that_cannot_be_told_stops_the_entry(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=90.0, iv_cannot=True, **{"USE_IV = False": "USE_IV = True"})
    assert wire.sent == [] and [c for c in calls if c[1] == "iv"]                                       # the IV was asked and could not be told: no trade
    svc.on_stop()


def run_two_crosses(tmp_path, fake_dh, monkeypatch, **replace):
    """Falling history, then live candles that rise sharply (cross up -> BUY, the next candle's jump hits the target), then fall sharply (cross down). Returns the subscribes sent."""
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=-0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=90.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False", **replace})
    price = last
    for i in range(80):
        bar = i // 5
        price = last + (10.0 * (bar + 1) if bar < 8 else 10.0 * (16 - bar)) + (i % 5) * 0.01             # 8 bars up, then 8 bars down
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"], [lab for _, lab, p in wire.sent]


def test_with_one_entry_per_stock_a_stock_is_not_entered_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    subs, labels = run_two_crosses(tmp_path, fake_dh, monkeypatch)
    assert "ONE_ENTRY_PER_STOCK = True" in SCRIPT
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and "squareup" in labels                              # the BUY, its exit, and the later cross down is left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    subs, labels = run_two_crosses(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    assert [p["SIDE"] for p in subs] == [1, 2]                                                          # with the rule off the same candles enter again, short, on the cross down
