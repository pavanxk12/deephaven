"""examples/13_india_fut_ema_cross_crossorder.py: 11 without the RSI. Entry on the EMA 5/10 cross of the 5-minute closes, looked at once per finished bar, only when the stock is EXTREME (percentile
below 25 or above 75) against its basket and against its own days, for the % from the open, the % from the VWAP and the IV. Same target / stop-loss / release rules as 11."""
import time

import numpy as np
import pytest

from tests.test_examples import EXAMPLES, Update, wait_for
from tests.test_india_fut import DAY0, start, write_two_contracts
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

pytest.importorskip("pyarrow")

EMA = (EXAMPLES / "13_india_fut_ema_cross_crossorder.py").read_text(encoding="utf-8")
SETTINGS = {"HIST_DAYS = 1": "HIST_DAYS = 3"}                                                   # three history days: enough bars for the EMAs (the owner's own HIST_DAYS is not what these tests are about)
NAME = "13_india_fut_ema_cross_crossorder.py"


def run(tmp_path, fake_dh, monkeypatch, rising_history, pct=50.0, **replace):
    """Three days of a falling (or rising) stock, then live candles that REVERSE sharply. Returns (svc, wire, entries), where entries = the candle minute at which each subscribe appeared."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25 if rising_history else -0.25)
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=pct, **{**SETTINGS, **replace})
    feed = fake_dh[-1].fn
    entries, price = [], last
    for i in range(40):                                                                                # 8 bars of one candle per minute
        bar = i // 5
        price = last + (-10.0 if rising_history else +10.0) * (bar + 1) + (i % 5) * 0.01
        before_n = len(wire.sent)
        feed(Update(added={"token": [100], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
        if len(wire.sent) > before_n:
            entries.append(i)
    return svc, wire, entries


def test_a_bullish_cross_with_a_strong_stock_buys_once_and_only_on_the_candle_that_ends_a_bar(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=90.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and subs[0]["TOKEN"] == 100                          # one BUY, on the cross
    assert entries and entries[0] % 5 == 4                                                              # the candle that ends a 5-minute bar, not a minute in the middle of one
    svc.on_stop()


def test_a_bullish_cross_with_a_middle_stock_does_not_trade(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=50.0)           # the middle band: no longer what is wanted
    assert wire.sent == []
    svc.on_stop()


def test_a_bearish_cross_with_a_weak_stock_sells(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=True, pct=10.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 2 and entries[0] % 5 == 4
    svc.on_stop()


def test_a_bullish_cross_with_a_weak_stock_is_refused_when_the_direction_must_match_and_taken_when_it_need_not(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=10.0)           # weak, but the cross is up
    assert wire.sent == []
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=10.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                          # either extreme passes
    svc.on_stop()


def test_the_iv_must_be_extreme_too_and_a_iv_that_cannot_be_told_stops_the_entry(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=90.0, iv_cannot=True)
    assert wire.sent == []                                                                              # percentiles are strong, the IV says "cannot tell": no trade
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, rising_history=False, pct=90.0, iv_cannot=True, **{"USE_IV = True": "USE_IV = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                          # without the IV filter it trades
    svc.on_stop()


def test_the_script_starts_the_straddle_itself_unless_the_iv_is_off(tmp_path, fake_dh, monkeypatch):
    write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], **SETTINGS)
    assert ns["_straddle_calls"] == [0]                                                                 # straddle_start(0) from the script
    svc.on_stop()
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], **{**SETTINGS, "USE_IV = True": "USE_IV = False"})
    assert ns["_straddle_calls"] == []
    svc.on_stop()


def test_the_ema_cross_rule_itself():
    """cross_of is inside the script; the same maths here: the fast EMA crossing the slow one on the LAST bar only."""
    def ema(values, n):
        k = 2.0 / (n + 1.0)
        e, out = float(values[0]), [float(values[0])]
        for v in values[1:]:
            e = float(v) * k + e * (1.0 - k)
            out.append(e)
        return out
    down = [100.0 - i for i in range(30)]
    up_last = down + [down[-1] + 25.0]
    fast, slow = ema(up_last, 5), ema(up_last, 10)
    assert fast[-2] - slow[-2] <= 0 < fast[-1] - slow[-1]
    assert "def cross_of(values):" in EMA and "before <= 0 < now" in EMA and "before >= 0 > now" in EMA


def run_two_crosses(tmp_path, fake_dh, monkeypatch, **replace):
    """Falling history, then live candles that rise sharply (cross up -> BUY, the next candle's jump hits the target), then fall sharply (cross down). Returns the subscribes sent."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=-0.25)
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=90.0,
                                                   **{**SETTINGS, "DIRECTION_MATCH = True": "DIRECTION_MATCH = False", **replace})
    for i in range(80):
        bar = i // 5
        price = last + (10.0 * (bar + 1) if bar < 8 else 10.0 * (16 - bar)) + (i % 5) * 0.01             # 8 bars up, then 8 bars down
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    svc.on_stop()
    return [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_13_does_not_enter_a_stock_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    assert run_two_crosses(tmp_path, fake_dh, monkeypatch) == [1]                                       # the BUY; the later cross down is left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    assert run_two_crosses(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"}) == [1, 2]      # with the rule off it sells on the cross down
