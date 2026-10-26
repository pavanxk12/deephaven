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
SETTINGS = {"HIST_DAYS = 1": "HIST_DAYS = 3", "ACT_ON_BAR_END = True": "ACT_ON_BAR_END = True"}                                                   # three history days: enough bars for the EMAs; the second key keeps the helper from switching bar-end entries off
NAME = "13_india_fut_ema_cross_crossorder.py"


def run(tmp_path, fake_dh, monkeypatch, up, pct=50.0, **replace):
    """Three days of a rising (up) or falling stock, then live candles that carry on the same way, 10 per bar. Returns (svc, wire, entries), where entries = the candle minute at which each subscribe appeared."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25 if up else -0.25)
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=pct, **{**SETTINGS, **replace})
    feed = fake_dh[-1].fn
    entries, price = [], last
    for i in range(40):                                                                                # 8 bars of one candle per minute
        bar = i // 5
        price = last + (+10.0 if up else -10.0) * (bar + 1) + (i % 5) * 0.01
        before_n = len(wire.sent)
        feed(Update(added={"token": [100], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
        if len(wire.sent) > before_n:
            entries.append(i)
    return svc, wire, entries


def test_fast_above_slow_with_a_strong_stock_buys_once_and_only_on_the_candle_that_ends_a_bar(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and subs[0]["TOKEN"] == 100                          # one BUY: the fast EMA is above the slow one
    assert entries and entries[0] % 5 == 4                                                              # the candle that ends a 5-minute bar, not a minute in the middle of one
    svc.on_stop()


def test_a_bullish_cross_with_a_middle_stock_does_not_trade(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=50.0)           # the middle band: no longer what is wanted
    assert wire.sent == []
    svc.on_stop()


def test_fast_below_slow_with_a_weak_stock_sells(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=False, pct=10.0)
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 2 and entries[0] % 5 == 4
    svc.on_stop()


def test_fast_above_slow_with_a_weak_stock_is_refused_when_the_direction_must_match_and_taken_when_it_need_not(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=10.0)           # weak, but the cross is up
    assert wire.sent == []
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=10.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                          # either extreme passes
    svc.on_stop()


def test_the_iv_must_be_extreme_too_and_a_iv_that_cannot_be_told_stops_the_entry(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0, iv_cannot=True)
    assert wire.sent == []                                                                              # percentiles are strong, the IV says "cannot tell": no trade
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0, iv_cannot=True, **{"USE_IV = True": "USE_IV = False"})
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


def test_the_ema_trend_rule_itself():
    """trend_of is inside the script; the same maths here: long while the fast EMA is above the slow one, short while below, none with too few bars."""
    def ema(values, n):
        k = 2.0 / (n + 1.0)
        e, out = float(values[0]), [float(values[0])]
        for v in values[1:]:
            e = float(v) * k + e * (1.0 - k)
            out.append(e)
        return out
    rising, falling = [100.0 + i for i in range(30)], [100.0 - i for i in range(30)]
    assert ema(rising, 5)[-1] > ema(rising, 10)[-1] and ema(falling, 5)[-1] < ema(falling, 10)[-1]
    assert "def trend_of(values):" in EMA and "diff > 0" in EMA and "diff < 0" in EMA and "cross_of" not in EMA and "len(values) < 2 * EMA_SLOW" in EMA


def run_two_crosses(tmp_path, fake_dh, monkeypatch, **replace):
    """Falling history, then live candles that rise sharply (fast above slow -> BUY, the next candle's jump hits the target), then fall sharply (fast below slow). Returns the subscribes sent."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25)
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=90.0,
                                                   **{**SETTINGS, "DIRECTION_MATCH = True": "DIRECTION_MATCH = False", **replace})
    for i in range(80):
        bar = i // 5
        price = last + (10.0 * (bar + 1) if bar < 8 else 80.0 - 15.0 * (bar - 7)) + (i % 5) * 0.01             # 8 bars up, then 8 bars down to a new low of the day
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    svc.on_stop()
    return [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_13_does_not_enter_a_stock_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    assert run_two_crosses(tmp_path, fake_dh, monkeypatch) == [1]                                       # the BUY; the later fall is left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    sides = run_two_crosses(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    assert sides[0] == 1 and len(sides) >= 2 and 2 in sides                                            # with the rule off it buys again while fast is above slow, and sells once it is below


def two_bars(tmp_path, fake_dh, monkeypatch, first_offset, second_offset, up=True, **replace):
    """History moving up (or down), then two 5-minute bars of live candles at last + first_offset and last + second_offset. Returns the subscribes."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25 if up else -0.25)
    ns, app, svc, wire, saved, rel, before = start(EMA, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=90.0 if up else 10.0, **{**SETTINGS, **replace})
    for i, off in enumerate([first_offset] * 5 + [second_offset] * 5):
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [last + off + (i % 5) * 0.01], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.2)
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_13_buy_needs_the_latest_5_minute_close_to_be_the_high_of_the_day_and_a_sell_the_low(tmp_path, fake_dh, monkeypatch):
    assert "DAY_EXTREME = True" in EMA
    assert two_bars(tmp_path, fake_dh, monkeypatch, 20.0, 10.0) == []                                   # fast is above slow, but the latest close is below the first bar's: not the day's high
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    subs = two_bars(tmp_path / "b", fake_dh, monkeypatch, 20.0, 10.0, **{"DAY_EXTREME = True": "DAY_EXTREME = False"})
    assert subs and subs[0]["SIDE"] == 1
    fake_dh.clear()
    (tmp_path / "c").mkdir()
    assert [p["SIDE"] for p in two_bars(tmp_path / "c", fake_dh, monkeypatch, 10.0, 20.0)] == [1]
    fake_dh.clear()
    (tmp_path / "d").mkdir()
    assert two_bars(tmp_path / "d", fake_dh, monkeypatch, -20.0, -10.0, up=False) == []
    fake_dh.clear()
    (tmp_path / "e").mkdir()
    assert [p["SIDE"] for p in two_bars(tmp_path / "e", fake_dh, monkeypatch, -10.0, -20.0, up=False)] == [2]
