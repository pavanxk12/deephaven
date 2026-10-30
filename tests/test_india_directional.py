"""examples/13_india_directional_crossorder.py: 11 without the RSI and without an EMA trend filter. Entry at the end of a finished 5-minute bar, only when three filters agree on one side:
TODAY (the latest bar close is the day's high or low), RELATIVE (BUY needs the BEST of the basket, SELL the WEAK side) and ABSOLUTE (BUY above 90, SELL below 20, against its own days).
Same target / stop-loss / release rules as 11."""
import time

import numpy as np
import pytest

from tests.test_examples import EXAMPLES, Update, wait_for
from tests.test_india_fut import DAY0, start, write_two_contracts
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

pytest.importorskip("pyarrow")

SCRIPT = (EXAMPLES / "13_india_directional_crossorder.py").read_text(encoding="utf-8")
SETTINGS = {"ACT_ON_BAR_END = True": "ACT_ON_BAR_END = True"}                                                   # keeps the helper from switching bar-end entries off
NAME = "13_india_directional_crossorder.py"


def run(tmp_path, fake_dh, monkeypatch, up, pct=50.0, **replace):
    """Candles that rise (up) or fall, 10 per bar. Returns (svc, wire, entries), where entries = the candle minute at which each subscribe appeared."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25 if up else -0.25)
    ns, app, svc, wire, saved, rel, before = start(SCRIPT, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=pct, **{**SETTINGS, **replace})
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


def test_the_best_of_the_basket_buys_once_and_only_on_the_candle_that_ends_a_bar(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0)                          # above EXTREME[1] (75) on both relative() and absolute()
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and subs[0]["TOKEN"] == 100                           # one BUY
    assert entries and entries[0] % 5 == 4                                                               # the candle that ends a 5-minute bar, not a minute in the middle of one
    svc.on_stop()


def test_a_middle_percentile_stock_does_not_trade(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=50.0)           # neither strong nor the worst of the basket: no side
    assert wire.sent == []
    svc.on_stop()


def test_the_worst_of_the_basket_sells(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=False, pct=0.0,                          # the worst of the basket (relative), and below 20 (absolute)
                              **{"USE_IV = True": "USE_IV = False"})                                     # the IV gate needs its own high percentile: off here, it has its own test
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 2 and entries[0] % 5 == 4
    svc.on_stop()


def test_a_weak_ranking_is_refused_when_direction_must_match_and_taken_when_it_need_not(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=0.0, **{"USE_IV = True": "USE_IV = False"})           # weak/worst ranking, but the day is making a new high
    assert wire.sent == []
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=0.0, **{"USE_IV = True": "USE_IV = False", "DIRECTION_MATCH = True": "DIRECTION_MATCH = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                           # either side passes: the day's own high/low decides
    svc.on_stop()


def test_the_iv_must_be_extreme_too_and_an_iv_that_cannot_be_told_stops_the_entry(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0, iv_cannot=True)
    assert wire.sent == []                                                                              # percentiles are strong, the IV says "cannot tell": no trade
    svc.on_stop()
    svc, wire, entries = run(tmp_path, fake_dh, monkeypatch, up=True, pct=90.0, iv_cannot=True, **{"USE_IV = True": "USE_IV = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                           # without the IV filter it trades
    svc.on_stop()


def test_the_script_starts_the_straddle_itself_unless_the_iv_is_off(tmp_path, fake_dh, monkeypatch):
    write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(SCRIPT, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], **SETTINGS)
    assert ns["_straddle_calls"] == [0]                                                                  # straddle_start(0) from the script
    svc.on_stop()
    ns, app, svc, wire, saved, rel, before = start(SCRIPT, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], **{**SETTINGS, "USE_IV = True": "USE_IV = False"})
    assert ns["_straddle_calls"] == []
    svc.on_stop()


def run_two_sides(tmp_path, fake_dh, monkeypatch, **replace):
    """A weak/worst ranking (so either side can pass once DIRECTION_MATCH is off), live candles that rise sharply (hits the day's high -> BUY, the jump then hits the target),
    then fall sharply (a new day's low -> SELL). Returns the subscribes sent."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25)
    ns, app, svc, wire, saved, rel, before = start(SCRIPT, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=0.0,
                                                   **{**SETTINGS, "DIRECTION_MATCH = True": "DIRECTION_MATCH = False", "USE_IV = True": "USE_IV = False", **replace})
    for i in range(80):
        bar = i // 5
        price = last + (10.0 * (bar + 1) if bar < 8 else 80.0 - 15.0 * (bar - 7)) + (i % 5) * 0.01             # 8 bars up, then 8 bars down to a new low of the day
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    svc.on_stop()
    return [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_13_does_not_enter_a_stock_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    assert run_two_sides(tmp_path, fake_dh, monkeypatch) == [1]                                       # the BUY; the later new low is left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    sides = run_two_sides(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    assert sides[0] == 1 and len(sides) >= 2 and 2 in sides                                           # with the rule off it buys on the new high, and sells once it makes a new low


def two_bars(tmp_path, fake_dh, monkeypatch, first_offset, second_offset, up=True, **replace):
    """Candles moving up (or down), then two 5-minute bars of live candles at last + first_offset and last + second_offset. Returns the subscribes."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20", step=+0.25 if up else -0.25)
    ns, app, svc, wire, saved, rel, before = start(SCRIPT, NAME, tmp_path, monkeypatch, unders=["S00", "S01", "S02"], pct=90.0 if up else 0.0,
                                                   **{**SETTINGS, "USE_IV = True": "USE_IV = False", **replace})
    for i, off in enumerate([first_offset] * 5 + [second_offset] * 5):
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [last + off + (i % 5) * 0.01], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.2)
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_13_buy_needs_the_latest_5_minute_close_to_be_the_high_of_the_day_and_a_sell_the_low(tmp_path, fake_dh, monkeypatch):
    assert "DAY_EXTREME = True" in SCRIPT
    assert two_bars(tmp_path, fake_dh, monkeypatch, 20.0, 10.0) == []                                  # the latest close is below the first bar's: not the day's high
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
