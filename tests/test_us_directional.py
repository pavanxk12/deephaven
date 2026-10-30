"""examples/14_us_directional_crossorder.py: the US version of 13, without an EMA trend filter. Entry at the end of a finished 5-minute bar, only when three filters agree on one side:
TODAY (the latest bar close is the day's high or low), RELATIVE (BUY needs the BEST of the basket on both the % from open and the % from VWAP, SELL needs the WEAK side on both) and
ABSOLUTE (BUY above 90, SELL below 20, against its own days, on both refs). USE_IV is False (the US has no IV series)."""
import re
import time

import numpy as np
import pytest

from algo.series.absolute import Absolute, Relative
from tests.test_examples import EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)
from tests.test_us_basket_rsi import ALWAYS_OPEN, DAY0, MY_BASKET, basket_block, write_history

pytest.importorskip("pyarrow")

SCRIPT = (EXAMPLES / "14_us_directional_crossorder.py").read_text(encoding="utf-8")
OPEN_WINDOW = re.search(r"WINDOW = [(]dtime[(]\d+, \d+[)], dtime[(]\d+, \d+[)][)]", SCRIPT).group(0)


def start(tmp_path, monkeypatch, pct=50.0, iv_cannot=False, **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, "ok")
    calls = []

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        calls.append(("relative", series, symbol))
        offset = (pct - 50.0) / 25.0                                  # so the basket's own average tracks pct too: positive above 50, negative below, ~0 at the middle
        return Relative(np.array([-1.0, 0.0, 1.0]) + offset, -1 if (series == "iv" and iv_cannot) else 3, now=0.5, n_valid=7, pctile=pct, symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        calls.append(("absolute", series, symbol))
        return Absolute(np.arange(float(n)), -1 if (series == "iv" and iv_cannot) else 11, now=0.5, n_valid=n, pctile=pct, series=series, level=level, symbol=symbol)

    ns["absolute_warm"] = lambda *a, **k: None
    ns["relative"], ns["absolute"], ns["basket"] = relative, absolute, (lambda *a, **k: None)
    _, b0, b1 = basket_block(SCRIPT)
    src = (SCRIPT[:b0] + MY_BASKET + SCRIPT[b1:]).replace(OPEN_WINDOW, ALWAYS_OPEN)
    assert "EXIT_AT = dtime(15, 55)" in src
    src = src.replace("EXIT_AT = dtime(15, 55)", "EXIT_AT = dtime(23, 59, 59)")                           # late: the real clock never matters
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    exec(compile(src, "14_us_directional_crossorder.py", "exec"), ns)
    return ns, svc, wire, calls


def run(tmp_path, fake_dh, monkeypatch, up, pct=50.0, **kw):
    """Three days of a rising (up) or falling stock, then live candles that carry on the same way, 10 per bar. Returns (svc, wire, entries, calls): entries = the live minute at which each subscribe appeared."""
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25 if up else -0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=pct, **kw)
    entries = []
    for i in range(40):                                                                                # 8 bars of one candle per minute
        price = last + (+10.0 if up else -10.0) * (i // 5 + 1) + (i % 5) * 0.01
        before = len(wire.sent)
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
        if len(wire.sent) > before:
            entries.append(i)
    return svc, wire, entries, calls


def test_the_script_is_the_us_one_with_the_iv_off():
    block = basket_block(SCRIPT)[0]
    names = re.findall(r"\"([a-z_]+)\":", block)
    tickers = re.findall(r"\"([A-Z][A-Z&-]*)\"", block)
    assert len(names) >= 1 and len(set(names)) == len(names) and len(tickers) >= 3 and len(set(tickers)) == len(tickers)       # one or more named baskets, at least 3 tickers total, none twice
    assert "America/New_York" in SCRIPT and "Asia/Kolkata" not in SCRIPT and "straddle_start" not in SCRIPT
    assert "USE_IV = False" in SCRIPT and 'SIGNAL_BAR = "5min"' in SCRIPT and "ACT_ON_BAR_END = True" in SCRIPT
    assert "def ema(" not in SCRIPT and "def trend_of(" not in SCRIPT and "EMA_FAST" not in SCRIPT and "RSI" not in SCRIPT


def test_the_best_of_the_basket_on_both_refs_buys_once_and_only_on_the_candle_that_ends_a_bar(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=100.0)                   # percentile 100 on both open and vwap: the best of the basket
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and subs[0]["TOKEN"] == FUT                            # one BUY
    assert entries and entries[0] % 5 == 4                                                                # the candle that ends a 5-minute bar, not a minute in the middle of one
    assert not [c for c in calls if c[1] == "iv"]                                                         # USE_IV = False: the IV is never asked
    svc.on_stop()


def test_a_middle_percentile_stock_does_not_trade(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=50.0)                    # neither the best, nor weak enough, on either ref
    assert wire.sent == []
    svc.on_stop()


def test_the_weak_side_on_both_refs_sells(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=False, pct=10.0)                   # below EXTREME[0] (20) on both open and vwap, and below 20 on absolute
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["SIDE"] == 2 and entries[0] % 5 == 4
    svc.on_stop()


def test_a_weak_ranking_is_refused_when_direction_must_match_and_taken_when_it_need_not(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=10.0)                    # weak ranking, but the day is making a new high
    assert wire.sent == []
    svc.on_stop()
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    svc, wire, entries, calls = run(tmp_path / "b", fake_dh, monkeypatch, up=True, pct=10.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False"})
    assert [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"] == [1]                             # either side passes: the day's own high/low decides
    svc.on_stop()


def test_with_the_iv_on_an_iv_that_cannot_be_told_stops_the_entry(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=100.0, iv_cannot=True, **{"USE_IV = False": "USE_IV = True"})
    assert wire.sent == [] and [c for c in calls if c[1] == "iv"]                                         # the IV was asked and could not be told: no trade
    svc.on_stop()


def run_two_sides(tmp_path, fake_dh, monkeypatch, **replace):
    """A weak ranking (so either side can pass once DIRECTION_MATCH is off), live candles that rise sharply (a new day's high -> BUY, the next candle's jump hits the target),
    then fall sharply (a new day's low -> SELL). Returns (subscribes, labels)."""
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=10.0, **{"DIRECTION_MATCH = True": "DIRECTION_MATCH = False", **replace})
    for i in range(80):
        bar = i // 5
        price = last + (10.0 * (bar + 1) if bar < 8 else 80.0 - 15.0 * (bar - 7)) + (i % 5) * 0.01             # 8 bars up, then 8 bars down to a new low of the day
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [price], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"], [lab for _, lab, p in wire.sent]


def test_with_one_entry_per_stock_a_stock_is_not_entered_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    subs, labels = run_two_sides(tmp_path, fake_dh, monkeypatch)
    assert "ONE_ENTRY_PER_STOCK = True" in SCRIPT
    assert len(subs) == 1 and subs[0]["SIDE"] == 1 and "squareup" in labels                               # the BUY, its exit, and the later new low is left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    subs, labels = run_two_sides(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    sides = [p["SIDE"] for p in subs]
    assert sides[0] == 1 and len(sides) >= 2 and 2 in sides                                               # with the rule off it buys on the new high, and sells on the new low


def test_entry_value_makes_the_quantity_int_value_over_the_close(tmp_path, fake_dh, monkeypatch, capsys):
    assert "ENTRY_VALUE = None" in SCRIPT
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=100.0, **{"ENTRY_VALUE = None": "ENTRY_VALUE = 100000.0"})
    px = float(re.search(r"BUY (\d+) lot\(s\) at ([0-9.]+) on PF", capsys.readouterr().out).group(2))
    subs = [p for _, lab, p in wire.sent if lab == "subscribe"]
    assert len(subs) == 1 and subs[0]["QTY"] == int(100000.0 / px) and subs[0]["QTY"] > 1
    svc.on_stop()
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    svc, wire, entries, calls = run(tmp_path / "b", fake_dh, monkeypatch, up=True, pct=100.0, **{"ENTRY_VALUE = None": "ENTRY_VALUE = 1.0"})
    assert wire.sent == [] and "ENTRY_VALUE 1 buys less than one unit" in capsys.readouterr().out
    svc.on_stop()


def test_the_filters_run_in_order_relative_then_absolute_and_absolute_only_for_the_stocks_that_passed(tmp_path, fake_dh, monkeypatch):
    svc, wire, entries, calls = run(tmp_path, fake_dh, monkeypatch, up=True, pct=50.0)                    # the middle band: relative says no side
    assert wire.sent == [] and calls and {c[0] for c in calls} == {"relative"}                            # absolute() was never asked
    svc.on_stop()
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    svc, wire, entries, calls = run(tmp_path / "b", fake_dh, monkeypatch, up=True, pct=100.0)
    first_abs = next(i for i, c in enumerate(calls) if c[0] == "absolute")
    assert len([c for c in calls[:first_abs] if c[0] == "relative"]) >= 2 and len(wire.sent) >= 1         # relative (open, vwap) came before the first absolute
    svc.on_stop()


def two_bars(tmp_path, fake_dh, monkeypatch, first_offset, second_offset, up=True, **replace):
    """History moving up (or down), then two 5-minute bars of live candles at last + first_offset and last + second_offset. Returns the subscribes."""
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25 if up else -0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=100.0 if up else 10.0, **replace)
    for i, off in enumerate([first_offset] * 5 + [second_offset] * 5):
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [last + off + (i % 5) * 0.01], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.2)
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"]


def test_a_buy_needs_the_latest_5_minute_close_to_be_the_high_of_the_day_and_a_sell_the_low(tmp_path, fake_dh, monkeypatch):
    assert "DAY_EXTREME = True" in SCRIPT
    assert two_bars(tmp_path, fake_dh, monkeypatch, 20.0, 10.0) == []                                     # the latest close is below the first bar's: not the day's high
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    subs = two_bars(tmp_path / "b", fake_dh, monkeypatch, 20.0, 10.0, **{"DAY_EXTREME = True": "DAY_EXTREME = False"})
    assert subs and subs[0]["SIDE"] == 1                                                                  # with the filter off the same candles buy
    fake_dh.clear()
    (tmp_path / "c").mkdir()
    subs = two_bars(tmp_path / "c", fake_dh, monkeypatch, 10.0, 20.0)
    assert [p["SIDE"] for p in subs] == [1]                                                                # the second bar closes at a new high of the day: BUY
    fake_dh.clear()
    (tmp_path / "d").mkdir()
    assert two_bars(tmp_path / "d", fake_dh, monkeypatch, -20.0, -10.0, up=False) == []                   # a falling stock whose latest close is above the day's low: no SELL
    fake_dh.clear()
    (tmp_path / "e").mkdir()
    assert [p["SIDE"] for p in two_bars(tmp_path / "e", fake_dh, monkeypatch, -10.0, -20.0, up=False)] == [2]      # a new low of the day: SELL


def flaky_ranking(ns, fails=1):
    """Make the script's relative() raise the Deephaven update-cycle error on its first `fails` calls, then answer as before."""
    calls, orig = {"n": 0}, ns["relative"]

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] <= fails:
            raise RuntimeError("io.deephaven.base.verify.AssertionFailure: asserted Previous values requested or inconsistent from update-processing thread")
        return orig(*a, **k)
    ns["relative"] = flaky
    return calls


def two_up_bars_with_a_flaky_ranking(tmp_path, fake_dh, monkeypatch, capsys, **replace):
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=100.0, **replace)
    flaky_ranking(ns)
    for i in range(11):                                                                                 # bar 0, bar 1 (its last candle is i = 9) and one candle of bar 2
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [last + (10.0 if i < 5 else 20.0 if i < 10 else 30.0) + (i % 5) * 0.01], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        time.sleep(0.05)
    time.sleep(0.3)
    ns["us_directional_status"]()
    out = capsys.readouterr().out
    svc.on_stop()
    return [p for _, lab, p in wire.sent if lab == "subscribe"], out


def test_a_ranking_that_fails_once_is_tried_again_on_the_next_candle_batch_and_the_entry_still_happens(tmp_path, fake_dh, monkeypatch, capsys):
    assert "RETRY_RANKING_S = 20" in SCRIPT
    subs, out = two_up_bars_with_a_flaky_ranking(tmp_path, fake_dh, monkeypatch, capsys)
    assert [p["SIDE"] for p in subs] == [1]                                                               # the failed look at the bar end was repeated on the next batch: one BUY
    assert "trying again on the next candle batch" in out and "ranking errors 1" in out and "entered 1" in out
    assert out.count("the ranking is not available") == 1                                                 # said once, and the message is one short line, not a stack trace
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    subs, out = two_up_bars_with_a_flaky_ranking(tmp_path / "b", fake_dh, monkeypatch, capsys, **{"RETRY_RANKING_S = 20": "RETRY_RANKING_S = 0"})
    assert subs == [] and "ranking errors 1" in out and "trying again" not in out                        # with the retry off that bar is lost, as before


def test_status_counts_where_the_signals_went(tmp_path, fake_dh, monkeypatch, capsys):
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], step=+0.25)
    ns, svc, wire, calls = start(tmp_path, monkeypatch, pct=50.0)                                        # the middle band: relative refuses every look
    for i in range(10):
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [last + (10.0 if i < 5 else 20.0) + (i % 5) * 0.01], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
    ns["us_directional_status"]()
    out = capsys.readouterr().out
    svc.on_stop()
    assert "signals: " in out and "refused by basket" in out and "looked at" in out and "entered" not in out
