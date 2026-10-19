"""examples/08_us_basket_rsi_crossorder.py: the US version of 07: one basket of 19 stocks, New York hours, a 5-minute RSI made from 1-minute candles and seeded from earlier saved days.
Real registry, book, bridge and NATS service; fake wire and fake Deephaven listener; relative() returns the real Relative type; the history files are real Parquet."""
import ast
import re
import time

import numpy as np
import pandas as pd
import pytest

from algo.series.absolute import Absolute, Relative
from tests.test_examples import EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

SCRIPT = (EXAMPLES / "08_us_basket_rsi_crossorder.py").read_text(encoding="utf-8")
def basket_block(src):
    """The text of the `    BASKETS = {...}` statement (one line or many) and its start and end in `src`."""
    start = src.index("\n    BASKETS = {") + 1
    i = src.index("{", start)
    depth = 0
    for j in range(i, len(src)):
        depth += {"{": 1, "}": -1}.get(src[j], 0)
        if depth == 0:
            end = src.index("\n", j) + 1
            return src[start:end], start, end
    raise AssertionError("the BASKETS statement is not closed")
ALWAYS_OPEN = "WINDOW = (dtime(0, 0), dtime(23, 59, 59))"
OPEN_WINDOW = re.search(r"WINDOW = [(]dtime[(]\d+, \d+[)], dtime[(]\d+, \d+[)][)]", SCRIPT).group(0)             # whatever times the owner has typed
ENGINE_NAME = re.search(r'"strategyname": "([^"]+)"', SCRIPT).group(1)                                              # the strategy name the owner has typed (CrossOrder or single)
USER_ID = int(re.search(r"^    USER = (\d+)", SCRIPT, re.M).group(1))                                                 # the user the script sends as
MY_BASKET = '    BASKETS = {"us_stocks": ["NIFTY", "A1", "A2"]}\n'                         # NIFTY is the one contract the test master has (token 1)
DAY0 = np.datetime64("2026-10-19T14:00")                                                # the first live minute (UTC), after the history days


def start(tmp_path, monkeypatch, status="ok", window=ALWAYS_OPEN, basket=MY_BASKET, abs_pct=50.0, abs_idx=11, rsi_live=True, **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, status)
    calls = []

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        calls.append((symbol, tuple(basket), ref))
        return Relative(np.array([1.0, 2.0, 3.0]), 3, now=0.5, n_valid=7, pctile=50.0, symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        absolute.calls.append((series, level, symbol, n, ref))
        return Absolute(np.arange(float(n)), abs_idx, now=0.5, n_valid=n if abs_idx >= 0 else 3, pctile=abs_pct if abs_idx >= 0 else float("nan"), series=series, level=level, symbol=symbol)

    absolute.calls = []
    absolute.warmed = []
    ns["absolute_warm"] = lambda series, level, symbols, n=22, ref=None, **kw: absolute.warmed.append((series, level, tuple(symbols), n, ref))
    ns["relative"] = relative
    ns["absolute"] = absolute
    ns["basket"] = lambda *a, **k: None
    assert OPEN_WINDOW in SCRIPT and USER_ID in (3, 4)                                                # the test service allows users 3 and 4
    _, b0, b1 = basket_block(SCRIPT)
    src = (SCRIPT[:b0] + basket + SCRIPT[b1:]).replace(OPEN_WINDOW, window)
    src = re.sub(r"RSI_LIVE = (True|False)", f"RSI_LIVE = {rsi_live}", src)                            # the owner's choice is an input; each test says which it means
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    before = set(ns)
    exec(compile(src, "08_us_basket_rsi_crossorder.py", "exec"), ns)
    return ns, app, svc, wire, calls, before


def feed(fake_dh, closes, first=0):
    """The traded stock's 1-minute closes, one candle each, from DAY0 + `first` minutes."""
    for i, c in enumerate(closes):
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [float(c)], "Minute": [DAY0 + np.timedelta64(first + i, "m")]}), False)


def write_history(folder, days, start_px=200.0, step=-0.25, symbol="NIFTY", minutes=60):
    """candles_<day>.parquet files in the real format: one falling stock, `minutes` 1-minute bars a day, UTC bar starts."""
    last = start_px
    for n, day in enumerate(days):
        base = pd.Timestamp(f"{day} 14:30", tz="UTC")
        rows = [(base + pd.Timedelta(minutes=i), symbol, 1, last + step * i) for i in range(minutes)]
        last = rows[-1][3] + step
        df = pd.DataFrame(rows, columns=["Minute", "Symbol", "token", "Close"])
        df["Open"] = df["High"] = df["Low"] = df["Close"]
        df["Volume"] = 100
        df["Date"] = day
        df = pd.concat([df, df.assign(Symbol="OTHER", token=2)])                                       # another stock that must be ignored
        pq.write_table(pa.Table.from_pandas(df.sort_values(["Symbol", "Minute"]), preserve_index=False), str(folder / f"candles_{day}.parquet"))
    return last


def test_the_basket_is_one_basket_of_us_tickers_in_new_york_time():
    block = basket_block(SCRIPT)[0]
    assert re.findall(r'"([a-z_]+)":', block) == ["us_stocks"]                                      # ONE basket
    tickers = re.findall(r'"([A-Z]+)"', block)
    assert len(tickers) >= 3 and len(set(tickers)) == len(tickers)                                # at least 3, none twice
    assert "America/New_York" in SCRIPT and "Asia/Kolkata" not in SCRIPT                           # no India clock left in it
    assert 'RSI_BAR = "5min"' in SCRIPT and "HIST_DAYS = " in SCRIPT and "ABS_DAYS = 22" in SCRIPT


def test_without_history_the_rsi_needs_ten_five_minute_bars_and_then_a_fall_sends_one_buy(tmp_path, fake_dh, monkeypatch, capsys):
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "history: no candles_<day>.parquet" in out
    leaked = sorted(n for n in set(ns) - before if not n.startswith("us_basket_rsi") and n != "run_us_basket_rsi" and not n.startswith("__"))
    assert leaked == []                                                                           # nothing that could overwrite another script's names
    feed(fake_dh, [100.0 - 0.25 * i for i in range(45)])                                          # 45 minutes = 9 five-minute bars: one short
    time.sleep(0.2)
    assert wire.sent == []
    ns["us_basket_rsi_status"]()
    assert "RSI is ready for 0 of 1" in capsys.readouterr().out
    feed(fake_dh, [100.0 - 0.25 * i for i in range(45, 50)], first=45)                            # the 10th bar: RSI 0, oversold
    assert wait_for(lambda: len(wire.sent) == 1)
    subject, label, payload = wire.sent[0]
    assert (subject, label) == ("cmd.hft.1001", "subscribe")
    assert {k: v for k, v in payload.items() if k not in ("USERIDENTIFIER", "user_id")} == {
        "strategyname": ENGINE_NAME, "TOKEN": FUT, "SIDE": 1, "QTY": 1, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}
    assert [c[2] for c in calls] == ["open", "vwap"] and calls[0][1] == ("NIFTY", "A1", "A2")      # ranked against its own basket
    svc.on_stop()


def test_earlier_days_seed_the_rsi_so_the_very_first_live_candle_can_trade(tmp_path, fake_dh, monkeypatch, capsys):
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"])                    # 3 falling days; also a file of the future that must be ignored
    write_history(tmp_path, ["2099-01-01"], start_px=500.0, step=+1.0)
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "history: 3 earlier day(s) (2020-01-13, 2020-01-14, 2020-01-15) seed the 5min RSI of 1 of 1 stocks" in out
    ns["us_basket_rsi_status"]()
    assert "RSI is ready for 1 of 1" in capsys.readouterr().out                                    # before a single live candle
    assert wire.sent == []
    feed(fake_dh, [last - 0.25])                                                                  # the first live minute, still falling: oversold through yesterday's prices
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    svc.on_stop()


def test_only_the_last_hist_days_are_used_and_a_missing_or_unreadable_file_is_said(tmp_path, fake_dh, monkeypatch, capsys):
    write_history(tmp_path, ["2020-01-10", "2020-01-13", "2020-01-14", "2020-01-15"])
    (tmp_path / "candles_2020-01-16.parquet").write_bytes(b"not a parquet file")                   # unreadable: left out, said, the rest carry on
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "2020-01-16 could not be read" in out
    assert "2 earlier day(s) (2020-01-14, 2020-01-15)" in out                                      # HIST_DAYS = 3 picked 01-14, 01-15, 01-16; 01-16 failed
    svc.on_stop()
    fake_dh.clear()
    ns, app, svc, wire, calls, before = start(tmp_path / "none", monkeypatch, **{"HIST_DAYS = 3": "HIST_DAYS = 0"})
    ns["us_basket_rsi_status"]()
    assert "RSI is ready for 0 of 1" in capsys.readouterr().out                                    # HIST_DAYS = 0: no history is read
    svc.on_stop()


def test_the_target_closes_with_squareup_and_a_symbol_outside_the_window_or_in_two_baskets_is_refused(tmp_path, fake_dh, monkeypatch):
    last = write_history(tmp_path, ["2020-01-14", "2020-01-15"])
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    entry = last - 0.25
    feed(fake_dh, [entry])
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(1.0)
    feed(fake_dh, [entry * 1.02], first=1)                                                        # +2% in favour of a long: the target
    assert wait_for(lambda: len(wire.sent) == 2) and wire.sent[1][:2] == ("cmd.hft.1001", "squareup")
    svc.on_stop()
    fake_dh.clear()
    ns, app, svc, wire, calls, before = start(tmp_path / "closed", monkeypatch, window="WINDOW = (dtime(0, 0), dtime(0, 1))")
    feed(fake_dh, [100.0 - 0.25 * i for i in range(60)])
    time.sleep(0.2)
    assert wire.sent == []                                                                         # outside the entry window nothing is sent
    svc.on_stop()
    fake_dh.clear()
    with pytest.raises(ValueError, match="in two baskets"):
        start(tmp_path / "dup", monkeypatch, basket='    BASKETS = {"a": ["NIFTY", "A1", "A2"], "b": ["B1", "A1", "B3"]}\n')
    with pytest.raises(ValueError, match="at least 3 symbols"):
        start(tmp_path / "small", monkeypatch, basket='    BASKETS = {"a": ["NIFTY", "A1"]}\n')


def test_the_stocks_own_22_day_comparison_must_also_be_in_q2_to_q3_and_is_asked_before_the_basket(tmp_path, fake_dh, monkeypatch):
    for i, (pct, idx) in enumerate(((10.0, 2), (90.0, 20), (24.9, 5), (50.0, -1))):             # below Q2, above Q3, just under the edge, and "cannot tell" (too few days)
        fake_dh.clear()
        folder = tmp_path / f"out{i}"
        folder.mkdir()
        last = write_history(folder, ["2020-01-14", "2020-01-15"])                              # a falling history: the RSI is oversold, so only the filters decide
        ns, app, svc, wire, calls, before = start(folder, monkeypatch, abs_pct=pct, abs_idx=idx)
        feed(fake_dh, [last - 0.25])
        time.sleep(0.2)
        assert wire.sent == [] and calls == []                                                  # no order, and the basket ranking was never even asked
        assert ns["absolute"].calls and all(c[:4] == ("pct", "intraday", "NIFTY", 22) for c in ns["absolute"].calls)
        svc.on_stop()
    fake_dh.clear()
    folder = tmp_path / "edge"
    folder.mkdir()
    last = write_history(folder, ["2020-01-14", "2020-01-15"])
    ns, app, svc, wire, calls, before = start(folder, monkeypatch, abs_pct=25.0, abs_idx=5)     # exactly on the edge of Q2: inside
    feed(fake_dh, [last - 0.25])
    assert wait_for(lambda: len(wire.sent) == 1)
    assert [c[4] for c in ns["absolute"].calls[:2]] == ["open", "vwap"] and [c[2] for c in calls] == ["open", "vwap"]    # both references, for itself and then for its basket
    assert ns["absolute"].warmed == [("pct", "intraday", ("NIFTY",), 22, "open"), ("pct", "intraday", ("NIFTY",), 22, "vwap")]      # preloaded at paste time, for both references
    svc.on_stop()


def test_with_abs_days_zero_the_comparison_is_off_and_never_asked(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, abs_pct=1.0, abs_idx=0, **{"ABS_DAYS = 22 ": "ABS_DAYS = 0  "})
    feed(fake_dh, [100.0 - 0.25 * k for k in range(60)])
    assert wait_for(lambda: len(wire.sent) == 1)                                                  # trades although the (unused) comparison would have said no
    assert ns["absolute"].calls == [] and ns["absolute"].warmed == []                              # nothing asked, nothing preloaded
    svc.on_stop()
    fake_dh.clear()
    with pytest.raises(ValueError, match="ABS_DAYS 0 to 400"):
        start(tmp_path / "bad", monkeypatch, **{"ABS_DAYS = 22 ": "ABS_DAYS = 500"})


def test_with_finished_bars_only_the_newest_bar_is_left_out_so_one_more_bar_is_needed(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, rsi_live=False)
    feed(fake_dh, [100.0 - 0.25 * i for i in range(50)])                                          # 10 five-minute bars, the 10th still forming: 9 finished
    time.sleep(0.2)
    assert wire.sent == []
    feed(fake_dh, [100.0 - 0.25 * i for i in range(50, 55)], first=50)                            # the 11th bar begins: 10 finished bars, RSI 0
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    svc.on_stop()
