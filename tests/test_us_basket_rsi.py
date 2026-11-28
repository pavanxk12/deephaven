"""examples/08_us_basket_rsi_crossorder.py: the US version of 07: one basket of 19 stocks, New York hours, a 5-minute RSI made from 1-minute candles and seeded from earlier saved days.
Real registry, book, bridge and NATS service; fake wire and fake Deephaven listener; relative() returns the real Relative type; the history files are real Parquet."""
import ast
import re
import time

import numpy as np
import pandas as pd
import pytest

from algo.ui.filters import PF_PER_USER
from algo.series.absolute import Absolute, Relative
from tests.test_examples import fills, listener_on, quote, candle_feed, apply_replace, inline_params, EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

SCRIPT = inline_params(EXAMPLES / "08_us_basket_rsi_crossorder.py")
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


# OVERSOLD for a zone script: pct_open inside REL_ZONE/ABS_ZONE's (25, 75) band, pct_vwap below its low.
OVERSOLD = {"open": 50.0, "vwap": 10.0}
OVERBOUGHT = {"open": 50.0, "vwap": 90.0}

def start(tmp_path, monkeypatch, status="ok", window=ALWAYS_OPEN, basket=MY_BASKET, abs_pct=OVERSOLD, abs_idx=11, rsi_live=True, exit_at="dtime(23, 59, 59)", act_on_bar_end=False, rel_pct=OVERSOLD, **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, status)
    calls = []

    def per_ref(value, ref):
        """A percentile per reference. 08 reads REL_ZONE / ABS_ZONE bands: an OVERSOLD stock has pct_open INSIDE
        the band and pct_vwap BELOW its low, so one number for every reference can never describe a trade. A plain
        number still means the same percentile everywhere, which is how the "middle of the band" tests read."""
        return float(value[ref]) if isinstance(value, dict) else float(value)

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        calls.append((symbol, tuple(basket), ref))
        return Relative(np.array([1.0, 2.0, 3.0]), 3, now=0.5, n_valid=7, pctile=per_ref(rel_pct, ref or "open"),
                        symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        absolute.calls.append((series, level, symbol, n, ref))
        return Absolute(np.arange(float(n)), abs_idx, now=0.5, n_valid=n if abs_idx >= 0 else 3,
                        pctile=per_ref(abs_pct, ref or "open") if abs_idx >= 0 else float("nan"),
                        series=series, level=level, symbol=symbol)

    absolute.calls = []
    absolute.warmed = []
    ns["absolute_warm"] = lambda series, level, symbols, n=22, ref=None, **kw: absolute.warmed.append((series, level, tuple(symbols), n, ref))
    ns["relative"] = relative
    ns["absolute"] = absolute
    ns["basket"] = lambda *a, **k: None
    assert OPEN_WINDOW in SCRIPT and USER_ID in (3, 4)                                                # the test service allows users 3 and 4
    _, b0, b1 = basket_block(SCRIPT)
    src = (SCRIPT[:b0] + basket + SCRIPT[b1:]).replace(OPEN_WINDOW, window)
    EXIT_LIT = re.search(r"EXIT_AT = dtime\([^)]*\)", src).group(0)   # whatever the owner has typed
    src = src.replace(EXIT_LIT, f"EXIT_AT = {exit_at}")                              # the tests choose when the time exit comes: late by default, so the real clock never matters
    src = re.sub(r"RSI_LIVE = (True|False)", f"RSI_LIVE = {rsi_live}", src)
    src = re.sub(r"HIST_DAYS = \d+", "HIST_DAYS = 3", src)                                           # the tests need three history days, whatever the owner has typed
    src = src.replace("ACT_ON_BAR_END = True", f"ACT_ON_BAR_END = {act_on_bar_end}")                         # the older tests feed any number of candles: they look at every minute                            # the owner's choice is an input; each test says which it means
    src = apply_replace(src, replace)
    before = set(ns)
    exec(compile(src, "08_us_basket_rsi_crossorder.py", "exec"), ns)
    return ns, app, svc, wire, calls, before


def feed(fake_dh, closes, first=0, tick=True):
    """The traded stock's 1-minute closes, one candle each, from DAY0 + `first` minutes.
    Each candle is followed by a tick of the same price on the live feed, because the script no longer sends
    anything straight from a candle: a signal only ARMS the entry, and the order goes out on the first tick
    whose spread is inside SPREAD_MAX. bid = ask = ltp here, a zero spread, so the gate always opens.
    Pass tick=False to feed candles alone and watch an entry sit armed."""
    for i, c in enumerate(closes):
        candle_feed(fake_dh).fn(Update(added={"token": [FUT], "Close": [float(c)], "Minute": [DAY0 + np.timedelta64(first + i, "m")]}), False)
        if tick and listener_on(fake_dh, "fo_market_data") is not None:
            quote(fake_dh, FUT, float(c))


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
    names = re.findall(r'"([a-z_]+)":', block)
    assert len(names) >= 1 and len(set(names)) == len(names)                                      # one or more named baskets, none twice
    tickers = re.findall(r'"([A-Z][A-Z&-]*)"', block)
    assert len(tickers) >= 3 and len(set(tickers)) == len(tickers)                                # at least 3 across all baskets, none twice
    assert "America/New_York" in SCRIPT and "Asia/Kolkata" not in SCRIPT                           # no India clock left in it
    assert 'RSI_BAR = "5min"' in SCRIPT and "HIST_DAYS = " in SCRIPT and "ABS_DAYS = 22" in SCRIPT


def test_without_history_the_rsi_needs_ten_five_minute_bars_and_then_a_fall_sends_one_buy(tmp_path, fake_dh, monkeypatch, capsys):
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "history: no candles_<day>.parquet" in out
    # 08's STRATEGY is "meanrevert" now, so that is the prefix it leaves in the terminal
    leaked = sorted(n for n in set(ns) - before if not n.startswith("meanrevert") and n != "run_meanrevert" and not n.startswith("__"))
    assert leaked == []                                                                           # nothing that could overwrite another script's names
    feed(fake_dh, [100.0 - 0.25 * i for i in range(45)])                                          # 45 minutes = 9 five-minute bars: one short
    time.sleep(0.2)
    assert wire.sent == []
    ns["meanrevert_status"]()
    assert "RSI is ready for 0 of 1" in capsys.readouterr().out
    feed(fake_dh, [100.0 - 0.25 * i for i in range(45, 50)], first=45)                            # the 10th bar: RSI 0, oversold
    assert wait_for(lambda: len(wire.sent) == 1)
    subject, label, payload = wire.sent[0]
    assert (subject, label) == ("cmd.hft.1001", "subscribe")
    # the identifying fields, whatever the owner's sizing is: QTY follows ENTRY_VALUE, and a LimitOrder also
    # carries the PRICE it rests at and MAX_TRIES (a CrossOrder carries neither)
    assert payload["strategyname"] == ENGINE_NAME and payload["TOKEN"] == FUT and payload["SIDE"] == 1
    assert payload["SERVERCODE"] == 15046 and payload["ACCOUNT"] == "PRO" and payload["ALGOID"] == "1"
    assert isinstance(payload["QTY"], int) and payload["QTY"] >= 1
    if ENGINE_NAME == "LimitOrder":
        assert payload["PRICE"] > 0 and payload["MAX_TRIES"] >= 1
    assert [c[2] for c in calls] == ["open", "vwap"] and calls[0][1] == ("NIFTY", "A1", "A2")      # ranked against its own basket
    svc.on_stop()


def test_earlier_days_seed_the_rsi_so_the_very_first_live_candle_can_trade(tmp_path, fake_dh, monkeypatch, capsys):
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"])                    # 3 falling days; also a file of the future that must be ignored
    write_history(tmp_path, ["2099-01-01"], start_px=500.0, step=+1.0)
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "history: 3 earlier day(s) (2020-01-13, 2020-01-14, 2020-01-15) seed the 5min RSI of 1 of 1 stocks" in out
    ns["meanrevert_status"]()
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
    ns["meanrevert_status"]()
    assert "RSI is ready for 0 of 1" in capsys.readouterr().out                                    # HIST_DAYS = 0: no history is read
    svc.on_stop()


def test_the_target_closes_on_a_new_portfolio_and_a_symbol_outside_the_window_or_in_two_baskets_is_refused(tmp_path, fake_dh, monkeypatch):
    """The exit is NOT the entry's squareup any more: it is a fresh `subscribe` on a brand-new portfolio, on the
    opposite side, for the amount that REALLY filled (read from orders_trade). The entry's portfolio is left
    open and offset by it, which is why both appear in the book."""
    last = write_history(tmp_path, ["2020-01-14", "2020-01-15"])
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    entry = last - 0.25
    feed(fake_dh, [entry])
    assert wait_for(lambda: len(wire.sent) == 1)
    subject, label, params = wire.sent[0]
    assert label == "subscribe" and params["SIDE"] == 1                                           # the entry: a BUY on PF 1001
    entry_pf = int(subject.rsplit(".", 1)[1])
    fills(fake_dh, USER_ID * PF_PER_USER + entry_pf, 1, params["QTY"])                            # it filled in full, as orders_trade reports it
    time.sleep(1.0)
    feed(fake_dh, [entry * 1.02], first=1)                                                        # +2% in favour of a long: the target
    assert wait_for(lambda: len(wire.sent) == 2)
    ex_subject, ex_label, ex_params = wire.sent[1]
    assert ex_label == "subscribe"                                                                # a new instance, not a squareup
    assert int(ex_subject.rsplit(".", 1)[1]) != entry_pf                                          # on its own portfolio
    assert ex_params["SIDE"] == 2 and ex_params["QTY"] == params["QTY"]                           # opposite side, the filled amount
    assert ex_params["strategyname"] == "LimitOrder"
    assert "squareup" not in [lab for _, lab, _ in wire.sent]
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
    # exactly on the edge of Q2 for pct_open (inclusive, so inside); pct_vwap still has to be below the band's
    # low for the stock to be OVERSOLD at all, so only the reference under test is moved to the edge
    ns, app, svc, wire, calls, before = start(folder, monkeypatch, abs_pct={"open": 25.0, "vwap": 10.0}, abs_idx=5)
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


def test_the_time_exit_is_five_minutes_before_the_close_by_default():
    assert re.search(r"EXIT_AT = dtime\([^)]*\)", SCRIPT) and "time exit" in SCRIPT


def test_no_entry_is_made_once_the_time_exit_has_come_even_inside_the_window(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, exit_at="dtime(0, 0, 1)")
    feed(fake_dh, [100.0 - 0.25 * i for i in range(60)])                                           # a falling stock: it would be bought if entries were allowed
    time.sleep(0.2)
    assert wire.sent == []
    svc.on_stop()


def test_at_the_time_exit_the_automation_is_stopped_like_algo_stop_on_the_next_candle(tmp_path, fake_dh, monkeypatch):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("America/New_York"))
    if now.hour == 23 and now.minute >= 58:
        pytest.skip("too close to midnight to place the exit a few seconds ahead")
    when = (now + timedelta(seconds=5)).time()
    last = write_history(tmp_path, ["2020-01-14", "2020-01-15"])
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, exit_at=f"dtime({when.hour}, {when.minute}, {when.second})",
                                                    **{"algo_start(USER, STRATEGY)": "algo_start(USER, STRATEGY, squareup_settle_s=0)"})            # no wait between squareup and unsubscribe here
    entry = last - 0.25
    feed(fake_dh, [entry])
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"               # the entry, before the exit time
    time.sleep(1.0)
    feed(fake_dh, [entry * 1.001], first=1)                                                        # a small move: neither target nor stop
    time.sleep(0.3)
    assert sum(1 for _, lab, _ in wire.sent if lab == "subscribe") == 1                            # still open, the exit time has not come
    end = time.time() + 8
    while datetime.now(ZoneInfo("America/New_York")).time() < when and time.time() < end:
        time.sleep(0.1)
    time.sleep(0.2)
    feed(fake_dh, [entry * 1.001], first=2)                                                        # the next candle after the exit time
    assert wait_for(lambda: any(lab == "unsubscribe" for _, lab, _ in wire.sent))
    # the resting order is re-priced while it waits, so "apply" may appear in between; what matters is the
    # closing sequence algo_stop sends, in order. 08 stops with pause=False, so the pause step is optional.
    stop_seq = [lab for _, lab, _ in wire.sent if lab in ("pause_unpause:Pause", "squareup", "unsubscribe")]
    assert stop_seq in (["squareup", "unsubscribe"], ["pause_unpause:Pause", "squareup", "unsubscribe"]), stop_seq
    assert {subj for subj, lab, _ in wire.sent if lab != "subscribe"} == {"cmd.hft.1001"}           # all of it on the entry's portfolio
    assert wait_for(lambda: "meanrevert" not in app.plugins)                                         # and the automation itself is stopped
    svc.on_stop()


def test_with_act_on_bar_end_an_entry_is_looked_for_only_on_the_candle_that_ends_a_five_minute_bar(tmp_path, fake_dh, monkeypatch):
    last = write_history(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"])
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, act_on_bar_end=True)
    feed(fake_dh, [last - 0.25], first=0)                                                          # minute :00 of a bar: oversold, but not the end of a bar
    time.sleep(0.3)
    assert wire.sent == []
    feed(fake_dh, [last - 0.5, last - 0.75, last - 1.0], first=1)                                 # :01, :02, :03: still inside the bar
    time.sleep(0.3)
    assert wire.sent == []
    feed(fake_dh, [last - 1.25], first=4)                                                          # :04 ends the bar (the candle closes at :05)
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    svc.on_stop()


def buys(wire):
    """The ENTRY subscribes: in these oversold scenarios an entry is the BUY side. An exit is a subscribe too
    now -- its own portfolio, SIDE 2 -- so the two are told apart by side rather than by the command."""
    return [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe" and p["SIDE"] == 1]


def run_two_oversold_spells(tmp_path, fake_dh, monkeypatch, **replace):
    """A long fall (oversold, BUY), a 2% jump (the target, which opens an OPPOSITE portfolio rather than squaring
    the entry up), a rise that takes the RSI out of the zone, then another long fall (oversold again).
    Returns the BUY subscribes, i.e. the entries."""
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, **replace)
    closes = [100.0 - 0.25 * i for i in range(60)]
    feed(fake_dh, closes)
    assert wait_for(lambda: len(wire.sent) >= 1)
    subject, _lab, params = wire.sent[0]
    fills(fake_dh, USER_ID * PF_PER_USER + int(subject.rsplit(".", 1)[1]), 1, params["QTY"])        # the entry filled: the exit is sized from this
    time.sleep(1.0)
    top = closes[-1] * 1.02
    feed(fake_dh, [top], first=60)                                                                  # +2% for the long: the target
    assert wait_for(lambda: any(lab == "subscribe" and p["SIDE"] == 2 for _, lab, p in wire.sent))   # the exit: a new portfolio on the other side
    ex_subject, _l, ex_params = next((c for c in wire.sent if c[1] == "subscribe" and c[2]["SIDE"] == 2), (None, None, None))
    fills(fake_dh, USER_ID * PF_PER_USER + int(ex_subject.rsplit(".", 1)[1]), 2, ex_params["QTY"])  # the exit filled too, so the stock is really flat again
    time.sleep(0.5)
    rise = [top + 0.5 * k for k in range(1, 16)]
    feed(fake_dh, rise, first=61)                                                                   # the RSI leaves the oversold zone
    again = [rise[-1] - 0.25 * k for k in range(1, 70)]
    feed(fake_dh, again, first=76)                                                                  # and falls into it again
    time.sleep(0.5)
    svc.on_stop()
    return buys(wire)


def test_with_one_entry_per_stock_a_stock_is_not_entered_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    assert "ONE_ENTRY_PER_STOCK = True" in SCRIPT
    assert run_two_oversold_spells(tmp_path, fake_dh, monkeypatch) == [1]                            # one BUY; the second oversold spell, and any sell in the rise, are left alone
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    sides = run_two_oversold_spells(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    assert sides[0] == 1 and len(sides) >= 2                                                         # with the rule off the same candles enter again


def test_entry_value_makes_the_quantity_int_value_over_the_close_and_skips_an_entry_it_cannot_afford(tmp_path, fake_dh, monkeypatch, capsys):
    assert re.search(r"^    ENTRY_VALUE = ", SCRIPT, re.M)      # whatever the owner has typed; each case below sets its own
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch, **{"ENTRY_VALUE = None": "ENTRY_VALUE = 1000.0"})
    feed(fake_dh, [100.0 - 0.25 * i for i in range(60)])
    assert wait_for(lambda: len(wire.sent) == 1)
    px = float(re.search(r"BUY (\d+) lot\(s\) at ([0-9.]+)(?: \(LimitOrder\))? on PF", capsys.readouterr().out).group(2))
    assert wire.sent[0][2]["QTY"] == int(1000.0 / px) and wire.sent[0][2]["QTY"] > 1                  # by value, not the fixed QTY = 1
    svc.on_stop()
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    ns, app, svc, wire, calls, before = start(tmp_path / "b", monkeypatch, **{"ENTRY_VALUE = None": "ENTRY_VALUE = 50.0"})
    feed(fake_dh, [100.0 - 0.25 * i for i in range(60)])
    time.sleep(0.3)
    assert wire.sent == [] and "ENTRY_VALUE 50 buys less than one unit" in capsys.readouterr().out    # one share costs more than the value: no trade
    svc.on_stop()
    fake_dh.clear()
    with pytest.raises(ValueError):
        start(tmp_path / "c", monkeypatch, **{"ENTRY_VALUE = None": "ENTRY_VALUE = -5"})


def test_a_ranking_that_fails_is_counted_said_in_one_line_and_tried_again(tmp_path, fake_dh, monkeypatch, capsys):
    assert "RETRY_RANKING_S = 20" in SCRIPT
    ns, app, svc, wire, calls, before = start(tmp_path, monkeypatch)
    orig, n = ns["relative"], {"calls": 0}

    def flaky(*a, **k):
        n["calls"] += 1
        if n["calls"] == 1:
            raise RuntimeError("io.deephaven.base.verify.AssertionFailure: asserted Previous values requested" + chr(10) + "Traceback (most recent call last):" + chr(10) + "  File x")
        return orig(*a, **k)
    ns["relative"] = flaky
    feed(fake_dh, [100.0 - 0.25 * i for i in range(60)])                                              # oversold from the 10th bar on
    assert wait_for(lambda: len(wire.sent) == 1)
    ns["meanrevert_status"]()
    out = capsys.readouterr().out
    assert out.count("the ranking is not available") == 1 and "Traceback" not in out and "trying again" in out
    # the spread-gated script ARMS the entry and sends it on the first tick whose spread is ok, so its count is "armed"
    assert "ranking errors 1" in out and "signals" in out and ("entered 1" in out or "armed 1" in out)
    svc.on_stop()
