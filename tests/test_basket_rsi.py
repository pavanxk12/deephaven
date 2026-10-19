"""examples/07_basket_rsi_crossorder.py: baskets, the middle-of-the-basket filter, the 1-minute RSI, the single-order strategy (one subscribe per trade on a new portfolio, squareup to exit).
Real registry, book, bridge and NATS service; fake wire and fake Deephaven listener; relative() is replaced by one that returns the real Relative type."""
import re
import time

import numpy as np
import pytest

from algo.series.absolute import Relative
from tests.test_examples import EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

BASKETS_RX = re.compile(r'"banks": \[[^\n]*\],\n        "it": \[[^\n]*\],')
ALWAYS_OPEN = "WINDOW = (dtime(0, 0), dtime(23, 59, 59))"
OPEN_WINDOW = "WINDOW = (dtime(9, 20), dtime(15, 20))"


def start(tmp_path, monkeypatch, status="ok", pct=None, window=ALWAYS_OPEN, baskets='"banks": ["NIFTY", "A1", "A2"],\n        "it": ["B1", "B2", "B3"],', **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, status)
    ranks = dict(pct or {"open": 50.0, "vwap": 50.0})
    calls = []

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        calls.append((series, level, symbol, tuple(basket), ref))
        return Relative(np.array([1.0, 2.0, 3.0]), 3, now=0.5, n_valid=7, pctile=ranks[ref], symbols=list(basket), series=series, level=level, symbol=symbol)

    ns["relative"] = relative
    ns["basket"] = lambda name, members, **k: calls.append(("basket", name, tuple(members)))
    src = (EXAMPLES / "07_basket_rsi_crossorder.py").read_text(encoding="utf-8")
    assert BASKETS_RX.search(src) and OPEN_WINDOW in src
    src = BASKETS_RX.sub(baskets, src).replace(OPEN_WINDOW, window)
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    before = set(ns)
    exec(compile(src, "07_basket_rsi_crossorder.py", "exec"), ns)
    return ns, app, svc, wire, calls, ranks, before


def bars(fake_dh, closes):
    for c in closes:
        fake_dh[0].fn(Update(added={"token": [FUT], "Close": [float(c)]}), False)


def fall(n=12, start=100.0, step=1.0):
    return [start - i * step for i in range(n)]


def test_an_oversold_symbol_in_the_middle_sends_one_single_order_subscribe_and_the_target_closes_it_with_squareup(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, ranks, before = start(tmp_path, monkeypatch)
    assert svc.is_enabled("basket_rsi") and [c[1] for c in calls if c[0] == "basket"] == ["banks", "it"]               # each basket saved under its name
    leaked = sorted(n for n in set(ns) - before if not n.startswith("basket_rsi") and n != "run_basket_rsi" and not n.startswith("__"))
    assert leaked == []                                                                                              # nothing that could overwrite another script's names
    bars(fake_dh, fall(9))
    time.sleep(0.1)
    assert wire.sent == []                                                                                           # not enough closes for an RSI of 9 yet
    bars(fake_dh, fall(10)[9:])                                                                                      # the 10th close (91.0): RSI 0, oversold -> BUY
    assert wait_for(lambda: len(wire.sent) == 1)
    subject, label, payload = wire.sent[0]
    assert (subject, label) == ("cmd.hft.1001", "subscribe")                                                         # ONE command, on the portfolio the book gave
    assert {k: v for k, v in payload.items() if k not in ("USERIDENTIFIER", "user_id")} == {
        "strategyname": "single", "TOKEN": FUT, "SIDE": 1, "QTY": 1, "SERVERCODE": 15046, "ACCOUNT": "PRO", "ALGOID": "1"}   # exactly the payload that worked on the engine
    assert payload["USERIDENTIFIER"] == 4 and "gtw" not in payload and "T1" not in payload
    ranked = [c for c in calls if c[0] == "pct"]
    assert [c[4] for c in ranked] == ["open", "vwap"] and ranked[0][3] == ("NIFTY", "A1", "A2")                      # ranked against its own basket, both ways
    assert wait_for(lambda: ns["basket_rsi_status"] is not None)
    time.sleep(1.0)
    bars(fake_dh, [90.9])                                                                                            # still oversold, 0.1% down: no second order, no stop
    time.sleep(0.2)
    assert len(wire.sent) == 1
    bars(fake_dh, [91.0 * 1.02])                                                                                     # +2%: the target
    assert wait_for(lambda: len(wire.sent) == 2)
    assert wire.sent[1][:2] == ("cmd.hft.1001", "squareup")                                                          # the exit is the portfolio's squareup
    time.sleep(1.0)
    bars(fake_dh, [100.0 + 3 * i for i in range(14)])                                                                # a steady rise: overbought -> SELL on a NEW portfolio
    assert wait_for(lambda: len(wire.sent) >= 3)
    assert wire.sent[2][:2] == ("cmd.hft.1002", "subscribe") and wire.sent[2][2]["SIDE"] == 2
    ns["basket_rsi_exit_all"]()
    assert wait_for(lambda: wire.sent[-1][:2] == ("cmd.hft.1002", "squareup"))
    ns["basket_rsi_stop"]()
    svc.on_stop()


def test_the_stop_loss_closes_a_long(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, ranks, before = start(tmp_path, monkeypatch)
    bars(fake_dh, fall(10))                                                                                          # the 10th close (91.0) is the entry price
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(0.2)
    entry = fall(10)[-1]
    bars(fake_dh, [entry * (1 - 0.004)])
    time.sleep(0.2)
    assert len(wire.sent) == 1                                                                                       # -0.4%: inside the stop
    bars(fake_dh, [entry * (1 - 0.006)])                                                                             # -0.6%: stopped out
    assert wait_for(lambda: len(wire.sent) == 2) and wire.sent[1][1] == "squareup"
    svc.on_stop()


def test_outside_the_middle_of_the_basket_nothing_is_sent_and_the_edges_are_inside(tmp_path, fake_dh, monkeypatch):
    for i, bad in enumerate(({"open": 10.0, "vwap": 50.0}, {"open": 50.0, "vwap": 90.0}, {"open": 24.9, "vwap": 50.0})):
        fake_dh.clear()
        ns, app, svc, wire, calls, ranks, before = start(tmp_path / str(i), monkeypatch, pct=bad)
        bars(fake_dh, fall(12))
        time.sleep(0.2)
        assert wire.sent == [] and app.plugins["registry"].state.portfolios() == []                                 # no order, no portfolio number used
        ranks.update({"open": 25.0, "vwap": 75.0})                                                                  # both on the edge of Q2 / Q3: inside
        bars(fake_dh, [70.0])
        assert wait_for(lambda: len(wire.sent) == 1)
        svc.on_stop()


def test_a_symbol_is_ranked_against_its_own_basket_only(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, ranks, before = start(tmp_path, monkeypatch)
    bars(fake_dh, fall(10))
    assert wait_for(lambda: len(wire.sent) == 1)
    assert {c[3] for c in calls if c[0] == "pct"} == {("NIFTY", "A1", "A2")}                                         # not against the other basket
    svc.on_stop()


def test_off_or_outside_the_window_nothing_is_sent_and_no_number_is_used(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, ranks, before = start(tmp_path, monkeypatch)
    ns["nats_send"](False, client="basket_rsi")
    bars(fake_dh, fall(12))
    time.sleep(0.2)
    assert wire.sent == [] and app.plugins["registry"].state.portfolios() == []
    svc.on_stop()
    fake_dh.clear()
    ns, app, svc, wire, calls, ranks, before = start(tmp_path / "w", monkeypatch, window="WINDOW = (dtime(0, 0), dtime(0, 1))")
    bars(fake_dh, fall(12))
    time.sleep(0.2)
    assert wire.sent == []
    svc.on_stop()


def test_a_refused_entry_leaves_nothing_open_and_a_refused_exit_is_tried_again(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, calls, ranks, before = start(tmp_path, monkeypatch, status="error")
    bars(fake_dh, fall(10))
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(1.0)
    bars(fake_dh, [89.0])                                                                                            # the cooldown has not passed and nothing is open to manage
    time.sleep(0.2)
    assert len(wire.sent) == 1
    svc.on_stop()
    fake_dh.clear()
    ns, app, svc, wire, calls, ranks, before = start(tmp_path / "x", monkeypatch)
    bars(fake_dh, fall(10))
    assert wait_for(lambda: len(wire.sent) == 1)
    time.sleep(1.0)
    wire.status = "error"
    entry = fall(10)[-1]
    bars(fake_dh, [entry * 0.99])                                                                                    # -1%: the stop; the squareup is refused
    assert wait_for(lambda: len(wire.sent) == 2)
    time.sleep(1.0)
    wire.status = "ok"
    bars(fake_dh, [entry * 0.99])                                                                                    # still open, so it is tried again at the next close
    assert wait_for(lambda: len(wire.sent) == 3 and wire.sent[2][1] == "squareup")
    svc.on_stop()


def test_a_symbol_in_two_baskets_or_a_basket_of_two_or_a_bad_setting_is_refused(tmp_path, fake_dh, monkeypatch):
    with pytest.raises(ValueError, match="in two baskets"):
        start(tmp_path / "a", monkeypatch, baskets='"banks": ["NIFTY", "A1", "A2"],\n        "it": ["B1", "A1", "B3"],')
    with pytest.raises(ValueError, match="at least 3 symbols"):
        start(tmp_path / "b", monkeypatch, baskets='"banks": ["NIFTY", "A1", "A2"],\n        "it": ["B1", "B2"],')
    for old, new in (("TARGET_PCT, STOP_PCT = 1.5, 0.5", "TARGET_PCT, STOP_PCT = 0, 0.5"), ("QTY = 1  ", "QTY = 0  "), ("MID_ZONE = (25.0, 75.0)", "MID_ZONE = (75.0, 25.0)")):
        fake_dh.clear()
        with pytest.raises(ValueError, match="MID_ZONE must be inside"):
            start(tmp_path / (new[:6].strip() + old[:3]), monkeypatch, **{old: new})
