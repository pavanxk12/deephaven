# ruff: noqa: F811
"""The example scripts in examples/ really do what they say.

01 and 02 are run with the session functions replaced by recorders, and the settings they build are compared with the constants of the
old scripts (so a wrong claim "this default equals your old value" fails here). 03 is run against the real registry, book, bridge and
NATS service with a fake connection.
"""
import time
import types
from datetime import time as dtime
from pathlib import Path

import pandas as pd
import pytest

import algo
from algo.comms.nats_service import NatsConfig
from algo.core.config import NSE
from algo.data.master import classify, normalise
from algo.strategy.config import BridgeConfig
from tests.conftest import secs
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)
from tests.test_nats_service import Wire
from tests.test_lookup import session, tables  # noqa: F401  (pytest fixtures: a console with a master and candles)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"

OLD_STOCKS_SUB = {"T1": 0, "gtw": 32395, "PRNT": 1, "code": "", "ival": 1, "jval": 0.75, "side": "BUY", "sval": 3, "trp": 0,
                  "jqty1": 1, "jqty2": 1, "openq": 0, "maxqty": 0, "target": 1000, "maxloss": -1000, "stoploss": -1000,
                  "lowerrange": 0, "upperrange": 10000, "max_unhedge": 1, "strategyname": "Churning"}


def run_example(name, monkeypatch):
    names = ("master_build", "candles_start", "straddle_start", "registry_start", "registry_connect", "nats_start", "nats_connect",
             "nats_send", "algo_status")
    calls, plugins = [], []
    ns = {n: (lambda *a, _n=n, **k: calls.append((_n, a, k))) for n in names}
    monkeypatch.setattr(algo, "current", lambda: types.SimpleNamespace(run=lambda p: plugins.append(p)))
    src = (EXAMPLES / name).read_text(encoding="utf-8")
    exec(compile(src, name, "exec"), ns)
    return {c[0]: c for c in calls}, plugins, [c[0] for c in calls]


# ---- 00: the shared services, started once -----------------------------------------------------------------------------
def test_the_services_example_starts_everything_once_for_all_the_users(monkeypatch):
    calls, plugins, order = run_example("00_services.py", monkeypatch)
    assert plugins == []                                                                     # no algo in here
    assert calls["master_build"][1] == ("NSE",) and calls["candles_start"][1:] == (("1min",), {"expiries": 1})
    assert "indicators" not in calls and calls["straddle_start"][1] == (0,)                    # indicators are not a service; the straddle is
    assert calls["nats_start"][1:] == (("192.168.1.130", 4222), {"users": [3, 4]})            # EVERY user, for every algo
    assert order.index("registry_start") < order.index("nats_start") and "nats_send" not in order    # no algo is switched on here


def test_every_script_that_uses_the_services_connects_to_them_first(monkeypatch):
    import ast
    for name in ("02_manual_trade.py", "03_algo_template.py", "04_nifty_multistrike.py"):
        tree = ast.parse((EXAMPLES / name).read_text(encoding="utf-8"))
        called = [n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        assert "nats_connect" in called and "registry_connect" in called, name


def test_the_old_stocks_subscribe_payload_and_trade_window_are_the_defaults_of_the_services():
    assert NatsConfig().sub_payload == OLD_STOCKS_SUB                       # the old SUB_PAYLOAD without the ids the service stamps
    assert NatsConfig().sender == "AUTOMATION" and NSE.trade_window == (dtime(9, 20), dtime(15, 0))   # TRADE_START / TRADE_END


def test_the_known_differences_from_the_old_nifty_script_are_real():
    """If one of these fails because the gap was closed, update examples/README.md."""
    assert NSE.trade_window[1] == dtime(15, 0)                              # the old NIFTY script traded until 15:20
    assert not hasattr(BridgeConfig(), "sub_payload")                       # the subscribe payload belongs to the service, not to an algo
    assert not hasattr(NatsConfig(), "servers")                              # and one service talks to one NATS server


# ---- 03 and 04: automations written as plain scripts (functions and listeners, no class), run for real ------------------------------
FUT = 1
CE, PE = (lambda i: 2000 + 2 * i), (lambda i: 2001 + 2 * i)                      # strike 24500 + 100 * i


def nifty_master(n_strikes=11):
    e1 = secs("2030-01-30")
    rows = [dict(Token=FUT, Symbol="NIFTY", ExpiryDate=e1, InstType="FUTIDX", OptionType="XX", StrikePrice=0, LotSize=50, TickSize=5)]
    for i in range(n_strikes):
        for ot, tok in (("CE", CE(i)), ("PE", PE(i))):
            rows.append(dict(Token=tok, Symbol="NIFTY", ExpiryDate=e1, InstType="OPTIDX", OptionType=ot,
                             StrikePrice=(24500 + 100 * i) * 100, LotSize=50, TickSize=5))
    return classify(normalise(pd.DataFrame(rows), NSE))


def prices(fut=25010.0, ce=120.0, pe=100.0, mid=5):
    """Future 25010, and at the middle strike (25000) CE 120 / PE 100, so the synthetic future is 25020; the rest 100."""
    p = {FUT: fut}
    p.update({t: 100.0 for i in range(11) for t in (CE(i), PE(i))})
    p[CE(mid)], p[PE(mid)] = ce, pe
    return p


def wait_for(cond, seconds=10):
    end = time.time() + seconds
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return False


class Update:
    def __init__(self, added=None, modified=None):
        self._a, self._m = added, modified

    def added(self, cols):
        return self._a

    def modified(self, cols):
        return self._m


def console_with_services(tmp_path, monkeypatch, status="ok", history=None):
    """An installed console: a NIFTY master, the registry, the NATS service (fake connection), and the candles table the scripts listen to
    (with `history` closes of the future in it, if given)."""
    from algo.comms.nats_service import NatsService
    from algo.core import app as app_module
    from algo.strategy.registry import Registry
    monkeypatch.setattr(app_module, "_APP", None)
    from algo.core.config import Paths
    ns = {}
    app = algo.install(ns, "NSE", paths=Paths(market_data=str(tmp_path), work=str(tmp_path)))     # files go in the test's own folder
    app.plugins["master"] = types.SimpleNamespace(data=nifty_master())
    app.plugins["registry"] = types.SimpleNamespace(state=Registry(str(tmp_path / "reg.json")), describe=lambda: "registry")
    wire = Wire(status=status)
    svc = NatsService(NatsConfig(users=(3, 4), send_on=True), transport_factory=lambda: wire)
    svc.connect()
    app.plugins["nats"] = svc
    if history is None:
        app.tables["candles"] = "candles-table"                                                  # as the service publishes it
    else:
        from tests.test_lookup import FakeTable
        from algo import lookup
        monkeypatch.setattr(lookup, "_frame", lambda t: t.df.reset_index(drop=True))
        app.tables["candles"] = FakeTable(pd.DataFrame({"token": [FUT] * len(history), "Close": [float(c) for c in history],
                                                        "Minute": [f"09:{15 + i:02d}" for i in range(len(history))]}))
    ns["candles"] = app.tables["candles"]
    return ns, app, svc, wire


def run_script(ns, name, **replace):
    src = (EXAMPLES / name).read_text(encoding="utf-8")
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    exec(compile(src, name, "exec"), ns)


def test_the_plain_scripts_have_no_class_and_start_no_shared_service():
    import ast
    for name in ("03_algo_template.py", "04_nifty_multistrike.py"):
        tree = ast.parse((EXAMPLES / name).read_text(encoding="utf-8"))
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)], name
        called = {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert not called & {"master_build", "candles_start", "nats_start", "registry_start", "nats_restart"}, name


def start_multistrike(tmp_path, monkeypatch, fake_dh, status="ok", **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, status)
    run_script(ns, "04_nifty_multistrike.py", **replace)
    ns["WINDOW"] = (dtime(0, 0), dtime(23, 59, 59))                                  # always inside this automation's own window
    ns["ltp"].update(prices())
    return ns, app, svc, wire


@pytest.mark.parametrize("mode, gap, leg, expect", [
    ("OTM", 1, "CALL", [CE(5), CE(6), CE(7)]),            # 25000, 25100, 25200: away from the money for a call is higher
    ("OTM", 1, "PUT", [PE(5), PE(4), PE(3)]),             # 25000, 24900, 24800: away from the money for a put is lower
    ("ITM", 1, "CALL", [CE(5), CE(4), CE(3)]),
    ("BOTH", 1, "CALL", [CE(5), CE(6), CE(4)]),           # 0, +1, -1
    ("OTM", 2, "CALL", [CE(5), CE(7), CE(9)]),            # every second strike
])
def test_the_strikes_are_picked_around_the_synthetic_future(tmp_path, fake_dh, monkeypatch, mode, gap, leg, expect):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    ns["STRIKE_MODE"], ns["STRIKE_GAP"] = mode, gap
    picked = ns["pick"](leg)
    assert list(picked.values()) == expect and list(picked) == [0, 1, 2]
    svc.on_stop()


def test_a_strike_past_the_end_of_the_chain_is_left_out_and_a_missing_price_picks_nothing(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    ns["ltp"].update(prices(fut=25500.0, ce=50.0, pe=50.0, mid=10))                  # synthetic = 25500 = the top strike
    assert ns["pick"]("CALL") == {0: CE(10)}
    ns["ltp"].pop(FUT)
    assert ns["pick"]("CALL") == {}
    ns["on_zone"]("OVERSOLD")                                                         # no price: nothing is marked, nothing is created
    assert ns["pf_leg"]("nifty_multi", FUT) is None and app.plugins["registry"].state.portfolios() == [] and wire.sent == []
    svc.on_stop()


def test_a_zone_buys_every_slot_through_the_services_in_the_right_order(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    reg = app.plugins["registry"].state
    ns["on_zone"]("OVERSOLD")                                                         # RSI under 30: BUY CALLs on 3 strikes
    assert wait_for(lambda: len(wire.sent) == 12)                                     # 3 slots x (subscribe, pause, apply, unpause)
    by_pf = {}
    for subject, label, payload in wire.sent:
        by_pf.setdefault(subject, []).append((label, payload))
    assert sorted(by_pf) == ["cmd.hft.1933", "cmd.hft.1934", "cmd.hft.1935"]          # numbers from the registry
    for cmds in by_pf.values():
        assert [c[0] for c in cmds] == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]
        assert {c[1]["USERIDENTIFIER"] for c in cmds} == {4}
        assert cmds[2][1]["maxqty"] == 3 and cmds[2][1]["side"] == "BUY" and cmds[2][1]["max_unhedge"] == 3
    assert [by_pf[f"cmd.hft.{1933 + k}"][0][1]["T1"] for k in range(3)] == [CE(5), CE(6), CE(7)]
    assert [r[:5] for r in reg.portfolios(4)] == [(4, 1933 + k, "nifty_multi_book", "NIFTY", "nifty_multi") for k in range(3)]
    table = ns["algo_portfolios"]("nifty_multi")
    assert [(r.pf, r.slot, r.option, r.leg, r.cap) for r in table.itertuples()] == [(1933 + k, k, CE(5 + k), "CALL", 3) for k in range(3)]
    ns["on_zone"]("OVERSOLD")                                                         # the same zone again: every slot already runs it
    time.sleep(0.2)
    assert len(wire.sent) == 12
    svc.on_stop()


def test_flipping_to_the_other_zone_retires_each_old_portfolio_and_opens_a_new_one(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    ns["on_zone"]("OVERSOLD")
    assert wait_for(lambda: len(wire.sent) == 12)
    ns["on_zone"]("OVERBOUGHT")                                                       # RSI over 70: PUTs now
    assert wait_for(lambda: len(wire.sent) == 12 + 3 * 7)                             # per slot: pause, squareup, unsubscribe the old; 4 for the new
    labels = {}
    for subject, label, payload in wire.sent[12:]:
        labels.setdefault(subject, []).append(label)
    for k in range(3):
        assert labels[f"cmd.hft.{1933 + k}"] == ["pause_unpause:Pause", "squareup", "unsubscribe"]          # the old ones, retired
        assert labels[f"cmd.hft.{1936 + k}"] == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]   # the new ones
    assert sorted(r[1] for r in app.plugins["registry"].state.portfolios(4)) == [1933, 1934, 1935, 1936, 1937, 1938]
    svc.on_stop()


def test_a_failed_slot_rolls_back_and_is_retried_only_after_the_cooldown(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh, status="error")      # the bridge refuses every command
    ns["on_zone"]("OVERSOLD")
    assert wait_for(lambda: len(wire.sent) == 3 and all(ns["pf_cooling"]("nifty_multi", FUT, 30, k) for k in range(3)))
    assert all(ns["pf_leg"]("nifty_multi", FUT, k) is None for k in range(3))        # rolled back: a later update may try again
    ns["on_zone"]("OVERSOLD")
    time.sleep(0.2)
    assert len(wire.sent) == 3                                                        # inside the cooldown: nothing new
    state = app["nifty_multi"].book.state
    for k in range(3):
        state.last_fail[app["nifty_multi"].book.key(FUT, k)] = time.time() - 31       # 31 seconds later
    ns["on_zone"]("OVERSOLD")
    assert wait_for(lambda: len(wire.sent) == 6)                                      # tried again
    svc.on_stop()


def test_off_and_outside_the_window_create_and_send_nothing(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    ns["nats_send"](False, client="nifty_multi")                                      # this automation is off
    ns["on_zone"]("OVERSOLD")
    assert app.plugins["registry"].state.portfolios() == [] and wire.sent == []
    ns["nats_send"](True, client="nifty_multi")
    ns["WINDOW"] = (dtime(0, 0), dtime(0, 0, 1))                                      # outside this automation's own window
    ns["on_zone"]("OVERSOLD")
    assert app.plugins["registry"].state.portfolios() == [] and wire.sent == []
    svc.on_stop()


def test_the_settings_are_checked_when_the_script_starts(tmp_path, fake_dh, monkeypatch):
    for old, new in (("N_STRIKES = 3 ", "N_STRIKES = 0 "), ("STRIKE_GAP = 1 ", "STRIKE_GAP = 0 "), ('STRIKE_MODE = "OTM"', 'STRIKE_MODE = "SIDEWAYS"'),
                     ("N_STRIKES = 3 ", "N_STRIKES = 100 ")):
        ns, app, svc, wire = console_with_services(tmp_path, monkeypatch)
        with pytest.raises(ValueError):
            run_script(ns, "04_nifty_multistrike.py", **{old: new})
        assert "nifty_multi" not in app.plugins                                       # and nothing was registered
        svc.on_stop()


def feed(fake_dh, closes):
    """Send the future's closes to the script's candle listener, one candle at a time (each with every option's price alongside)."""
    base = prices()
    others = [t for t in base if t != FUT]
    for close in closes:
        fake_dh[0].fn(Update(added={"token": [FUT, *others], "Close": [float(close), *[base[t] for t in others]]}), False)


def falling(n, start=25100.0, step=10.0):
    return [start - i * step for i in range(n)]            # ends at 25010 for n = 10: the strike nearest is 25000


def rising(n, start=25000.0, step=60.0):
    return [start + i * step for i in range(1, n + 1)]


def test_the_template_is_a_complete_working_automation(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch)
    run_script(ns, "03_algo_template.py")
    assert svc.is_enabled("template") and len(fake_dh) == 1                           # the last line switched it on; one listener: the candles
    feed(fake_dh, falling(5))
    time.sleep(0.1)
    assert wire.sent == [] and ns["rsi"](ns["closes"]) is None                        # not enough candles for an RSI(9) yet
    fake_dh[0].fn(Update(added={"token": [999], "Close": [1.0]}), False)             # another instrument's candle is no signal for the future
    feed(fake_dh, falling(10, start=25050.0, step=5.0)[5:])                            # the RSI of a straight fall is 0: oversold -> a CALL
    assert wait_for(lambda: len(wire.sent) == 4)
    assert [lab for _, lab, _ in wire.sent] == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]
    assert wire.sent[0][2]["T1"] == CE(5) and {p["USERIDENTIFIER"] for _, _, p in wire.sent} == {3}
    assert wire.sent[2][2]["maxqty"] == 3 and wire.sent[2][2]["ival"] > 1.0 and wire.sent[2][2]["target"] == 1000
    rec = app.plugins["registry"].state.get(3, 5000)
    assert (rec["owner"], rec["algo"], rec["name"]) == ("template_book", "template", "NIFTY")
    assert ns["pf_leg"]("template", FUT) == "CALL"
    feed(fake_dh, falling(3, start=25000.0))                                           # still oversold: it already runs the CALL
    time.sleep(0.2)
    assert len(wire.sent) == 4
    feed(fake_dh, rising(40))                                                          # a strong rise: overbought -> a PUT, the CALL's portfolio retires
    assert wait_for(lambda: len(wire.sent) == 4 + 3 + 4)
    assert [lab for subject, lab, _ in wire.sent[4:7]] == ["pause_unpause:Pause", "squareup", "unsubscribe"]
    assert wire.sent[7][1] == "subscribe" and ns["pf_leg"]("template", FUT) == "PUT"
    svc.on_stop()


def test_the_rsi_function_in_the_scripts_is_wilders_rsi(tmp_path, fake_dh, monkeypatch):
    """Against the published values of the textbook 14-period example (they are rounded, so within 0.1), and exactly against the definition."""
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch)
    run_script(ns, "03_algo_template.py")
    closes = [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41,
              46.22, 45.64, 46.21, 46.25, 45.71, 46.45, 45.78, 45.35, 44.03, 44.18, 44.22, 44.57, 43.42, 42.66, 43.13]
    published = [70.53, 66.32, 66.55, 69.41, 66.36, 57.97, 62.93, 63.26, 56.06, 62.38, 54.71, 50.42, 39.99, 41.46, 41.87, 45.46, 37.30, 33.08,
                 37.77]
    for k, want in zip(range(15, 34), published):
        assert ns["rsi"](closes[:k], 14) == pytest.approx(want, abs=0.1), k
    n = 9
    gains = [max(b - a, 0) for a, b in zip(closes, closes[1:])]
    losses = [max(a - b, 0) for a, b in zip(closes, closes[1:])]
    au, ad = sum(gains[:n]) / n, sum(losses[:n]) / n
    for g, l_ in zip(gains[n:], losses[n:]):
        au, ad = (au * (n - 1) + g) / n, (ad * (n - 1) + l_) / n
    assert ns["rsi"](closes, n) == pytest.approx(100 - 100 / (1 + au / ad), abs=1e-9)
    assert ns["rsi"](closes[:n], n) is None and ns["rsi"]([5.0] * 12, 9) == 50.0 and ns["rsi"](list(range(1, 15)), 9) == 100.0
    assert ns["rsi"](list(range(15, 1, -1)), 9) == 0.0
    svc.on_stop()


def test_the_template_starts_its_rsi_from_the_candles_already_built(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, history=falling(12, start=25200.0, step=15.0))
    run_script(ns, "03_algo_template.py")
    assert len(ns["closes"]) == 12                                                      # seeded from last_candles at start
    fake_dh[0].fn(Update(added={"token": [CE(5), PE(5)], "Close": [120.0, 100.0]}), False)    # option prices first
    fake_dh[0].fn(Update(added={"token": [FUT], "Close": [25010.0]}), False)             # the first live candle: oversold at once, no warm-up
    assert wait_for(lambda: len(wire.sent) == 4) and wire.sent[0][2]["T1"] == CE(5)
    svc.on_stop()


def test_the_template_does_nothing_while_it_is_off(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch)
    run_script(ns, "03_algo_template.py")
    ns["nats_send"](False, client="template")
    feed(fake_dh, falling(14))
    time.sleep(0.1)
    assert wire.sent == [] and app.plugins["registry"].state.portfolios() == []      # and no portfolio number was used
    svc.on_stop()


def test_the_multistrike_script_computes_its_rsi_from_the_futures_candles_only(tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire = start_multistrike(tmp_path, monkeypatch, fake_dh)
    seen = []
    ns["on_zone"] = seen.append
    ns["ltp"].clear()
    feed(fake_dh, falling(9))
    assert seen == [] and len(ns["closes"]) == 9                                       # 9 closes: not enough for an RSI(9) yet
    fake_dh[0].fn(Update(added={"token": [999, 7], "Close": [1.0, float("nan")]}), False)    # other instruments, and a bad price
    assert seen == [] and 7 not in ns["ltp"] and 999 in ns["ltp"] and len(ns["closes"]) == 9
    feed(fake_dh, falling(10)[9:])                                                     # the 10th close of the fall: oversold
    assert seen == ["OVERSOLD"]
    feed(fake_dh, rising(40))
    assert seen[-1] == "OVERBOUGHT" and set(seen) == {"OVERSOLD", "OVERBOUGHT"}
    feed(fake_dh, [25000.0] * 3)                                                        # a flat price after the rise: no zone, no call
    assert len(ns["closes"]) <= 300
    svc.on_stop()


# ---- 01 and 02: plain function calls, run for real in an installed console ----------------------------------------------------
def run_in_console(ns, name):
    src = (EXAMPLES / name).read_text(encoding="utf-8")
    exec(compile(src, name, "exec"), ns)


def test_the_lookups_example_runs_and_answers_from_the_master_and_candles(tables, capsys):
    ns, app = tables
    run_in_console(ns, "01_lookups.py")
    out = capsys.readouterr().out
    assert "NIFTY 30JAN30 FUT" in out                                                  # contract("NIFTY")
    assert "2030-01-09, 2030-01-16" in out                                             # expiries
    assert "nearest to 25030.5" in out                                                 # the price came from the candles
    assert "09JAN30 25000 CE" in out and "09:17" in out
    assert "the nearest call is NIFTY 09JAN30 25000 CE tick 0.05 lot 50" in out        # the result used in the script's own code


def test_the_manual_trade_example_makes_a_portfolio_and_sends_the_commands_in_order(tables, tmp_path, capsys):
    from types import SimpleNamespace

    from algo.comms.nats_service import NatsConfig, NatsService
    from algo.strategy.registry import Registry
    from tests.test_nats_service import Wire
    ns, app = tables
    app.plugins["registry"] = SimpleNamespace(state=Registry(str(tmp_path / "reg.json")), describe=lambda: "registry")
    wire = Wire()
    svc = NatsService(NatsConfig(users=(3, 4), send_on=True), transport_factory=lambda: wire)
    svc.connect()
    app.plugins["nats"] = svc
    run_in_console(ns, "02_manual_trade.py")
    assert [lab for _, lab, _ in wire.sent] == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]
    assert {s for s, _, _ in wire.sent} == {"cmd.hft.1001"} and {p["USERIDENTIFIER"] for _, _, p in wire.sent} == {4}
    assert wire.sent[0][2]["T1"] == 502                                                # the CE nearest 25030 in the nearest expiry
    assert wire.sent[2][2]["maxqty"] == 3 and wire.sent[2][2]["side"] == "BUY"
    rec = app.plugins["registry"].state.get(4, 1001)
    assert (rec["owner"], rec["algo"], rec["name"]) == ("manual", "manual", "NIFTY")
    out = capsys.readouterr().out
    assert "PF 1001 pause: ok" in out and "created portfolio 1001 for manual" in out and "user 4  portfolio 1001  NIFTY" in out
    assert "connected to the NATS service" in out and "connected to the portfolio registry" in out         # it connects first
    svc.on_stop()


def test_the_plain_examples_start_nothing_and_define_nothing():
    import ast
    for name in ("01_lookups.py", "02_manual_trade.py"):
        tree = ast.parse((EXAMPLES / name).read_text(encoding="utf-8"))
        assert not [n for n in ast.walk(tree) if isinstance(n, (ast.ClassDef, ast.FunctionDef))], name       # no class, no def
        called = {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert not called & {"master_build", "candles_start", "nats_start", "registry_start", "nats_restart"}, name
