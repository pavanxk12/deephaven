# ruff: noqa: F811
"""The plain functions for writing an automation as a script (algo_start, pf_assign, pf_activate ...). Real registry, book, bridge and
NATS service; only the network connection and Deephaven are fakes."""
import time
import types

import pytest

from algo import api
from algo.automation import (algo_exit, algo_pause, algo_portfolios, algo_start, algo_stop, algo_unpause, algo_unsubscribe, pf_activate,
                             pf_assign, pf_cooling, pf_leg, pf_mark, pf_track, watch)
from algo.comms.nats_service import NatsConfig, NatsService
from algo.core import app as app_module
from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.strategy.registry import Registry
from tests.test_nats_service import Wire
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)


def make_app(tmp_path, users=(3, 4), master_on=True, status="ok"):
    app = App({}, NSE, Paths(market_data=str(tmp_path), work=str(tmp_path)))
    app.plugins["registry"] = types.SimpleNamespace(state=Registry(str(tmp_path / "reg.json")), describe=lambda: "registry")
    wire = Wire(status=status)
    svc = NatsService(NatsConfig(users=tuple(users), send_on=master_on), transport_factory=lambda: wire)
    svc.connect()
    app.plugins["nats"] = svc
    return app, svc, wire


@pytest.fixture
def console(tmp_path, monkeypatch):
    """A console session: the registry, the NATS service (fake connection) and the app the functions use."""
    app, svc, wire = make_app(tmp_path)
    monkeypatch.setattr(app_module, "_APP", app)
    yield app, svc, wire
    svc.on_stop()


def wait_for(cond, seconds=10):
    end = time.time() + seconds
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return False


def labels(wire, pf=None):
    return [lab for subject, lab, _ in wire.sent if pf is None or subject == f"cmd.hft.{pf}"]


def on(strategy):
    api.nats_send(True, client=strategy)


# ---- starting one --------------------------------------------------------------------------------------------------------
def test_algo_start_checks_its_inputs_and_leaves_nothing_behind_when_it_fails(console):
    app, svc, wire = console
    for bad in (None, 0, -1, "x", True):
        with pytest.raises(ValueError, match="user"):
            algo_start(bad, "mine")
    for bad in ("", "  ", None, 5):
        with pytest.raises(ValueError, match="strategy"):
            algo_start(4, bad)
    with pytest.raises(ValueError, match="another service"):
        algo_start(4, "nats")                                                         # the name of a running service
    with pytest.raises(TypeError):
        algo_start(4, "mine", colour="red")                                           # a misspelt option is an error
    assert not {"mine", "mine_book", "mine_bridge"} & set(app.plugins)


def test_algo_start_says_so_when_the_registry_is_not_running(console, capsys):
    app, svc, wire = console
    del app.plugins["registry"]
    assert algo_start(4, "mine") is False and "registry is not running" in capsys.readouterr().out
    assert "mine" not in app.plugins


def test_a_started_automation_has_its_own_book_and_switch_and_starts_off(console, capsys):
    app, svc, wire = console
    assert algo_start(4, "mine", pf_start=2000) is True
    assert {"mine", "mine_book", "mine_bridge"} <= set(app.plugins)
    assert svc.is_enabled("mine") is False and app["mine"].nats.state == "off"
    on("mine")
    assert svc.is_enabled("mine") and app["mine"].nats.state == "on"                  # the switch is named after the automation
    assert "user 4, 0 portfolios" in app["mine"].describe()
    api.nats_send(False, client="mine")
    assert not svc.is_enabled("mine")
    algo_stop("mine")
    assert not {"mine", "mine_book", "mine_bridge"} & set(app.plugins)                # stopping it stops what it started


def test_restarting_an_automation_switches_it_off_again_and_keeps_its_portfolios(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    pf = pf_assign("mine", 1, 55, name="NIFTY")["pf"]
    assert svc.is_enabled("mine")
    algo_start(4, "mine", pf_start=2000)                                              # re-pasted
    assert svc.is_enabled("mine") is False and "mine_book" in app.plugins
    pf_track("mine", [1])
    df = algo_portfolios("mine")
    assert list(df["pf"]) == [pf] and list(df["name"]) == ["NIFTY"]                   # the number, and what it is, were remembered


def test_a_user_the_service_does_not_have_is_warned_about(console, caplog):
    app, svc, wire = console
    with caplog.at_level("WARNING", logger="algo"):
        algo_start(9, "mine")
    assert any("not one of the NATS service's users" in r.getMessage() for r in caplog.records)


# ---- the book -----------------------------------------------------------------------------------------------------------------
def test_pf_assign_takes_no_number_while_the_automation_is_off(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    assert pf_assign("mine", 1, 55, name="NIFTY") is None
    assert app.plugins["registry"].state.portfolios() == []                           # no number was used up
    on("mine")
    a = pf_assign("mine", 1, 55, name="NIFTY")
    assert a == {"strategy": "mine", "pf": 2000, "instrument": 1, "option": 55, "slot": 0, "old_pf": None, "created": True}
    rec = app.plugins["registry"].state.get(4, 2000)
    assert (rec["owner"], rec["algo"], rec["name"]) == ("mine_book", "mine", "NIFTY")        # user, number, strategy, name remembered


def test_the_book_keeps_a_portfolio_while_the_option_is_the_same_and_replaces_it_when_it_changes(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    first = pf_assign("mine", 1, 55)
    app["mine"].book.state.subscribe(first["pf"], 55)                                  # the bridge subscribed it
    same = pf_assign("mine", 1, 55)
    assert (same["pf"], same["old_pf"], same["created"]) == (first["pf"], None, False)
    moved = pf_assign("mine", 1, 56)
    assert (moved["pf"], moved["old_pf"], moved["created"]) == (first["pf"] + 1, first["pf"], True)


def test_slots_give_one_instrument_several_portfolios_and_the_table_lists_them(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    got = [pf_assign("mine", 7, 100 + k, name="NIFTY", slot=k) for k in range(3)]
    assert [a["pf"] for a in got] == [2000, 2001, 2002] and [a["slot"] for a in got] == [0, 1, 2]
    pf_mark("mine", 7, "CALL", 100, slot=0)
    df = algo_portfolios("mine")
    assert [(r.pf, r.instrument, r.slot, r.name, r.algo) for r in df.itertuples()] == [
        (2000, 7, 0, "NIFTY", "mine"), (2001, 7, 1, "NIFTY", "mine"), (2002, 7, 2, "NIFTY", "mine")]
    assert df["leg"].iloc[0] == "CALL" and df["leg"].isna().tolist() == [False, True, True]      # a slot with no leg shows as empty
    assert pf_leg("mine", 7) == "CALL" and pf_leg("mine", 7, 1) is None
    with pytest.raises(ValueError):
        pf_assign("mine", 7, 1, slot=100)


def test_two_automations_of_one_user_never_share_a_number_and_two_users_may(console):
    app, svc, wire = console
    for user, name in ((3, "algo_a"), (3, "algo_b"), (4, "algo_c")):
        algo_start(user, name, pf_start=1111)
        on(name)
    got = [pf_assign(n, 1, 5)["pf"] for n in ("algo_a", "algo_b", "algo_c")]
    assert got == [1111, 1112, 1111]


def test_pf_cooling_is_true_only_just_after_a_failure(console):
    app, svc, wire = console
    algo_start(4, "mine")
    on("mine")
    assert pf_cooling("mine", 1, 30) is False
    app["mine"].book.state.last_fail[1] = time.time()
    assert pf_cooling("mine", 1, 30) is True and pf_cooling("mine", 1, 0) is False
    app["mine"].book.state.last_fail[1] = time.time() - 31
    assert pf_cooling("mine", 1, 30) is False


# ---- the commands ------------------------------------------------------------------------------------------------------------
def test_pf_activate_runs_the_standard_sequence_as_this_user_and_automation(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000, add_qty=3)
    on("mine")
    a = pf_assign("mine", 1, 55, name="NIFTY")
    assert pf_activate(a, "CALL", {"ival": 1.0, "side": "BUY"}) is True               # returns at once
    assert wait_for(lambda: len(wire.sent) == 4)
    assert labels(wire) == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]
    assert {p["USERIDENTIFIER"] for _, _, p in wire.sent} == {4} and wire.sent[2][2]["maxqty"] == 3
    (row,) = algo_portfolios("mine").itertuples()
    assert (row.option, row.leg, row.cap, row.paused) == (55, "CALL", 3, False) and pf_leg("mine", 1) == "CALL"


def test_activating_a_changed_option_retires_the_old_portfolio_first(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    pf_activate(pf_assign("mine", 1, 55), "CALL", {})
    assert wait_for(lambda: len(wire.sent) == 4)
    pf_activate(pf_assign("mine", 1, 56), "PUT", {})
    assert wait_for(lambda: len(wire.sent) == 4 + 7)
    assert labels(wire, 2000)[4:] == ["pause_unpause:Pause", "squareup", "unsubscribe"]
    assert labels(wire, 2001) == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause"]


def test_pf_activate_does_nothing_and_marks_nothing_when_it_cannot_send(console):
    app, svc, wire = console
    algo_start(4, "mine")
    on("mine")
    a = pf_assign("mine", 1, 55)
    api.nats_send(False, client="mine")
    assert pf_activate(a, "CALL", {}) is False and pf_leg("mine", 1) is None and wire.sent == []
    assert pf_activate(None, "CALL", {}) is False and pf_activate({}, "CALL", {}) is False        # nothing assigned: nothing to do


def test_a_failed_activation_rolls_back_and_the_slot_cools_down(tmp_path, monkeypatch):
    app, svc, wire = make_app(tmp_path, status="error")
    monkeypatch.setattr(app_module, "_APP", app)
    algo_start(4, "mine")
    on("mine")
    pf_activate(pf_assign("mine", 1, 55), "CALL", {})
    assert wait_for(lambda: pf_cooling("mine", 1, 30))
    assert pf_leg("mine", 1) is None                                                  # rolled back: a later update may try again
    svc.on_stop()


def test_the_many_calls_cover_every_portfolio_of_the_automation(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    a, b = pf_assign("mine", 1, 55), pf_assign("mine", 2, 66)
    algo_pause("mine")
    assert wait_for(lambda: labels(wire, a["pf"]) == ["pause_unpause:Pause"] and labels(wire, b["pf"]) == ["pause_unpause:Pause"])
    algo_unpause("mine")
    algo_exit("mine")
    assert wait_for(lambda: "squareup" in labels(wire, a["pf"]) and "squareup" in labels(wire, b["pf"]))
    algo_unsubscribe("mine")                                                          # nothing is subscribed here: nothing to do


# ---- nothing raises ------------------------------------------------------------------------------------------------------------
def test_an_unknown_automation_is_said_not_raised(console, capsys):
    results = [pf_assign("ghost", 1, 5), pf_activate({"strategy": "ghost", "pf": 1, "instrument": 1, "option": 5}, "CALL", {}),
               pf_leg("ghost", 1), pf_cooling("ghost", 1), algo_portfolios("ghost")]
    pf_track("ghost", [1])
    pf_mark("ghost", 1, "CALL")
    for fn in (algo_pause, algo_unpause, algo_exit, algo_unsubscribe):
        fn("ghost")
    assert results == [None, False, None, False, None]
    assert capsys.readouterr().out.count("no automation called 'ghost'") == 11


def test_nothing_raises_when_the_nats_service_is_down(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    a = pf_assign("mine", 1, 55)
    app.plugins.pop("nats")
    svc.on_stop()                                                                      # the service goes away
    assert app["mine"].nats.state == "service_down" and app["mine"].nats.sending is False
    assert pf_activate(a, "CALL", {}) is False and pf_assign("mine", 1, 56) is None
    algo_pause("mine")
    algo_exit("mine")


def test_nats_send_with_the_strategy_name_reaches_the_automation_and_the_help_lists_the_functions(console, capsys):
    app, svc, wire = console
    algo_start(4, "mine")
    seen = []
    app["mine"].nats.bridge.on_enable(lambda: seen.append("recheck"))
    on("mine")
    assert seen == ["recheck"]                                                         # it went through the automation's own bridge
    api.algo_help()
    out = capsys.readouterr().out
    assert "AUTOMATIONS (a script you write)" in out and "pf_assign()" in out and "algo_start()" in out


# ---- the portfolios of an older script ---------------------------------------------------------------------------------------
def test_algo_start_adopts_the_portfolios_in_an_older_scripts_files(console, tmp_path):
    import json
    app, svc, wire = console
    (tmp_path / "old_map.json").write_text(json.dumps({"10": 1111, "11": 1112}))               # token -> portfolio
    (tmp_path / "old_sub.json").write_text(json.dumps({"1111": 555}))                         # portfolio -> option
    assert algo_start(3, "stocks", pf_start=1111, map_file=str(tmp_path / "old_map.json"), sub_file=str(tmp_path / "old_sub.json"))
    pf_track("stocks", [10, 11])
    table = algo_portfolios("stocks")
    assert [(r.pf, r.instrument) for r in table.itertuples()] == [(1111, 10), (1112, 11)] and table["option"].iloc[0] == 555
    reg = app.plugins["registry"].state
    assert reg.owner_of(3, 1111) == reg.owner_of(3, 1112) == "stocks_book"                       # registered for user 3
    on("stocks")
    pf_track("stocks", [12])
    assert pf_assign("stocks", 12, 777, name="TCS")["pf"] == 1113                                # numbering continues after the old ones
    assert pf_assign("stocks", 10, 555, name="RELIANCE")["pf"] == 1111                           # and an old stock keeps its portfolio


def test_a_number_in_an_older_file_that_another_automation_holds_is_reported_as_a_clash(console, tmp_path, caplog):
    import json
    app, svc, wire = console
    app.plugins["registry"].state.register(3, 1111, "someone_else")
    (tmp_path / "old_map.json").write_text(json.dumps({"10": 1111}))
    with caplog.at_level("ERROR", logger="algo"):
        algo_start(3, "stocks", map_file=str(tmp_path / "old_map.json"), sub_file=str(tmp_path / "none.json"))
    assert any("CLASH" in r.getMessage() and "someone_else" in r.getMessage() for r in caplog.records)


def test_a_file_in_another_format_is_refused_and_left_exactly_as_it_was(console, tmp_path):
    app, svc, wire = console
    text = '{"47159:0": 1933, "47159:1": 1934}'                                                    # the old NIFTY token:slot format
    path = tmp_path / "nifty_map.json"
    path.write_text(text)
    with pytest.raises(RuntimeError, match="cannot be read as a portfolio file"):
        algo_start(4, "nifty", map_file=str(path), sub_file=str(tmp_path / "none.json"))
    assert path.read_text() == text                                                              # not overwritten
    assert not {"nifty", "nifty_book", "nifty_bridge"} & set(app.plugins)                       # and nothing was left half-started
    for bad in ("not json at all", "[1, 2]", '{"1": "x"}'):
        path.write_text(bad)
        with pytest.raises(RuntimeError, match="cannot be read"):
            algo_start(4, "nifty", map_file=str(path), sub_file=str(tmp_path / "none.json"))
        assert path.read_text() == bad
    path.write_text("")                                                                          # an empty file is a fresh start
    assert algo_start(4, "nifty", map_file=str(path), sub_file=str(tmp_path / "none.json")) is True


# ---- watch: a listener that follows a rebuilt table ------------------------------------------------------------------------------
def test_watch_follows_a_table_that_is_rebuilt_and_stops_cleanly(console, fake_dh):
    app, svc, wire = console
    app.tables["candles"] = "candles-1"
    seen = []
    w = watch("candles", lambda update, is_replay: seen.append(update))
    assert len(fake_dh) == 1 and fake_dh[0].replay is False
    fake_dh[0].fn("tick-1", False)
    app.publish("candles", "candles-2")                               # candle_start was run again: the table is replaced and republished
    assert fake_dh[0].stopped is True and len(fake_dh) == 2           # the old listener is dropped, a new one is on the new table
    fake_dh[1].fn("tick-2", False)
    assert seen == ["tick-1", "tick-2"]
    w.stop()
    assert fake_dh[1].stopped is True
    app.publish("candles", "candles-3")
    assert len(fake_dh) == 2                                          # stopped for good: it does not come back


def test_watch_logs_an_error_in_your_function_and_carries_on(console, fake_dh, caplog):
    app, svc, wire = console
    app.tables["candles"] = "candles-1"
    calls = []

    def fn(update, is_replay):
        calls.append(update)
        if update == "bad":
            raise ZeroDivisionError("boom")
    watch("candles", fn)
    with caplog.at_level("ERROR"):
        fake_dh[0].fn("bad", False)
        fake_dh[0].fn("good", False)
    assert calls == ["bad", "good"] and any("failed" in r.getMessage() for r in caplog.records)      # logged, not raised


def test_watch_says_when_the_table_is_not_there(console, capsys):
    app, svc, wire = console
    assert watch("straddle_series", lambda u, r: None) is None and watch("candles", lambda u, r: None) is None
    out = capsys.readouterr().out
    assert "straddle_start(0)" in out and "candles_start()" in out


def test_a_strategy_sends_its_own_strategyname_on_subscribe_and_unsubscribe(console):
    app, svc, wire = console
    algo_start(3, "a", subscribe={"strategyname": "MyLogic"})
    algo_start(3, "b")
    pf_track("a", [1])
    pf_track("b", [2])
    on("a")
    on("b")
    for name, ins, opt in (("a", 1, 55), ("b", 2, 66)):
        assert pf_activate(pf_assign(name, ins, opt, name="N"), "CALL", {"ival": 1})
    assert wait_for(lambda: len(wire.sent) == 8)
    subs = {p["T1"]: p for _, lab, p in wire.sent if lab == "subscribe"}
    assert subs[55]["strategyname"] == "MyLogic" and "strategyname" not in subs[66]        # the service no longer adds one
    assert subs[55]["gtw"] == 32395                                                       # the rest of the service's defaults are kept


# ---- algo_stop closes the automation down, then stops it ---------------------------------------------------------------------
def test_algo_stop_pauses_squares_up_and_unsubscribes_every_portfolio_then_stops(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000, squareup_settle_s=0)
    on("mine")
    a = pf_assign("mine", 1, 55)
    b = pf_assign("mine", 2, 66)
    assert pf_activate(a, "CALL", {}) and pf_activate(b, "PUT", {})
    assert wait_for(lambda: all(labels(wire, pf)[-1:] == ["pause_unpause:UnPause"] for pf in (a["pf"], b["pf"])))     # both subscribed and running
    wire.sent.clear()
    algo_stop("mine")
    for pf in (a["pf"], b["pf"]):
        assert labels(wire, pf) == ["pause_unpause:Pause", "squareup", "unsubscribe"], labels(wire, pf)        # in this order, on each portfolio
    assert not {"mine", "mine_book", "mine_bridge"} & set(app.plugins)                                     # and only then it stopped (it waited for them)
    assert not svc.is_enabled("mine")


def test_algo_stop_does_not_stop_while_the_portfolios_cannot_be_unsubscribed_and_can_be_forced(console, capsys):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    a = pf_assign("mine", 1, 55)
    assert pf_activate(a, "CALL", {})
    assert wait_for(lambda: labels(wire, a["pf"])[-1:] == ["pause_unpause:UnPause"])
    api.nats_send(False, client="mine")                                                                    # sending is OFF (a breach, or switched off by hand)
    wire.sent.clear()
    algo_stop("mine")
    out = capsys.readouterr().out
    assert "NOT stopped" in out and "unsubscribe=False" in out and "mine" in app.plugins and wire.sent == []
    algo_stop("mine", unsubscribe=False)                                                                   # stop it as it is
    assert "mine" not in app.plugins and wire.sent == []


def test_algo_stop_of_an_automation_with_no_portfolios_just_stops(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    algo_stop("mine")
    assert "mine" not in app.plugins and wire.sent == []
    algo_stop("ghost")                                                                                     # an unknown name is not an error


def test_algo_stop_also_closes_a_portfolio_the_script_subscribed_by_itself(console):
    """A CrossOrder script (08, 10) sends its own subscribe: the book is not told, the NATS service is. algo_stop must still pause, square up and unsubscribe it."""
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000, squareup_settle_s=0)
    on("mine")
    a = pf_assign("mine", 1, 1)
    svc.subscribe(a["pf"], 1, {"strategyname": "CrossOrder", "TOKEN": 1, "SIDE": 1, "QTY": 1}, user=4, client="mine")
    assert wait_for(lambda: svc.pf_state(a["pf"], 4) is not None and svc.pf_state(a["pf"], 4).subscribed)
    assert wait_for(lambda: dict(app["mine"].book.state.subscribed) == {a["pf"]: 1})              # the book learned it from the confirmed subscribe, though the script sent it itself
    import json, os
    sub_file = app["mine"].book.state.sub_file
    assert wait_for(lambda: os.path.exists(sub_file) and json.load(open(sub_file)) == {str(a["pf"]): 1})                                           # and wrote it down (Postgres' algo_pf_sub when that store is on)
    wire.sent.clear()
    algo_stop("mine")
    assert labels(wire, a["pf"]) == ["pause_unpause:Pause", "squareup", "unsubscribe"], labels(wire, a["pf"])
    assert "mine" not in app.plugins


def test_a_portfolio_nobody_subscribed_is_still_left_alone_by_algo_stop(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    a = pf_assign("mine", 1, 1)                                                                     # a number from the book, never subscribed
    wire.sent.clear()
    algo_stop("mine")
    assert labels(wire, a["pf"]) == [] and "mine" not in app.plugins


def test_the_book_also_learns_a_subscribe_typed_on_the_console_and_ignores_other_users_and_strategies(console):
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000)
    on("mine")
    a = pf_assign("mine", 1, 1)
    svc.subscribe(a["pf"], 1, {"strategyname": "CrossOrder"}, user=4)                                # no client name: an operator's typed command
    assert wait_for(lambda: dict(app["mine"].book.state.subscribed) == {a["pf"]: 1})
    svc.subscribe(777, 5, {"strategyname": "CrossOrder"}, user=4)                                    # a portfolio that is not this strategy's
    assert wait_for(lambda: svc.pf_state(777, 4) is not None and svc.pf_state(777, 4).subscribed)
    assert dict(app["mine"].book.state.subscribed) == {a["pf"]: 1}
    svc.unsubscribe(a["pf"], 1, user=4)                                                              # an unsubscribe does not erase what the book remembers (a retired portfolio keeps its option)
    assert wait_for(lambda: not svc.pf_state(a["pf"], 4).subscribed)
    assert dict(app["mine"].book.state.subscribed) == {a["pf"]: 1}


def test_the_unsubscribe_waits_for_the_closing_order_after_the_squareup(console):
    """The unsubscribe destroys a plugin portfolio: it must come only after the settle time, so the plugin can send and work its closing order."""
    app, svc, wire = console
    algo_start(4, "mine", pf_start=2000, squareup_settle_s=0.5)
    on("mine")
    a = pf_assign("mine", 1, 1)
    svc.subscribe(a["pf"], 1, {"strategyname": "CrossOrder", "TOKEN": 1, "SIDE": 1, "QTY": 1}, user=4, client="mine")
    assert wait_for(lambda: svc.pf_state(a["pf"], 4) is not None and svc.pf_state(a["pf"], 4).subscribed)
    wire.sent.clear()
    app["mine"].nats.bridge.unsubscribe_all()
    assert wait_for(lambda: "squareup" in labels(wire, a["pf"]))
    time.sleep(0.2)
    assert "unsubscribe" not in labels(wire, a["pf"])                                                # still waiting: the plugin is closing
    assert wait_for(lambda: labels(wire, a["pf"])[-1:] == ["unsubscribe"])                          # then it goes
    assert labels(wire, a["pf"]) == ["pause_unpause:Pause", "squareup", "unsubscribe"]
