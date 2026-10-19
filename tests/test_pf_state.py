# ruff: noqa: F811
"""Pause, unpause, square up, subscribe and unsubscribe are ONE state shared by the algos and the console.

The console sends its commands through the same NATS service as an algo (with no client name: an operator), so a click and an algo's
command are both seen by the service, which records what the bridge confirmed. Real registry, book, bridge and service; fake wire."""
import threading
import types

import pytest

from algo.automation import algo_start, algo_portfolios, pf_activate, pf_allowed, pf_assign, pf_leg, pf_track
from algo.core.pfstate import PfStates, pf_states
from tests.test_automation import console, on, wait_for  # noqa: F401  (the fixture)

PARAMS = {"ival": 0.2, "target": 100}


def started(app, svc, wire, user=3, name="mine", instrument=1, option=55):
    """An automation with one portfolio running: subscribed, applied, unpaused."""
    algo_start(user, name, pf_start=1001)
    pf_track(name, [instrument])
    on(name)
    a = pf_assign(name, instrument, option, name="NIFTY")
    assert pf_activate(a, "CALL", PARAMS)
    assert wait_for(lambda: len(wire.sent) == 4)
    return a["pf"]


# ---- the shared states themselves -----------------------------------------------------------------------------------------------
def test_each_confirmed_command_changes_the_state():
    s = PfStates()
    s.record(3, 1001, "subscribe", {"T1": 55}, "mine")
    st = s.get(3, 1001)
    assert (st.option, st.subscribed, st.paused, st.squared, st.by) == (55, True, False, False, "mine")
    s.record(3, 1001, "pause_unpause", {"Action": "Pause"}, None)
    assert s.get(3, 1001).paused is True and s.get(3, 1001).by == "operator"              # no client name = the console / a typed command
    s.record(3, 1001, "squareup", None, None)
    assert s.get(3, 1001).squared is True
    s.record(3, 1001, "apply", {"maxqty": 3}, "mine")
    assert s.get(3, 1001).squared is False                                                 # a new apply starts it again
    s.record(3, 1001, "pause_unpause", {"Action": "UnPause"}, "mine")
    s.record(3, 1001, "unsubscribe", {"T1": 55}, None)
    st = s.get(3, 1001)
    assert (st.subscribed, st.paused, st.option, st.cmd) == (False, False, 55, "unsubscribe")
    assert s.record(3, 1001, "something_else", {}, None) is None


def test_the_same_number_under_two_users_is_two_portfolios():
    s = PfStates()
    s.record(3, 1001, "pause_unpause", {"Action": "Pause"}, None)
    assert s.get(4, 1001) is None and s.get(3, 1001).paused


def test_by_option_finds_the_portfolio_a_row_belongs_to_preferring_the_subscribed_one():
    s = PfStates()
    s.record(3, 1001, "subscribe", {"T1": 55}, "a")
    s.record(3, 1001, "unsubscribe", {"T1": 55}, "a")
    s.record(3, 1002, "subscribe", {"T1": 55}, "a")
    s.record(3, 1003, "subscribe", {"T1": 77}, "a")
    found = s.by_option()
    assert found[55][:2] == (3, 1002) and found[77][:2] == (3, 1003)
    assert s.by_option(4) == {}


def test_adopt_never_overwrites_what_was_seen_and_listeners_hear_every_change():
    s = PfStates()
    heard = []
    undo = s.on_change(lambda u, p, st: heard.append((u, p, st.cmd)))
    s.record(3, 1001, "pause_unpause", {"Action": "Pause"}, None)
    s.adopt(3, 1001, 55)                                                                   # already known: left as it is
    s.adopt(3, 1002, 56, paused=True)
    assert s.get(3, 1001).paused and s.get(3, 1001).option is None
    assert (s.get(3, 1002).option, s.get(3, 1002).subscribed, s.get(3, 1002).paused) == (56, True, True)
    assert heard == [(3, 1001, "pause_unpause"), (3, 1002, "adopted")]
    undo()
    s.record(3, 1001, "squareup", None, None)
    assert len(heard) == 2


def test_a_listener_that_fails_does_not_stop_the_others_and_the_same_one_is_added_once():
    s = PfStates()
    got = []

    def bad(u, p, st):
        raise RuntimeError("boom")

    def good(u, p, st):
        got.append(p)

    s.on_change(bad)
    s.on_change(good)
    s.on_change(good)
    s.record(3, 7, "squareup", None, None)
    assert got == [7]


def test_one_hub_per_app_and_many_threads_may_write():
    shared = {}
    assert pf_states(shared) is pf_states(shared)
    s = pf_states(shared)

    def work(user):
        for pf in range(200):
            s.record(user, pf, "pause_unpause", {"Action": "Pause"}, None)

    ts = [threading.Thread(target=work, args=(u,)) for u in range(1, 5)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(s) == 800


# ---- the service records what the bridge confirmed -----------------------------------------------------------------------------
def test_the_service_records_a_confirmed_command_with_who_sent_it_and_a_refused_one_not_at_all(console):
    app, svc, wire = console
    assert svc.pause(1001, user=3).result(5).ok
    st = svc.pf_state(1001, user=3)
    assert st.paused and st.by == "operator"
    assert svc.pause(2001, user=3, client="mine").result(5).status == "off"               # refused: that client is not switched on
    assert svc.pf_state(2001, user=3) is None
    svc.enable("mine")
    svc.unpause(1001, user=3, client="mine").result(5)
    st = svc.pf_state(1001, user=3)
    assert not st.paused and st.by == "mine"
    wire.status = "error"
    assert not svc.pause(1001, user=3).result(5).ok
    assert not svc.pf_state(1001, user=3).paused                                           # the bridge said no: nothing changed


def test_one_state_is_kept_per_user_and_number_and_a_row_is_written_for_every_change(console):
    app, svc, wire = console

    class Rows:
        def __init__(self):
            self.rows = []

        def write_row(self, *r):
            self.rows.append(r)

    svc.state_log, svc.zone = Rows(), app.market.zone
    svc.subscribe(1001, 55, user=3).result(5)
    svc.pause(1001, user=3).result(5)
    svc.pause(1001, user=4).result(5)
    rows = svc.state_log.rows
    assert [(r[1], r[2], r[3], r[5], r[6], r[7]) for r in rows] == [(3, 1001, "subscribe", 55, 1, 0), (3, 1001, "pause_unpause", 55, 1, 1),
                                                                    (4, 1001, "pause_unpause", 0, 0, 1)]
    assert svc.pf_state(1001, user=4).paused and svc.pf_state(1001, user=3).option == 55


# ---- the algo's commands show on the console side -----------------------------------------------------------------------------
def test_what_an_algo_does_is_in_the_shared_state_the_console_reads(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    st = svc.states.by_option()[55]
    assert st[:2] == (3, pf) and st[2].subscribed and not st[2].paused and st[2].by == "mine"
    assert st[2].cmd == "pause_unpause"                                                    # the last step of the sequence: unpause
    from algo.automation import algo_pause
    algo_pause("mine")
    assert wait_for(lambda: svc.states.get(3, pf).paused)


# ---- the console's clicks reach the algo --------------------------------------------------------------------------------------
def test_a_pause_from_the_console_is_in_the_algos_book(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    svc.pause(pf, user=3).result(5)                                                        # a click on Pause
    assert pf in app["mine"].book.state.paused and algo_portfolios("mine")["paused"].iloc[0]
    svc.unpause(pf, user=3).result(5)                                                      # a click on Resume
    assert pf not in app["mine"].book.state.paused


def test_a_square_up_from_the_console_lets_the_algo_start_the_slot_again(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    assert pf_leg("mine", 1) == "CALL"
    svc.pause(pf, user=3).result(5)
    svc.squareup(pf, user=3).result(5)
    assert pf_leg("mine", 1) is None


def test_unticking_the_box_blocks_the_slot_and_ticking_it_allows_it_again(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    assert pf_allowed("mine", 1)
    svc.pause(pf, user=3).result(5)
    svc.squareup(pf, user=3).result(5)
    svc.unsubscribe(pf, 55, user=3).result(5)                                              # the tick box, unticked
    assert not pf_allowed("mine", 1)
    assert pf_assign("mine", 1, 55, name="NIFTY") is None                                  # no portfolio, no order, while it is unticked
    assert pf_assign("mine", 1, 56, name="NIFTY") is None                                  # not even for another option
    svc.subscribe(pf, 55, user=3).result(5)                                                # ticked again
    assert pf_allowed("mine", 1)
    assert pf_assign("mine", 1, 55, name="NIFTY")["pf"] == pf


def test_the_algos_own_unsubscribe_does_not_block_its_slot(console):
    """Only the console's tick box blocks; an algo that retires a portfolio moves on to the next one."""
    app, svc, wire = console
    pf = started(app, svc, wire)
    svc.unsubscribe(pf, 55, user=3, client="mine").result(5)
    assert pf_allowed("mine", 1)


def test_a_pause_made_on_the_console_is_not_undone_by_the_algos_next_order(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    svc.pause(pf, user=3).result(5)                                                        # the operator holds it
    n = len(wire.sent)
    a = pf_assign("mine", 1, 55, name="NIFTY")
    assert pf_activate(a, "CALL", PARAMS)
    assert wait_for(lambda: len(wire.sent) >= n + 2)
    sent = [lab for _, lab, _ in wire.sent[n:]]
    assert sent == ["pause_unpause:Pause", "apply"]                                        # it applied, and did NOT unpause
    assert svc.pf_state(pf, 3).paused and pf in app["mine"].book.state.paused
    svc.unpause(pf, user=3).result(5)                                                      # the operator resumes it
    assert not svc.pf_state(pf, 3).paused


def test_the_algos_own_pause_does_not_hold_the_next_order(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    from algo.automation import algo_pause
    algo_pause("mine")
    assert wait_for(lambda: svc.pf_state(pf, 3).paused)
    n = len(wire.sent)
    assert pf_activate(pf_assign("mine", 1, 55, name="NIFTY"), "CALL", PARAMS)
    assert wait_for(lambda: len(wire.sent) >= n + 3)
    assert [lab for _, lab, _ in wire.sent[n:n + 3]] == ["pause_unpause:Pause", "apply", "pause_unpause:UnPause"]


def test_another_users_portfolio_with_the_same_number_does_not_touch_this_algo(console):
    app, svc, wire = console
    pf = started(app, svc, wire)
    svc.pause(pf, user=4).result(5)                                                        # user 4's portfolio 1001, not ours
    assert pf not in app["mine"].book.state.paused


def test_portfolios_the_book_already_had_are_known_to_the_console_from_the_start(console, tmp_path):
    import json
    app, svc, wire = console
    (tmp_path / "m.json").write_text(json.dumps({"1": 1001}))
    (tmp_path / "s.json").write_text(json.dumps({"1001": 55}))                             # what an earlier session had subscribed
    algo_start(3, "mine", pf_start=1001, map_file=str(tmp_path / "m.json"), sub_file=str(tmp_path / "s.json"))
    st = svc.states.get(3, 1001)
    assert st.option == 55 and st.subscribed and st.cmd == "adopted"
    assert svc.states.by_option()[55][:2] == (3, 1001)


def test_the_states_survive_a_restart_of_the_service_when_the_app_shares_them():
    shared = {}
    a, b = pf_states(shared), pf_states(shared)
    a.record(3, 1001, "pause_unpause", {"Action": "Pause"}, None)
    assert b.get(3, 1001).paused


@pytest.mark.parametrize("cmd,payload", [("pause_unpause", {"Action": "Pause"}), ("squareup", None), ("unsubscribe", {"T1": 5})])
def test_the_console_may_act_on_a_portfolio_nobody_has_seen_yet(cmd, payload):
    s = PfStates()
    assert s.record(9, 99, cmd, payload, None) is not None and s.get(9, 99) is not None


def test_by_option_can_leave_out_another_strategys_portfolios():
    s = PfStates()
    s.record(3, 1001, "subscribe", {"T1": 55}, "a")
    s.record(3, 1002, "subscribe", {"T1": 55}, "b")                                        # the same option, another strategy
    s.get(3, 1002).at += 1                                                                 # (the clock may not tick between two calls)
    assert s.by_option()[55][1] == 1002                                                    # the later one
    assert s.by_option(keep=lambda u, p, st: st.by == "a")[55][1] == 1001
    assert s.by_option(keep=lambda u, p, st: False) == {}


def test_algo_start_asks_a_running_console_for_the_strategys_window_and_a_failing_console_does_not_stop_it(console):
    app, svc, wire = console
    made = []
    app.plugins["console"] = types.SimpleNamespace(open=lambda name: made.append(name))
    assert algo_start(3, "stocks", pf_start=1001) and made == ["stocks"]
    app.plugins["console"] = types.SimpleNamespace(open=lambda name: 1 / 0)
    assert algo_start(3, "nifty", pf_start=2001) and "nifty" in app.plugins


def test_console_open_says_so_when_there_is_no_console(console, capsys):
    from algo.api import console_open
    assert console_open("stocks") is False and "not running" in capsys.readouterr().out


def test_a_subscribe_that_names_its_contract_as_token_is_linked_to_that_contract():
    s = PfStates()
    s.record(4, 1030, "subscribe", {"strategyname": "single", "TOKEN": 49013, "SIDE": 1, "QTY": 1}, "basket_rsi")        # the single-order strategy has no T1
    st = s.get(4, 1030)
    assert (st.option, st.subscribed) == (49013, True)
    assert s.by_option()[49013][:2] == (4, 1030)                                                                         # so the positions table and the toolbar find it
    s.record(4, 1031, "subscribe", {"T1": 55, "TOKEN": 99}, "mine")
    assert s.get(4, 1031).option == 55                                                                                   # T1 wins when both are there
    s.record(4, 1030, "unsubscribe", {"T1": 49013}, None)
    assert s.get(4, 1030).subscribed is False and s.get(4, 1030).option == 49013
