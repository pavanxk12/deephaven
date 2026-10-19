# ruff: noqa: F811
"""The ON / OFF button: the all-strategies console flips the master switch, a strategy's own window flips only that strategy (algo/ui/controls.py)."""
import types

from algo.automation import algo_start
from algo.ui.controls import is_sending, master_is_off, set_sending
from tests.test_automation import console, on  # noqa: F401  (the fixture)


def test_a_strategy_window_switches_only_its_own_strategy(console):
    app, svc, wire = console
    algo_start(3, "a")
    algo_start(4, "b")
    assert not is_sending(svc, "a") and not is_sending(svc, "b")                        # both start OFF
    assert set_sending(app, svc, "a", True) == ""
    assert is_sending(svc, "a") and not is_sending(svc, "b") and svc.sending            # b is untouched, the master is still on
    assert set_sending(app, svc, "a", False) == "" and not is_sending(svc, "a")
    assert svc.sending                                                                    # switching a strategy off never touches the master


def test_the_all_strategies_window_switches_the_master_for_everyone(console):
    app, svc, wire = console
    algo_start(3, "a")
    on("a")
    assert is_sending(svc, None) and is_sending(svc, "a")
    assert set_sending(app, svc, None, False) == "" and not is_sending(svc, None) and not is_sending(svc, "a")          # nothing goes out for anyone
    assert master_is_off(svc)
    assert set_sending(app, svc, None, True) == "" and is_sending(svc, None) and is_sending(svc, "a")                    # the strategy's own switch was kept


def test_a_strategy_switched_on_while_the_master_is_off_waits_for_the_master(console):
    app, svc, wire = console
    algo_start(3, "a")
    set_sending(app, svc, None, False)
    assert set_sending(app, svc, "a", True) == "" and svc.is_enabled("a")                 # its own switch is on ...
    assert not is_sending(svc, "a") and master_is_off(svc)                                 # ... but the button shows OFF and the window says the master is off
    set_sending(app, svc, None, True)
    assert is_sending(svc, "a")                                                            # it takes effect as soon as the master comes on


def test_no_connection_says_so(console):
    app, svc, wire = console
    algo_start(3, "a")
    svc.transport = None                                                                   # a dry run, or the connection is down
    assert "no NATS connection" in set_sending(app, svc, "a", True)


def test_an_unknown_strategy_or_a_stopped_service_says_so(console):
    app, svc, wire = console
    assert "not running (algo_start)" in set_sending(app, svc, "nobody", True)
    app.plugins["notanalgo"] = types.SimpleNamespace()                                    # a plugin of that name that cannot switch itself
    assert "not running" in set_sending(app, svc, "notanalgo", True)
    svc.on_stop()
    from algo.comms.nats_service import NO_SERVICE
    assert set_sending(app, NO_SERVICE, None, True) == "the NATS service is not running" and not is_sending(NO_SERVICE, None) and not is_sending(NO_SERVICE, "a")


def test_starting_a_strategy_again_for_another_user_says_it_replaced_the_first(console, capsys):
    app, svc, wire = console
    algo_start(3, "a")
    capsys.readouterr()
    algo_start(3, "a")                                                                     # the same user again: a plain restart, nothing to warn about
    assert "replaced" not in capsys.readouterr().out
    algo_start(4, "a")
    out = capsys.readouterr().out
    assert "was running for user 3" in out and "now runs for user 4" in out and "give each its own strategy name" in out
    assert app["a"].user == 4


def test_a_stopped_strategy_reads_off_and_a_restart_begins_off(console):
    from algo.automation import algo_stop
    app, svc, wire = console
    algo_start(3, "a")
    on("a")
    assert is_sending(svc, "a")
    algo_stop("a")
    assert not is_sending(svc, "a") and not svc.is_enabled("a")                         # the button no longer shows ON for a strategy that is not running
    assert "not running (algo_start)" in set_sending(app, svc, "a", True)               # and it cannot be switched on from the window
    algo_start(3, "a")
    assert not is_sending(svc, "a")                                                     # started again: OFF until it says so
