# ruff: noqa: F811
"""Turnover and M2M limits per strategy: the numbers and latches (core/limits.py) and what a breach does (strategy/guard.py). Real registry, book, bridge and
NATS service; fake wire."""
import json

import pytest

from algo.automation import algo_start, pf_activate, pf_assign, pf_track
from algo.core.limits import Limits
from algo.strategy.guard import Guard
from algo.ui.controls import set_sending
from tests.test_automation import console, labels, on, wait_for  # noqa: F401  (the fixture)
from tests.test_pf_state import PARAMS, started  # noqa: F401


# ---- the numbers ----------------------------------------------------------------------------------------------------------------
def test_no_limit_means_nothing_is_ever_breached():
    lim = Limits()
    assert lim.update("a", 50.0, -1e9) == [] and lim.blocked("a") == ""


def test_a_turnover_limit_is_breached_once_and_stays_breached_until_it_is_raised():
    lim = Limits()
    lim.set_limit("a", "turnover", 5)
    assert lim.update("a", 4.9, 0) == [] and lim.blocked("a") == ""
    assert lim.update("a", 5.0, 0) == ["turnover"]                       # reaching it counts
    assert lim.update("a", 5.5, 0) == []                                  # reported once, not on every tick
    assert "turnover limit 5 reached" in lim.blocked("a")
    lim.set_limit("a", "turnover", 5.2)                                   # still past it: stays blocked
    assert lim.blocked("a")
    assert lim.set_limit("a", "turnover", 6) == [] and lim.blocked("a") == ""      # raised past the value: free again
    assert lim.update("a", 6.0, 0) == ["turnover"]                        # and it can be breached again


def test_an_m2m_limit_is_the_number_as_typed_and_breaches_when_live_m2m_is_at_or_below_it():
    lim = Limits()
    lim.set_limit("a", "m2m", -5000)
    assert lim.update("a", 1, 80000) == [] and lim.update("a", 1, -4999) == []        # a profit, or a small loss: fine
    assert lim.update("a", 1, -5000) == ["m2m"] and "M2M limit -5,000 reached" in lim.blocked("a")
    assert lim.update("a", 1, 10000) == [] and lim.blocked("a")           # M2M recovered: still blocked, only a changed limit lifts it
    assert lim.set_limit("a", "m2m", -20000) == [] and lim.blocked("a") == ""        # a looser limit than the current M2M (10000): free
    assert lim.set_limit("a", "m2m", 0) == [] and lim.blocked("a") == ""  # 0 = no limit


def test_a_positive_m2m_limit_is_not_turned_into_a_loss_limit():
    lim = Limits()
    lim.update("a", 1, 2000)
    assert lim.set_limit("a", "m2m", 50000) == ["m2m"] and lim.get("a")["m2m"] == 50000       # 50000 is 50000: live M2M 2000 is at or below it
    assert "M2M limit +50,000 reached" in lim.blocked("a")
    lim.update("b", 1, 60000)
    assert lim.set_limit("b", "m2m", 50000) == [] and lim.update("b", 1, 50000) == ["m2m"]    # above it: fine; at it: breached


def test_lowering_a_limit_below_the_current_value_breaches_it_at_once():
    lim = Limits()
    lim.update("a", 3.0, -20000)
    assert lim.set_limit("a", "turnover", 2) == ["turnover"] and lim.set_limit("a", "m2m", -10000) == ["m2m"]


def test_a_missing_value_never_breaches_and_the_names_are_listed():
    lim = Limits()
    lim.set_limit("a", "m2m", -5000)
    lim.set_limit("b", "turnover", 10)
    assert lim.update("a", float("nan"), -1.7976931348623157e308) == [] and lim.blocked("a") == ""      # NaN and Deephaven's double null
    assert lim.seen("a") == (None, None) and lim.names() == ["a", "b"]


def test_the_limits_of_two_strategies_are_separate_and_bad_values_are_refused():
    lim = Limits()
    lim.set_limit("a", "turnover", 1)
    lim.update("a", 2, 0)
    lim.update("b", 2, 0)
    assert lim.blocked("a") and lim.blocked("b") == ""
    for bad in (-1, "x"):
        with pytest.raises(ValueError):
            lim.set_limit("a", "turnover", bad)
    with pytest.raises(ValueError):
        lim.set_limit("a", "m2m", "x")
    lim.set_limit("a", "m2m", -50000)                                     # kept as typed
    assert lim.get("a")["m2m"] == -50000 and lim.update("a", 1, -50000) == ["m2m"]
    with pytest.raises(ValueError):
        lim.set_limit("a", "pnl", 1)


def test_limits_and_breaches_survive_a_restart(tmp_path):
    path = str(tmp_path / "state" / "limits.json")
    lim = Limits(path)
    lim.set_limit("a", "m2m", -1000)
    lim.update("a", 0, -2000)
    again = Limits(path)
    assert again.get("a")["m2m"] == -1000 and again.get("a")["m2m_hit"] is True
    (tmp_path / "state" / "limits.json").write_text("{not json")
    assert Limits(path).get("a")["m2m"] is None                          # unreadable file: no limits, no crash


def test_listeners_hear_a_changed_limit_and_a_breach_but_not_every_value():
    lim, heard = Limits(), []
    lim.on_change(heard.append)
    lim.update("a", 1, 0)
    assert heard == []
    lim.set_limit("a", "turnover", 2)
    lim.update("a", 3, 0)
    assert heard == ["a", "a"]


# ---- what a breach does -----------------------------------------------------------------------------------------------------------
def test_a_turnover_breach_switches_only_that_strategy_off_and_it_cannot_be_switched_on(console):
    app, svc, wire = console
    algo_start(3, "a")
    algo_start(4, "b")
    on("a")
    on("b")
    g = Guard(app, app.shared.setdefault("limits", Limits()))
    assert g.set_limit("a", "turnover", 2) == ""
    g.on_values("a", 2.5, 0)
    assert not svc.allowed("a") and svc.allowed("b") and svc.sending      # a is off; b and the master are not touched
    assert wire.sent == []                                                # turnover only stops new orders: nothing is squared up
    assert "cannot switch ON: turnover limit" in set_sending(app, svc, "a", True)
    app["a"].send_on()
    assert not svc.allowed("a")                                           # a script asking for it is refused as well
    assert g.set_limit("a", "turnover", 3) == "" and set_sending(app, svc, "a", True) == "" and svc.allowed("a")


def test_an_m2m_breach_squares_up_every_portfolio_of_the_strategy_and_switches_it_off(console):
    app, svc, wire = console
    pf = started(app, svc, wire, user=3, name="a", instrument=1, option=55)
    started(app, svc, wire, user=4, name="b", instrument=2, option=66)
    assert wait_for(lambda: len(wire.sent) == 8)                         # both strategies are running; they use the same portfolio number under different users
    wire.sent.clear()
    g = Guard(app, app.shared.setdefault("limits", Limits()))
    g.set_limit("a", "m2m", -10000)
    g.on_values("a", 1.0, -10000)
    assert wait_for(lambda: len(wire.sent) == 2)
    assert [(lab, p["user_id"]) for _, lab, p in wire.sent] == [("pause_unpause:Pause", 3), ("squareup", 3)]        # only a's portfolio (user 3), paused first
    assert not svc.allowed("a") and svc.allowed("b")
    g.on_values("a", 1.0, -12000)
    assert len(wire.sent) == 2                                            # one square up per breach, not one per tick
    assert wait_for(lambda: svc.pf_state(pf, 3).squared and svc.pf_state(pf, 3).paused)


def test_the_message_is_in_the_nats_events_table_and_a_stopped_service_does_not_crash(console):
    app, svc, wire = console
    rows = []
    svc.events = type("T", (), {"write_row": lambda self, *r: rows.append(r)})()
    algo_start(3, "a")
    on("a")
    g = Guard(app, app.shared.setdefault("limits", Limits()))
    g.set_limit("a", "turnover", 1)
    g.on_values("a", 1.5, 0)
    assert any(r[3] == "limit" and r[4] == "turnover_breached" and "TURNOVER limit 1 reached" in r[5] for r in rows)
    algo_start(3, "c")
    on("c")
    svc.on_stop()
    g.set_limit("c", "m2m", -5)
    g.on_values("c", 0, -9)                                               # the service is gone: logged, nothing raised


def test_listeners_are_told_after_the_strategy_was_switched_off(console):
    app, svc, wire = console
    algo_start(3, "a")
    on("a")
    lim = app.shared.setdefault("limits", Limits())
    seen = []
    lim.on_change(lambda s: seen.append(svc.allowed("a")))                 # what a redrawing button would read
    g = Guard(app, lim)
    g.set_limit("a", "turnover", 1)
    seen.clear()
    g.on_values("a", 2, 0)
    assert seen and seen[-1] is False                                      # the last call comes after send_off
