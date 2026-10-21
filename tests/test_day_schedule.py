"""The day's start-up on a timer: master, candles and the other services start by themselves at set times of the exchange's clock."""
import contextlib
import time
import types
from datetime import datetime, time as dtime

import pytest

import algo
from algo import api
from algo.core import app as app_module
from algo.core.config import NSE, US, Paths
from algo.schedule import DaySchedule, Step


class Clock:
    """A clock the test moves by hand (naive datetimes are enough: the plugin only reads the date, the weekday and the time)."""

    def __init__(self, text):
        self.now = datetime.fromisoformat(text)

    def __call__(self):
        return self.now


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return False


def fake_app(market=NSE):
    return types.SimpleNamespace(market=market, in_ctx=lambda: contextlib.nullcontext())


def plugin(steps, clock, **kw):
    p = DaySchedule(steps, retry_s=0.01, poll_s=0.02, clock=clock, **kw)
    p._bind(fake_app())
    return p


def steps_logging(order, fail=None):
    def step(label, at):
        def run():
            order.append(label)
            if fail and fail.get(label, 0) > 0:
                fail[label] -= 1
                raise RuntimeError(f"{label} not ready")
        return Step(label, at, run)
    return step


def test_nse_defaults_are_master_0900_and_candles_0914_30():
    assert NSE.start_master == dtime(9, 0) and NSE.start_candles == dtime(9, 14, 30)


def test_nothing_runs_before_its_time_and_each_step_runs_in_order_when_it_comes():
    order, clock = [], Clock("2026-10-20 08:59:00")                 # a Tuesday
    step = steps_logging(order)
    p = plugin([step("master", dtime(9, 0)), step("candles", dtime(9, 14, 30)), step("registry", dtime(9, 14, 30)), step("nats", dtime(9, 14, 30))], clock)
    p.start = lambda: p.spawn(p._loop)
    p.start()
    time.sleep(0.3)
    assert order == []                                               # 08:59: too early
    clock.now = datetime.fromisoformat("2026-10-20 09:00:05")
    assert wait_for(lambda: order == ["master"])
    time.sleep(0.3)
    assert order == ["master"]                                       # 09:00:05: the candles are not due until 09:14:30
    clock.now = datetime.fromisoformat("2026-10-20 09:14:29")
    time.sleep(0.3)
    assert order == ["master"]
    clock.now = datetime.fromisoformat("2026-10-20 09:14:31")
    assert wait_for(lambda: order == ["master", "candles", "registry", "nats"])    # the services follow the candles, in the order given
    p._teardown()


def test_a_start_after_the_times_runs_everything_at_once_in_order():
    order, clock = [], Clock("2026-10-20 10:30:00")
    step = steps_logging(order)
    p = plugin([step("master", dtime(9, 0)), step("candles", dtime(9, 14, 30)), step("nats", dtime(9, 14, 30))], clock)
    p.spawn(p._loop)
    assert wait_for(lambda: order == ["master", "candles", "nats"])
    time.sleep(0.2)
    assert order == ["master", "candles", "nats"]                    # once only
    p._teardown()


def test_a_failing_step_is_tried_again_and_the_others_wait_their_turn_not_for_it():
    order, clock = [], Clock("2026-10-20 10:30:00")
    step = steps_logging(order, fail={"master": 2})
    p = plugin([step("master", dtime(9, 0)), step("candles", dtime(9, 14, 30))], clock)
    p.spawn(p._loop)
    assert wait_for(lambda: order == ["master", "candles", "master", "master"])       # the candles do not wait for the master's retries: two failures, then success
    assert p.failed == {}
    p._teardown()


def test_a_step_that_never_works_is_given_up_for_the_day_and_the_rest_go_on():
    order, clock = [], Clock("2026-10-20 10:30:00")
    step = steps_logging(order, fail={"master": 99})
    p = plugin([step("master", dtime(9, 0)), step("nats", dtime(9, 14, 30))], clock, retries=3)
    p.spawn(p._loop)
    assert wait_for(lambda: order == ["master", "nats", "master", "master"] and "master" in p.failed)        # the nats step goes ahead at once
    assert "master" in p.failed and "master" in p.describe() and "failed" in p.describe()
    p._teardown()


def test_the_weekend_is_skipped_unless_asked_for_and_the_next_day_runs_again():
    order, clock = [], Clock("2026-10-17 10:30:00")                  # a Saturday
    step = steps_logging(order)
    p = plugin([step("master", dtime(9, 0))], clock)
    p.spawn(p._loop)
    time.sleep(0.3)
    assert order == []
    p._teardown()
    clock2 = Clock("2026-10-17 10:30:00")
    p2 = plugin([step("master", dtime(9, 0))], clock2, weekdays_only=False)
    p2.spawn(p2._loop)
    assert wait_for(lambda: order == ["master"])
    clock2.now = datetime.fromisoformat("2026-10-18 09:30:00")       # the next day: it runs again
    assert wait_for(lambda: order == ["master", "master"])
    p2._teardown()


@pytest.fixture
def session(monkeypatch, tmp_path):
    monkeypatch.setattr(app_module, "_APP", None)
    monkeypatch.delenv("ALGO_MARKET", raising=False)
    ns = {}
    app = algo.install(ns, "NSE", Paths(market_data=str(tmp_path), work=str(tmp_path)))
    calls = []
    monkeypatch.setattr(api, "master_build", lambda *a, **k: calls.append(("master", a, k)))
    monkeypatch.setattr(api, "candles_start", lambda *a, **k: calls.append(("candles", a, k)))
    monkeypatch.setattr(api, "registry_start", lambda *a, **k: calls.append(("registry", a, k)))
    monkeypatch.setattr(api, "nats_start", lambda *a, **k: calls.append(("nats", a, k)))
    seen = []
    monkeypatch.setattr(app, "run", lambda plug: seen.append(plug) or plug)
    return app, calls, seen


def test_day_auto_builds_the_nse_steps_from_the_market_defaults(session):
    app, calls, seen = session
    api.day_auto()
    plug = seen[0]
    assert isinstance(plug, DaySchedule)
    assert [(s.label, s.at) for s in plug.steps] == [("master", dtime(9, 0)), ("candles", dtime(9, 14, 30)), ("registry", dtime(9, 14, 30)),
                                                     ("nats", dtime(9, 14, 30)), ("console", dtime(9, 14, 30)), ("close", dtime(15, 30))]
    for s in plug.steps[:4]:
        s.run()
    assert [c[0] for c in calls] == ["master", "candles", "registry", "nats"]
    assert calls[1][1] == ("1min",) and calls[3][2] == {"users": (3, 4)}


def test_day_auto_takes_your_times_with_or_without_seconds_and_refuses_a_wrong_order(session):
    app, calls, seen = session
    api.day_auto("08:45", "09:10:15", "09:10:45", console=False, users=[3], nats_ip="10.0.0.5")
    plug = seen[0]
    assert [(s.label, s.at) for s in plug.steps] == [("master", dtime(8, 45)), ("candles", dtime(9, 10, 15)), ("registry", dtime(9, 10, 45)), ("nats", dtime(9, 10, 45)), ("close", dtime(15, 30))]
    plug.steps[3].run()
    assert calls[-1][1] == ("10.0.0.5", None) and calls[-1][2] == {"users": [3]}
    with pytest.raises(ValueError, match="cannot start before the master"):
        api.day_auto("09:30", "09:00")
    with pytest.raises(ValueError, match="09:14:30"):
        api.day_auto("9am")


def test_day_auto_stop_switches_it_off(session):
    app, calls, seen = session
    stopped = []
    monkey = app.stop
    app.stop = lambda name: stopped.append(name)
    api.day_auto(stop=True)
    assert stopped == ["schedule"] and not seen
    app.stop = monkey


def test_nse_closes_the_day_at_1530():
    assert NSE.close_at == dtime(15, 30)


def test_the_close_saves_both_tables_first_then_stops_the_straddle_and_the_candles(session, monkeypatch):
    app, calls, seen = session
    events = []
    monkeypatch.setattr(api, "save_tables", lambda a, tables: events.append(("save", tuple(tables))) or [
        {"table": "candles", "status": "saved", "rows": 10, "path": "c.parquet"}, {"table": "straddle_series", "status": "saved", "rows": 5, "path": "s.parquet"}])
    monkeypatch.setattr(app, "stop", lambda name: events.append(("stop", name)))
    api.day_auto()
    close = seen[0].steps[-1]
    assert close.label == "close"
    close.run()
    assert events == [("save", ("candles", "straddle_series")), ("stop", "straddle"), ("stop", "candles")]       # the whole day is on disk before anything stops


def test_a_straddle_that_is_not_running_is_just_skipped_by_the_save(session, monkeypatch):
    app, calls, seen = session
    events = []
    monkeypatch.setattr(api, "save_tables", lambda a, tables: [{"table": "candles", "status": "saved", "rows": 10, "path": "c"},
                                                                {"table": "straddle_series", "status": "missing", "rows": 0, "path": None}])
    monkeypatch.setattr(app, "stop", lambda name: events.append(name))
    api.day_auto()
    seen[0].steps[-1].run()
    assert events == ["straddle", "candles"]                                                                      # stopping what is not running is harmless


def test_a_failed_save_raises_so_it_is_tried_again_and_nothing_is_stopped(session, monkeypatch):
    app, calls, seen = session
    events = []
    monkeypatch.setattr(api, "save_tables", lambda a, tables: [{"table": "candles", "status": "failed", "rows": 0, "path": None}])
    monkeypatch.setattr(app, "stop", lambda name: events.append(name))
    api.day_auto()
    with pytest.raises(RuntimeError, match="saving candles failed"):
        seen[0].steps[-1].run()
    assert events == []


def test_close_can_be_left_out_or_moved_and_must_come_after_the_candles(session):
    app, calls, seen = session
    api.day_auto(close=False)
    assert [s.label for s in seen[-1].steps][-1] == "console"
    api.day_auto(close="15:35")
    assert seen[-1].steps[-1].at == dtime(15, 35) and seen[-1].steps[1].until == dtime(15, 35)
    with pytest.raises(ValueError, match="must come after the candles"):
        api.day_auto(close="09:00")


def test_a_container_started_after_the_close_does_not_start_the_candles(tmp_path):
    order, clock = [], Clock("2026-10-20 16:00:00")
    step = steps_logging(order)
    p = plugin([step("master", dtime(9, 0)), Step("candles", dtime(9, 14, 30), lambda: order.append("candles"), until=dtime(15, 30)),
                step("nats", dtime(9, 14, 30)), step("close", dtime(15, 30))], clock)
    p.spawn(p._loop)
    assert wait_for(lambda: order == ["master", "nats", "close"])                                                # no candles after 15:30
    p._teardown()


def test_the_us_day_has_the_nse_shape_in_new_york_time():
    assert (US.start_master, US.start_candles, US.close_at) == (dtime(9, 15), dtime(9, 29, 30), dtime(16, 0))
    assert US.start_master < US.start_candles < US.session[0] and US.close_at == US.session[1]
