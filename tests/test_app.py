import pytest

from algo.core.app import App, MissingDependency
from algo.core.config import NSE, Paths, market_for
from algo.core.plugin import Plugin


class Probe(Plugin):
    name = "probe"

    def __init__(self, requires=(), fail=False):
        super().__init__()
        self.requires, self.fail = requires, fail
        self.events = []

    def start(self):
        if self.fail:
            raise RuntimeError("boom")
        self.events.append("start")
        self.app.publish("probe_table", object())
        self.spawn(self._wait)

    def _wait(self):
        self.halted.wait(5)
        self.events.append("thread-exit")

    def on_stop(self):
        self.events.append("stop")


def app():
    return App({}, NSE, Paths())


def test_run_publishes_and_registers():
    a = app()
    p = a.run(Probe())
    assert a["probe"] is p and "probe_table" in a.ns and a.has("probe_table")
    a.stop_all()


def test_rerun_replaces_and_tears_down_the_old_instance():
    a = app()
    old = a.run(Probe())
    new = a.run(Probe())
    assert "stop" in old.events and a["probe"] is new and old.halted.is_set()
    a.stop_all()


def test_missing_dependency_is_reported_before_start():
    a = app()
    with pytest.raises(MissingDependency, match="candles"):
        a.run(Probe(requires=("candles",)))
    assert "probe" not in a.plugins


def test_failed_start_is_torn_down_and_not_registered():
    a = app()
    p = Probe(fail=True)
    with pytest.raises(RuntimeError):
        a.run(p)
    assert "probe" not in a.plugins and "stop" in p.events


def test_stop_all_halts_the_plugin_threads():
    a = app()
    p = a.run(Probe())
    a.stop_all()
    assert p.halted.is_set() and a.plugins == {}


def test_table_lookup_falls_back_to_session_namespace():
    a = app()
    a.ns["fo_market_data"] = "feed"
    assert a.table("fo_market_data") == "feed"
    with pytest.raises(MissingDependency):
        a.table("nope")


def test_market_lookup():
    assert market_for("nse") is NSE and market_for("US").code == "US"
    with pytest.raises(ValueError):
        market_for("lse")
    assert NSE.zone.key == "Asia/Kolkata"
