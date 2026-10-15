"""Plugin start() wiring against a stub `deephaven` package: checks the glue (table publish, listener, row order)
without a server. The real engine is only exercised in a Deephaven session."""
import sys
import types

import pytest

from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.data.candles import Candles


class FakeTable:
    def __init__(self):
        self.views = []

    def view(self, cols):
        self.views.append(cols)
        return self

    def natural_join(self, other, on=None, joins=None):
        self.joins = getattr(self, "joins", []) + [(on, joins)]
        return self


class FakeWriter:
    def __init__(self, schema):
        self.schema, self.table, self.rows = schema, FakeTable(), []

    def write_row(self, *row):
        self.rows.append(row)


@pytest.fixture
def fake_dh(monkeypatch):
    listeners = []

    def listen(table, fn, do_replay=False):
        h = types.SimpleNamespace(fn=fn, replay=do_replay, stopped=False)
        h.stop = lambda: setattr(h, "stopped", True)
        listeners.append(h)
        return h

    dh = types.ModuleType("deephaven")
    dh.DynamicTableWriter = FakeWriter
    dh.dtypes = types.SimpleNamespace(int64="int64", double="double", string="string")
    tl = types.ModuleType("deephaven.table_listener")
    tl.listen = listen
    dhp = types.ModuleType("deephaven.pandas")
    dhp.to_table = lambda df: df                         # the lookup table is only handed to natural_join
    monkeypatch.setitem(sys.modules, "deephaven", dh)
    monkeypatch.setitem(sys.modules, "deephaven.table_listener", tl)
    monkeypatch.setitem(sys.modules, "deephaven.pandas", dhp)
    return listeners


class Update:
    def __init__(self, ticks):
        self.ticks = ticks

    def added(self, cols):
        t = self.ticks
        return {"token": [x[0] for x in t], "ltp": [x[1] for x in t], "vol": [x[2] for x in t], "ltt": [x[3] for x in t]}

    def modified(self, cols):
        return None


def test_candles_publishes_a_table_and_listens_with_replay(fake_dh, master):
    app = App({"fo_market_data": FakeTable()}, NSE, Paths())
    app.plugins["master"] = type("M", (), {"name": "master", "data": master, "_teardown": lambda self: None})()
    app.run(Candles(build_from=0))
    assert "candles" in app.ns and len(fake_dh) == 1 and fake_dh[0].replay is True


def test_candles_rows_reach_the_writer(fake_dh, master, monkeypatch):
    writers = []
    orig = FakeWriter.__init__

    def tracking(self, schema):
        orig(self, schema)
        writers.append(self)

    monkeypatch.setattr(FakeWriter, "__init__", tracking)
    app = App({"fo_market_data": FakeTable()}, NSE, Paths())
    app.plugins["master"] = type("M", (), {"name": "master", "data": master, "_teardown": lambda self: None})()
    app.run(Candles(build_from=0))
    fake_dh[0].fn(Update([(100, 1040, 5, "14-10-2030 10:15:05"), (100, 1120, 9, "14-10-2030 10:15:40"),
                          (100, 960, 12, "14-10-2030 10:16:30")]), False)
    (w,) = writers
    (minute, token, o, h, l, close, vol) = w.rows[0]
    # the NSE feed is in paise: the default multiplier 0.01 gives rupees, and float noise is rounded away
    assert (token, o, h, l, close, vol) == (100, 10.4, 11.2, 10.4, 11.2, 4)
    assert minute % 60 == 0
    assert w.schema["Close"] == "double" and w.schema["Volume"] == "int64"
    assert "Symbol" not in w.schema                       # the text is joined from a static table, not stored per row
    published = app.tables["candles"]
    assert published.joins and published.joins[0] == ("token", "Symbol")


def test_a_bad_tick_does_not_stop_the_listener(fake_dh, master, monkeypatch):
    app = App({"fo_market_data": FakeTable()}, NSE, Paths())
    app.plugins["master"] = type("M", (), {"name": "master", "data": master, "_teardown": lambda self: None})()
    c = app.run(Candles(build_from=0))
    fake_dh[0].fn(Update([(100, 10.0, 1, "garbage"), (101, float("nan"), 1, "14-10-2030 10:15:05"),
                          (102, 10.0, 1, None)]), False)
    assert c.rows == 0                                  # nothing built, nothing raised


def test_stopping_the_plugin_stops_its_listener(fake_dh, master):
    app = App({"fo_market_data": FakeTable()}, NSE, Paths())
    app.plugins["master"] = type("M", (), {"name": "master", "data": master, "_teardown": lambda self: None})()
    app.run(Candles())
    app.stop("candles")
    assert fake_dh[0].stopped
