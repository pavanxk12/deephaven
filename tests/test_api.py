"""The session functions (master_build, candles_start, indicators, straddle_start) and the plugin plumbing behind them."""
import pytest

import algo
from algo import api
from algo.core import app as app_module
from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.core.plugin import Plugin
from algo.data.indicators import EMA, RSI, Indicators, group_by_bar, resolve_timeframe


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    app = algo.install(ns, "NSE")
    app.shared["candle_seconds"] = 60
    app.tables["candles"] = "candles-table"
    return ns, app


@pytest.fixture
def no_build(monkeypatch):
    """Indicators without a Deephaven query: record builds instead."""
    builds = []
    monkeypatch.setattr(Indicators, "_build", lambda self: builds.append(self.columns()))
    return builds


# ---- install ----------------------------------------------------------------------------------------
def test_install_puts_the_functions_and_indicator_makers_in_the_session(session):
    ns, _ = session
    for name in ("master_build", "candles_start", "candles_stop", "indicators", "indicators_remove",
                 "straddle_start", "straddle_stop", "algo_status", "EMA", "RSI"):
        assert callable(ns[name])
    assert ns["EMA"](9).column == "EMA_9" and ns["RSI"](5).column == "RSI_5"


# ---- indicators() adds, never replaces --------------------------------------------------------------
def test_indicators_called_again_adds_to_the_running_set(session, no_build):
    ns, app = session
    ns["indicators"](ns["EMA"](9), ns["RSI"](5))
    ns["indicators"](ns["EMA"](6))
    assert app["indicators"].columns() == ["EMA_9", "RSI_5", "EMA_6"]
    assert no_build == [["EMA_9", "RSI_5"], ["EMA_9", "RSI_5", "EMA_6"]]


def test_a_repeated_indicator_is_not_computed_twice_or_rebuilt(session, no_build):
    ns, app = session
    ns["indicators"](EMA(9))
    ns["indicators"](EMA(9))
    assert app["indicators"].columns() == ["EMA_9"] and len(no_build) == 1


def test_indicators_without_arguments_only_lists(session, no_build, capsys):
    ns, _ = session
    ns["indicators"](EMA(9))
    ns["indicators"]()
    assert "EMA_9" in capsys.readouterr().out and len(no_build) == 1


def test_remove_drops_one_column_and_keeps_the_rest(session, no_build):
    ns, app = session
    ns["indicators"](EMA(9), RSI(5), EMA(6))
    ns["indicators_remove"]("RSI_5", "NOPE")
    assert app["indicators"].columns() == ["EMA_9", "EMA_6"]


def test_a_bare_number_is_rejected_and_nothing_changes(session, no_build):
    ns, app = session
    ns["indicators"](EMA(9))
    with pytest.raises(TypeError, match="not an indicator"):
        ns["indicators"](9)
    assert app["indicators"].columns() == ["EMA_9"]


def test_a_failed_rebuild_rolls_the_set_back(session, no_build, monkeypatch):
    ns, app = session
    ns["indicators"](EMA(9))
    monkeypatch.setattr(Indicators, "_build", lambda self: (_ for _ in ()).throw(RuntimeError("query failed")))
    with pytest.raises(RuntimeError):
        ns["indicators"](RSI(5))
    assert app["indicators"].columns() == ["EMA_9"]


def test_indicators_need_candles_and_say_which_function_to_call(monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    algo.install(ns, "NSE")
    with pytest.raises(RuntimeError, match=r"candles_start\(\)"):
        ns["indicators"](EMA(9))


def test_indicator_bar_size_must_be_a_multiple_of_the_candle_size():
    assert resolve_timeframe(None, 30, 30) == 30
    assert resolve_timeframe("5min", 30, 30) == 300 and resolve_timeframe("1min", 60, 60) == 60
    assert resolve_timeframe(None, 300, 60) == 300                      # falls back to the set default
    with pytest.raises(ValueError, match="whole multiple"):
        resolve_timeframe("30s", 60, 60)
    with pytest.raises(ValueError):
        resolve_timeframe("90s", 30, 30)
    with pytest.raises(ValueError, match="whole multiple"):
        resolve_timeframe("2min", 300, 300)


def test_each_indicator_can_have_its_own_bar_size(session, no_build):
    ns, app = session
    ns["indicators"](ns["EMA"](9), ns["EMA"](9, "5min"), ns["RSI"](5, "15 MIN"))
    assert app["indicators"].columns() == ["EMA_9", "EMA_9_5min", "RSI_5_15min"]
    with pytest.raises(ValueError):
        ns["EMA"](9, "7x")
    with pytest.raises(ValueError):
        ns["EMA"](9, 5)


def test_grouping_by_bar_size_puts_the_finest_first():
    e1, e5, r5, r15 = EMA(9), EMA(9, "5min"), RSI(5, "5min"), RSI(5, "15min")
    g = group_by_bar([r15, e5, e1, r5], default_seconds=60, candle_seconds=60)
    assert list(g) == [60, 300, 900] and g[300] == [e5, r5] and g[60] == [e1]
    assert group_by_bar([e1], 300, 60) == {300: [e1]}                   # the set default applies to EMA(9)
    assert group_by_bar([], 60, 60) == {60: []}                         # Pct only
    with pytest.raises(ValueError):
        group_by_bar([EMA(9, "30s")], 60, 60)                           # finer than the candles


def test_helper_columns_do_not_clash_between_bar_sizes():
    a, b = EMA(9), EMA(9, "5min")
    assert a.final().split("=")[0].strip() == "EMA_9" and b.final().split("=")[0].strip() == "EMA_9_5min"
    assert "EMA_9_5min_raw" in b.final() and "EMA_9_5min_raw" not in a.final()
    r, q = RSI(5), RSI(5, "5min")
    assert "AvgG_RSI_5_5min" in q.final() and "AvgG_RSI_5_5min" not in r.final()
    assert "NULL_LONG" in r.final()


# ---- the other starters -----------------------------------------------------------------------------
@pytest.fixture
def started(monkeypatch):
    plugins = []
    monkeypatch.setattr(api, "_run", lambda plugin: plugins.append(plugin))
    return plugins


def test_candles_start_takes_the_bar_size(session, started):
    ns, _ = session
    for text, seconds in (("30s", 30), ("1min", 60), ("5min", 300), ("15min", 900)):
        ns["candles_start"](text)
        assert started[-1].bucket == seconds
    ns["candles_start"]()
    assert started[-1].bucket == 60                                     # default
    with pytest.raises(ValueError):
        ns["candles_start"]("7min2")
    with pytest.raises(ValueError):
        ns["candles_start"](5)


def test_straddle_start_n_is_extra_expiries_beyond_the_nearest(session, started):
    ns, _ = session
    for n, expiries in ((0, 1), (1, 2), (2, 3), (5, 6)):
        ns["straddle_start"](n)
        assert started[-1].cfg.expiries == expiries
    ns["straddle_start"](1, symbols=["NIFTY", "BANKNIFTY"], window=3)
    cfg = started[-1].cfg
    assert cfg.symbols == ("NIFTY", "BANKNIFTY") and cfg.window == 3
    for bad in (-1, 1.5, "1", True):
        with pytest.raises(ValueError):
            ns["straddle_start"](bad)


def test_master_build_passes_day_and_force(session, started):
    ns, _ = session
    ns["master_build"](day="20261015", force=True)
    assert started[-1].day == "20261015" and started[-1].force is True


def test_master_build_takes_the_exchange(session, started):
    ns, app = session
    ns["master_build"]()                                   # no argument: keeps the current exchange
    assert app_module.current().market.code == "NSE"
    ns["master_build"]("nse")
    assert app_module.current() is app                      # same exchange: nothing is stopped


def test_switching_exchange_stops_what_was_running_for_the_other_one(session, no_build, monkeypatch):
    ns, app = session
    ns["indicators"](EMA(9))
    assert "indicators" in app.plugins
    monkeypatch.setattr(api, "_run", lambda plugin: None)         # do not build a real master
    ns["master_build"]("US")
    now = app_module.current()
    assert now.market.code == "US" and now is not app and now.plugins == {}
    assert app.plugins == {}                                # the NSE plugins were stopped
    assert now.ns is ns and now.paths == app.paths


def test_unknown_exchange_is_refused_and_changes_nothing(session, started):
    ns, app = session
    with pytest.raises(ValueError, match="unknown market"):
        ns["master_build"]("LSE")
    assert app_module.current() is app and started == []


# ---- republish plumbing -----------------------------------------------------------------------------
def test_republish_notifies_followers_but_the_first_publish_does_not():
    a = App({}, NSE, Paths())
    seen = []
    a.on_publish("t", seen.append)
    a.publish("t", "first")
    a.publish("t", "second", ns_name="var")
    assert seen == ["second"] and a.ns == {"t": "first", "var": "second"} and a.table("t") == "second"


def test_a_plugin_stops_following_when_it_stops():
    a = App({}, NSE, Paths())
    seen = []

    class P(Plugin):
        name = "p"

        def start(self):
            self.follow("t", seen.append)

    a.run(P())
    a.publish("t", 1)
    a.publish("t", 2)
    a.stop("p")
    a.publish("t", 3)
    assert seen == [2]


def test_indicators_follow_a_restarted_candles_table(monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    a = App({}, NSE, Paths())
    a.shared["candle_seconds"] = 60
    a.publish("candles", "old")
    builds = []
    monkeypatch.setattr(Indicators, "_build", lambda self: builds.append(self.app.table("candles")))
    a.run(Indicators([EMA(9)]))
    a.publish("candles", "new")
    assert builds == ["old", "new"]
