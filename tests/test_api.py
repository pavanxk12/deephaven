"""The session functions (master_build, candles_start, straddle_start, nats_*, pf_*) and the plugin plumbing behind them."""
import pytest

import algo
from algo import api
from algo.core import app as app_module
from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.core.plugin import Plugin


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    app = algo.install(ns, "NSE")
    app.shared["candle_seconds"] = 60
    app.tables["candles"] = "candles-table"
    return ns, app


# ---- install ----------------------------------------------------------------------------------------
def test_install_puts_the_functions_in_the_session_and_no_indicator_makers(session):
    ns, _ = session
    for name in ("master_build", "candles_start", "candles_stop", "straddle_start", "straddle_stop", "algo_status", "algo_help",
                 "contract", "pf_create", "nats_send", "algo_start"):
        assert callable(ns[name])
    for gone in ("indicators", "indicators_remove", "EMA", "RSI", "last_indicators"):
        assert gone not in ns                                                   # indicators are not a service any more


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


def test_switching_exchange_stops_what_was_running_for_the_other_one(session, monkeypatch):
    class Running(Plugin):
        name = "running"

        def start(self):
            pass
    ns, app = session
    app.run(Running())
    assert "running" in app.plugins
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


# ---- instrument scope ---------------------------------------------------------------------------------
def test_candles_start_passes_the_instrument_scope(session, started):
    ns, _ = session
    ns["candles_start"]("5min", symbols=["NIFTY"], kind="FUT")
    assert started[-1].symbols == ["NIFTY"] and started[-1].kind == "FUT" and started[-1].bucket == 300
    ns["candles_start"]()
    assert started[-1].symbols is None and started[-1].kind is None


# ---- NATS: one service, the six per-portfolio commands as plain functions -----------------------------
def _with_nats(app, **cfg):
    from algo.comms.nats_service import NatsConfig, NatsService
    from tests.test_nats_service import Wire
    wire = Wire()
    svc = NatsService(NatsConfig(**cfg), transport_factory=lambda: wire)
    svc.connect()
    app.plugins["nats"] = svc
    return svc, wire


def test_pf_functions_say_the_service_is_down_instead_of_raising(session, capsys):
    ns, _ = session
    ns["pf_subscribe"](1913, 47162)
    ns["pf_pause"](1913)
    ns["pf_unpause"](1913)
    ns["pf_squareup"](1913)
    ns["pf_unsubscribe"](1913, 47162)
    ns["pf_apply"](1913, ival=1)
    ns["nats_send"](True)
    out = capsys.readouterr().out
    assert out.count("NATS service is not running") == 7 and "nats_start(" in out
    assert "nats_start" in ns and "pf_apply" in ns


def test_a_dropped_nats_connection_is_said_in_words(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True)
    wire.is_connected = False
    ns["pf_pause"](1913)
    assert "disconnected" in capsys.readouterr().out
    svc.on_stop()


def test_pf_functions_send_the_six_commands_and_report(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True)
    ns["pf_subscribe"](1913, 47162)
    ns["pf_pause"](1913)
    ns["pf_apply"](1913, ival=1.1, maxqty=3, side="BUY")
    ns["pf_unpause"](1913)
    ns["pf_squareup"](1913)
    ns["pf_unsubscribe"](1913, 47162)
    assert [lab for _, lab, _ in wire.sent] == ["subscribe", "pause_unpause:Pause", "apply", "pause_unpause:UnPause",
                                                 "squareup", "unsubscribe"]
    assert wire.sent[2][2]["maxqty"] == 3 and wire.sent[0][2]["T1"] == 47162
    assert "PF 1913 apply: ok" in capsys.readouterr().out
    svc.on_stop()


def test_pf_functions_explain_when_sending_is_off(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app)
    ns["pf_pause"](5)
    assert wire.sent == [] and "nats_send(True)" in capsys.readouterr().out
    ns["nats_send"](True)
    ns["pf_pause"](5)
    assert [lab for _, lab, _ in wire.sent] == ["pause_unpause:Pause"]
    svc.on_stop()


def test_nats_start_takes_ip_and_port_and_starting_again_does_not_restart_it(session, monkeypatch, capsys):
    ns, app = session
    monkeypatch.setattr(api, "_run", lambda plugin: app.plugins.__setitem__(plugin.name, plugin))
    ns["nats_start"]("10.0.0.5", 4333, users=[3, 4])
    first = app["nats"]
    cfg = first.cfg
    assert (cfg.ip, cfg.port, cfg.users, cfg.url) == ("10.0.0.5", 4333, (3, 4), "nats://10.0.0.5:4333")
    ns["nats_start"]()                                             # every algo script may call it: nothing happens
    ns["nats_start"]("10.0.0.5", 4333, users=[3, 4])               # the same settings: nothing happens
    ns["nats_start"](users=[3])                                    # a user the service already has: nothing happens
    assert app["nats"] is first and "already running" in capsys.readouterr().out
    with pytest.raises(RuntimeError, match="nats_restart"):         # different settings must be asked for on purpose
        ns["nats_start"](port=4223)
    with pytest.raises(RuntimeError, match="nats_add_user"):        # a new user is added without a restart
        ns["nats_start"](users=[3, 4, 9])
    assert app["nats"] is first and first.cfg.port == 4333
    ns["nats_restart"](port=4223, timeout_s=2)                     # the restart: only what is named changes
    assert app["nats"] is not first
    assert (app["nats"].cfg.url, app["nats"].cfg.users, app["nats"].cfg.timeout_s) == ("nats://10.0.0.5:4223", (3, 4), 2)
    ns["nats_stop"]()
    assert app.get("nats") is None
    ns["nats_start"]()                                             # stopped: forgotten, back to the defaults
    assert app["nats"].cfg.url == "nats://192.168.1.130:4222" and app["nats"].cfg.users == (3,)


def test_nats_start_master_switch_is_on_by_default_and_a_bad_ip_or_port_is_rejected(session, monkeypatch):
    ns, app = session
    monkeypatch.setattr(api, "_run", lambda plugin: app.plugins.__setitem__(plugin.name, plugin))
    ns["nats_start"]("10.0.0.5", 4222)
    assert app["nats"].cfg.send_on is True                         # on for everyone; each algo still starts OFF
    ns["nats_restart"](send=False)
    assert app["nats"].cfg.send_on is False
    for bad in (dict(ip="nats://10.0.0.5:4222"), dict(ip="10.0.0.5:4222"), dict(ip=""), dict(port="abc"), dict(port=0),
                dict(port=70000)):
        with pytest.raises(ValueError):
            ns["nats_start"](**bad)


# ---- several users from the console --------------------------------------------------------------------------------
def test_nats_start_takes_one_user_or_several_and_rejects_nonsense(session, monkeypatch):
    ns, app = session
    monkeypatch.setattr(api, "_run", lambda plugin: app.plugins.__setitem__(plugin.name, plugin))
    ns["nats_start"](users=4)
    assert app["nats"].cfg.users == (4,)
    ns["nats_restart"](users=[3, 4, 3])                             # duplicates are dropped, the order is kept
    assert app["nats"].cfg.users == (3, 4)
    assert app["nats"].lift_blocks == (3, 4)                        # naming users lifts their blocks
    ns["nats_restart"]()                                            # remembered
    assert app["nats"].cfg.users == (3, 4) and app["nats"].lift_blocks == ()     # a bare restart lifts nothing
    for bad in (0, -1, [], "abc", [3, "x"], True, [1.5]):
        with pytest.raises(ValueError):
            ns["nats_start"](users=bad)


def test_pf_functions_name_the_user_and_say_when_it_is_missing_or_wrong(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True, users=(3, 4))
    ns["pf_pause"](1913, user=4)
    ns["pf_pause"](1913)
    ns["pf_pause"](1913, user=9)
    out = capsys.readouterr().out
    assert "PF 1913 pause: ok" in out and "several users are allowed" in out and "was not given to nats_start" in out
    assert [p["USERIDENTIFIER"] for _, _, p in wire.sent] == [4]
    svc.on_stop()


def test_nats_block_and_unblock_from_the_console(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True, users=(3, 4))
    ns["nats_block"](user=4)
    ns["pf_pause"](1, user=4)
    ns["pf_pause"](2, user=3)
    out = capsys.readouterr().out
    assert "blocked: {'users': [4], 'portfolios': {}}" in out and "nats_unblock" in out and "PF 2 pause: ok" in out
    assert len(wire.sent) == 1
    ns["nats_block"]()                                              # nothing named: just lists
    assert "'users': [4]" in capsys.readouterr().out
    ns["nats_unblock"](user=4)
    ns["pf_pause"](1, user=4)
    assert "PF 1 pause: ok" in capsys.readouterr().out
    svc.on_stop()


def test_block_commands_without_the_service_say_so_instead_of_raising(session, capsys):
    ns, _ = session
    ns["nats_block"](user=4)
    ns["nats_unblock"](user=4)
    assert capsys.readouterr().out.count("NATS service is not running") == 2


def test_a_portfolio_block_from_the_console_must_name_its_user(session, capsys):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True, users=(3, 4))
    with pytest.raises(ValueError, match="user"):
        ns["nats_block"](pf=1913)
    ns["nats_block"](user=4, pf=1913)
    ns["pf_pause"](1913, user=4)
    ns["pf_pause"](1913, user=3)
    out = capsys.readouterr().out
    assert "'portfolios': {4: [1913]}" in out and "NOT ok - blocked" in out and "PF 1913 pause: ok" in out
    assert [p["USERIDENTIFIER"] for _, _, p in wire.sent] == [3]
    svc.on_stop()


def test_candles_start_passes_expiries_on_and_straddle_warns_when_candles_have_too_few(session, monkeypatch, capsys):
    ns, app = session
    started = []
    monkeypatch.setattr(api, "_run", lambda plugin: (started.append(plugin), app.plugins.__setitem__(plugin.name, plugin)))
    ns["candles_start"]("1min", expiries=1)
    assert started[0].expiries == 1 and "nearest 1 option expiries" in started[0].describe()
    ns["straddle_start"](0)
    assert "warning" not in capsys.readouterr().out                           # one expiry is enough for straddle_start(0)
    ns["straddle_start"](1)
    assert "needs the 2 nearest expiries" in capsys.readouterr().out
    ns["candles_start"]("1min", expiries=2)
    ns["straddle_start"](1)
    assert "warning" not in capsys.readouterr().out


# ---- started once, then connected to --------------------------------------------------------------------------------------
def test_nats_connect_only_checks_and_changes_nothing(session, capsys):
    ns, app = session
    assert ns["nats_connect"]() is False and "Start it once" in capsys.readouterr().out         # the service is not running
    svc, wire = _with_nats(app, send_on=True, users=(3, 4))
    app.plugins["nats"] = svc
    before = (svc.cfg.users, svc.sending, set(svc._enabled))
    assert ns["nats_connect"](user=4, client="bridge_nifty") is True
    out = capsys.readouterr().out
    assert "connected to the NATS service" in out and "bridge_nifty sending OFF" in out
    assert ns["nats_connect"](user=9) is False and "nats_add_user(9)" in capsys.readouterr().out
    assert (svc.cfg.users, svc.sending, set(svc._enabled)) == before and app["nats"] is svc    # nothing was started or changed
    for bad in (0, "x", True):
        with pytest.raises(ValueError):
            ns["nats_connect"](user=bad)
    svc.on_stop()


def test_nats_add_user_adds_without_a_restart(session, capsys):
    ns, app = session
    ns["nats_add_user"](4)
    assert "NOT sent" in capsys.readouterr().out                                                # not running: said, not raised
    svc, wire = _with_nats(app, send_on=True, users=(3,))
    app.plugins["nats"] = svc
    ns["nats_add_user"]([4, 5])
    assert svc.cfg.users == (3, 4, 5) and app["nats"] is svc
    ns["pf_pause"](1, user=5)
    assert wire.sent and wire.sent[0][2]["USERIDENTIFIER"] == 5
    svc.on_stop()


def test_nats_send_has_a_master_switch_and_one_for_each_algo(session, tmp_path):
    ns, app = session
    svc, wire = _with_nats(app, send_on=True)
    app.plugins["nats"] = svc
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    bridge = Bridge(BridgeConfig(), name="bridge_nifty")
    bridge.bind(svc, book)
    bridge._bind(app)
    app.plugins["bridge_nifty"] = bridge
    seen = []
    bridge.on_enable(lambda: seen.append("recheck"))
    ns["nats_send"](True, client="bridge_nifty")                       # goes through the bridge, so the algo re-checks its zones
    assert bridge.sending and seen == ["recheck"]
    ns["nats_send"](True, client="console_panel")                      # a name with no bridge behind it: the service's own list
    assert svc.is_enabled("console_panel")
    ns["nats_send"](False, client="bridge_nifty")
    assert not bridge.sending and svc.is_enabled("console_panel")      # only that one stopped
    ns["nats_send"](False)                                             # the master switch
    assert svc.sending is False and svc.pause(1, client="console_panel").result(5).status == "off"
    ns["nats_send"](True)
    assert svc.sending is True and svc.pause(1, client="console_panel").result(5).ok
    svc.on_stop()


def test_registry_start_twice_is_safe_and_registry_connect_only_checks(session, tmp_path, capsys, monkeypatch):
    ns, app = session
    assert ns["registry_connect"]() is False and "Start it once" in capsys.readouterr().out
    from algo.strategy.registry import Registry
    started = []
    monkeypatch.setattr(api, "_run", lambda plugin: (started.append(plugin), app.plugins.__setitem__(plugin.name, plugin)))
    ns["registry_start"]()
    assert len(started) == 1
    plugin = app["registry"]
    plugin.state = Registry(str(tmp_path / "reg.json"))               # as start() would have made it
    ns["registry_start"]()
    ns["registry_start"]()
    assert len(started) == 1 and app["registry"] is plugin             # every algo script may call it: nothing is restarted
    assert "already running" in capsys.readouterr().out
    with pytest.raises(RuntimeError, match="another file"):
        ns["registry_start"](file=str(tmp_path / "other.json"))
    assert ns["registry_connect"]() is True and "connected to the portfolio registry" in capsys.readouterr().out
