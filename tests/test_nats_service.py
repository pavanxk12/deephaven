"""The shared NATS service against a fake transport: ordering per portfolio, overlap across portfolios, the in-flight cap,
fairness, coalescing, bounded lanes, timeouts, disconnects and shutdown. No NATS, no Deephaven."""
import asyncio
import json
import threading
import time
from concurrent.futures import wait

from algo.comms.nats_service import NatsConfig, NatsService


class Wire:
    """Fake transport: records every request, can be slow, can answer badly, can time out."""

    def __init__(self, delay=0.0, status="ok", raw=None, timeout_on=None):
        self.sent, self.delay, self.status, self.raw, self.timeout_on = [], delay, status, raw, timeout_on
        self.now = self.peak = 0
        self.is_connected = True

    async def request(self, subject, data, timeout):
        body = json.loads(data)
        payload = json.loads(body["Payload"])
        label = body["Cmd"] + (":" + payload["Action"] if "Action" in payload else "")
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            self.sent.append((subject, label, payload))
            await asyncio.sleep(self.delay)
            if label == self.timeout_on:
                raise asyncio.TimeoutError()
            return self.raw if self.raw is not None else json.dumps({"Status": self.status}).encode()
        finally:
            self.now -= 1

    async def close(self):
        pass


class Rows:
    def __init__(self):
        self.rows = []

    def write_row(self, *r):
        self.rows.append(r)


def make(wire=None, **cfg):
    wire = wire or Wire()
    svc = NatsService(NatsConfig(send_on=True, **cfg), transport_factory=lambda: wire)
    svc.audit = Rows()
    svc.connect()
    return svc, wire


def test_one_portfolios_commands_keep_their_order_while_many_threads_submit():
    svc, wire = make(Wire(delay=0.001))
    futs = []

    def worker(pf):
        for i in range(30):
            futs.append(svc.send(pf, "apply" if i % 2 else "squareup", {"n": i}))
    threads = [threading.Thread(target=worker, args=(pf,)) for pf in range(1, 9)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wait(futs, timeout=20)
    for pf in range(1, 9):
        seen = [p["n"] for s, _, p in wire.sent if s == f"cmd.hft.{pf}"]
        assert seen == list(range(30)), pf
    assert wire.peak > 1                                   # different portfolios overlapped
    assert svc.counts["ok"] == 240 and svc.queued == 0 and svc.in_flight == 0
    svc.on_stop()


def test_the_in_flight_cap_holds_for_all_portfolios_together():
    svc, wire = make(Wire(delay=0.01), max_in_flight=3)
    wait([svc.pause(pf) for pf in range(1, 41)], timeout=20)
    assert 1 < wire.peak <= 3 and len(wire.sent) == 40
    svc.on_stop()


def test_a_busy_portfolio_cannot_starve_another():
    svc, wire = make(Wire(delay=0.03), max_in_flight=1)    # longer than the ~15 ms Windows timer tick, so the lanes really queue
    hot = [svc.send(1, "squareup") for _ in range(15)]
    quiet = svc.send(2, "squareup")
    quiet.result(10)
    place = [s for s, _, _ in wire.sent].index("cmd.hft.2")
    assert place <= 3, place                               # next in line, not behind the hot lane's backlog
    wait(hot, timeout=20)
    svc.on_stop()


def test_every_apply_is_sent_in_the_order_given():
    svc, wire = make(Wire(delay=0.05))
    futs = [svc.apply(7, {"n": n}) for n in range(1, 6)]
    assert all(f.result(5).ok for f in futs)
    assert [p["n"] for _, _, p in wire.sent] == [1, 2, 3, 4, 5]
    assert svc.counts["dropped"] == 0
    svc.on_stop()


def test_a_full_lane_refuses_instead_of_losing_work_silently():
    svc, wire = make(Wire(delay=0.2), lane_depth=3)
    futs = [svc.squareup(5)]
    for _ in range(200):                                   # the first one must really be on the wire (running, not waiting)
        if wire.sent:
            break
        time.sleep(0.005)
    futs += [svc.squareup(5) for _ in range(5)]            # 1 running + 3 waiting + 2 refused
    res = [f.result(10) for f in futs]
    assert [r.status for r in res].count("full") == 2 and [r.ok for r in res].count(True) == 4
    svc.on_stop()


def test_a_job_runs_its_commands_back_to_back():
    svc, wire = make(Wire(delay=0.01))

    async def job(call):
        await call("pause_unpause", {"Action": "Pause"})
        await call("apply", {"n": 1})
        return await call("pause_unpause", {"Action": "UnPause"})
    a = svc.submit(3, job)
    b = svc.squareup(3)                                    # queued behind the job, not in the middle of it
    assert a.result(5).ok and b.result(5).ok
    assert [lab for _, lab, _ in wire.sent] == ["pause_unpause:Pause", "apply", "pause_unpause:UnPause", "squareup"]
    svc.on_stop()


def test_every_command_carries_the_user_and_only_non_subscribes_carry_sender():
    svc, wire = make(users=(4,), sender="ALGO7")
    wait([svc.subscribe(1, 47158), svc.unsubscribe(1, 47158), svc.pause(1), svc.squareup(1), svc.apply(1, {"ival": 1.5})], timeout=5)
    by = {lab: p for _, lab, p in wire.sent}
    for p in by.values():
        assert p["USERIDENTIFIER"] == 4 and p["user_id"] == 4
    assert "SENDER" not in by["subscribe"] and "SENDER" not in by["unsubscribe"]
    assert by["pause_unpause:Pause"]["SENDER"] == by["squareup"]["SENDER"] == by["apply"]["SENDER"] == "ALGO7"
    assert by["subscribe"]["T1"] == 47158 and by["subscribe"]["strategyname"] == "Churning" and by["apply"]["ival"] == 1.5
    svc.on_stop()


def test_the_wire_format_is_cmd_payload_replyid_on_cmd_hft_pf():
    seen = []

    class Raw(Wire):
        async def request(self, subject, data, timeout):
            seen.append((subject, json.loads(data)))
            return await super().request(subject, data, timeout)
    svc, wire = make(Raw())
    svc.pause(1913).result(5)
    subject, body = seen[0]
    assert subject == "cmd.hft.1913" and set(body) == {"Cmd", "Payload", "ReplyId"} and isinstance(body["Payload"], str)
    assert json.loads(body["Payload"]) == {"Action": "Pause", "USERIDENTIFIER": 3, "user_id": 3, "SENDER": "AUTOMATION"}
    assert svc.audit.rows[0][:3] == (1913, "pause_unpause", body["Payload"]) and svc.audit.rows[0][3] == body["ReplyId"]
    svc.on_stop()


def test_a_timeout_is_a_reply_not_an_exception_and_is_never_resent():
    svc, wire = make(Wire(timeout_on="squareup"))
    r = svc.squareup(9).result(5)
    assert (r.ok, r.status) == (False, "timeout") and len(wire.sent) == 1
    assert svc.counts["timeout"] == 1 and svc.audit.rows == []
    assert svc.pause(9).result(5).ok                       # the lane carries on
    svc.on_stop()


def test_the_bridges_own_refusal_and_garbage_replies_are_not_ok():
    svc, _ = make(Wire(status="error"))
    r = svc.pause(1).result(5)
    assert (r.ok, r.status) == (False, "error") and svc.audit.rows == []
    svc.on_stop()
    svc, _ = make(Wire(raw=b"<html>oops</html>"))
    r = svc.pause(1).result(5)
    assert (r.ok, r.status) == (False, "bad_reply") and "oops" in r.data["raw"]
    svc.on_stop()
    svc, _ = make(Wire(raw=b"[1, 2]"))
    assert svc.pause(1).result(5).status == "bad_reply"
    svc.on_stop()


def test_a_dropped_connection_fails_fast_instead_of_waiting_for_the_timeout():
    svc, wire = make(Wire(delay=5), timeout_s=30)
    wire.is_connected = False
    t0 = time.time()
    r = svc.pause(1).result(5)
    assert r.status == "disconnected" and time.time() - t0 < 1 and wire.sent == [] and not svc.connected
    wire.is_connected = True
    assert svc.connected
    svc.on_stop()


def test_nothing_is_sent_while_sending_is_off_and_a_dry_run_only_logs():
    svc, wire = make()
    svc.send_off()
    assert svc.pause(1).result(5).status == "off" and svc.go(asyncio.sleep(0)) is None and wire.sent == []
    assert svc.send_on() is True and svc.pause(1).result(5).ok
    svc.on_stop()
    dry = NatsService(NatsConfig(dry_run=True, send_on=True))
    dry.connect()
    r = dry.apply(1, {}).result(1)
    assert (r.ok, r.status) == (True, "dry") and dry.loop is None and dry.send_on() is False


def test_stopping_answers_everything_still_waiting_and_refuses_new_work():
    svc, wire = make(Wire(delay=0.3))
    futs = [svc.squareup(4) for _ in range(5)]
    time.sleep(0.05)
    svc.on_stop()
    res = [f.result(5) for f in futs]
    assert [r.status for r in res].count("stopped") >= 3
    assert svc.pause(4).result(1).status == "stopped"
    svc.on_stop()                                          # a second stop is harmless


def test_a_cancelled_job_that_has_not_started_is_skipped():
    svc, wire = make(Wire(delay=0.2))
    first = svc.squareup(2)
    time.sleep(0.05)
    waiting = svc.squareup(2)
    assert waiting.cancel()
    assert first.result(5).ok
    time.sleep(0.1)
    assert len(wire.sent) == 1
    svc.on_stop()


def test_a_job_that_raises_reaches_its_caller_and_the_lane_survives():
    svc, wire = make()

    async def bad(call):
        raise ValueError("boom")
    f = svc.submit(6, bad)
    try:
        f.result(5)
        raised = False
    except ValueError:
        raised = True
    assert raised and svc.pause(6).result(5).ok
    svc.on_stop()


def test_stats_and_describe_report_what_happened():
    svc, _ = make()
    svc.pause(1).result(5)
    s = svc.stats()
    assert s["sent"] == 1 and s["ok"] == 1 and s["connected"] and s["sending"] and s["queued"] == 0
    assert "sending ON" in svc.describe() and "sent 1" in svc.describe()
    svc.on_stop()


def test_a_subject_with_nobody_listening_is_its_own_status():
    class NoRespondersError(Exception):
        pass

    class Nobody(Wire):
        async def request(self, subject, data, timeout):
            raise NoRespondersError()
    svc, _ = make(Nobody())
    r = svc.pause(77).result(5)
    assert (r.ok, r.status) == (False, "no_responders") and svc.counts["failed"] == 1
    svc.on_stop()


class Events:
    def __init__(self):
        self.rows = []

    def write_row(self, *r):
        self.rows.append(r)


def test_every_failure_and_connection_change_becomes_an_event_row():
    svc, wire = make(Wire(timeout_on="squareup"))
    svc.events = Events()
    import zoneinfo
    svc.zone = zoneinfo.ZoneInfo("Asia/Kolkata")
    svc.squareup(1).result(5)
    wire.is_connected = False
    svc.pause(2).result(5)
    wire.is_connected = True
    svc._on_event("down")
    svc._on_event("up")
    seen = [(pf, cmd, status) for _, _, pf, cmd, status, _ in svc.events.rows]
    assert seen == [(1, "squareup", "timeout"), (2, "pause_unpause", "disconnected"), (0, "connection", "down"),
                    (0, "connection", "up")]
    assert svc.pause(3).result(5).ok and len(svc.events.rows) == 4        # a success is not an event
    svc.on_stop()


def test_a_failed_swap_is_an_event_and_a_ui_action_gets_its_reply_back(tmp_path):
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    svc, wire = make(Wire(status="error"))
    svc.events = Events()
    import zoneinfo
    svc.zone = zoneinfo.ZoneInfo("Asia/Kolkata")
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    b = Bridge(BridgeConfig())
    b.bind(svc, book)
    b.send_on()
    book.track([10])
    pf = book.create(10)
    svc._run(b.swap(pf, 10, 55, {}, None, None))
    assert any(cmd == "swap" and status == "failed" for _, _, _, cmd, status, _ in svc.events.rows)
    fut = b.pause_pf(pf)                                                 # the UI gets the Future back
    assert fut is not None and fut.result(5).status == "error"
    svc.on_stop()


# ---- our service not running: nothing raises, everyone is told ---------------------------------------------------
def test_state_tells_a_ui_what_is_wrong():
    svc, wire = make()
    svc.send_off()
    assert svc.state == "off"
    svc.send_on()
    assert svc.state == "on"
    wire.is_connected = False
    assert svc.state == "disconnected"
    wire.is_connected = True
    svc.on_stop()
    assert svc.state == "service_down"
    assert NatsService(NatsConfig(dry_run=True)).state == "dry"


def test_the_stand_in_answers_service_down_to_everything_and_raises_nothing():
    from algo.comms.nats_service import NO_SERVICE
    for fut in (NO_SERVICE.subscribe(1, 2), NO_SERVICE.unsubscribe(1, 2), NO_SERVICE.pause(1), NO_SERVICE.unpause(1),
                NO_SERVICE.squareup(1), NO_SERVICE.apply(1, {}), NO_SERVICE.send(1, "x"), NO_SERVICE.submit(1, None)):
        r = fut.result(1)
        assert (r.ok, r.status) == (False, "service_down")
    assert NO_SERVICE.go(asyncio.sleep(0)) is None and NO_SERVICE.send_on() is False and NO_SERVICE.sending is False
    assert NO_SERVICE.state == "service_down" and "not running" in NO_SERVICE.describe()


def test_the_bridge_without_a_service_does_not_raise_and_says_so(tmp_path, caplog):
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    b = Bridge(BridgeConfig())
    b.bind(None, book)
    book.track([10])
    pf = book.create(10)
    assert b.state == "service_down" and b.sending is False and b.dry_run is False
    assert b.pause_pf(pf) is None and b.unpause_pf(pf) is None and b.squareup_pf(pf) is None
    b.pause_all()
    b.unpause_all()
    b.exit_all()
    b.unsubscribe_all()
    assert b.submit(asyncio.sleep(0)) is False and b.send_on() is False
    assert "service" in b.describe()


def test_the_bridge_picks_up_a_service_that_is_stopped_and_started_again(tmp_path):
    from algo.core.config import NSE, Paths
    from algo.core.app import App
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    app = App({}, NSE, Paths())
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    b = Bridge(BridgeConfig())
    b.bind(None, book)
    b._bind(app)
    assert b.state == "service_down"
    first, w1 = make()
    app.plugins["nats"] = first
    assert b.state == "off" and b.nats is first                  # the service is up, but this algo has not switched itself on
    assert b.send_on() and b.state == "on"
    app.plugins.pop("nats")
    first.on_stop()
    assert b.state == "service_down"
    book.track([10])
    pf = book.create(10)
    assert b.pause_pf(pf) is None                          # not sent, not raised
    second, w2 = make()
    app.plugins["nats"] = second
    assert b.nats is second and b.pause_pf(pf) is None            # a restarted service starts every algo OFF again
    assert b.send_on() and b.pause_pf(pf).result(5).ok and len(w2.sent) == 1 and w1.sent == []
    second.on_stop()


def _swap_setup(tmp_path, wire, **cfg):
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    svc, _ = make(wire, **cfg)
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    b = Bridge(BridgeConfig())
    b.bind(svc, book)
    book.track([10])
    return svc, b, book, book.create(10)


def test_a_swap_already_started_finishes_when_the_service_is_stopped(tmp_path):
    svc, b, book, pf = _swap_setup(tmp_path, Wire(delay=0.02))
    fut = svc.go(b.swap(pf, 10, 55, {}, None, None))
    time.sleep(0.03)
    svc.on_stop()                                          # it waits for the swap that is under way
    fut.result(5)
    assert book.subscribed[pf] == 55 and 10 not in book.last_fail


def test_a_swap_that_hangs_when_the_service_stops_is_cancelled_rolled_back_and_does_not_raise(tmp_path):
    svc, b, book, pf = _swap_setup(tmp_path, Wire(delay=5), timeout_s=0.2)
    fut = svc.go(b.swap(pf, 10, 55, {}, None, None))
    time.sleep(0.05)
    t0 = time.time()
    svc.on_stop()
    assert time.time() - t0 < 3                            # bounded by the timeout, not by the hanging request
    fut.result(5)                                          # the swap ended, it did not raise or hang
    assert 10 in book.last_fail and book.subscribed.get(pf) is None      # marked to be retried later


# ---- several users, one service ------------------------------------------------------------------------------------
def test_each_command_goes_out_as_the_user_it_names():
    svc, wire = make(users=(3, 4))
    a, b = svc.pause(1, user=4).result(5), svc.pause(2, user=3).result(5)
    assert a.ok and b.ok and (a.user, b.user) == (4, 3)
    by_pf = {s: p for s, _, p in wire.sent}
    assert by_pf["cmd.hft.1"]["USERIDENTIFIER"] == by_pf["cmd.hft.1"]["user_id"] == 4
    assert by_pf["cmd.hft.2"]["USERIDENTIFIER"] == by_pf["cmd.hft.2"]["user_id"] == 3
    svc.on_stop()


def test_a_command_with_no_user_or_the_wrong_user_is_refused_and_never_sent():
    svc, wire = make(users=(3, 4))
    svc.events = Events()
    import zoneinfo
    svc.zone = zoneinfo.ZoneInfo("Asia/Kolkata")
    none = svc.pause(1).result(5)
    wrong = svc.pause(1, user=5).result(5)
    junk = svc.pause(1, user="abc").result(5)
    assert (none.status, wrong.status, junk.status) == ("no_user", "user_not_allowed", "user_not_allowed")
    assert not (none.ok or wrong.ok or junk.ok) and wire.sent == []
    assert [r[4] for r in svc.events.rows] == ["no_user", "user_not_allowed", "user_not_allowed"]
    svc.on_stop()


def test_one_configured_user_is_the_default_and_a_dry_run_still_checks_the_user():
    svc, wire = make(users=(7,))
    r = svc.pause(1).result(5)
    assert r.ok and r.user == 7 and wire.sent[0][2]["USERIDENTIFIER"] == 7
    assert svc.pause(1, user=8).result(5).status == "user_not_allowed"
    svc.on_stop()
    dry = NatsService(NatsConfig(dry_run=True, users=(3, 4)))
    assert dry.pause(1).result(1).status == "no_user" and dry.pause(1, user=4).result(1).status == "dry"


def test_blocking_a_user_stops_only_that_user_and_unblocking_restores_it():
    svc, wire = make(users=(3, 4))
    svc.block(user=4)
    r = svc.pause(1, user=4).result(5)
    assert (r.ok, r.status) == (False, "blocked") and wire.sent == []
    assert svc.pause(2, user=3).result(5).ok                       # the other user is untouched
    svc.unblock(user=4)
    assert svc.pause(1, user=4).result(5).ok and svc.blocks() == {"users": [], "portfolios": {}}
    svc.on_stop()


def test_a_portfolio_is_blocked_under_its_own_user_only():
    svc, wire = make(users=(3, 4))
    svc.block(user=4, pf=1913)
    assert svc.pause(1913, user=4).result(5).status == "blocked" and svc.squareup(1913, user=4).result(5).status == "blocked"
    assert svc.pause(1913, user=3).result(5).ok          # the same number under user 3 is another portfolio
    assert svc.pause(1914, user=4).result(5).ok          # and user 4's other portfolios are untouched
    svc.unblock(user=4, pf=1913)
    assert svc.pause(1913, user=4).result(5).ok
    svc.block(user=4, pf=[1, 2])                          # several portfolios of one user
    assert svc.blocks() == {"users": [], "portfolios": {4: [1, 2]}}
    svc.on_stop()


def test_blocking_a_user_blocks_every_portfolio_under_that_user_even_ones_not_seen_yet():
    svc, wire = make(users=(3, 4))
    svc.block(user=4)
    assert all(svc.pause(pf, user=4).result(5).status == "blocked" for pf in (1, 1913, 99999))
    assert svc.pause(1913, user=3).result(5).ok and len(wire.sent) == 1
    svc.on_stop()


def test_unblocking_a_user_lifts_every_portfolio_blocked_under_that_user_and_only_theirs():
    svc, wire = make(users=(3, 4))
    svc.block(user=4)
    svc.block(user=4, pf=[1, 2])
    svc.block(user=3, pf=7)
    svc.unblock(user=4)
    assert svc.blocks() == {"users": [], "portfolios": {3: [7]}}
    assert svc.pause(1, user=4).result(5).ok and svc.pause(2, user=4).result(5).ok
    assert svc.pause(7, user=3).result(5).status == "blocked"
    svc.on_stop()


def test_unblocking_one_portfolio_does_not_lift_a_whole_user_block():
    svc, wire = make(users=(3, 4))
    svc.block(user=4)
    svc.unblock(user=4, pf=1913)
    assert svc.pause(1913, user=4).result(5).status == "blocked"
    svc.unblock(user=4)
    assert svc.pause(1913, user=4).result(5).ok
    svc.on_stop()


def test_a_portfolio_must_be_named_with_exactly_one_user():
    import pytest
    svc, _ = make(users=(3, 4))
    for call in (lambda: svc.block(pf=1913), lambda: svc.unblock(pf=1913), lambda: svc.block(user=[3, 4], pf=1913),
                 lambda: svc.unblock(user=[3, 4], pf=1913)):
        with pytest.raises(ValueError, match="user"):
            call()
    assert svc.blocks() == {"users": [], "portfolios": {}}
    svc.on_stop()


def test_the_same_portfolio_number_under_two_users_has_two_independent_lanes():
    svc, wire = make(Wire(delay=0.1), users=(3, 4))
    a, b = svc.pause(1, user=3), svc.pause(1, user=4)    # same number, two users
    c = svc.squareup(1, user=3)                          # same user and number as a: waits its turn
    assert a.result(5).ok and b.result(5).ok and c.result(5).ok
    assert wire.peak == 2                                # a and b were on the wire together
    users = [(p["USERIDENTIFIER"], lab) for _, lab, p in wire.sent]
    assert users.index((3, "squareup")) > users.index((3, "pause_unpause:Pause"))
    svc.on_stop()


def test_a_block_stops_the_rest_of_a_sequence_that_is_already_under_way():
    svc, wire = make(Wire(delay=0.05), users=(3, 4))

    async def job(call):
        first = await call("pause_unpause", {"Action": "Pause"})
        second = await call("apply", {"n": 1})
        return first, second
    fut = svc.submit(5, job, user=4)
    time.sleep(0.02)
    svc.block(user=4)                                              # while the first command is on the wire
    first, second = fut.result(5)
    assert first.ok and (second.ok, second.status) == (False, "blocked")
    assert [lab for _, lab, _ in wire.sent] == ["pause_unpause:Pause"]
    svc.on_stop()


def test_blocking_needs_a_user_or_a_portfolio_and_whole_numbers():
    import pytest
    svc, _ = make()
    for bad in (dict(), dict(user="x"), dict(pf=[1, "y"]), dict(user=True)):
        with pytest.raises(ValueError):
            svc.block(**bad)
    with pytest.raises(ValueError):
        svc.unblock()
    svc.on_stop()


def test_blocks_are_kept_in_the_shared_store_so_they_survive_a_restart_of_the_service():
    shared = {}
    first = NatsService(NatsConfig(users=(3, 4)))
    first.adopt_blocks(shared)
    first.block(user=4, pf=1913)
    first.block(user=3)
    second = NatsService(NatsConfig(users=(3, 4)))         # the service is stopped and started again
    second.adopt_blocks(shared)
    assert second.blocks() == {"users": [3], "portfolios": {4: [1913]}}
    assert second.pause(1913, user=4).result(1).status == "blocked" and second.pause(5, user=3).result(1).status == "blocked"


def test_the_bridge_sends_as_its_own_user_and_has_to_name_one_when_there_are_several(tmp_path):
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    svc, wire = make(users=(3, 4))
    svc.events = Events()
    import zoneinfo
    svc.zone = zoneinfo.ZoneInfo("Asia/Kolkata")
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    book.track([10, 20])
    p4, p3 = book.create(10), book.create(20)
    four, nameless = Bridge(BridgeConfig(user=4)), Bridge(BridgeConfig())
    four.bind(svc, book)
    nameless.bind(svc, book)
    four.send_on()
    nameless.send_on()
    svc._run(four.swap(p4, 10, 55, {}, None, None))
    assert wire.sent and {p["USERIDENTIFIER"] for _, _, p in wire.sent} == {4}
    sent_before = len(wire.sent)
    svc._run(nameless.swap(p3, 20, 66, {}, None, None))            # no user named: refused, rolled back, and said so
    assert len(wire.sent) == sent_before and 20 in book.last_fail
    assert any(r[4] == "no_user" for r in svc.events.rows) and any(r[3] == "swap" and r[4] == "failed" for r in svc.events.rows)
    assert nameless.pause_pf(p3).result(5).status == "no_user"
    svc.on_stop()


def test_a_block_stays_through_a_nats_outage_and_the_reconnect():
    svc, wire = make(users=(3, 4))
    svc.block(user=4)
    wire.is_connected = False                                      # the NATS server goes away
    assert svc.pause(1, user=4).result(5).status == "blocked"      # still blocked while it is down (not "disconnected")
    assert svc.pause(2, user=3).result(5).status == "disconnected"  # the other user just sees the outage
    wire.is_connected = True                                       # and comes back
    assert svc.pause(1, user=4).result(5).status == "blocked" and wire.sent == []
    assert svc.pause(2, user=3).result(5).ok
    svc.unblock(user=4)
    assert svc.pause(1, user=4).result(5).ok
    svc.on_stop()


def test_naming_a_blocked_user_in_a_new_start_unblocks_that_user_and_their_portfolios_only():
    shared = {}
    old = NatsService(NatsConfig(users=(3, 4, 5)))
    old.adopt_blocks(shared)
    old.block(user=4)
    old.block(user=4, pf=2000)
    old.block(user=5, pf=1913)
    old.block(user=3)
    new = NatsService(NatsConfig(users=(3, 4, 5)), lift_blocks=(4,))         # nats_start(users=[4, ...]) names user 4
    new.adopt_blocks(shared, new.lift_blocks)
    assert new.blocks() == {"users": [3], "portfolios": {5: [1913]}}           # 4 and all under 4 lifted; the rest stay
    assert new.pause(1, user=4).result(1).status == "off"                       # 4 reaches the next check (sending is off)
    assert new.pause(2000, user=4).result(1).status == "off"
    assert new.pause(1, user=3).result(1).status == "blocked"
    assert new.pause(1913, user=5).result(1).status == "blocked"


def test_a_start_that_names_no_users_keeps_every_block():
    shared = {}
    old = NatsService(NatsConfig(users=(3, 4)))
    old.adopt_blocks(shared)
    old.block(user=4)
    again = NatsService(NatsConfig(users=(3, 4)))                          # nats_start() with nothing: a plain reconnect
    again.adopt_blocks(shared, again.lift_blocks)
    assert again.blocks()["users"] == [4] and again.pause(1, user=4).result(1).status == "blocked"


# ---- lean: a cap on the commands inside the service, so a burst cannot grow memory without limit ---------------------------
def test_the_total_cap_refuses_the_excess_and_everything_else_still_runs(tmp_path):
    svc, wire = make(Wire(delay=0.3), max_queued=10, lane_depth=100, users=(3,))
    first = [svc.squareup(pf) for pf in (1, 2, 3)]                          # three portfolios each have one command on the wire
    for _ in range(200):
        if len(wire.sent) == 3:
            break
        time.sleep(0.005)
    more = [svc.squareup(pf) for pf in (1, 2, 3) for _ in range(20)]         # 60 more: only 7 fit under 10 in all
    res = [f.result(30) for f in more]
    assert [r.status for r in res].count("full") == 53 and [r.ok for r in res].count(True) == 7
    assert all(f.result(30).ok for f in first)
    assert svc.counts["dropped"] == 53 and svc.queued == 0 and svc._lanes == {}      # nothing left behind
    assert svc.pause(99).result(5).ok                                         # and it carries on normally
    svc.on_stop()


def test_a_refused_command_for_a_new_portfolio_does_not_leave_a_lane_that_never_runs():
    svc, wire = make(Wire(delay=0.6), max_queued=2, users=(3,))
    a, b = svc.squareup(1), svc.squareup(2)                                   # two running: the service is full
    for _ in range(200):
        if len(wire.sent) == 2:
            break
        time.sleep(0.005)
    refused = svc.squareup(3).result(5)
    assert refused.status == "full" and (3, 3) not in svc._lanes
    assert a.result(5).ok and b.result(5).ok
    assert svc.squareup(3).result(5).ok                                       # pf 3 works once there is room: its lane was not stuck
    svc.on_stop()


def test_a_cap_of_zero_means_no_cap():
    svc, wire = make(Wire(delay=0.2), max_queued=0, users=(3,))
    res = [f.result(30) for f in [svc.squareup(pf) for pf in range(1, 41)]]          # 40 at once: a cap of 10 would have refused 30
    assert all(r.ok for r in res)
    svc.on_stop()


# ---- started once for everyone; each algo and the UI switches its own sending on --------------------------------------------
def test_a_client_starts_off_and_only_its_own_switch_and_the_master_switch_let_it_send():
    svc, wire = make()                                       # master switch on
    assert svc.pause(1, client="rsi_stocks").result(5).status == "off"          # the algo has not switched itself on
    assert "has not switched sending on" in svc.pause(1, client="rsi_stocks").result(5).data["detail"]
    assert svc.enable("rsi_stocks") is True
    assert svc.pause(1, client="rsi_stocks").result(5).ok and len(wire.sent) == 1
    assert svc.pause(1).result(5).ok                                              # a typed command (no client): master switch only
    svc.send_off()                                                                # the master switch stops everyone
    assert svc.pause(1, client="rsi_stocks").result(5).status == "off" and svc.pause(1).result(5).status == "off"
    svc.send_on()
    assert svc.pause(1, client="rsi_stocks").result(5).ok                         # and back
    svc.on_stop()


def test_one_algo_switching_off_never_stops_another():
    svc, wire = make()
    svc.enable("rsi_stocks")
    svc.enable("rsi_nifty")
    svc.disable("rsi_nifty")
    assert svc.pause(1, client="rsi_nifty").result(5).status == "off"
    assert svc.pause(2, client="rsi_stocks").result(5).ok                         # the other algo carries on
    assert svc.is_enabled("rsi_stocks") and not svc.is_enabled("rsi_nifty")
    svc.on_stop()


def test_a_client_cannot_switch_on_without_a_connection_and_a_restart_switches_everyone_off():
    dry = NatsService(NatsConfig(dry_run=True))
    assert dry.enable("rsi_stocks") is False and not dry.is_enabled("rsi_stocks")
    first, _ = make()
    first.enable("rsi_stocks")
    second, _ = make()                                                            # a restarted service: a new object
    assert second.is_enabled("rsi_stocks") is False                               # everyone has to say "send" again
    first.on_stop()
    second.on_stop()


def test_new_users_can_be_added_to_the_running_service_without_a_restart():
    svc, wire = make(users=(3,))
    assert svc.pause(1, user=4).result(5).status == "user_not_allowed"
    svc.add_users(4, 5)
    svc.add_users(4)                                                              # again: nothing doubled
    assert svc.cfg.users == (3, 4, 5)
    assert svc.pause(1, user=4).result(5).ok
    svc.on_stop()


def test_the_bridge_has_its_own_switch_and_the_stand_in_never_allows_sending(tmp_path):
    from algo.comms.nats_service import NO_SERVICE
    from algo.strategy.book import Portfolios
    from algo.strategy.bridge import Bridge
    from algo.strategy.config import BridgeConfig
    svc, wire = make()
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    stocks, nifty = Bridge(BridgeConfig(), name="bridge_stocks"), Bridge(BridgeConfig(), name="bridge_nifty")
    stocks.bind(svc, book)
    nifty.bind(svc, book)
    book.track([1, 2])
    p1, p2 = book.create(1), book.create(2)
    assert stocks.state == "off" and stocks.pause_pf(p1) is None                  # an algo starts OFF
    assert stocks.send_on() is True and stocks.state == "on" and nifty.state == "off"
    assert stocks.pause_pf(p1).result(5).ok and nifty.pause_pf(p2) is None         # one is sending, the other is not
    nifty.send_on()
    stocks.send_off()
    assert nifty.pause_pf(p2).result(5).ok and stocks.pause_pf(p1) is None         # one stopping does not stop the other
    svc.send_off()
    assert nifty.state == "off" and nifty.pause_pf(p2) is None                      # the master switch stops all
    svc.on_stop()
    assert NO_SERVICE.enable("x") is False and NO_SERVICE.allowed("x") is False and NO_SERVICE.allowed() is False
    assert NO_SERVICE.pause(1, client="x").result(1).status == "service_down" and NO_SERVICE.go(asyncio.sleep(0), client="x") is None
