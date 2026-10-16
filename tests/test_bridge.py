"""The command sequences against a fake transport (no NATS, no Deephaven)."""
import asyncio
import json
import time

from algo.comms.nats_service import NatsConfig, NatsService
from algo.strategy.book import Portfolios
from algo.strategy.bridge import Bridge
from algo.strategy.config import BridgeConfig


class FakeTransport:
    def __init__(self, fail_on=None):
        self.sent, self.fail_on = [], fail_on

    async def request(self, subject, data, timeout):
        body = json.loads(data)
        payload = json.loads(body["Payload"])
        label = body["Cmd"] + (":" + payload["Action"] if "Action" in payload else "")
        self.sent.append((subject, label, payload))
        ok = label != self.fail_on
        await asyncio.sleep(0)
        return json.dumps({"Status": "ok" if ok else "error"}).encode()

    async def close(self):
        pass


class Rows:
    def __init__(self):
        self.rows = []

    def write_row(self, *r):
        self.rows.append(r)


def make(tmp_path, fail_on=None, **cfg):
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1000)
    fake = FakeTransport(fail_on)
    svc = NatsService(NatsConfig(send_on=True, **cfg), transport_factory=lambda: fake)
    svc.audit = Rows()
    svc.connect()
    b = Bridge(BridgeConfig())
    b.bind(svc, book)
    b.send_on()                                           # an algo starts OFF; this one switches itself on
    return b, book, fake


def run(b, coro):
    return b.nats._run(coro)


def labels(fake):
    return [(s, lab) for s, lab, _ in fake.sent]


def test_new_portfolio_sequence(tmp_path):
    b, book, fake = make(tmp_path)
    book.track([10])
    pf = book.create(10)
    run(b, b.swap(pf, 10, 55, {"ival": 0.2}, None, None))
    assert labels(fake) == [(f"cmd.hft.{pf}", "subscribe"), (f"cmd.hft.{pf}", "pause_unpause:Pause"),
                            (f"cmd.hft.{pf}", "apply"), (f"cmd.hft.{pf}", "pause_unpause:UnPause")]
    applied = fake.sent[2][2]
    assert applied["maxqty"] == 5 and applied["ival"] == 0.2 and applied["SENDER"] == "AUTOMATION"
    assert book.caps[pf] == 5 and book.subscribed[pf] == 55 and pf not in book.paused
    assert [r[1] for r in b.nats.audit.rows] == ["subscribe", "pause_unpause", "apply", "pause_unpause"]
    b.nats.on_stop()


def test_existing_portfolio_skips_subscribe_and_caps_accumulate(tmp_path):
    b, book, fake = make(tmp_path)
    book.track([10])
    pf = book.create(10)
    book.subscribe(pf, 55)
    run(b, b.swap(pf, 10, 55, {}, None, None))
    run(b, b.swap(pf, 10, 55, {}, "CALL", 55))
    assert [lab for _, lab in labels(fake)].count("subscribe") == 0
    assert book.caps[pf] == 10
    b.nats.on_stop()


def test_repoint_retires_the_old_portfolio_first(tmp_path):
    b, book, fake = make(tmp_path)
    book.track([10])
    old = book.create(10)
    book.subscribe(old, 55)
    new = book.create(10)
    run(b, b.swap(new, 10, 56, {}, "CALL", 55, old_pf=old))
    seq = labels(fake)
    assert seq[:3] == [(f"cmd.hft.{old}", "pause_unpause:Pause"), (f"cmd.hft.{old}", "squareup"),
                       (f"cmd.hft.{old}", "unsubscribe")]
    assert seq[3] == (f"cmd.hft.{new}", "subscribe") and old in book.paused
    assert fake.sent[2][2]["T1"] == 55                                  # unsubscribes the OLD option
    b.nats.on_stop()


def test_failed_apply_leaves_pf_paused_and_rolls_back(tmp_path):
    b, book, fake = make(tmp_path, fail_on="apply")
    book.track([10])
    pf = book.create(10)
    book.subscribe(pf, 55)
    book.set_live(10, "PUT", 55)
    run(b, b.swap(pf, 10, 56, {}, "CALL", 55))
    assert pf in book.paused and book.caps.get(pf, 0) == 0
    assert book.live[10] == "CALL" and book.opt_of[10] == 55 and 10 in book.last_fail
    assert labels(fake)[-1][1] == "apply"                               # no UnPause after a failed apply
    b.nats.on_stop()


def test_failed_new_pf_goes_back_to_the_old_one(tmp_path):
    b, book, fake = make(tmp_path, fail_on="subscribe")
    book.track([10])
    old = book.create(10)
    book.subscribe(old, 55)
    new = book.create(10)
    run(b, b.swap(new, 10, 56, {}, "CALL", 55, old_pf=old))
    assert book.pf_of(10) == old and new not in book.session_pfs and book.live[10] == "CALL"
    b.nats.on_stop()


def test_nothing_is_sent_while_sending_is_off(tmp_path):
    b, book, fake = make(tmp_path)
    b.send_off()
    book.track([10])
    pf = book.create(10)
    assert b.submit(b.swap(pf, 10, 55, {}, None, None)) is False
    b.pause_pf(pf)
    b.exit_all()
    assert fake.sent == []
    b.nats.on_stop()


def test_operator_actions_when_sending(tmp_path):
    b, book, fake = make(tmp_path)
    book.track([10, 20])
    p1, p2 = book.create(10), book.create(20)
    b.pause_all()
    for _ in range(200):
        if book.paused == {p1, p2}:
            break
        time.sleep(0.01)
    assert {s for s, _, _ in fake.sent} == {f"cmd.hft.{p1}", f"cmd.hft.{p2}"} and book.paused == {p1, p2}
    b.exit_all()
    time.sleep(0.3)
    assert [lab for _, lab in labels(fake)].count("squareup") == 2 and book.live[10] is None
    b.nats.on_stop()


def test_send_on_calls_strategies_back_and_dry_run_refuses(tmp_path):
    b, book, fake = make(tmp_path)
    b.send_off()
    calls = []
    undo = b.on_enable(lambda: calls.append(1))
    assert b.send_on() is True and calls == [1]
    undo()
    b.send_on()
    assert calls == [1]
    b.nats.on_stop()
    dry = Bridge(BridgeConfig())
    dry.bind(NatsService(NatsConfig(dry_run=True)), book)
    assert dry.send_on() is False and dry.sending is False
