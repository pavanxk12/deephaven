"""Sending commands to the trading bridge over NATS.

`Bridge` owns an asyncio loop in a background thread. Every portfolio has a lock, so commands for one portfolio run in
order while different portfolios overlap. The `send_on` switch decides whether anything leaves the process at all:
with it off, signals are still computed but no command is sent.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from typing import Awaitable, Callable, Protocol

from algo.core.plugin import Plugin
from algo.strategy.book import Portfolios
from algo.strategy.config import BridgeConfig


class Transport(Protocol):
    async def request(self, subject: str, data: bytes, timeout: float) -> bytes: ...
    async def close(self) -> None: ...


class NatsTransport:
    def __init__(self, nc):
        self.nc = nc

    @classmethod
    async def connect(cls, url: str):
        import nats
        return cls(await nats.connect(url, connect_timeout=3))

    async def request(self, subject: str, data: bytes, timeout: float) -> bytes:
        return (await self.nc.request(subject, data, timeout=timeout)).data

    async def close(self) -> None:
        await self.nc.close()


class Bridge(Plugin):
    name = "bridge"
    requires = ("book",)

    def __init__(self, cfg: BridgeConfig = BridgeConfig(), transport_factory: Callable[[], Transport] | None = None,
                 name: str = "bridge", book: str = "book"):
        super().__init__(name)
        self.requires = (book,)
        self.book_name = book
        self.cfg = cfg
        self._transport_factory = transport_factory       # tests inject a fake transport
        self.transport: Transport | None = None
        self.sending = cfg.send_on
        self.book: Portfolios | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._locks: dict[int, asyncio.Lock] = {}
        self._sem: asyncio.Semaphore | None = None
        self._writer = None
        self._enable_cbs: list[Callable[[], None]] = []
        self._halt_cbs: list[Callable[[], None]] = []

    # ---- lifecycle ---------------------------------------------------------------------------
    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht

        self.book = self.app[self.book_name].state
        shared = self.app.shared
        if "cmd_writer" not in shared:                    # audit table survives restarts of this plugin
            w = DynamicTableWriter({"PF": dht.int64, "Cmd": dht.string, "Payload": dht.string, "ReplyId": dht.string})
            shared["cmd_writer"] = w
            self.app.publish("cmd_queue_automated", w.table)
        self._writer = shared["cmd_writer"]
        self.connect()
        self.log.info("%s, sending %s", "DRY RUN, no connection" if self.cfg.dry_run else f"connected to {self.cfg.nats_url}",
                      "ON" if self.sending else "OFF")

    def connect(self) -> None:
        """Start the loop thread and connect (no-op in dry run)."""
        if self.cfg.dry_run:
            return
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True, name="algo-bridge").start()
        factory = self._transport_factory or (lambda: self._run(NatsTransport.connect(self.cfg.nats_url)))
        self.transport = factory()
        if self.cfg.max_parallel > 0:
            async def make():
                return asyncio.Semaphore(self.cfg.max_parallel)
            self._sem = self._run(make())

    def on_stop(self) -> None:
        if self.loop is None:
            return
        try:
            if self.transport is not None:
                self._run(self.transport.close())
        except Exception:
            pass
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop = None
        self.transport = None

    def describe(self) -> str:
        mode = "DRY RUN" if self.cfg.dry_run else self.cfg.nats_url
        return f"{mode}, sending {'ON' if self.sending else 'OFF'}"

    def _run(self, coro, timeout: float | None = None):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout or self.cfg.timeout_s + 5)

    # ---- the send switch -----------------------------------------------------------------------
    def on_enable(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Called after send_on(): strategies re-check tokens already sitting in a signal zone. Returns an unregister function."""
        self._enable_cbs.append(callback)
        return lambda: self._enable_cbs.remove(callback) if callback in self._enable_cbs else None

    def on_halt(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Called by unsubscribe_all(): strategies stop producing signals. Returns an unregister function."""
        self._halt_cbs.append(callback)
        return lambda: self._halt_cbs.remove(callback) if callback in self._halt_cbs else None

    def send_off(self) -> None:
        """Keep computing signals but send nothing. A swap already in flight finishes."""
        self.sending = False
        self.log.info("sending OFF: computing only, no messages to NATS")
        self.book.changed()

    def send_on(self) -> bool:
        if self.transport is None:
            self.log.warning("cannot turn sending ON: dry run, no NATS connection (set dry_run=False and restart the bridge)")
            return False
        self.sending = True
        self.log.info("sending ON")
        self.book.changed()
        for cb in list(self._enable_cbs):
            cb()
        return True

    # ---- plumbing ----------------------------------------------------------------------------
    def submit(self, coro: Awaitable) -> bool:
        """Schedule a coroutine on the loop without waiting. Returns False (and drops it) while sending is off."""
        if not self.sending or self.loop is None:
            coro.close()                                   # avoid Python's "never awaited" warning
            return False

        def done(f):
            if not f.cancelled() and f.exception():
                self.log.error("job FAILED: %r", f.exception())
        asyncio.run_coroutine_threadsafe(coro, self.loop).add_done_callback(done)
        return True

    def lock(self, pf: int) -> asyncio.Lock:
        return self._locks.setdefault(pf, asyncio.Lock())

    def payload(self, d: dict | None = None) -> dict:
        out = dict(d or {})
        out.update({"USERIDENTIFIER": self.cfg.user_id, "user_id": self.cfg.user_id, "SENDER": self.cfg.sender})
        return out

    async def request(self, pf: int, cmd: str, payload: dict) -> dict:
        """One command; returns the parsed reply. Raises on timeout. Replies with Status ok are recorded."""
        reply_id = str(uuid.uuid4())
        payload_str = json.dumps(payload)
        data = json.dumps({"Cmd": cmd, "Payload": payload_str, "ReplyId": reply_id}).encode()
        subject = f"cmd.hft.{pf}"
        if self._sem is not None:
            async with self._sem:
                raw = await self.transport.request(subject, data, self.cfg.timeout_s)
        else:
            raw = await self.transport.request(subject, data, self.cfg.timeout_s)
        self.log.info("[%s] %s -> %s", cmd, subject, raw.decode())
        reply = json.loads(raw.decode())
        if reply.get("Status") == "ok":
            self._writer.write_row(int(pf), cmd, payload_str, reply_id)
            if cmd == "pause_unpause":
                self.book.set_paused(pf, payload.get("Action") == "Pause")
        return reply

    async def _step(self, pf: int, cmd: str, payload: dict, what: str) -> dict:
        reply = await self.request(pf, cmd, payload)
        if reply.get("Status") != "ok":
            raise RuntimeError(f"{what}: {reply}")
        return reply

    # ---- command sequences ---------------------------------------------------------------------
    async def retire(self, old_pf: int) -> None:
        """The stock moves to another option: pause the old portfolio, optionally square it up and unsubscribe it.
        It keeps its number and option; a new portfolio takes over the stock."""
        cfg = self.cfg
        async with self.lock(old_pf):
            await self._step(old_pf, "pause_unpause", self.payload({"Action": "Pause"}),
                             f"pause of old PF {old_pf} not ok, new PF skipped")
            if cfg.squareup_on_repoint:
                await self._step(old_pf, "squareup", self.payload(), f"squareup of old PF {old_pf} not ok, new PF skipped")
            if cfg.unsubscribe_on_retire:
                reply = await self.request(old_pf, "unsubscribe", {**cfg.sub_payload, "T1": self.book.subscribed.get(old_pf, 0)})
                if reply.get("Status") != "ok":            # already paused and squared up: do not block the new PF
                    self.log.warning("unsubscribe of old PF %s not ok (left paused): %s", old_pf, reply)

    async def swap(self, pf: int, token: int, option: int, params: dict, prev_leg, prev_option,
                   old_pf: int | None = None) -> None:
        """[retire old PF] -> [subscribe the option] -> pause -> apply (maxqty = cap + add_qty) -> unpause.
        Stops, leaves the portfolio paused, and rolls the bookkeeping back on any failure."""
        book, cfg = self.book, self.cfg
        try:
            if old_pf is not None:
                await self.retire(old_pf)
            async with self.lock(pf):
                if book.subscribed.get(pf) is None:                       # new PF: subscribe it to the option
                    await self._step(pf, "subscribe", {**cfg.sub_payload, "T1": option}, "subscribe not ok, apply skipped")
                    book.subscribe(pf, option)
                await self._step(pf, "pause_unpause", self.payload({"Action": "Pause"}), "pause not ok, apply skipped")
                cap = book.caps.get(pf, 0) + cfg.add_qty
                await self._step(pf, "apply", self.payload({**params, "maxqty": cap}), "apply not ok, left paused")
                book.caps[pf] = cap
                await self._step(pf, "pause_unpause", self.payload({"Action": "UnPause"}), "unpause not ok, may still be paused")
        except Exception as e:
            if old_pf is not None and book.subscribed.get(pf) is None:   # the new PF never reached the bridge
                book.revert(token, old_pf, pf)
            book.set_live(token, prev_leg, prev_option)                 # allow a retry on a later update
            book.last_fail[token] = time.time()
            self.log.error("PF %s swap FAILED: %r", pf, e)

    async def _one(self, pf: int, cmd: str, payload: dict) -> None:
        async with self.lock(pf):
            await self.request(pf, cmd, payload)

    async def _exit(self, pf: int) -> None:
        async with self.lock(pf):
            await self.request(pf, "pause_unpause", self.payload({"Action": "Pause"}))
            await self.request(pf, "squareup", self.payload())

    async def _unsubscribe(self, pf: int) -> None:
        async with self.lock(pf):
            opt = self.book.subscribed.get(pf)
            if opt is None:
                return
            for cmd, pl in (("pause_unpause", self.payload({"Action": "Pause"})), ("squareup", self.payload()),
                            ("unsubscribe", {**self.cfg.sub_payload, "T1": opt})):
                reply = await self.request(pf, cmd, pl)
                if reply.get("Status") != "ok":
                    self.log.warning("PF %s %s not ok, stopped for this PF: %s", pf, cmd, reply)
                    return

    # ---- operator actions (used by the console and by hand) ------------------------------------
    def _guard(self, what: str) -> bool:
        if not self.sending:
            self.log.info("[OFF] %s NOT sent", what)
            return False
        return True

    def _dry(self, what: str) -> bool:
        if self.cfg.dry_run:
            self.log.info("[DRY] %s", what)
            return True
        return False

    def pause_pf(self, pf: int) -> None:
        if not self._guard(f"pause PF {pf}"):
            return
        if self._dry(f"pause {pf}"):
            self.book.set_paused(pf, True)
        else:
            self.submit(self._one(pf, "pause_unpause", self.payload({"Action": "Pause"})))

    def unpause_pf(self, pf: int) -> None:
        if not self._guard(f"unpause PF {pf}"):
            return
        if self._dry(f"unpause {pf}"):
            self.book.set_paused(pf, False)
        else:
            self.submit(self._one(pf, "pause_unpause", self.payload({"Action": "UnPause"})))

    def squareup_pf(self, pf: int) -> None:
        """The console only allows this while the PF is paused."""
        if not self._guard(f"squareup PF {pf}"):
            return
        if not self._dry(f"squareup {pf}"):
            self.submit(self._one(pf, "squareup", self.payload()))
        t = self.book.token_of_pf(pf)
        if t is not None:
            self.book.set_live(t, None)                    # a later signal may start this portfolio again

    def pause_all(self) -> None:
        if not self._guard("pause_all"):
            return
        for pf in self.book.all_pfs():
            if not self._dry(f"pause {pf}"):
                self.submit(self._one(pf, "pause_unpause", self.payload({"Action": "Pause"})))

    def unpause_all(self) -> None:
        """Current portfolios only: a retired one stays paused."""
        if not self._guard("unpause_all"):
            return
        for pf in self.book.current_pfs():
            if not self._dry(f"unpause {pf}"):
                self.submit(self._one(pf, "pause_unpause", self.payload({"Action": "UnPause"})))

    def exit_all(self) -> None:
        """Pause + square up every portfolio."""
        if not self._guard("exit_all"):
            return
        for pf in self.book.all_pfs():
            if not self._dry(f"pause + squareup {pf}"):
                self.submit(self._exit(pf))
        for t in list(self.book.tracked):
            self.book.set_live(t, None)

    def unsubscribe_all(self) -> None:
        """Stop signals, then pause + square up + unsubscribe every current portfolio."""
        if self.transport is None or not self.sending:
            self.log.warning("sending is OFF or dry run: call send_on() first (dry_run must be False)")
            return
        for cb in list(self._halt_cbs):
            cb()
        pfs = self.book.current_pfs()
        for pf in pfs:
            self.submit(self._unsubscribe(pf))
        for t in list(self.book.tracked):
            self.book.set_live(t, None)
        self.log.info("unsubscribing %d portfolio(s), watch the log", len(pfs))
