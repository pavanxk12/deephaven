"""The strategy's command sequences, sent through the shared NATS service (algo.comms.nats_service.NatsService).

`Bridge` holds no connection of its own. It knows the portfolio book and the order of commands for a swap, a retire, an
exit; the service does the sending, keeps the commands of one portfolio in order and lets different portfolios overlap.
Each bridge is one named client of the service and has its own sending switch (`send_on()` / `send_off()`, starting OFF), so
one algo stopping never stops another. A command goes out only while the service's master switch AND this bridge's own switch
are on; with either off, signals are still computed but no command is sent.
"""
from __future__ import annotations

import time
from concurrent.futures import Future
from typing import Awaitable, Callable

from algo.comms.nats_service import NO_SERVICE, Call, NatsService, Reply
from algo.core.plugin import Plugin
from algo.strategy.book import Portfolios
from algo.strategy.config import BridgeConfig


def _need(reply: Reply, what: str) -> Reply:
    if not reply.ok:
        raise RuntimeError(f"{what}: {reply.status} {reply.data or ''}".rstrip())
    return reply


async def _step(call: Call, cmd: str, payload: dict | None, what: str) -> Reply:
    return _need(await call(cmd, payload), what)


class Bridge(Plugin):
    name = "bridge"

    def __init__(self, cfg: BridgeConfig = BridgeConfig(), name: str = "bridge", book: str = "book", nats: str = "nats",
                 client: str | None = None):
        super().__init__(name)
        self.client = client or name                      # the name of this sender in the NATS service: its own sending switch
        self.requires = (book,)                           # the NATS service may start later, or be restarted
        self.book_name, self.nats_name = book, nats
        self.cfg = cfg
        self.book: Portfolios | None = None
        self._nats: NatsService | None = None
        self._enable_cbs: list[Callable[[], None]] = []
        self._halt_cbs: list[Callable[[], None]] = []

    # ---- lifecycle ---------------------------------------------------------------------------
    def start(self) -> None:
        self.bind(None, self.app[self.book_name].state)
        self.nats.disable(self.client)                    # every start begins OFF, even a re-paste of an algo that was sending
        self.log.info("sending through %s: %s", self.nats_name, self.nats.describe())
        book_user = getattr(self.book, "user", None)
        if self.cfg.user is not None and book_user is not None and self.cfg.user != book_user:
            self.log.warning("this bridge sends as user %s but its book belongs to user %s", self.cfg.user, book_user)
        users = self.nats.cfg.users if self.nats is not NO_SERVICE else ()
        if self.user is None and len(users) > 1:
            self.log.warning("the NATS service has users %s and this bridge names none: every command will be refused (no_user). "
                             "Use BridgeConfig(user=...)", list(users))
        elif self.user is not None and users and self.user not in users:
            self.log.warning("user %s is not one of the NATS service's users %s: commands will be refused", self.user, list(users))

    def bind(self, nats: "NatsService | None", book: Portfolios) -> None:
        """`nats` is for tests; a running bridge finds the service in the app by name each time it needs it."""
        self._nats, self.book = nats, book

    @property
    def nats(self):
        """The running NATS service, looked up every time so a stopped and restarted service is picked up. While there is
        none, a stand-in that answers "service_down" to everything and raises nothing."""
        svc = self._app.get(self.nats_name) if self._app is not None else self._nats
        return svc if svc is not None else NO_SERVICE

    @property
    def user(self) -> "int | None":
        """The user this strategy sends as: BridgeConfig(user=...), else the user of its book."""
        return self.cfg.user if self.cfg.user is not None else getattr(self.book, "user", None)

    @property
    def state(self) -> str:
        """For a UI: service_down, disconnected, dry, off or on (off also when this bridge has not switched sending on)."""
        base = self.nats.state
        return "off" if base == "on" and not self.nats.is_enabled(self.client) else base

    async def _pause(self, call: Call, pf: int, action: str) -> Reply:
        """Pause or unpause; the book is told only after the bridge has answered ok."""
        reply = await call("pause_unpause", {"Action": action})
        if reply.status == "ok":
            self.book.set_paused(pf, action == "Pause")
        return reply

    def _after_pause(self, fut: Future, pf: int, paused: bool) -> Future:
        """For a pause sent on its own: update the book when the answer arrives (and only if it was ok)."""
        def done(f):
            if not f.cancelled() and f.exception() is None and f.result().status == "ok":
                self.book.set_paused(pf, paused)
        fut.add_done_callback(done)
        return fut

    def describe(self) -> str:
        return f"via {self.nats_name}: {self.nats.describe()}"

    @property
    def sending(self) -> bool:
        """May this bridge send right now? The service's master switch and this bridge's own switch are both on."""
        return self.nats.allowed(self.client)

    @property
    def dry_run(self) -> bool:
        return self.nats.cfg.dry_run

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
        """This algo keeps computing signals but sends nothing; other algos are not affected. A swap in flight finishes."""
        self.nats.disable(self.client)
        self.book.changed()

    def send_on(self) -> bool:
        """This algo may send (it starts OFF). The service's master switch must be on too."""
        if not self.nats.enable(self.client):
            return False
        self.book.changed()
        for cb in list(self._enable_cbs):
            cb()
        return True

    def submit(self, coro: Awaitable) -> bool:
        """Run a workflow (such as `swap`) without waiting. Returns False (and drops it) while sending is off."""
        return self.nats.go(coro, client=self.client) is not None

    # ---- command sequences ---------------------------------------------------------------------
    async def retire(self, old_pf: int) -> None:
        """The stock moves to another option: pause the old portfolio, optionally square it up and unsubscribe it.
        It keeps its number and option; a new portfolio takes over the stock."""
        cfg = self.cfg

        async def job(call: Call) -> None:
            _need(await self._pause(call, old_pf, "Pause"), f"pause of old PF {old_pf} not ok, new PF skipped")
            if cfg.squareup_on_repoint:
                await _step(call, "squareup", None, f"squareup of old PF {old_pf} not ok, new PF skipped")
            if cfg.unsubscribe_on_retire:
                reply = await call("unsubscribe", {**self.nats.cfg.sub_payload, "T1": self.book.subscribed.get(old_pf, 0)})
                if not reply.ok:                           # already paused and squared up: do not block the new PF
                    self.log.warning("unsubscribe of old PF %s not ok (left paused): %s", old_pf, reply.status)

        await self.nats.lane(old_pf, job, user=self.user, client=self.client)

    async def swap(self, pf: int, token: int, option: int, params: dict, prev_leg, prev_option,
                   old_pf: int | None = None) -> None:
        """[retire old PF] -> [subscribe the option] -> pause -> apply (maxqty = cap + add_qty) -> unpause.
        Stops, leaves the portfolio paused, and rolls the bookkeeping back on any failure."""
        book, cfg = self.book, self.cfg

        async def job(call: Call) -> None:
            if book.subscribed.get(pf) is None:                           # new PF: subscribe it to the option
                await _step(call, "subscribe", {**self.nats.cfg.sub_payload, "T1": option}, "subscribe not ok, apply skipped")
                book.subscribe(pf, option)
            _need(await self._pause(call, pf, "Pause"), "pause not ok, apply skipped")
            cap = book.caps.get(pf, 0) + cfg.add_qty
            await _step(call, "apply", {**params, "maxqty": cap}, "apply not ok, left paused")
            book.caps[pf] = cap
            _need(await self._pause(call, pf, "UnPause"), "unpause not ok, may still be paused")

        try:
            if old_pf is not None:
                await self.retire(old_pf)
            await self.nats.lane(pf, job, user=self.user, client=self.client)
        except Exception as e:
            if old_pf is not None and book.subscribed.get(pf) is None:   # the new PF never reached the bridge
                book.revert(token, old_pf, pf)
            book.set_live(token, prev_leg, prev_option)                 # allow a retry on a later update
            book.last_fail[token] = time.time()
            self.log.error("PF %s swap FAILED: %r", pf, e)
            self.nats.note(pf, "swap", "failed", str(e), self.user or 0)

    def _exit_job(self, pf: int):
        async def job(call: Call) -> None:
            await self._pause(call, pf, "Pause")
            await call("squareup", None)
        return job

    def _unsubscribe_job(self, pf: int):
        async def job(call: Call) -> None:
            opt = self.book.subscribed.get(pf)
            if opt is None:
                return
            steps = (("pause_unpause", lambda: self._pause(call, pf, "Pause")),
                     ("squareup", lambda: call("squareup", None)),
                     ("unsubscribe", lambda: call("unsubscribe", {**self.nats.cfg.sub_payload, "T1": opt})))
            for cmd, step in steps:
                reply = await step()
                if not reply.ok:
                    self.log.warning("PF %s %s not ok, stopped for this PF: %s", pf, cmd, reply.status)
                    return
        return job

    # ---- operator actions (used by the console and by hand) ------------------------------------
    def _guard(self, what: str) -> bool:
        if not self.sending:
            if self.nats is NO_SERVICE:
                self.log.warning("%s NOT sent: the NATS service is not running (nats_start())", what)
            elif not self.nats.sending:
                self.log.info("[OFF] %s NOT sent: sending is off for everyone (nats_send(True))", what)
            else:
                self.log.info("[OFF] %s NOT sent: %s has not switched sending on (send_on())", what, self.client)
            return False
        return True

    def _dry(self, what: str) -> bool:
        if self.dry_run:
            self.log.info("[DRY] %s", what)
            return True
        return False

    def pause_pf(self, pf: int) -> "Future | None":
        """Returns the Future of the command (its Reply says how it went), or None when nothing was sent."""
        if not self._guard(f"pause PF {pf}"):
            return None
        if self._dry(f"pause {pf}"):
            self.book.set_paused(pf, True)
        else:
            return self._after_pause(self.nats.pause(pf, user=self.user, client=self.client), pf, True)

    def unpause_pf(self, pf: int) -> "Future | None":
        if not self._guard(f"unpause PF {pf}"):
            return None
        if self._dry(f"unpause {pf}"):
            self.book.set_paused(pf, False)
        else:
            return self._after_pause(self.nats.unpause(pf, user=self.user, client=self.client), pf, False)

    def squareup_pf(self, pf: int) -> "Future | None":
        """The console only allows this while the PF is paused."""
        if not self._guard(f"squareup PF {pf}"):
            return None
        fut = None if self._dry(f"squareup {pf}") else self.nats.squareup(pf, user=self.user, client=self.client)
        t = self.book.token_of_pf(pf)
        if t is not None:
            self.book.set_live(t, None)                    # a later signal may start this portfolio again
        return fut

    def pause_all(self) -> None:
        if not self._guard("pause_all"):
            return
        for pf in self.book.all_pfs():
            if not self._dry(f"pause {pf}"):
                self._after_pause(self.nats.pause(pf, user=self.user, client=self.client), pf, True)

    def unpause_all(self) -> None:
        """Current portfolios only: a retired one stays paused."""
        if not self._guard("unpause_all"):
            return
        for pf in self.book.current_pfs():
            if not self._dry(f"unpause {pf}"):
                self._after_pause(self.nats.unpause(pf, user=self.user, client=self.client), pf, False)

    def exit_all(self) -> None:
        """Pause + square up every portfolio."""
        if not self._guard("exit_all"):
            return
        for pf in self.book.all_pfs():
            if not self._dry(f"pause + squareup {pf}"):
                self.nats.submit(pf, self._exit_job(pf), user=self.user, client=self.client, label="exit")
        for t in list(self.book.tracked):
            self.book.set_live(t, None)

    def unsubscribe_all(self) -> None:
        """Stop signals, then pause + square up + unsubscribe every current portfolio."""
        if self.dry_run or not self.sending:
            self.log.warning("the NATS service is not running (nats_start())" if self.nats is NO_SERVICE
                             else "sending is OFF or dry run: call send_on() first (dry_run must be False)")
            return
        for cb in list(self._halt_cbs):
            cb()
        pfs = self.book.current_pfs()
        for pf in pfs:
            self.nats.submit(pf, self._unsubscribe_job(pf), user=self.user, client=self.client, label="unsubscribe")
        for t in list(self.book.tracked):
            self.book.set_live(t, None)
        self.log.info("unsubscribing %d portfolio(s), watch the log", len(pfs))
