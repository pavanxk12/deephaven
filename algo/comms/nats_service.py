"""The one NATS connection that every algo and the UI send portfolio commands through.

    nats.subscribe(pf, option)     nats.pause(pf)      nats.unpause(pf)
    nats.squareup(pf)              nats.apply(pf, {...})     nats.unsubscribe(pf, option)

Every command is sent AS a user (`user=3`; optional when the service has only one). The service is started with the users it
allows, a command for any other user is refused, and `block(user=..)` / `block(user=.., pf=..)` stop a user (every portfolio under
it) or some of one user's portfolios. A portfolio number belongs to a user: user + portfolio is the unique id.

Each call returns at once with a `concurrent.futures.Future[Reply]`, so a Deephaven listener or a UI callback is never
held up by the network. `.result(timeout)` waits when you want the answer.

How the load is shared:
  * ONE asyncio loop in ONE background thread owns the connection. A NATS request is waiting on the network, not computing,
    so one loop carries thousands of requests in flight; more threads would only fight over the GIL.
  * Every portfolio has its own lane: a bounded FIFO. Commands for one portfolio always run in the order they were
    submitted (pause, apply, unpause can never overtake each other); different portfolios run side by side.
  * `max_in_flight` caps the requests on the wire for all portfolios together. The cap is a first-come queue, so one busy
    portfolio cannot starve the others.
  * A lane holds `lane_depth` waiting jobs. A job that does not fit is refused with status "full", never silently lost.
    Nothing is ever dropped or replaced: every command is sent, in the order it was given.
  * A command is never re-sent by itself: after a timeout it is unknown whether the bridge acted, so repeating a
    squareup or subscribe blindly could double it. The caller sees status "timeout" and decides.

Everything that touches a lane runs on the loop thread, so there are no locks.
"""
from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import threading
import time
import uuid
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from algo.core.plugin import Plugin

NO_SENDER = ("subscribe", "unsubscribe")          # these two carry the user id but not SENDER


def _sub_payload() -> dict:
    """Sent on `subscribe` / `unsubscribe`; only T1 (the option token) is replaced per portfolio."""
    return {"T1": 0, "gtw": 32395, "PRNT": 1, "code": "", "ival": 1, "jval": 0.75, "side": "BUY", "sval": 3, "trp": 0,
            "jqty1": 1, "jqty2": 1, "openq": 0, "maxqty": 0, "target": 1000, "maxloss": -1000, "stoploss": -1000,
            "lowerrange": 0, "upperrange": 10000, "max_unhedge": 1, "strategyname": "Churning"}


@dataclass
class NatsConfig:
    ip: str = "192.168.1.130"              # the NATS server
    port: int = 4222
    users: tuple = (3,)                    # the users that may send; every command is for one of them (stamped as USERIDENTIFIER, user_id)
    sender: str = "AUTOMATION"             # stamped as SENDER (not on subscribe / unsubscribe)
    timeout_s: float = 5                   # per request, counted from the moment it goes on the wire
    max_in_flight: int = 64                # requests on the wire at once, all portfolios together; 0 = unlimited
    lane_depth: int = 256                  # jobs waiting per portfolio
    max_queued: int = 5000                 # commands inside the service (running or waiting), all portfolios together; about 2 KB each; 0 = no cap
    subject: str = "cmd.hft.{pf}"
    dry_run: bool = False                  # True: no connection, commands are only logged
    send_on: bool = False                  # False: nothing is sent (switch live with send_on())
    sub_payload: dict = field(default_factory=_sub_payload)   # defaults of subscribe / unsubscribe

    @property
    def url(self) -> str:
        return f"nats://{self.ip}:{self.port}"


@dataclass(frozen=True)
class Reply:
    """The outcome of one command. `ok` is True only when the bridge answered Status "ok" (or in a dry run).

    status: the bridge's own Status, or one of: timeout, no_responders, disconnected, service_down, off, dry, full, stopped,
    bad_reply, error, no_user (several users are allowed and the command named none), user_not_allowed, blocked."""
    ok: bool
    status: str
    pf: int = 0
    cmd: str = ""
    data: dict = field(default_factory=dict)
    user: int = 0

    def __bool__(self) -> bool:
        return self.ok


REFUSED = ("stopped", "full", "no_user", "user_not_allowed", "blocked")      # the job never ran


def _ints(x) -> set:
    """None -> {}, 3 -> {3}, [3, 4] -> {3, 4}; anything that is not a whole number is an error."""
    if x is None:
        return set()
    items = [x] if isinstance(x, (int, str)) else list(x)
    out = set()
    for i in items:
        if isinstance(i, bool) or not str(i).strip().lstrip("-").isdigit():
            raise ValueError(f"expected a whole number or a list of them, got {i!r}")
        out.add(int(i))
    return out


Call = Callable[..., Awaitable[Reply]]            # call(cmd, payload=None) inside a job
Job = Callable[[Call], Awaitable[Any]]


class Transport(Protocol):
    async def request(self, subject: str, data: bytes, timeout: float) -> bytes: ...
    async def close(self) -> None: ...


class NatsTransport:
    """nats-py behind the small Transport interface. It reconnects by itself and says so through `on_event`."""

    def __init__(self, nc):
        self.nc = nc

    @classmethod
    async def connect(cls, url: str, connect_timeout: float = 3, on_event: Callable[[str, str], None] | None = None):
        import nats

        def tell(kind: str, detail: str = "") -> None:
            if on_event is not None:
                on_event(kind, detail)

        async def down():
            tell("down")

        async def up():
            tell("up")

        async def error(e):
            tell("error", repr(e))

        # max_reconnect_attempts=-1 keeps reconnecting after a drop, but it also makes nats-py retry a FIRST connection for
        # ever when the server is not there; the wait_for turns that into one clear error
        try:
            nc = await asyncio.wait_for(nats.connect(url, connect_timeout=connect_timeout, max_reconnect_attempts=-1,
                                                     reconnect_time_wait=1, disconnected_cb=down, reconnected_cb=up,
                                                     error_cb=error), timeout=connect_timeout + 2)
        except asyncio.TimeoutError:
            raise ConnectionError(f"no NATS server answered at {url}") from None
        return cls(nc)

    @property
    def is_connected(self) -> bool:
        return bool(self.nc.is_connected)

    async def request(self, subject: str, data: bytes, timeout: float) -> bytes:
        return (await self.nc.request(subject, data, timeout=timeout)).data

    async def close(self) -> None:
        await self.nc.close()


class NoService:
    """Stands in for the NatsService while it is not running (never started, or stopped). It has the same calls; each one
    answers at once with status "service_down" and nothing raises, so an algo or a UI keeps working and can say what is wrong."""
    sending = False
    connected = False
    state = "service_down"
    cfg = NatsConfig()
    counts: dict = {}

    def _down(self, pf, label: str) -> Future:
        fut: Future = Future()
        fut.set_result(Reply(False, "service_down", int(pf), label))
        return fut

    def send(self, pf, cmd, payload=None, *, user=None): return self._down(pf, cmd)
    def subscribe(self, pf, option, params=None, *, user=None): return self._down(pf, "subscribe")
    def unsubscribe(self, pf, option, params=None, *, user=None): return self._down(pf, "unsubscribe")
    def pause(self, pf, *, user=None): return self._down(pf, "pause_unpause")
    def unpause(self, pf, *, user=None): return self._down(pf, "pause_unpause")
    def squareup(self, pf, *, user=None): return self._down(pf, "squareup")
    def apply(self, pf, params, *, user=None): return self._down(pf, "apply")
    def submit(self, pf, job, *, user=None, label="job"): return self._down(pf, label)

    async def lane(self, pf, job, *, user=None):
        raise RuntimeError("the NATS service is not running")

    def go(self, coro):
        coro.close()
        return None

    def send_on(self) -> bool:
        return False

    def send_off(self) -> None:
        pass

    def note(self, pf, cmd, status, detail="", user=0) -> None:
        pass

    def describe(self) -> str:
        return "the NATS service is not running (nats_start())"

    def stats(self) -> dict:
        return {"state": self.state}


NO_SERVICE = NoService()


class NatsService(Plugin):
    name = "nats"

    def __init__(self, cfg: NatsConfig = NatsConfig(), transport_factory: Callable[[], Transport] | None = None,
                 name: str = "nats", lift_blocks: tuple = ()):
        super().__init__(name)
        self.cfg = cfg
        self.lift_blocks = tuple(lift_blocks)             # users named in this start: their blocks are lifted
        self._factory = transport_factory                 # tests inject a fake transport
        self.sending = cfg.send_on
        self.transport: Transport | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.audit = None                                 # anything with write_row(pf, cmd, payload, reply_id)
        self.events = None                                # anything with write_row(time, user, pf, cmd, status, detail)
        self.blocked_users: set = set()                   # nothing is sent for these users / portfolios until unblocked
        self.blocked_pfs: set = set()                     # (user, portfolio): a portfolio number belongs to a user
        self.zone = None
        self._thread: threading.Thread | None = None
        self._sem: asyncio.Semaphore | None = None
        self._lanes: dict[tuple, deque] = {}              # (user, portfolio) -> waiting jobs, loop thread only
        self._drains: set = set()                         # the tasks that run the lanes, loop thread only
        self._closing = False
        self._stopped = False
        self.counts = {"sent": 0, "ok": 0, "failed": 0, "timeout": 0, "dropped": 0}
        self.in_flight = self.queued = self.running = 0     # on the wire / waiting in a lane / being run by a lane
        self.rtt_ms = self.rtt_max_ms = 0.0

    # ---- lifecycle ---------------------------------------------------------------------------
    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht

        shared = self.app.shared
        if "cmd_writer" not in shared:                    # the audit table survives restarts of this plugin
            w = DynamicTableWriter({"PF": dht.int64, "Cmd": dht.string, "Payload": dht.string, "ReplyId": dht.string})
            shared["cmd_writer"] = w
            self.app.publish("cmd_queue_automated", w.table)
        self.audit = shared["cmd_writer"]
        if "nats_events_writer" not in shared:            # what went wrong, for a table in the UI to show
            w = DynamicTableWriter({"Time": dht.string, "User": dht.int64, "PF": dht.int64, "Cmd": dht.string,
                                    "Status": dht.string, "Detail": dht.string})
            shared["nats_events_writer"] = w
            self.app.publish("nats_events", w.table)
        self.events = shared["nats_events_writer"]
        self.adopt_blocks(shared, self.lift_blocks)
        if self.blocked_users or self.blocked_pfs:
            self.log.warning("still BLOCKED from before: %s (nats_unblock(...) lifts it)", self.blocks())
        self.zone = self.app.market.zone
        self.connect()
        self.log.info("%s, users %s, sending %s, up to %s requests at once", "DRY RUN, no connection" if self.cfg.dry_run
                      else f"connected to {self.cfg.url}", ", ".join(map(str, self.cfg.users)), "ON" if self.sending else "OFF",
                      self.cfg.max_in_flight or "any number of")

    def connect(self) -> None:
        """Start the loop thread and connect (nothing happens in a dry run)."""
        if self.cfg.dry_run:
            return
        self._closing = self._stopped = False
        loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=loop.run_forever, daemon=True, name="algo-nats")
        self._thread.start()
        self.loop = loop
        try:
            if self._factory is not None:
                self.transport = self._factory()
            else:
                self.transport = self._run(NatsTransport.connect(self.cfg.url, on_event=self._on_event))
            if self.cfg.max_in_flight > 0:
                async def make():
                    return asyncio.Semaphore(self.cfg.max_in_flight)
                self._sem = self._run(make())
        except Exception as e:
            self.on_stop()
            raise RuntimeError(f"cannot connect to {self.cfg.url}: {e!r}") from None

    def on_stop(self) -> None:
        loop, self.loop = self.loop, None                 # from here nothing new is accepted
        if loop is None:
            return
        self._closing = self._stopped = True
        try:
            asyncio.run_coroutine_threadsafe(self._shutdown(), loop).result(self.cfg.timeout_s + 5)
        except Exception:
            self.log.exception("shutdown")
        loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=3)
        self._thread = self.transport = self._sem = None

    async def _shutdown(self) -> None:
        for (_, pf), lane in list(self._lanes.items()):   # whatever is still waiting is told it will not run
            while lane:
                _, fut, user = lane.popleft()
                self.queued -= 1
                if fut.set_running_or_notify_cancel():
                    fut.set_result(Reply(False, "stopped", pf, user=user))
        # Whatever is already running (a request on the wire, a swap in the middle) may finish, for up to the timeout.
        others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if others:
            _, late = await asyncio.wait(others, timeout=self.cfg.timeout_s)
            for t in late:
                if t in self._drains:
                    t.cancel()                            # the stuck request ends and its caller hears "stopped" ...
            if late:                                      # ... so a workflow waiting on it rolls itself back
                _, late = await asyncio.wait(late, timeout=1)
            for t in late:
                t.cancel()
            await asyncio.gather(*late, return_exceptions=True)
        if self.transport is not None:
            with contextlib.suppress(Exception):
                await self.transport.close()

    def _run(self, coro, timeout: float | None = None):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout or self.cfg.timeout_s + 5)

    def _on_event(self, kind: str, detail: str = "") -> None:
        if self._closing:
            return                                        # our own close() reports a disconnect too
        if kind == "down":
            self.log.warning("NATS connection lost, reconnecting; commands fail fast until it is back")
            self.note(0, "connection", "down", "reconnecting about once a second; commands fail at once until it is back")
        elif kind == "up":
            self.log.info("NATS connection restored")
            self.note(0, "connection", "up", "")
        else:
            self.log.warning("NATS: %s", detail)
            self.note(0, "connection", "error", detail)

    def note(self, pf: int, cmd: str, status: str, detail: str = "", user: int = 0) -> None:
        """One row in the `nats_events` table: a failure or a connection change that a person or an algo should see."""
        if self.events is None:
            return
        try:
            from datetime import datetime
            self.events.write_row(datetime.now(self.zone).strftime("%H:%M:%S"), int(user), int(pf), cmd, status, detail[:300])
        except Exception:
            self.log.exception("event row not written")

    # ---- state -------------------------------------------------------------------------------
    @property
    def connected(self) -> bool:
        return self.cfg.dry_run or (self.transport is not None and getattr(self.transport, "is_connected", True))

    @property
    def state(self) -> str:
        """One word for a UI: service_down (stopped), disconnected (the NATS server cannot be reached), dry, off or on."""
        if self._stopped:
            return "service_down"
        if self.cfg.dry_run:
            return "dry"
        return "on" if self.sending and self.connected else ("off" if self.connected else "disconnected")

    def stats(self) -> dict:
        return {**self.counts, "connected": self.connected, "sending": self.sending, "in_flight": self.in_flight,
                "queued": self.queued, "portfolios_busy": len(self._lanes), "rtt_ms": round(self.rtt_ms, 1),
                "rtt_max_ms": round(self.rtt_max_ms, 1)}

    def describe(self) -> str:
        c = self.counts
        link = "DRY RUN" if self.cfg.dry_run else (self.cfg.url if self.connected else f"{self.cfg.url} DISCONNECTED")
        stopped = f", BLOCKED {self.blocks()}" if self.blocked_users or self.blocked_pfs else ""
        return (f"{link}, users {', '.join(map(str, self.cfg.users))}, sending {'ON' if self.sending else 'OFF'}, "
                f"sent {c['sent']} (ok {c['ok']}, failed {c['failed']}, timeout {c['timeout']}, dropped {c['dropped']}), "
                f"waiting {self.queued}, last {self.rtt_ms:.0f} ms{stopped}")

    def send_on(self) -> bool:
        if self.transport is None:
            self.log.warning("cannot turn sending ON: dry run or no NATS connection (set dry_run=False and restart nats)")
            return False
        self.sending = True
        self.log.info("sending ON")
        return True

    def send_off(self) -> None:
        """Nothing new is accepted. Jobs already waiting or running finish."""
        self.sending = False
        self.log.info("sending OFF: no new commands are accepted")

    # ---- the six commands ---------------------------------------------------------------------
    # `user` is the user the command is sent as. It may be left out when the service has only one user.
    def subscribe(self, pf: int, option: int, params: dict | None = None, *, user: int | None = None) -> Future:
        return self.send(pf, "subscribe", {**self.cfg.sub_payload, **(params or {}), "T1": int(option)}, user=user)

    def unsubscribe(self, pf: int, option: int, params: dict | None = None, *, user: int | None = None) -> Future:
        return self.send(pf, "unsubscribe", {**self.cfg.sub_payload, **(params or {}), "T1": int(option)}, user=user)

    def pause(self, pf: int, *, user: int | None = None) -> Future:
        return self.send(pf, "pause_unpause", {"Action": "Pause"}, user=user)

    def unpause(self, pf: int, *, user: int | None = None) -> Future:
        return self.send(pf, "pause_unpause", {"Action": "UnPause"}, user=user)

    def squareup(self, pf: int, *, user: int | None = None) -> Future:
        return self.send(pf, "squareup", user=user)

    def apply(self, pf: int, params: dict, *, user: int | None = None) -> Future:
        return self.send(pf, "apply", dict(params), user=user)

    # ---- general entry points -----------------------------------------------------------------
    def send(self, pf: int, cmd: str, payload: dict | None = None, *, user: int | None = None) -> Future:
        """One command on `pf`'s lane. Any thread may call this; it never blocks."""
        return self.submit(pf, lambda call: call(cmd, payload), user=user, label=cmd)

    def submit(self, pf: int, job: Job, *, user: int | None = None, label: str = "job") -> Future:
        """Several commands that must run back to back on one portfolio, nothing from that portfolio in between:

            async def job(call):
                await call("pause_unpause", {"Action": "Pause"})
                return await call("apply", {...})

        `call(cmd, payload)` returns a Reply. Refused (not queued) while sending is off, or for a user that is not allowed
        or is blocked, or a blocked portfolio."""
        return self._submit(int(pf), job, label, user, gated=True)

    async def lane(self, pf: int, job: Job, *, user: int | None = None):
        """For a workflow that is already running on the loop (it spans portfolios): run `job` on `pf`'s lane and return its
        result. Not stopped by the send switch (a started workflow finishes); raises if the job was refused."""
        res = await asyncio.wrap_future(self._submit(int(pf), job, "job", user, gated=False))
        if isinstance(res, Reply) and res.status in REFUSED:
            raise RuntimeError(f"PF {pf}: {res.status}")
        return res

    # ---- users and blocks -----------------------------------------------------------------------
    def block(self, user=None, pf=None) -> None:
        """Stop sending for a user, or for some of a user's portfolios. A portfolio number is only meaningful together with its
        user (user + portfolio is the unique id), so:

            block(user=4)                 user 4 and every portfolio under user 4
            block(user=4, pf=1913)        only portfolio 1913 of user 4  (pf=[1913, 1914] for several)

        `pf` without exactly one `user` is an error. A block also stops the rest of a sequence already under way, and a
        squareup too: that is what blocking means. Blocks outlive a restart of the service but not of Deephaven."""
        users, pfs = _ints(user), _ints(pf)
        if not users:
            raise ValueError("block(user=4) or block(user=4, pf=1913): a portfolio number belongs to a user, so name the user")
        if pfs:
            if len(users) != 1:
                raise ValueError("a portfolio belongs to one user: block(user=4, pf=[1913, 1914]), one user at a time")
            (u,) = users
            self.blocked_pfs.update((u, p) for p in pfs)
            what = f"user {u} portfolios {sorted(pfs)}"
        else:
            self.blocked_users.update(users)
            what = f"users {sorted(users)} (every portfolio under them)"
        self.log.warning("BLOCKED %s: nothing is sent for them", what)
        self.note(0, "block", "blocked", what)

    def unblock(self, user=None, pf=None) -> None:
        """unblock(user=4) lifts user 4 and every portfolio blocked under user 4;
        unblock(user=4, pf=1913) lifts only that portfolio (if the whole user is blocked, unblock the user first)."""
        users, pfs = _ints(user), _ints(pf)
        if not users:
            raise ValueError("unblock(user=4) or unblock(user=4, pf=1913): a portfolio number belongs to a user, so name the user")
        if pfs:
            if len(users) != 1:
                raise ValueError("a portfolio belongs to one user: unblock(user=4, pf=[1913, 1914]), one user at a time")
            (u,) = users
            self.blocked_pfs.difference_update((u, p) for p in pfs)
            what = f"user {u} portfolios {sorted(pfs)}"
            if u in self.blocked_users:
                self.log.warning("user %s is blocked as a whole, so those portfolios stay blocked: unblock(user=%s)", u, u)
        else:
            self._lift(users)
            what = f"users {sorted(users)} and every portfolio under them"
        self.log.info("unblocked %s", what)
        self.note(0, "block", "unblocked", what)

    def _lift(self, users) -> list:
        """Remove the blocks of these users and of all their portfolios. Returns the users that had any."""
        users = set(users)
        had = sorted(users & ({u for u, _ in self.blocked_pfs} | self.blocked_users))
        self.blocked_users.difference_update(users)
        self.blocked_pfs.difference_update({(u, p) for u, p in self.blocked_pfs if u in users})
        return had

    def adopt_blocks(self, shared: dict, lift_users: tuple = ()) -> None:
        """Keep the blocks in the app's shared store, so they outlive a restart of this service (not of Deephaven).
        Users named in this start (`lift_users`) are let back in, with all their portfolios: naming them again is the
        operator allowing them."""
        self.blocked_users = shared.setdefault("nats_blocked_users", set())
        self.blocked_pfs = shared.setdefault("nats_blocked_pfs", set())     # (user, portfolio) pairs
        lifted = self._lift(lift_users)
        if lifted:
            self.log.warning("users %s had blocks; naming them in nats_start() unblocks them and all their portfolios", lifted)

    def blocks(self) -> dict:
        """{"users": [4], "portfolios": {3: [1913, 1914]}}: blocked users, and blocked portfolios by the user they are under."""
        by_user: dict = {}
        for u, p in sorted(self.blocked_pfs):
            by_user.setdefault(u, []).append(p)
        return {"users": sorted(self.blocked_users), "portfolios": by_user}

    def _resolve(self, user) -> tuple[int, str]:
        """(user, "") when the command may go on, else (0, why). One configured user is the default."""
        users = self.cfg.users
        if user is None:
            return (users[0], "") if len(users) == 1 else (0, "no_user")
        try:
            u = int(user)
        except (TypeError, ValueError):
            return 0, "user_not_allowed"
        return (u, "") if u in users else (0, "user_not_allowed")

    def go(self, coro: Awaitable) -> "Future | None":
        """Run a free-form coroutine on the loop (a workflow that spans portfolios, which calls `lane` itself).
        Returns None, and drops it, while sending is off."""
        loop = self.loop
        if self.cfg.dry_run or not self.sending or loop is None:
            coro.close()                                  # avoid Python's "never awaited" warning
            return None
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError as e:                         # the loop was closed between the check and now
            coro.close()
            self.log.error("not sent, the NATS loop is closed: %s", e)
            return None

        def done(f):
            if not f.cancelled() and f.exception():
                self.log.error("job FAILED: %r", f.exception())
        fut.add_done_callback(done)
        return fut

    # ---- the lanes (loop thread) --------------------------------------------------------------
    def _submit(self, pf: int, job: Job, label: str, user, gated: bool) -> Future:
        fut: Future = Future()
        uid, why = self._resolve(user)
        if not why and (uid in self.blocked_users or (uid, pf) in self.blocked_pfs):
            why = "blocked"
        if why:                                           # said loudly: a person or an algo has to fix or lift it
            detail = {"no_user": f"several users are allowed {list(self.cfg.users)}: say which one",
                      "user_not_allowed": f"user {user!r} is not one of {list(self.cfg.users)}",
                      "blocked": "this user or portfolio is blocked"}[why]
            self.log.warning("[%s] PF %s NOT sent: %s", label, pf, detail)
            self.note(pf, label, why, detail, uid)
            fut.set_result(Reply(False, why, pf, label, {"detail": detail}, uid))
            return fut
        if self.cfg.dry_run:
            self.log.info("[DRY] %s PF %s user %s", label, pf, uid)
            fut.set_result(Reply(True, "dry", pf, label, user=uid))
            return fut
        if gated and not self.sending:
            self.log.debug("[OFF] %s PF %s NOT sent", label, pf)
            fut.set_result(Reply(False, "off", pf, label, user=uid))
            return fut
        loop = self.loop
        try:
            if loop is None:
                raise RuntimeError("stopped")
            loop.call_soon_threadsafe(self._enqueue, pf, job, fut, uid)
        except RuntimeError:                              # stopped, or the loop closed just now
            fut.set_result(Reply(False, "stopped", pf, label, user=uid))
        return fut

    def _enqueue(self, pf: int, job: Job, fut: Future, user: int) -> None:
        try:
            key = (user, pf)                              # the same number under another user is another portfolio
            lane = self._lanes.get(key)
            fresh = lane is None
            waiting = 0 if fresh else len(lane)
            inside = self.queued + self.running           # waiting plus running
            if waiting >= self.cfg.lane_depth or (self.cfg.max_queued and inside >= self.cfg.max_queued):
                self.counts["dropped"] += 1
                why = (f"{waiting} jobs already waiting for this portfolio" if waiting >= self.cfg.lane_depth
                       else f"{inside} commands already inside the service")
                self.log.warning("PF %s: %s, this one is refused", pf, why)
                self.note(pf, "queue", "full", why, user)
                fut.set_result(Reply(False, "full", pf, user=user))
                return
            if fresh:
                lane = self._lanes[key] = deque()
            lane.append((job, fut, user))
            self.queued += 1
            if fresh:
                task = asyncio.ensure_future(self._drain(key, lane))
                self._drains.add(task)
                task.add_done_callback(self._drains.discard)
        except Exception as e:                            # a bug here must not leave the caller waiting for ever
            if not fut.done():
                fut.set_exception(e)

    async def _drain(self, key: tuple, lane: deque) -> None:
        pf = key[1]
        try:
            while lane:
                job, fut, user = lane.popleft()
                self.queued -= 1
                if not fut.set_running_or_notify_cancel():
                    continue                              # the caller cancelled it while it waited
                self.running += 1
                try:
                    result = await job(functools.partial(self._call, pf, user))
                except asyncio.CancelledError:            # the service is stopping and this request did not finish in time
                    fut.set_result(Reply(False, "stopped", pf, user=user))
                    raise
                except Exception as e:
                    self.log.error("PF %s job failed: %r", pf, e)
                    fut.set_exception(e)
                else:
                    fut.set_result(result)
                finally:
                    self.running -= 1
        finally:
            if self._lanes.get(key) is lane:
                del self._lanes[key]

    # ---- one request --------------------------------------------------------------------------
    def _stamp(self, cmd: str, payload: dict | None, user: int) -> dict:
        out = dict(payload or {})
        out["USERIDENTIFIER"] = out["user_id"] = user
        if cmd not in NO_SENDER:
            out["SENDER"] = self.cfg.sender
        return out

    async def _call(self, pf: int, user: int, cmd: str, payload: dict | None = None) -> Reply:
        counts = self.counts
        if user in self.blocked_users or (user, pf) in self.blocked_pfs:  # blocked in the middle of a sequence
            counts["failed"] += 1
            self.log.warning("[%s] PF %s user %s NOT sent: blocked", cmd, pf, user)
            self.note(pf, cmd, "blocked", "this user or portfolio is blocked", user)
            return Reply(False, "blocked", pf, cmd, user=user)
        if not getattr(self.transport, "is_connected", True):
            counts["failed"] += 1
            self.log.warning("[%s] PF %s not sent: NATS is disconnected", cmd, pf)
            self.note(pf, cmd, "disconnected", "not sent: no connection to NATS", user)
            return Reply(False, "disconnected", pf, cmd, user=user)
        payload = self._stamp(cmd, payload, user)
        payload_str = json.dumps(payload)
        reply_id = str(uuid.uuid4())
        data = json.dumps({"Cmd": cmd, "Payload": payload_str, "ReplyId": reply_id}).encode()
        counts["sent"] += 1
        t0 = time.perf_counter()
        raw = b""
        try:
            async with (self._sem or contextlib.nullcontext()):
                t0 = time.perf_counter()                  # the timeout clock starts on the wire, not in the queue
                self.in_flight += 1
                try:
                    raw = await self.transport.request(self.cfg.subject.format(pf=pf), data, self.cfg.timeout_s)
                finally:
                    self.in_flight -= 1
        except asyncio.TimeoutError:
            counts["timeout"] += 1
            counts["failed"] += 1
            self.log.error("[%s] PF %s TIMED OUT after %.1fs: it is not known whether the bridge acted", cmd, pf, self.cfg.timeout_s)
            reply = Reply(False, "timeout", pf, cmd, user=user)
            self.note(pf, cmd, "timeout", f"no answer in {self.cfg.timeout_s:g}s: it is not known whether the bridge acted", user)
        except Exception as e:
            counts["failed"] += 1
            nobody = type(e).__name__ == "NoRespondersError"           # nothing is listening on cmd.hft.<pf>
            self.log.error("[%s] PF %s %s", cmd, pf, "has no listener on its subject" if nobody else f"failed: {e!r}")
            reply = Reply(False, "no_responders" if nobody else "error", pf, cmd, {"error": repr(e)}, user)
            self.note(pf, cmd, reply.status, "nothing listens on this portfolio's subject" if nobody else repr(e), user)
        else:
            self.rtt_ms = (time.perf_counter() - t0) * 1000
            self.rtt_max_ms = max(self.rtt_max_ms, self.rtt_ms)
            try:
                body = json.loads(raw.decode())
                if not isinstance(body, dict):
                    raise ValueError("not an object")
            except Exception:
                reply = Reply(False, "bad_reply", pf, cmd, {"raw": raw[:200].decode(errors="replace")}, user)
            else:
                status = str(body.get("Status", "")) or "no_status"
                reply = Reply(status == "ok", status, pf, cmd, body, user)
            counts["ok" if reply.ok else "failed"] += 1
            if reply.ok:
                self.log.info("[%s] PF %s -> ok (%.0f ms)", cmd, pf, self.rtt_ms)
                self._record(pf, cmd, payload_str, reply_id)
            else:
                self.log.warning("[%s] PF %s -> %s: %s", cmd, pf, reply.status, raw.decode(errors="replace"))
                self.note(pf, cmd, reply.status, raw.decode(errors="replace"), user)
        return reply

    def _record(self, pf: int, cmd: str, payload_str: str, reply_id: str) -> None:
        if self.audit is None:
            return
        try:
            self.audit.write_row(int(pf), cmd, payload_str, reply_id)
        except Exception:
            self.log.exception("audit row not written")
