"""Plain functions for writing an automation as a script in the Deephaven terminal. No class to define.

    algo_start(3, "stocks", pf_start=1001)               register the automation: its user, its name, where its portfolio numbers begin.
                                                         It starts OFF. Run it again to restart it (its portfolios are kept).
    a = pf_assign("stocks", token, option, name="RELIANCE")    THE BOOK picks the portfolio: new, the same, or a replacement. None while OFF.
    pf_activate(a, "CALL", params)                       THE NATS SERVICE: [retire old] subscribe, pause, apply, unpause. Returns at once.
    nats_send(True, client="stocks")                     switch THIS automation on (nats_send(False, client="stocks") stops only it)

    algo_sending("stocks")                               True when it may send right now (a quick check, prints nothing)

    pf_leg("stocks", token)                              "CALL", "PUT" or None: what it last started on that instrument
    pf_mark("stocks", token, "CALL", option)             remember a leg without sending
    pf_cooling("stocks", token, 30)                      True while the last failure was less than 30 seconds ago
    pf_allowed("stocks", token)                          False while the operator has UNTICKED it on the console (pf_assign then returns None)
    pf_track("stocks", [token1, token2])                 find the portfolios it had before a restart (call it once after algo_start)
    watch("candles", on_price)                           call your function on every update of a published table ("candles" or "straddle_series");
                                                         it follows the table if the service restarts it (candles_start again)
    algo_portfolios("stocks")                            its portfolios, as a table
    algo_pause("stocks"), algo_unpause, algo_exit, algo_unsubscribe      the same command for all of its portfolios
    algo_stop("stocks")                                  pause + square up + unsubscribe all of them, wait for it, THEN stop the automation (see its description)

Your script reacts to the market with an ordinary function and a listener, like your old scripts did; these functions never wait for the
network, so they are safe to call from the listener (algo_stop is the one exception: it waits for the unsubscribes, so call it from the terminal). Nothing here raises because a service is down or sending is off: an unknown name or a
service that is not running prints a plain message and the call returns None / False.

An instrument with several portfolios (several strikes of one future) uses `slot=0, 1, 2 ...`.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Callable

import pandas as pd

from algo.core.app import MissingDependency, current
from algo.core.config import FIRST_PF
from algo.core.plugin import Plugin
from algo.strategy.book import Book as _BookPlugin
from algo.strategy.book import Portfolios
from algo.strategy.bridge import Bridge
from algo.strategy.config import BridgeConfig

SLOT_STEP = 100                       # slot k of instrument I is known to the book as I * 100 + k (slot 0 is I itself)


@dataclass(frozen=True)
class Assignment:
    pf: int
    instrument: int
    option: int
    slot: int = 0
    old_pf: int | None = None          # the portfolio this one replaces: activating it pauses, squares up and unsubscribes the old one
    created: bool = False              # a new portfolio number was taken


@dataclass(frozen=True)
class Portfolio:
    pf: int
    instrument: int
    slot: int
    option: int | None                 # the option it is subscribed to
    leg: str | None                    # "CALL" / "PUT" / None
    cap: int                           # cumulative maxqty sent
    paused: bool                       # the bridge confirmed it paused
    name: str                          # what it trades, as recorded in the registry
    algo: str                          # the automation recorded as using it


class _Book:
    """The book service as one automation sees it (internal)."""

    def __init__(self, run: "_AlgoRun", state: Portfolios):
        self._run, self.state = run, state
        self._ids: dict[int, tuple[int, int]] = {}                 # book id -> (instrument, slot)

    @staticmethod
    def key(instrument: int, slot: int = 0) -> int:
        if not 0 <= slot < SLOT_STEP:
            raise ValueError(f"slot must be 0 to {SLOT_STEP - 1}, got {slot!r}")
        return int(instrument) if slot == 0 else int(instrument) * SLOT_STEP + slot

    def track(self, instrument: int, slot: int = 0) -> int:
        id_ = self.key(instrument, slot)
        if id_ not in self._ids:
            self._ids[id_] = (int(instrument), slot)
            self.state.track([id_])
        return id_

    def assign(self, instrument: int, option: int, slot: int = 0, name: str = "") -> Assignment | None:
        if not self._run.nats.sending:
            self._run.log.info("[OFF] no portfolio assigned for %s: this automation is not sending", instrument)
            return None
        id_ = self.track(instrument, slot)
        if not self.state.allow.get(id_, True):
            self._run.log.info("[BLOCKED] no portfolio assigned for %s slot %s: it is unticked on the console", instrument, slot)
            return None
        pf, old_pf, created = self.state.assign(id_, int(option), name=name or str(instrument), algo=self._run.name)
        return Assignment(pf, int(instrument), int(option), slot, old_pf, created)

    def snapshot(self, id_: int, pf: int) -> Portfolio:
        instrument, slot = self._ids[id_]
        st = self.state
        rec = (st.registry.get(st.user, pf) if st.registry is not None and st.user is not None else None) or {}
        return Portfolio(pf, instrument, slot, st.subscribed.get(pf), st.live.get(id_), st.caps.get(pf, 0), pf in st.paused,
                         rec.get("name", ""), rec.get("algo", ""))

    def all(self) -> list[Portfolio]:
        current_pfs = self.state.current_pf_map()
        return sorted((self.snapshot(i, pf) for i, pf in current_pfs.items() if i in self._ids), key=lambda p: p.pf)

    def cooling(self, instrument: int, seconds: float, slot: int = 0, now: Callable[[], float] = time.time) -> bool:
        return bool(seconds) and now() - self.state.last_fail.get(self.track(instrument, slot), 0.0) < seconds


class _Nats:
    """The NATS service as one automation sees it: its user and its own sending switch are filled in (internal)."""

    def __init__(self, run: "_AlgoRun", bridge: Bridge):
        self._run, self.bridge = run, bridge

    @property
    def sending(self) -> bool:
        return self.bridge.sending

    @property
    def state(self) -> str:
        return self.bridge.state

    def activate(self, a: Assignment, leg: str, params: dict) -> bool:
        id_ = self._run.book.key(a.instrument, a.slot)
        state = self._run.book.state
        prev_leg, prev_opt = state.live.get(id_), state.opt_of.get(id_)
        state.set_live(id_, leg, a.option)                         # mark now so the same thing is not queued twice
        queued = self.bridge.submit(self.bridge.swap(a.pf, id_, a.option, params, prev_leg, prev_opt, a.old_pf))
        if not queued:
            state.set_live(id_, prev_leg, prev_opt)
        return queued


class _AlgoRun(Plugin):
    """One started automation: its book and its bridge. Created by algo_start; you never use it directly."""

    def __init__(self, user: int, strategy: str, pf_start: int, options: dict, map_file: str | None = None, sub_file: str | None = None):
        super().__init__(strategy)
        self.user, self.pf_start, self.options = int(user), pf_start, options
        self.map_file, self.sub_file = map_file, sub_file
        self.requires = ("registry",)
        self.book: _Book | None = None
        self.nats: _Nats | None = None
        self._children: list[str] = []

    def start(self) -> None:
        app = self.app
        self._children = [f"{self.name}_book", f"{self.name}_bridge"]
        book = app.run(_BookPlugin(user=self.user, name=self._children[0], pf_start=self.pf_start, map_file=self.map_file,
                                   sub_file=self.sub_file))
        bridge = Bridge(BridgeConfig(user=self.user, **self.options), name=self._children[1], book=self._children[0], client=self.name)
        app.run(bridge)
        self.book, self.nats = _Book(self, book.state), _Nats(self, bridge)
        users = bridge.nats.cfg.users if bridge.nats.state != "service_down" else ()
        if users and self.user not in users:
            self.log.warning("user %s is not one of the NATS service's users %s: commands will be refused (nats_add_user(%s))",
                             self.user, list(users), self.user)
        self.log.info("started as user %s; sending OFF (switch on with nats_send(True, client=%r))", self.user, self.name)

    def on_stop(self) -> None:
        for w in self.app.shared.get("watches", {}).pop(self.name, []):      # its candle listeners go with it
            w.stop()
        for child in reversed(self._children):
            self.app.stop(child)

    def describe(self) -> str:
        return f"user {self.user}, {len(self.book.all()) if self.book else 0} portfolios, sending {self.nats.state if self.nats else '-'}"

    def send_on(self) -> bool:
        """So that  nats_send(True, client=<strategy>)  works. Refused while a turnover or M2M limit of the strategy is breached."""
        limits = self.app.shared.get("limits")
        why = limits.blocked(self.name) if limits is not None else ""
        if why:
            print(f"{self.name}: cannot switch ON: {why}")
            self.log.warning("switch ON refused: %s", why)
            return False
        return self.nats.bridge.send_on()

    def send_off(self) -> None:
        self.nats.bridge.send_off()


# ---- the functions --------------------------------------------------------------------------------------------------------------
def _run(strategy: str, what: str) -> "_AlgoRun | None":
    run = current().get(strategy) if strategy else None
    if not isinstance(run, _AlgoRun):
        print(f"{what}: no automation called {strategy!r} is started. Call algo_start(user, {strategy!r}) first.")
        return None
    return run


def algo_start(user: int, strategy: str, pf_start: int = FIRST_PF, map_file: str | None = None, sub_file: str | None = None,
               **options) -> bool:
    """Register an automation: algo_start(3, "stocks", pf_start=1001). Gives it its own book (portfolio numbers, unique for the user,
    remembered with the strategy name) and its own sending switch, which starts OFF. Run it again to restart it: it starts OFF again and keeps
    its portfolios. Options: add_qty (5), unsubscribe_on_retire (True), squareup_on_repoint (True). Needs the registry running.
    To keep the portfolios of an older script, name its two files: map_file="/data/stocks_pf.json" (token -> portfolio) and
    sub_file="/data/stocks_pf_sub.json" (portfolio -> option). Their numbers are registered for this user; one that another automation holds
    is reported as a CLASH. A file in another format (such as the old NIFTY token:slot file) is refused, never overwritten."""
    if isinstance(user, bool) or not str(user).isdigit() or int(user) <= 0:
        raise ValueError(f"user must be a whole number above 0, like 3, got {user!r}")
    if not isinstance(strategy, str) or not strategy.strip():
        raise ValueError('strategy must be a name, like "stocks"')
    strategy = strategy.strip()
    app = current()
    other = app.get(strategy)
    if other is not None and not isinstance(other, _AlgoRun):
        raise ValueError(f"{strategy!r} is already the name of another service; choose a different strategy name")
    if isinstance(other, _AlgoRun) and other.user != int(user):
        print(f"algo_start: {strategy!r} was running for user {other.user}; it is replaced and now runs for user {int(user)} (its portfolios stay with user {other.user}). "
              f"To run the same logic for two users, give each its own strategy name.")
    try:
        app.run(_AlgoRun(int(user), strategy, pf_start, options, map_file, sub_file))
    except MissingDependency:
        print("algo_start: the portfolio registry is not running. Start it once with registry_start().")
        return False
    window = app.get("console")                                  # the console, if it is running, makes this strategy its own window
    if window is not None and hasattr(window, "open"):
        try:
            window.open(strategy)
        except Exception:
            import logging
            logging.getLogger("algo.automation").exception("the console window of %r was not made", strategy)
    return True


class _Watch:
    """A listener that follows a published table by name (internal; you get it from watch() and only call .stop())."""

    def __init__(self, name: str, fn: Callable, replay: bool):
        self.name, self.fn, self.replay = name, fn, replay
        self.handle = None
        self._undo = None
        self.stopped = False

    def _safe(self, update, is_replay) -> None:
        try:
            self.fn(update, is_replay)
        except Exception:
            import logging
            logging.getLogger(f"algo.watch.{self.name}").exception("the function you gave to watch(%r) failed", self.name)

    def attach(self, table) -> None:
        """Listen to `table`; when the service republishes the table (candles restarted) this is called again with the new one."""
        from deephaven.table_listener import listen
        if self.stopped:
            return
        if self.handle is not None:
            try:
                self.handle.stop()
            except Exception:
                pass
        self.handle = listen(table, self._safe, do_replay=self.replay)

    def stop(self) -> None:
        self.stopped = True
        if self._undo is not None:
            self._undo()
        if self.handle is not None:
            try:
                self.handle.stop()
            except Exception:
                pass
            self.handle = None


def watch(table: str, fn: Callable, replay: bool = False, owner: str | None = None) -> "_Watch | None":
    """Call fn(update, is_replay) on every update of a table a service publishes: watch("candles", on_price).
    Unlike a plain Deephaven listen(), it FOLLOWS the table: when the table is restarted (candles_start was run again) it
    moves to the new one by itself, so your script does not keep listening to a stale copy. An error in your function is logged and the
    listener carries on. Returns a handle: handle.stop(). Prints a message and returns None if that table is not there.
    Give `owner=<your strategy name>` in a script: the listener then belongs to that automation, so pasting the script again (or algo_stop) stops the
    old listener first. Without it, every paste adds another listener, and your function runs once per paste."""
    app = current()
    published = app.tables.get(table)
    if published is None:
        print(f"watch: the {table} table is not there: start it first ({'candles_start()' if table == 'candles' else 'straddle_start(0)'})")
        return None
    w = _Watch(table, fn, replay)
    if owner:
        mine = app.shared.setdefault("watches", {}).setdefault(owner, [])
        for old in [x for x in mine if x.name == table]:      # a re-pasted script replaces its own listener
            old.stop()
            mine.remove(old)
        mine.append(w)
    w.attach(published)
    w._undo = app.on_publish(table, w.attach)
    return w


def algo_sending(strategy: str) -> bool:
    """True when this automation may send right now (the service is up, its master switch is on and the automation has switched itself on).
    Quick and silent: made for use inside a listener."""
    run = current().get(strategy) if strategy else None
    return isinstance(run, _AlgoRun) and run.nats.sending


def algo_stop(strategy: str, unsubscribe: bool = True, wait_s: float = 30.0) -> None:
    """Stop an automation: first close it down on the trading side, then stop it.

    By default it does what algo_exit does (pauses and squares up every portfolio) AND unsubscribes every current portfolio, waits for those commands to finish
    (up to `wait_s` seconds), and only then stops the automation (its book, bridge and candle listeners; the portfolio numbers and files are kept).
    If the portfolios cannot be unsubscribed because sending is OFF (a switched-off strategy, a breached limit) or the NATS service is down, NOTHING is stopped and it says
    so: switch the strategy ON and call it again, or algo_stop(name, unsubscribe=False) to stop it as it is (its portfolios stay subscribed, positions open).
    A dry run, or an automation with no portfolios, just stops. It waits, so call it from the terminal, not from a listener."""
    run = current().get(strategy) if strategy else None
    if unsubscribe and isinstance(run, _AlgoRun):
        bridge = run.nats.bridge
        if not bridge.dry_run and bridge.book is not None and bridge.book.current_pfs():
            futures = bridge.unsubscribe_all()
            if futures is None:
                print(f"{strategy}: NOT stopped: its portfolios could not be unsubscribed (sending is OFF or the NATS service is down). Switch it ON and call algo_stop again, "
                      f"or algo_stop({strategy!r}, unsubscribe=False) to stop it as it is, with its portfolios still subscribed")
                return
            from concurrent.futures import wait
            _, pending = wait(futures, timeout=wait_s)
            if pending:
                print(f"{strategy}: {len(pending)} of {len(futures)} portfolio(s) were still being unsubscribed after {wait_s:g} s; stopping now, watch the log")
    current().stop(strategy)


def pf_track(strategy: str, instruments, slots: int = 1) -> None:
    """Tell the book which instruments (and how many slots of each) this automation may trade, so the portfolios it had before a restart are
    found again: pf_track("stocks", [token1, token2]). Call it once after algo_start."""
    run = _run(strategy, "pf_track")
    if run is not None:
        for i in ([instruments] if isinstance(instruments, int) else instruments):
            for k in range(slots):
                run.book.track(i, k)


def pf_assign(strategy: str, instrument: int, option: int, name: str = "", slot: int = 0) -> dict | None:
    """THE BOOK picks the portfolio for this instrument trading this option; you never pick a number. None yet: a new one (unique for the
    user). Same option: that one. Another option: a NEW one, and the old is reported as old_pf. Returns a dict (pf, old_pf, created ...) to
    pass to pf_activate, or None while this automation is not sending (no number is used up)."""
    run = _run(strategy, "pf_assign")
    if run is None:
        return None
    a = run.book.assign(instrument, option, slot=slot, name=name)
    return None if a is None else {"strategy": strategy, **asdict(a)}


def pf_activate(assignment: dict, leg: str, params: dict) -> bool:
    """Run the standard sequence for an assignment: retire the old portfolio, subscribe if new, pause, apply (maxqty = cap + add_qty), unpause.
    Returns at once (safe in a listener); stops and rolls back on any failure. False, and nothing marked, if it could not be queued."""
    if not assignment:
        return False
    run = _run(assignment.get("strategy"), "pf_activate")
    if run is None:
        return False
    fields = {k: v for k, v in assignment.items() if k != "strategy"}
    return run.nats.activate(Assignment(**fields), leg, params)


def pf_leg(strategy: str, instrument: int, slot: int = 0) -> str | None:
    """What this automation last started on the instrument: "CALL", "PUT" or None."""
    run = _run(strategy, "pf_leg")
    return None if run is None else run.book.state.live.get(run.book.track(instrument, slot))


def pf_mark(strategy: str, instrument: int, leg: str | None, option: int | None = None, slot: int = 0) -> None:
    """Remember what the automation has started, without sending (pf_activate does this itself)."""
    run = _run(strategy, "pf_mark")
    if run is not None:
        run.book.state.set_live(run.book.track(instrument, slot), leg, option)


def pf_allowed(strategy: str, instrument: int, slot: int = 0) -> bool:
    """False while the operator has UNTICKED this slot on the console (it is unsubscribed from there). pf_assign then returns None by itself;
    use this to skip the work earlier. Ticking it again allows it."""
    run = _run(strategy, "pf_allowed")
    return True if run is None else run.book.state.allow.get(run.book.key(instrument, slot), True)


def pf_cooling(strategy: str, instrument: int, seconds: float = 30, slot: int = 0) -> bool:
    """True while the last activation of this slot failed less than `seconds` ago: do not try again yet."""
    run = _run(strategy, "pf_cooling")
    return False if run is None else run.book.cooling(instrument, seconds, slot)


def algo_portfolios(strategy: str) -> pd.DataFrame | None:
    """The automation's current portfolios as a table: number, instrument, slot, option, leg, cap, paused, name."""
    run = _run(strategy, "algo_portfolios")
    if run is None:
        return None
    df = pd.DataFrame([asdict(p) for p in run.book.all()],
                      columns=["pf", "instrument", "slot", "option", "leg", "cap", "paused", "name", "algo"])
    print(df.to_string(index=False) if len(df) else f"{strategy}: no portfolios yet")
    return df


def _all(strategy: str, what: str, call: str) -> None:
    run = _run(strategy, what)
    if run is not None:
        getattr(run.nats.bridge, call)()


def algo_pause(strategy: str) -> None:
    """Pause every portfolio of this automation (results go to the log and the nats_events table)."""
    _all(strategy, "algo_pause", "pause_all")


def algo_unpause(strategy: str) -> None:
    """Unpause every current portfolio of this automation."""
    _all(strategy, "algo_unpause", "unpause_all")


def algo_exit(strategy: str) -> None:
    """Pause, then square up, every portfolio this automation has touched."""
    _all(strategy, "algo_exit", "exit_all")


def algo_unsubscribe(strategy: str) -> None:
    """Pause, square up and unsubscribe every current portfolio of this automation."""
    _all(strategy, "algo_unsubscribe", "unsubscribe_all")
