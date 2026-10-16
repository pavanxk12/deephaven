"""What the commands have done to each portfolio, in ONE place that algos and screens both read and listen to.

The NATS service writes here after every command the bridge answered "ok", whoever sent it (an algo or the console), so:

    an algo pauses / squares up / subscribes   ->  the console shows it
    the console pauses / squares up / unticks  ->  the algo is told (`on_change`) and can look (`get`)

A portfolio is (user, number). Each one holds only a few small fields, so thousands of portfolios cost a few hundred KB.
The state is what was CONFIRMED this session; after a Deephaven restart it starts empty (the book re-adopts what it subscribed).
"""
from __future__ import annotations

import threading
import time
from typing import Callable


class PfState:
    """One portfolio as the bridge last confirmed it."""
    __slots__ = ("option", "subscribed", "paused", "squared", "by", "cmd", "at")

    def __init__(self):
        self.option: int | None = None      # the option token it is subscribed to
        self.subscribed = False             # the checkbox: True after subscribe, False after unsubscribe
        self.paused = False                 # the Pause / Resume button
        self.squared = False                # a square up went through and nothing has been applied since
        self.by = ""                        # who did the last change: an algo's name, or "operator" for the console / a typed command
        self.cmd = ""                       # the last command that changed it
        self.at = 0.0                       # when (epoch seconds)

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__slots__}


class PfStates:
    """Portfolio states, shared. Reads and writes may come from any thread."""

    def __init__(self):
        self._d: dict[tuple, PfState] = {}
        self._cbs: list[Callable] = []
        self._lock = threading.Lock()

    # ---- reading ---------------------------------------------------------------------------------
    def get(self, user: int, pf: int) -> PfState | None:
        return self._d.get((int(user), int(pf)))

    def items(self, user: int | None = None) -> list[tuple]:
        """[(user, pf, state)], for one user or everybody."""
        return [(u, p, s) for (u, p), s in list(self._d.items()) if user is None or u == user]

    def by_option(self, user: int | None = None, keep: Callable[[int, int, PfState], bool] | None = None) -> dict:
        """option token -> (user, pf, state): the portfolio that trades it. If several do, a subscribed one wins, then the latest change.
        `keep(user, pf, state)` may leave some out first (for instance another strategy's portfolios)."""
        out: dict = {}
        for u, p, s in self.items(user):
            if s.option is None or (keep is not None and not keep(u, p, s)):
                continue
            old = out.get(s.option)
            if old is None or (s.subscribed, s.at) > (old[2].subscribed, old[2].at):
                out[s.option] = (u, p, s)
        return out

    def __len__(self) -> int:
        return len(self._d)

    # ---- changing --------------------------------------------------------------------------------
    def record(self, user: int, pf: int, cmd: str, payload: dict | None, by: str | None) -> PfState | None:
        """Apply one command the bridge confirmed. Commands that change nothing shown here (apply only clears `squared`) are fine."""
        payload = payload or {}
        key = (int(user), int(pf))
        with self._lock:
            st = self._d.get(key)
            if st is None:
                st = self._d[key] = PfState()
            if cmd == "subscribe":
                st.option, st.subscribed, st.squared = _int(payload.get("T1"), st.option), True, False
            elif cmd == "unsubscribe":
                st.subscribed, st.option = False, _int(payload.get("T1"), st.option)
            elif cmd == "pause_unpause":
                st.paused = payload.get("Action") == "Pause"
            elif cmd == "squareup":
                st.squared = True
            elif cmd == "apply":
                st.squared = False
            else:
                return None
            st.by, st.cmd, st.at = by or "operator", cmd, time.time()
        self._tell(key[0], key[1], st)
        return st

    def adopt(self, user: int, pf: int, option: int, paused: bool | None = None) -> None:
        """A portfolio the book already had subscribed (from its file) when this session began. Never overwrites what was seen."""
        key = (int(user), int(pf))
        with self._lock:
            if key in self._d:
                return
            st = self._d[key] = PfState()
            st.option, st.subscribed, st.cmd, st.at = int(option), True, "adopted", time.time()
            if paused is not None:
                st.paused = bool(paused)
        self._tell(key[0], key[1], st)

    def forget(self, user: int | None = None) -> None:
        with self._lock:
            for k in [k for k in self._d if user is None or k[0] == user]:
                del self._d[k]

    # ---- listening -------------------------------------------------------------------------------
    def on_change(self, fn: Callable[[int, int, PfState], None]) -> Callable[[], None]:
        """fn(user, pf, state) after every change, from whatever thread made it (keep it short). Returns a function that removes it.
        Registering the same function twice registers it once."""
        with self._lock:
            if fn not in self._cbs:
                self._cbs.append(fn)

        def undo():
            with self._lock:
                if fn in self._cbs:
                    self._cbs.remove(fn)
        return undo

    def _tell(self, user: int, pf: int, st: PfState) -> None:
        for fn in list(self._cbs):
            try:
                fn(user, pf, st)
            except Exception:
                import logging
                logging.getLogger("algo.pfstate").exception("a pf-state listener failed")


def _int(x, default):
    try:
        return int(x)
    except (TypeError, ValueError):
        return default


def pf_states(shared: dict) -> PfStates:
    """The one PfStates of an app (in `app.shared`, so it survives a restart of the NATS service)."""
    hub = shared.get("pf_states")
    if hub is None:
        hub = shared["pf_states"] = PfStates()
    return hub
