"""Plugin base class: lifecycle plus the helpers every component needs.

A plugin is one independent component (master, candles, straddle, the NATS service, the book...).
It declares what it `requires`, builds its output in `start()`, and everything it created
(listeners, threads) is torn down automatically when it is stopped or replaced.
"""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable, Iterable

from algo.core import log

if TYPE_CHECKING:
    from algo.core.app import App


class Plugin:
    name: str = ""
    requires: tuple[str, ...] = ()        # plugin or table names that must exist before start()

    def __init__(self, name: str | None = None):
        if name:
            self.name = name
        if not self.name:
            raise ValueError(f"{type(self).__name__} needs a name")
        self._app: "App | None" = None
        self._handles: list = []
        self._threads: list[threading.Thread] = []
        self._halt = threading.Event()
        self._undo: list[Callable[[], None]] = []
        self.log = log.get(self.name)

    # ---- to implement -------------------------------------------------------------------
    def start(self) -> None:
        """Build outputs, attach listeners. Runs once per `app.run(plugin)`."""
        raise NotImplementedError

    def on_stop(self) -> None:
        """Extra cleanup beyond listeners and threads (close connections, ...)."""

    def describe(self) -> str:
        """One line for `app.status()`."""
        return ""

    # ---- available inside start() --------------------------------------------------------
    @property
    def app(self) -> "App":
        if self._app is None:
            raise RuntimeError(f"plugin {self.name!r} is not running")
        return self._app

    @property
    def halted(self) -> threading.Event:
        """Set when the plugin is stopped; loops should exit when it is."""
        return self._halt

    def listen(self, table, fn: Callable, replay: bool = False):
        """Deephaven table listener that is stopped with the plugin and cannot be killed by an exception in `fn`."""
        from deephaven.table_listener import listen as dh_listen

        def safe(update, is_replay):
            try:
                fn(update, is_replay)
            except Exception:
                self.log.exception("listener failed")

        handle = dh_listen(table, safe, do_replay=replay)
        self._handles.append(handle)
        return handle

    def follow(self, table_name: str, callback: Callable) -> None:
        """Call `callback(new_table)` whenever another plugin republishes `table_name` (e.g. candles restarted with a
        new bar size). Unregistered automatically when this plugin stops."""
        self._undo.append(self.app.on_publish(table_name, callback))

    def stop_listener(self, handle) -> None:
        try:
            handle.stop()
        except Exception:
            pass
        if handle in self._handles:
            self._handles.remove(handle)

    def spawn(self, fn: Callable, *args) -> threading.Thread:
        """Daemon thread; `fn` should exit when `self.halted` is set."""
        t = threading.Thread(target=self._guard, args=(fn, args), daemon=True, name=f"algo-{self.name}")
        self._threads.append(t)
        t.start()
        return t

    def _guard(self, fn: Callable, args: Iterable) -> None:
        try:
            fn(*args)
        except Exception:
            self.log.exception("thread failed")

    # ---- used by App ------------------------------------------------------------------
    def _bind(self, app: "App") -> None:
        self._app = app
        self._halt.clear()

    def _teardown(self) -> None:
        self._halt.set()
        for h in self._handles:
            try:
                h.stop()
            except Exception:
                pass
        self._handles.clear()
        for undo in self._undo:
            try:
                undo()
            except Exception:
                pass
        self._undo.clear()
        try:
            self.on_stop()
        except Exception:
            self.log.exception("on_stop failed")
        self._threads.clear()
        self._app = None
