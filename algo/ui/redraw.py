"""A batched, closable redraw for a console component (no Deephaven import, so it can be tested on its own)."""
from __future__ import annotations

import threading


class Redraw:
    """Redraws one component. `redraw()` does it at once (a click on a button). `redraw.soon()` is what the listeners of the shared states call: it batches a burst of changes into ONE
    redraw per `gap` seconds (a stop of forty portfolios is forty changes, and each one used to queue a redraw in every open window), and does nothing once the window is closed
    (`close()`), so a late change never reaches a component that is gone ("Key N not initialized")."""

    def __init__(self, fn, gap: float = 0.5):
        self.fn, self.gap = fn, gap
        self._lock = threading.Lock()
        self._timer = None
        self._alive = True

    def __call__(self):
        if self._alive:
            self.fn()

    def soon(self):
        with self._lock:
            if not self._alive or self._timer is not None:
                return                                           # closed, or a redraw is already on its way: it will show this change too
            self._timer = threading.Timer(self.gap, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self):
        with self._lock:
            self._timer = None
            if not self._alive:
                return
        try:
            self.fn()
        except Exception:
            pass

    def close(self):
        with self._lock:
            self._alive = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
