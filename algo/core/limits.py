"""Risk limits per strategy: a TURNOVER limit (in ₹, the value traded: buys + sells) and an M2M limit (in ₹). Both are plain rupee values.

    turnover limit    breached when the strategy's turnover reaches it            -> sending is switched OFF for the strategy
    M2M limit         breached when the strategy's live gross M2M is AT OR BELOW the limit, the number as typed: -5000 breaches at -5000 and lower,
                      50000 breaches at 50000 and lower (so a positive limit is breached at once unless M2M is above it)  -> its portfolios are paused, squared up and unsubscribed, and sending is switched OFF

A breach is LATCHED: the strategy stays blocked (it cannot be switched ON) until that limit is changed to a value the strategy is no longer past.
The sign of an M2M limit counts. A limit of 0 or None (an empty box) means no limit. Limits can be changed at any time; they are kept in a small JSON file (or, with ALGO_STATE_STORE=postgres, the table algo_limits, with the file written as well as a backup copy) so a restart keeps them and the latches.

This module holds the numbers only (no Deephaven, no NATS): algo.strategy.guard feeds it the live values and acts on a breach.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Callable

KINDS = ("turnover", "m2m")


def _num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v and abs(v) < 1.0e300 else None     # NaN and the null of a Deephaven double (about -1.8e308) -> None: a missing value must never breach a limit


class Limits:
    def __init__(self, path: str | None = None, db=None):
        self.path = path
        self.db = db                                           # a StateDB: the limits live in Postgres (the file is only imported once)
        self._d: dict[str, dict] = {}
        self._seen: dict[str, tuple] = {}
        self._cbs: list[Callable[[str], None]] = []
        self._lock = threading.Lock()
        self._load()

    # ---- reading -------------------------------------------------------------------------------
    def get(self, strategy: str) -> dict:
        """{'turnover': limit or None, 'm2m': limit or None, 'turnover_hit': bool, 'm2m_hit': bool}"""
        with self._lock:
            return dict(self._rec(strategy))

    def names(self) -> list[str]:
        """The strategies that have a limit (or had one) on record."""
        with self._lock:
            return sorted(self._d)

    def seen(self, strategy: str) -> tuple:
        """(turnover in ₹, M2M in ₹) as last reported, None where nothing was reported."""
        return self._seen.get(strategy, (None, None))

    def blocked(self, strategy: str) -> str:
        """Why this strategy may not send ('' = it may)."""
        r = self.get(strategy)
        turnover, m2m = self.seen(strategy)
        why = []
        if r["turnover_hit"]:
            why.append(f"turnover limit {r['turnover']:,.0f} reached ({'-' if turnover is None else format(turnover, ',.0f')})")
        if r["m2m_hit"]:
            why.append(f"M2M limit {r['m2m']:+,.0f} reached ({'-' if m2m is None else format(m2m, '+,.0f')})")
        return "; ".join(why) + ("; change the limit to send again" if why else "")

    # ---- changing ------------------------------------------------------------------------------
    def update(self, strategy: str, turnover, m2m) -> list[str]:
        """The live values. Returns the limits that were breached by this update for the first time (to act on)."""
        t, m = _num(turnover), _num(m2m)
        with self._lock:
            old_t, old_m = self._seen.get(strategy, (None, None))
            self._seen[strategy] = (t if t is not None else old_t, m if m is not None else old_m)
            newly = self._check(strategy)
        self._after(strategy, newly, save=bool(newly))
        return newly

    def set_limit(self, strategy: str, kind: str, value) -> list[str]:
        """Change one limit (0 or None = no limit). Past the new limit already: it is breached now (returned). Not past it any more: the latch is lifted."""
        if kind not in KINDS:
            raise ValueError(f"limit must be one of {KINDS}, got {kind!r}")
        v = _num(value)
        if value is not None and (v is None or (v < 0 and kind == "turnover")):
            raise ValueError(f"the {kind} limit must be a number from 0 up (0 = no limit), got {value!r}" if kind == "turnover"
                             else f"the m2m limit must be a number (empty or 0 = no limit), got {value!r}")
        with self._lock:
            r = self._rec(strategy)
            r[kind] = v if v else None
            if not self._past(strategy, kind):
                r[kind + "_hit"] = False
            newly = self._check(strategy)
        self._after(strategy, newly, save=True)
        return newly

    def forget(self, strategy: str) -> None:
        with self._lock:
            self._d.pop(strategy, None)
            self._seen.pop(strategy, None)
        self._after(strategy, [], save=True)

    # ---- listening -----------------------------------------------------------------------------
    def on_change(self, fn: Callable[[str], None]) -> Callable[[], None]:
        """fn(strategy) after a limit or a breach changes (not on every new value). Returns a function that removes it."""
        with self._lock:
            if fn not in self._cbs:
                self._cbs.append(fn)

        def undo():
            with self._lock:
                if fn in self._cbs:
                    self._cbs.remove(fn)
        return undo

    def notify(self, strategy: str) -> None:
        """Tell the listeners again (the guard calls it AFTER it has switched the strategy off, so a screen that redraws reads the new switch)."""
        self._after(strategy, [], save=False, tell=True)

    # ---- inside --------------------------------------------------------------------------------
    def _rec(self, strategy: str) -> dict:
        return self._d.setdefault(strategy, {"turnover": None, "m2m": None, "turnover_hit": False, "m2m_hit": False})

    def _past(self, strategy: str, kind: str) -> bool:
        limit, (turnover, m2m) = self._rec(strategy)[kind], self._seen.get(strategy, (None, None))
        if not limit:
            return False
        return (turnover is not None and turnover >= limit) if kind == "turnover" else (m2m is not None and m2m <= limit)

    def _check(self, strategy: str) -> list[str]:
        r, newly = self._rec(strategy), []
        for kind in KINDS:
            if not r[kind + "_hit"] and self._past(strategy, kind):
                r[kind + "_hit"] = True
                newly.append(kind)
        return newly

    def _after(self, strategy: str, newly: list, save: bool, tell: bool = False) -> None:
        if save:
            self._save()
        if save or tell:
            for fn in list(self._cbs):
                try:
                    fn(strategy)
                except Exception:
                    import logging
                    logging.getLogger("algo.limits").exception("a limits listener failed")

    def _load(self) -> None:
        if self.db is None:
            return self._load_file()
        rows = self.db.load_limits()
        for name, turnover, m2m, t_hit, m_hit in rows:
            self._d[name] = {"turnover": _num(turnover), "m2m": _num(m2m), "turnover_hit": bool(t_hit), "m2m_hit": bool(m_hit)}
        if not rows:
            self._load_file()                                   # an empty table and an old file: import the file, once
            if self._d:
                self.db.save_limits(self._d)
                import logging
                logging.getLogger("algo.limits").info("imported the limits of %d strategies from %s into the table algo_limits", len(self._d), self.path)
        if self._d:
            self._save_file()                                   # the backup file now says exactly what the table says

    def _load_file(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            for name, r in data.items():
                self._d[name] = {"turnover": _num(r.get("turnover")), "m2m": _num(r.get("m2m")),
                                 "turnover_hit": bool(r.get("turnover_hit")), "m2m_hit": bool(r.get("m2m_hit"))}
        except (OSError, ValueError, AttributeError):
            import logging
            logging.getLogger("algo.limits").exception("%s could not be read: starting with no limits", self.path)

    def _save(self) -> None:
        if self.db is not None:
            with self._lock:
                snapshot = {k: dict(v) for k, v in self._d.items()}
            try:
                self.db.save_limits(snapshot)                   # Postgres first; then the backup copy in the file below
            except Exception:
                import logging
                logging.getLogger("algo.limits").exception("the limits could not be written to Postgres")
        self._save_file()

    def _save_file(self) -> None:
        if not self.path:
            return
        with self._lock:
            text = json.dumps(self._d, indent=1, sort_keys=True)
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, self.path)
        except OSError:
            import logging
            logging.getLogger("algo.limits").exception("%s could not be written", self.path)


def limits_of(shared: dict, path: str | None = None) -> Limits:
    """The one Limits of an app (in `app.shared`, so it survives a restart of the console)."""
    hub = shared.get("limits")
    if hub is None:
        from algo.core.statedb import state_db
        hub = shared["limits"] = Limits(path, db=state_db(shared))
    return hub
