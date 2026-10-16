"""State shared by the console panels: the time window and the strategy filter (pure Python, no UI imports)."""
from __future__ import annotations

import re
from typing import Callable

PF_PER_USER = 10000      # PF in the order / trade tables = user id * 10000 + portfolio number (31147 = user 3, portfolio 1147)

_SAFE = re.compile(r"^[A-Za-z0-9_.\-]{1,60}$")


def safe_strategy(name) -> bool:
    """A strategy name that may go into a table query and a variable name (letters, digits, _ . -)."""
    return isinstance(name, str) and bool(_SAFE.match(name))


def strategy_cond(name: str | None) -> list[str]:
    """Query conditions selecting one strategy's rows (the tables carry a `Strategy` column); none for "all strategies"."""
    if name is None:
        return []
    if not safe_strategy(name):
        raise ValueError(f"strategy name {name!r} may only hold letters, digits, _ . -")
    return [f"Strategy == `{name}`"]


def time_conds(column: str, window: tuple[int | None, int | None]) -> list[str]:
    a, b = window
    return ([f"{column} >= {a}"] if a is not None else []) + ([f"{column} <= {b}"] if b is not None else [])


class Strategies:
    """The names of the strategies that have portfolios (from the registry); panels subscribe to changes."""

    def __init__(self):
        self._names: set[str] = set()
        self._callbacks: list[Callable[[], None]] = []

    def names(self) -> list[str]:
        return sorted(self._names)

    def add(self, name: str) -> bool:
        """True if it was new (and the listeners were told)."""
        if not name or name in self._names:
            return False
        self._names.add(name)
        for cb in list(self._callbacks):
            try:
                cb()
            except Exception:
                pass
        return True

    def on_change(self, callback: Callable[[], None]) -> Callable[[], None]:
        self._callbacks.append(callback)
        return lambda: self._callbacks.remove(callback) if callback in self._callbacks else None


class Filters:
    """(from, to) epoch seconds (None = no limit) and a strategy name (None = all strategies); panels subscribe to changes."""

    def __init__(self):
        self.window: tuple[int | None, int | None] = (None, None)
        self.strategy: str | None = None
        self._callbacks: list[Callable[[], None]] = []

    def on_change(self, callback: Callable[[], None]) -> Callable[[], None]:
        self._callbacks.append(callback)
        return lambda: self._callbacks.remove(callback) if callback in self._callbacks else None

    def set_window(self, start: int | None, end: int | None) -> None:
        self.window = (start, end)
        self._notify()

    def set_strategy(self, strategy: str | None) -> None:
        self.strategy = strategy
        self._notify()

    def _notify(self) -> None:
        for cb in list(self._callbacks):
            cb()
