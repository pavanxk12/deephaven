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


def plain(x) -> str:
    """A limit as the text of its box: no thousands separators, no trailing zeros; nothing for no limit."""
    return "" if x is None or x == 0 else (f"{x:.2f}".rstrip("0").rstrip("."))


def number_of(text):
    """What was typed in a limit box -> a number, or None for an empty box. Commas, spaces and ₹ are ignored. Raises ValueError for anything else."""
    t = str(text if text is not None else "").replace(",", "").replace("₹", "").replace(" ", "").strip()
    return None if t == "" else float(t)


def row_value(row, name, default=None):
    """One value of a clicked row of a ui.table. The row maps column -> the value, or column -> {"value": ..., "text": ...}; both are read."""
    if not isinstance(row, dict) or name not in row:
        return default
    x = row[name]
    for _ in range(4):                                             # a cell may be wrapped more than once: {"value": {"value": 5, "text": "5"}}
        if not isinstance(x, dict):
            break
        x = x.get("value", x.get("text", default))
    if isinstance(x, dict):
        return default
    return default if x is None or (isinstance(x, float) and x != x) else x


def as_int(x, default: int = 0) -> int:
    """A number out of whatever a table cell handed over (an int, a float, a string such as '+1,200' or a cell dict); `default` when it is none of those. Never raises."""
    if isinstance(x, dict):
        x = x.get("value", x.get("text"))
    try:
        return int(float(str(x).replace(",", "").replace("+", "").strip()))
    except (TypeError, ValueError):
        return default


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
