"""State shared by the console panels: the time window and the user filter (pure Python, no UI imports)."""
from __future__ import annotations

from typing import Callable

PF_PER_USER = 10000      # PF = user id * 10000 + portfolio number (31147 = user 3, portfolio 1147)


def user_cond(pf_col: str, user: int, per_user: int = PF_PER_USER) -> str:
    """Query condition selecting the PFs of one user (no division, so integer maths cannot surprise)."""
    return f"{pf_col} >= {user * per_user} && {pf_col} < {(user + 1) * per_user}"


def time_conds(column: str, window: tuple[int | None, int | None]) -> list[str]:
    a, b = window
    return ([f"{column} >= {a}"] if a is not None else []) + ([f"{column} <= {b}"] if b is not None else [])


class Filters:
    """(from, to) epoch seconds (None = no limit) and a user id (None = everyone); panels subscribe to changes."""

    def __init__(self):
        self.window: tuple[int | None, int | None] = (None, None)
        self.user: int | None = None
        self._callbacks: list[Callable[[], None]] = []

    def on_change(self, callback: Callable[[], None]) -> Callable[[], None]:
        self._callbacks.append(callback)
        return lambda: self._callbacks.remove(callback) if callback in self._callbacks else None

    def set_window(self, start: int | None, end: int | None) -> None:
        self.window = (start, end)
        self._notify()

    def set_user(self, user: int | None) -> None:
        self.user = user
        self._notify()

    def _notify(self) -> None:
        for cb in list(self._callbacks):
            cb()
