"""Bar sizes written as text: "30s", "1min", "5min", "15min" (case and spacing do not matter)."""
from __future__ import annotations

import re

_UNIT = {"s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
         "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
         "h": 3600, "hr": 3600, "hour": 3600, "hours": 3600}
_PATTERN = re.compile(r"^\s*(\d+)\s*([a-z]+)\s*$", re.IGNORECASE)
MAX_SECONDS = 1800


def parse_timeframe(value) -> int:
    """"30s" -> 30, "1min" -> 60, "5min" -> 300. A bar must divide a minute or be whole minutes, at most 30 minutes,
    so bars line up with the exchange clock."""
    if not isinstance(value, str):
        raise ValueError(f"write the bar size with a unit, e.g. \"30s\", \"1min\", \"5min\", \"15min\" (got {value!r})")
    m = _PATTERN.match(value)
    if not m or m.group(2).lower() not in _UNIT:
        raise ValueError(f"cannot read bar size {value!r}; use e.g. \"30s\", \"1min\", \"5min\", \"15min\"")
    seconds = int(m.group(1)) * _UNIT[m.group(2).lower()]
    if seconds < 1 or seconds > MAX_SECONDS or not (60 % seconds == 0 or seconds % 60 == 0):
        raise ValueError(f"bar size {value!r} = {seconds}s must divide a minute or be whole minutes, at most "
                         f"{MAX_SECONDS // 60} minutes")
    return seconds


MAX_LOOKBACK_SECONDS = 6 * 3600


def parse_duration(value) -> int:
    """A time span written with a unit: "90s", "5min", "2h" -> seconds. Unlike a bar size it need not line up with anything."""
    if not isinstance(value, str):
        raise ValueError(f"write the time span with a unit, e.g. \"5min\", \"90s\", \"2h\" (got {value!r})")
    m = _PATTERN.match(value)
    if not m or m.group(2).lower() not in _UNIT:
        raise ValueError(f"cannot read time span {value!r}; use e.g. \"5min\", \"90s\", \"2h\"")
    seconds = int(m.group(1)) * _UNIT[m.group(2).lower()]
    if not 1 <= seconds <= MAX_LOOKBACK_SECONDS:
        raise ValueError(f"time span {value!r} must be between 1 second and {MAX_LOOKBACK_SECONDS // 3600} hours")
    return seconds


def format_timeframe(seconds: int) -> str:
    return f"{seconds}s" if seconds % 60 else f"{seconds // 60}min"
