"""Deephaven's null sentinels and the checks every numeric input goes through, so pure code and tests do not need the
deephaven package and a missing value can never be mistaken for a price.

In listener callbacks a null arrives as the sentinel (Long.MIN_VALUE, -Double.MAX_VALUE), in pandas as NaN / NA / None.
"""
from __future__ import annotations

import math

NULL_LONG = -(2 ** 63)
NULL_DOUBLE = -1.7976931348623157e308


def is_null(x) -> bool:
    """None, NaN, +-inf or a Deephaven null sentinel."""
    if x is None:
        return True
    try:
        v = float(x)
    except (TypeError, ValueError):
        return True
    return not math.isfinite(v) or v == NULL_DOUBLE or v == float(NULL_LONG)


def good_price(x) -> bool:
    """A usable price: a finite number above zero."""
    return not is_null(x) and float(x) > 0


def good_volume(x) -> bool:
    """A usable cumulative volume: a finite number, zero or more."""
    return not is_null(x) and float(x) >= 0


def as_float(x) -> float | None:
    """float(x), or None for anything that is not a usable number."""
    return None if is_null(x) else float(x)
