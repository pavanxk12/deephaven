"""Order parameter sizing (pure functions).

The bridge converts rupees to paise by truncating (x100, int). Prices are therefore nudged up by 1e-6 so that
a value like 2.3 (stored as 2.2999999...) still lands on 230.
"""
from __future__ import annotations

import math

from algo.strategy.config import RsiOptionsConfig

NUDGE = 1e-6


def floor_tick(x: float, tick: float) -> float:
    n = math.floor(x / tick + 1e-9)
    return round(n * tick, 2) + NUDGE


def ceil_tick(x: float, tick: float) -> float:
    n = math.ceil(x / tick - 1e-9)
    return round(n * tick, 2) + NUDGE


def on_tick(v: float, tick: float) -> bool:
    """True if the bridge, which truncates v*100 to an int, ends up on a whole tick."""
    return int(v * 100) % int(round(tick * 100)) == 0


def option_params(premium: float, tick: float, cfg: RsiOptionsConfig) -> tuple[dict | None, str]:
    """Apply-payload fields for an option with this premium (currency units) and tick.

    Returns (params, "") or (None, reason) if any value would not sit on the tick.
    """
    ival = max(floor_tick(premium * cfg.ival_pct, tick), tick)
    jval = max(floor_tick(ival * cfg.jval_frac, tick), tick)
    sval = max(floor_tick(ival * cfg.sval_mult, tick), tick)
    trp = ceil_tick(premium * (1 + cfg.trp_pct / 100), tick)
    lower = max(floor_tick(premium * (1 - cfg.range_below_pct), tick), tick)
    upper = floor_tick(premium * (1 + cfg.range_above_pct), tick)
    for name, v in (("ival", ival), ("jval", jval), ("sval", sval), ("trp", trp),
                    ("lowerrange", lower), ("upperrange", upper)):
        if not on_tick(v, tick):
            return None, f"{name} {v} is not a multiple of tick {tick}"
    params = dict(cfg.apply_params)
    params.update({"ival": ival, "jval": jval, "sval": sval, "trp": trp,
                   "lowerrange": lower, "upperrange": upper, "side": "BUY"})      # always BUY the option
    return params, ""
