"""Which option to buy for a signal (pure; prices come in through a callback)."""
from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd

from algo.core.nulls import good_price, is_null
from algo.strategy.config import RsiOptionsConfig

LEG_TYPE = {"CALL": "CE", "PUT": "PE"}


class OptionPicker:
    """Strike nearest to a reference price: the future close, or the synthetic future K + CE(K) - PE(K).

    All prices are in currency units. `price_fn(token)` returns an option's last price or None.
    """

    def __init__(self, master, cfg: RsiOptionsConfig, price_fn: Callable[[int], float | None]):
        self.master, self.cfg, self.price = master, cfg, price_fn

    def expiry(self, symbol: str, option_type: str, fut_expiry, today: pd.Timestamp):
        exps = self.master.expiries(symbol, option_type)
        if self.cfg.expiry_rule == "same_as_fut":
            if fut_expiry in exps:
                return fut_expiry
            later = [e for e in exps if e >= today]
            return later[0] if later else None
        later = [e for e in exps if e >= today]
        return later[self.cfg.expiry_index] if len(later) > self.cfg.expiry_index else None

    def synthetic(self, symbol: str, expiry, spot: float) -> float | None:
        """K + CE(K) - PE(K) in currency units, K = strike nearest to `spot`. None if CE or PE has no price."""
        ce_strikes, ce_toks = self.master.strikes(symbol, expiry, "CE")
        pe_strikes, pe_toks = self.master.strikes(symbol, expiry, "PE")
        i = int(np.abs(ce_strikes - spot).argmin())
        k = ce_strikes[i]
        j = np.flatnonzero(pe_strikes == k)
        if len(j) == 0:
            return None
        ce, pe = self.price(int(ce_toks[i])), self.price(int(pe_toks[j[0]]))
        if not good_price(ce) or not good_price(pe):
            return None
        return float(k + (ce - pe))

    def pick(self, symbol: str, fut_expiry, fut_close: float, leg: str, today: pd.Timestamp) -> tuple[int | None, str]:
        """(option token or None, human-readable explanation)."""
        if not good_price(fut_close):                      # NaN would silently pick strike 0
            return None, "no usable future price"
        ot = LEG_TYPE[leg]
        exp = self.expiry(symbol, ot, fut_expiry, today)
        if exp is None:
            return None, f"no {ot} expiry for {symbol}"
        fut = float(fut_close)
        ref, how = fut, "future close"
        if self.cfg.synthetic:
            syn = self.synthetic(symbol, exp, fut)
            if syn is not None and not is_null(syn) and syn > 0:
                ref, how = syn, "synthetic"
            else:
                how = "future close (no CE/PE price at the nearest strike)"
        strikes, toks = self.master.strikes(symbol, exp, ot)
        i = int(np.abs(strikes - ref).argmin())
        return int(toks[i]), f"future {fut:.2f} {how} {ref:.2f} -> {symbol} {exp:%d%b%y} {strikes[i]:g} {ot}"
