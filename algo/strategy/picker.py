"""Which option to buy for a signal (pure; prices come in through a callback)."""
from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd

from algo.strategy.config import RsiOptionsConfig

LEG_TYPE = {"CALL": "CE", "PUT": "PE"}


class OptionPicker:
    """Strike nearest to a reference price: the future close, or the synthetic future K + CE(K) - PE(K).

    `price_fn(token)` returns the option's last price in raw units (paise) or None.
    """

    def __init__(self, master, cfg: RsiOptionsConfig, price_div: float, price_fn: Callable[[int], float | None]):
        self.master, self.cfg, self.div, self.price = master, cfg, price_div, price_fn

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
        if ce is None or pe is None:
            return None
        return k + (ce - pe) / self.div

    def pick(self, symbol: str, fut_expiry, fut_close: float, leg: str, today: pd.Timestamp) -> tuple[int | None, str]:
        """(option token or None, human-readable explanation)."""
        ot = LEG_TYPE[leg]
        exp = self.expiry(symbol, ot, fut_expiry, today)
        if exp is None:
            return None, f"no {ot} expiry for {symbol}"
        fut = fut_close / self.div
        ref, how = fut, "future close"
        if self.cfg.synthetic:
            syn = self.synthetic(symbol, exp, fut)
            if syn is not None:
                ref, how = syn, "synthetic"
            else:
                how = "future close (no CE/PE price at the nearest strike)"
        strikes, toks = self.master.strikes(symbol, exp, ot)
        i = int(np.abs(strikes - ref).argmin())
        return int(toks[i]), f"future {fut:.2f} {how} {ref:.2f} -> {symbol} {exp:%d%b%y} {strikes[i]:g} {ot}"
