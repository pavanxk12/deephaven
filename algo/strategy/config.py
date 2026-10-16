"""Strategy configuration. Every number that used to sit in the USER INPUT block of the scripts is a field here.

`RsiOptionsConfig.stocks()` and `.nifty()` reproduce the two scripts (automation_rsi.py, niftyrsi.py) as they were.
The first portfolio number (pf_start) belongs to the `Book` plugin, e.g. Book(name="book_nifty", pf_start=1555).
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace


def _apply_params() -> dict:
    """Fixed fields of every `apply`; maxqty, ranges, ival / jval / sval / trp and side are added per signal."""
    return {"jqty1": 1, "jqty2": 1, "target": 5000, "stoploss": -5000, "maxloss": -5000, "max_unhedge": 1, "PRNT": 1}


@dataclass
class BridgeConfig:
    """How the strategy sequences its commands. The connection itself (url, user, timeout, the send switch) is `NatsConfig`."""
    unsubscribe_on_retire: bool = True     # after the squareup, unsubscribe the old portfolio from its option
    squareup_on_repoint: bool = True       # square up the old portfolio when a stock moves to another option
    add_qty: int = 5                       # added to the cumulative maxqty on every activation / flip
    user: int | None = None                # the user this strategy sends as; needed when the NATS service has several users


@dataclass
class RsiOptionsConfig:
    name: str = "rsi_stocks"

    # ---- what is tracked ----
    universe: str = "stocks"               # "stocks": symbols from stocks_file;  "single": nearest future of `underlying`
    stocks_file: str = "stocks.txt"        # relative to paths.market_data
    underlying: str = "NIFTY"

    # ---- signal ----
    rsi_col: str = "RSI_9"
    pct_col: str = "PCT_OPEN"             # percent from the open, from indicators(PCT()); only read when mid_filter is on
    rsi_high: float = 50                   # RSI above this -> buy PUT
    rsi_low: float = 50                    # RSI below this -> buy CALL
    mid_filter: bool = True                # trade only the middle 50% of each basket by percent-from-open (needs indicators(PCT()))

    # ---- option choice ----
    expiry_rule: str = "same_as_fut"       # "same_as_fut" (falls back to nearest) | "index" (expiry_index-th non-expired)
    expiry_index: int = 0
    synthetic: bool = False                # strike nearest to K + CE(K) - PE(K) instead of the future close

    # ---- sizing (percent of the OPTION premium) ----
    ival_pct: float = 0.002
    range_below_pct: float = 0.05
    range_above_pct: float = 0.05
    trp_pct: float = 0.01                  # trp = premium * (1 + trp_pct / 100)
    jval_frac: float = 0.75
    sval_mult: float = 3
    apply_params: dict = field(default_factory=_apply_params)

    # ---- behaviour ----
    book: str = "book"                     # name of the Book plugin that holds this strategy's portfolios
    bridge: str = "bridge"                 # name of the Bridge plugin that sends its commands
    act_on_start: bool = True              # act at once on tokens already in a zone when the strategy starts
    reset_on_start: bool = True            # forget live legs and caps on every start
    fail_cooldown_s: float = 30            # after a failed swap, wait before retrying the same stock
    log_rsi: bool = True                   # one log line per indicator update

    def warnings(self) -> list[str]:
        out = []
        if self.rsi_high <= self.rsi_low:
            out.append(f"rsi_high ({self.rsi_high}) <= rsi_low ({self.rsi_low}): almost every reading is a signal")
        if self.universe not in ("stocks", "single"):
            out.append(f"unknown universe {self.universe!r}")
        if self.expiry_rule not in ("same_as_fut", "index"):
            out.append(f"unknown expiry_rule {self.expiry_rule!r}")
        return out

    def with_(self, **kw) -> "RsiOptionsConfig":
        return replace(self, **kw)

    # ---- the two live setups ----
    @classmethod
    def stocks(cls, **kw) -> "RsiOptionsConfig":
        return cls(**kw)

    @classmethod
    def nifty(cls, **kw) -> "RsiOptionsConfig":
        base = dict(name="rsi_nifty", universe="single", underlying="NIFTY", mid_filter=False,
                    rsi_high=60, rsi_low=40, expiry_rule="index", expiry_index=0, synthetic=True,
                    ival_pct=0.01, range_below_pct=0.2, range_above_pct=0.2)
        base.update(kw)
        return cls(**base)
