"""Static configuration: which market we run and where files live.

Everything that used to be copied into six scripts (timezone, price scale, session windows,
file locations) is defined once here.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import time as dtime
from typing import Mapping
from zoneinfo import ZoneInfo

CONTRACT_COLUMNS: Mapping[str, str] = {
    "token": "Token", "symbol": "Symbol", "expiry": "ExpiryDate", "inst": "InstType",
    "opt": "OptionType", "strike": "StrikePrice", "lot": "LotSize", "tick": "TickSize",
}


@dataclass(frozen=True)
class Market:
    """Everything that differs between exchanges."""
    code: str
    tz: str                        # exchange timezone (session windows, "today", contract file date)
    ltt_tz: str                    # timezone the feed's `ltt` string is written in
    ltt_fmt: str                   # strptime format of `ltt` up to the minute (seconds are parsed separately)
    price_div: float               # raw price -> currency units (paise -> rupees = 100)
    int_prices: bool               # True: candle OHLC stored as int64 raw units; False: double
    expiry_epoch: str              # epoch of numeric contract expiries
    strike_div: float
    tick_div: float
    session: tuple[dtime, dtime]          # data / series window (exchange tz)
    trade_window: tuple[dtime, dtime]     # strategies ignore signals outside this (exchange tz)
    feed: str = "fo_market_data"          # live feed table in the session
    contract_columns: Mapping[str, str] = field(default_factory=lambda: dict(CONTRACT_COLUMNS))

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)

    @property
    def ltt_zone(self) -> ZoneInfo:
        return ZoneInfo(self.ltt_tz)


NSE = Market(
    code="NSE", tz="Asia/Kolkata", ltt_tz="Asia/Kolkata", ltt_fmt="%d-%m-%Y %H:%M",
    price_div=100.0, int_prices=True, expiry_epoch="1980-01-01", strike_div=100.0, tick_div=100.0,
    session=(dtime(9, 15), dtime(15, 30)), trade_window=(dtime(9, 20), dtime(15, 0)),
)

# US values below are carried over from the old scripts or are placeholders to verify against a real feed row:
# the feed stamps IST, prices are assumed to be dollars (double), the session is the New York cash session.
US = Market(
    code="US", tz="America/New_York", ltt_tz="Asia/Kolkata", ltt_fmt="%Y-%m-%dT%H:%M",
    price_div=1.0, int_prices=False, expiry_epoch="1970-01-01", strike_div=1.0, tick_div=1.0,
    session=(dtime(9, 30), dtime(16, 0)), trade_window=(dtime(9, 35), dtime(15, 45)),
)

MARKETS: Mapping[str, Market] = {"NSE": NSE, "US": US}


def market_for(market: "str | Market") -> Market:
    if isinstance(market, Market):
        return market
    try:
        return MARKETS[market.upper()]
    except KeyError:
        raise ValueError(f"unknown market {market!r}; choose from {sorted(MARKETS)}") from None


@dataclass(frozen=True)
class Paths:
    """Container paths. `market_data` is a read-only host mount, `work` is the writable named volume."""
    market_data: str = "/data/fo_market_data"     # contract .db3, stocks.txt, logo, history parquet (read only)
    work: str = "/data/deephaven"                 # writable volume

    @property
    def state(self) -> str:        # strategy state that must survive a container recreate
        return os.path.join(self.work, "state")

    @property
    def saved(self) -> str:        # end-of-day parquet files (docker cp'd to the host)
        return os.path.join(self.work, "fo_market_data")

    @property
    def cache(self) -> str:        # rebuildable caches (master pickle)
        return os.path.join(self.work, "cache")

    def with_(self, **kw) -> "Paths":
        return replace(self, **kw)
