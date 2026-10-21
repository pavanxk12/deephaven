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

FIRST_PF = 1001        # the first portfolio number anybody gets: the reference point of every numbering

CONTRACT_COLUMNS: Mapping[str, str] = {
    "token": "Token", "symbol": "Symbol", "expiry": "ExpiryDate", "inst": "InstType",
    "opt": "OptionType", "strike": "StrikePrice", "lot": "LotSize", "tick": "TickSize", "divisor": "Divisor",
    "exch": "Exch",
}


@dataclass(frozen=True)
class Market:
    """Everything that differs between exchanges."""
    code: str
    tz: str                        # exchange timezone (session windows, "today", contract file date)
    ltt_tz: str                    # timezone the feed's `ltt` string is written in
    ltt_fmt: str                   # strptime format of `ltt` up to the minute (seconds are parsed separately)
    price_div: float               # raw feed / order price -> currency units (paise -> rupees = 100); used where the contract
                                   # file has no Divisor column. Strike, tick and the default candle multiplier take the
                                   # file's own Divisor per contract (NSE 100; US options 10000, US equities 100)
    expiry_epoch: str              # epoch of numeric contract expiries
    strike_div: float
    tick_div: float
    session: tuple[dtime, dtime]          # data / series window (exchange tz)
    trade_window: tuple[dtime, dtime]     # strategies ignore signals outside this (exchange tz)
    feed: str = "fo_market_data"          # live feed table in the session
    start_master: dtime = dtime(9, 0)             # day_auto(): when the contract master is built every trading day (exchange tz)
    start_candles: dtime = dtime(9, 14, 30)       # day_auto(): when the candles start forming; the registry, NATS and the console start with them, right after
    close_at: dtime = dtime(15, 30)               # day_auto(): when the candles and straddle tables are saved and the builders stopped
    straddle_at_close: "int | None" = None        # day_auto(): at the close, start the straddle series (straddle_start(n)) once, built from the whole day's candles, before the save; None = not started
    currency: str = "\u20b9"                      # the symbol the console shows on every money figure (rupee)
    expense_per_crore: Mapping[str, tuple] = field(default_factory=dict)   # the console's EXPENSE: {"FUT": (buy, sell), "OPT": (...), "EQ": (...)} in rupees per crore (1.0E7) of the value bought / sold; none = no expense shown
    contract_columns: Mapping[str, str] = field(default_factory=lambda: dict(CONTRACT_COLUMNS))

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)

    @property
    def ltt_zone(self) -> ZoneInfo:
        return ZoneInfo(self.ltt_tz)


NSE = Market(
    code="NSE", tz="Asia/Kolkata", ltt_tz="Asia/Kolkata", ltt_fmt="%d-%m-%Y %H:%M",
    price_div=100.0, expiry_epoch="1980-01-01", strike_div=100.0, tick_div=100.0,
    session=(dtime(9, 15), dtime(15, 30)), trade_window=(dtime(9, 20), dtime(15, 0)),
    straddle_at_close=0,                                                                               # straddle_start(0): the nearest option expiry, started at the close, then saved
    expense_per_crore={"FUT": (600.0, 5400.0), "OPT": (5625.0, 20328.0), "EQ": (200.0, 200.0)},       # (rupees per crore bought, per crore sold), given by the owner
)

# US: expiry epoch 1970, strike / tick / price scale 10000 (from the contract file's Divisor), checked against
# 20261014DB.db3. The feed's ltt is text like "2026-10-15 11:01:04" in UTC (checked against table-2026-10-15-171828.csv:
# the Kafka time is always at or after it, and the first trades fall in New York pre-market). Still unchecked: that the feed's
# option prices follow the file's Divisor (the equity rows do: 34,669 with divisor 100 is 346.69). The session is the
# New York cash session.
US = Market(
    code="US", tz="America/New_York", ltt_tz="UTC", ltt_fmt="%Y-%m-%d %H:%M",
    price_div=10000.0, expiry_epoch="1970-01-01", strike_div=10000.0, tick_div=10000.0,
    session=(dtime(9, 30), dtime(16, 0)), trade_window=(dtime(9, 35), dtime(15, 45)),
    currency="$",
    start_master=dtime(9, 15), start_candles=dtime(9, 29, 30), close_at=dtime(16, 0),      # the NSE day's shape: master 15 minutes before the cash open, candles 30 seconds before it, close at the end of the cash session. Change in day_auto(...)
)

MARKETS: Mapping[str, Market] = {"NSE": NSE, "US": US}


def default_market() -> str:
    """The market a call uses when it names none: the environment variable ALGO_MARKET ("NSE" or "US"), else "NSE". The US server's image sets ALGO_MARKET=US."""
    return os.environ.get("ALGO_MARKET", "").strip() or "NSE"


def default_nats_ip() -> str:
    """The NATS server's address when a call names none: ALGO_NATS_IP, else the India server's. The US server's image sets ALGO_NATS_IP to its own address."""
    return os.environ.get("ALGO_NATS_IP", "").strip() or "192.168.1.130"


def market_for(market: "str | Market | None" = None) -> Market:
    if isinstance(market, Market):
        return market
    market = market or default_market()
    try:
        return MARKETS[market.upper()]
    except KeyError:
        raise ValueError(f"unknown market {market!r}; choose from {sorted(MARKETS)}") from None


@dataclass(frozen=True)
class Paths:
    """Container paths. `market_data` is a read-only host mount, `work` is the writable named volume."""
    market_data: str = "/data/fo_market_data"     # contract .db3, stocks.txt, logo, history parquet (read only)
    work: str = "/data/deephaven"                 # writable volume
    eod_dir: str | None = None                    # where end-of-day Parquet files go; None = the market_data folder itself (it must be mounted writable).
                                                  # Point it at a folder mounted from the host to find the files outside Docker

    @property
    def state(self) -> str:        # strategy state that must survive a container recreate
        return os.path.join(self.work, "state")

    @property
    def saved(self) -> str:        # end-of-day parquet files
        return self.eod_dir or self.market_data          # the market-data folder itself: the saved days sit next to the contract files and the history that absolute() reads

    @property
    def cache(self) -> str:        # rebuildable caches (master pickle)
        return os.path.join(self.work, "cache")

    def with_(self, **kw) -> "Paths":
        return replace(self, **kw)
