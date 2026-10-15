"""Shared fixtures: a small synthetic contract master (no Deephaven, no files)."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from algo.core.config import NSE            # noqa: E402
from algo.data.master import classify, normalise  # noqa: E402

EPOCH = pd.Timestamp("1980-01-01")


def secs(date: str) -> int:
    return int((pd.Timestamp(date) - EPOCH).total_seconds())


def raw_contracts(weekly: bool = False) -> pd.DataFrame:
    """NSE-style rows: prices in paise, expiry in seconds since 1980.
    weekly=True adds NIFTY weekly options (Jan 9, 16, 23) and next-month options (Feb 27) with 3 strikes each."""
    e1, e2 = secs("2030-01-30"), secs("2030-02-27")
    rows = []
    # futures: token, symbol, expiry, tick(paise), lot
    for tok, sym, exp in ((1, "NIFTY", e1), (2, "NIFTY", e2), (3, "RELIANCE", e1)):
        rows.append(dict(Token=tok, Symbol=sym, ExpiryDate=exp, InstType="FUTIDX" if sym == "NIFTY" else "FUTSTK",
                         OptionType="XX", StrikePrice=0, LotSize=50, TickSize=5))
    # NIFTY options, nearest expiry, strikes 24900..25100
    tok = 100
    for strike in (24900, 25000, 25100):
        for ot in ("CE", "PE"):
            rows.append(dict(Token=tok, Symbol="NIFTY", ExpiryDate=e1, InstType="OPTIDX", OptionType=ot,
                             StrikePrice=strike * 100, LotSize=50, TickSize=5))
            tok += 1
    # RELIANCE options, nearest expiry
    for strike in (2900, 3000, 3100):
        for ot in ("CE", "PE"):
            rows.append(dict(Token=tok, Symbol="RELIANCE", ExpiryDate=e1, InstType="OPTSTK", OptionType=ot,
                             StrikePrice=strike * 100, LotSize=250, TickSize=5))
            tok += 1
    if weekly:
        tok = 500
        for exp in ("2030-01-09", "2030-01-16", "2030-01-23", "2030-02-27"):
            for strike in (24900, 25000, 25100):
                for ot in ("CE", "PE"):
                    rows.append(dict(Token=tok, Symbol="NIFTY", ExpiryDate=secs(exp), InstType="OPTIDX", OptionType=ot,
                                     StrikePrice=strike * 100, LotSize=50, TickSize=5))
                    tok += 1
    # an equity with no expiry
    rows.append(dict(Token=900, Symbol="RELIANCE", ExpiryDate=None, InstType="EQ", OptionType="XX",
                     StrikePrice=0, LotSize=1, TickSize=5))
    return pd.DataFrame(rows)


@pytest.fixture
def master():
    return classify(normalise(raw_contracts(), NSE))


@pytest.fixture
def master_weekly():
    return classify(normalise(raw_contracts(weekly=True), NSE))
