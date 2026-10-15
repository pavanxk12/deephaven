"""Checks a real day's contract file (NSE or US; the exchange is read from the file's Exch column).
Skipped unless a file is named:

    set ALGO_TEST_DB=C:\\Users\\PC\\Documents\\20261015DB.db3      (Windows)
    ALGO_TEST_DB=/data/fo_market_data/20261015DB.db3 python -m pytest tests/test_real_master.py -q

It never writes to the file (opened read-only) and needs no Deephaven.
"""
import os
import re

import pandas as pd
import pytest

from algo.core.config import NSE, US
from algo.data.master import classify, normalise, read_contracts
from algo.series.straddle import StraddleConfig, build_series

DB = os.environ.get("ALGO_TEST_DB")
pytestmark = pytest.mark.skipif(not DB or not os.path.exists(DB or ""), reason="set ALGO_TEST_DB to a contract .db3")


def market_of(raw):
    return US if set(raw["Exch"].astype(str)) & {"XCBO", "XNAS", "XNYS"} else NSE


@pytest.fixture(scope="module")
def loaded():
    raw, _ = read_contracts(DB, NSE)                # same column names for both exchanges
    mk = market_of(raw)
    return raw, classify(normalise(raw, mk)), mk


def test_every_row_lands_somewhere(loaded):
    raw, m, mk = loaded
    itype = raw["InstType"].astype(str).str.upper()
    assert len(m.opt) == int(itype.str.startswith("OPT").sum())
    assert len(m.futs) == int(itype.str.startswith("FUT").sum())
    assert len(m.eqs) == int(itype.isin(["EQUITY", "EQ", "STK", "ETF", "INDEX"]).sum())
    assert len(m.symbol_of) == len(m.opt) + len(m.futs) + len(m.eqs)


def test_tokens_and_symbols_are_unique(loaded):
    raw, m, mk = loaded
    assert not raw["Token"].duplicated().any()
    assert len(set(m.symbol_of.values())) == len(m.symbol_of)


def test_strike_text_is_exactly_the_strike(loaded):
    _, m, mk = loaded
    for r in m.opt.itertuples(index=False):
        assert float(m.symbol_of[int(r.Token)].split()[2]) == r.strike


def test_every_tick_is_usable_by_the_bridge(loaded):
    _, m, mk = loaded
    ticks = list(m.tick_of.values())
    assert ticks and min(ticks) > 0
    assert not m.futs["tick"].isna().any() and not m.opt["tick"].isna().any()
    if mk is NSE:
        assert min(ticks) >= 0.01                   # the US file lists option ticks of 0.0001: sizing refuses those (see sizing.py)


def test_no_expiry_is_missing_or_in_the_past_of_the_file_date(loaded):
    _, m, mk = loaded
    day = pd.Timestamp(re.search(r"(20\d{6})", os.path.basename(DB)).group(1))
    assert m.opt["expiry"].notna().all() and (m.opt["expiry"] >= day).all()
    assert m.futs["expiry"].notna().all() and (m.futs["expiry"] >= day).all()


def test_chain_pairs_are_real_pairs(loaded):
    _, m, mk = loaded
    ce = m.opt[m.opt.OptionType == "CE"].set_index("Token")
    pe = m.opt[m.opt.OptionType == "PE"].set_index("Token")
    sample = m.chain.sample(min(500, len(m.chain)), random_state=1)
    for r in sample.itertuples(index=False):
        a, b = ce.loc[r.ce_token], pe.loc[r.pe_token]
        assert (a.TckrSymb, a.expiry, a.strike) == (b.TckrSymb, b.expiry, b.strike) == (r.TckrSymb, r.expiry, r.strike)


def test_each_straddle_expiry_uses_a_future_that_does_not_expire_before_it(loaded):
    _, m, mk = loaded
    if not len(m.futs):
        pytest.skip("this file has no futures (US): the straddle needs an equity-underlying mode")
    today = pd.Timestamp(re.search(r"(20\d{6})", os.path.basename(DB)).group(1))
    for n in (0, 1, 2):
        defs = build_series(m, StraddleConfig(expiries=n + 1), today)
        assert defs
        for (sym, exp), s in defs.items():
            fut_sym, fut_exp = m.fut_info[s.fut]
            assert fut_sym == sym
            later = m.futs[(m.futs.TckrSymb == sym) & (m.futs.expiry >= pd.Timestamp(exp))]
            if len(later):
                assert fut_exp >= pd.Timestamp(exp)
            assert len(s.strikes) == len(s.ce) == len(s.pe) and list(s.strikes) == sorted(s.strikes)


def test_nearest_nifty_future_has_a_tick(loaded):
    _, m, mk = loaded
    if mk is not NSE:
        pytest.skip("NSE only")
    day = pd.Timestamp(re.search(r"(20\d{6})", os.path.basename(DB)).group(1))
    tok = m.nearest_future("NIFTY", day)
    assert tok is not None and m.tick_of[tok] > 0


def test_every_contract_has_the_files_divisor(loaded):
    raw, m, mk = loaded
    d = raw.set_index("Token")["Divisor"].astype(float).to_dict()
    assert all(m.divisor_of[t] == d[t] for t in m.symbol_of)


def test_strikes_match_the_files_own_description(loaded):
    """FullName reads like `OPT AAPL 260.00 CE 2026-10-26`: its strike must equal what we computed."""
    raw, m, mk = loaded
    opts = raw[raw["InstType"].astype(str).str.startswith("OPT")].head(30000)
    strike = m.opt.set_index("Token")["strike"]
    for r in opts.itertuples():
        assert abs(float(r.FullName.split()[2]) - strike[r.Token]) < 1e-9
