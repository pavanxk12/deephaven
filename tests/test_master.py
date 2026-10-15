import pandas as pd

from algo.core.config import NSE
from algo.data.master import MasterData, classify, normalise, parse_expiry
from tests.conftest import raw_contracts, secs


def test_classify_splits_instruments(master):
    assert set(master.futs["fut_token"]) == {1, 2, 3}
    assert len(master.opt) == 12 and len(master.chain) == 6
    assert list(master.eqs["eq_token"]) == [900]                 # equity without an expiry is kept


def test_symbol_map_format(master):
    assert master.symbol_of[1] == "NIFTY 30JAN30 FUT"
    assert master.symbol_of[100] == "NIFTY 30JAN30 24900 CE"
    assert master.symbol_of[101] == "NIFTY 30JAN30 24900 PE"
    assert master.symbol_of[900] == "RELIANCE"


def test_tick_in_rupees_and_equity_uses_future_tick(master):
    assert master.tick_of[1] == 0.05 and master.tick_of[100] == 0.05
    assert master.tick_of[900] == 0.05


def test_strikes_sorted_and_aligned_with_tokens(master):
    exp = pd.Timestamp("2030-01-30")
    strikes, toks = master.strikes("NIFTY", exp, "CE")
    assert list(strikes) == [24900.0, 25000.0, 25100.0]
    assert [master.symbol_of[int(t)].endswith("CE") for t in toks] == [True] * 3
    assert master.expiries("NIFTY", "CE") == [exp]


def test_nearest_future_skips_expired(master):
    assert master.nearest_future("NIFTY", pd.Timestamp("2030-01-01")) == 1
    assert master.nearest_future("NIFTY", pd.Timestamp("2030-02-01")) == 2
    assert master.nearest_future("NIFTY", pd.Timestamp("2031-01-01")) is None


def test_token_of_is_upper_case_symbol(master):
    assert master.token_of["NIFTY 30JAN30 FUT"] == 1


def test_roundtrip_dict(master):
    again = MasterData.from_dict(master.as_dict())
    assert again.symbol_of == master.symbol_of


def test_parse_expiry_accepts_three_formats():
    epoch = "1980-01-01"
    e = pd.Timestamp("2030-01-30")
    assert parse_expiry(pd.Series([secs("2030-01-30")]), epoch)[0] == e                 # epoch seconds
    assert parse_expiry(pd.Series([20300130]), epoch)[0] == e                           # yyyymmdd
    assert parse_expiry(pd.Series(["2030-01-30"]), epoch)[0] == e                       # iso string
    ms = int((e - pd.Timestamp("1970-01-01")).total_seconds() * 1000)
    assert parse_expiry(pd.Series([ms]), "1970-01-01")[0] == e                         # epoch milliseconds


def test_normalise_scales_paise(master):
    df = normalise(raw_contracts(), NSE)
    row = df[df["Token"] == 100].iloc[0]
    assert row["strike"] == 24900.0 and row["tick"] == 0.05
