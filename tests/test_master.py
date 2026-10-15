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


def fractional_contracts():
    """A low-priced stock with strikes like 16.25 (StrikePrice is paise: 1625) and a large one with a fraction."""
    e = secs("2030-01-30")
    rows, tok = [], 1
    for sym, strikes in (("PENNY", (1600, 1625, 1650, 1675)), ("BIG", (10000050, 10000075))):
        rows.append(dict(Token=900 + tok, Symbol=sym, ExpiryDate=e, InstType="FUTSTK", OptionType="XX",
                         StrikePrice=0, LotSize=1, TickSize=5))
        for strike in strikes:
            for ot in ("CE", "PE"):
                rows.append(dict(Token=tok, Symbol=sym, ExpiryDate=e, InstType="OPTSTK", OptionType=ot,
                                 StrikePrice=strike, LotSize=1, TickSize=5))
                tok += 1
    return pd.DataFrame(rows)


def test_fractional_strikes_survive_exactly():
    m = classify(normalise(fractional_contracts(), NSE))
    exp = pd.Timestamp("2030-01-30")
    strikes, toks = m.strikes("PENNY", exp, "CE")
    assert list(strikes) == [16.0, 16.25, 16.5, 16.75]
    assert m.symbol_of[int(toks[1])] == "PENNY 30JAN30 16.25 CE"
    assert len(m.chain[m.chain["TckrSymb"] == "PENNY"]) == 4          # CE and PE matched on the exact strike
    pe_strikes, _ = m.strikes("PENNY", exp, "PE")
    assert list(pe_strikes) == list(strikes)


def test_large_fractional_strikes_do_not_lose_digits_in_the_symbol():
    m = classify(normalise(fractional_contracts(), NSE))
    names = {m.symbol_of[t] for t in m.opt[m.opt["TckrSymb"] == "BIG"]["Token"]}
    assert "BIG 30JAN30 100000.5 CE" in names and "BIG 30JAN30 100000.75 PE" in names
    assert len(names) == 4                                              # two strikes, CE and PE: nothing merged


def test_the_picker_chooses_between_fractional_strikes():
    from algo.strategy.config import RsiOptionsConfig
    from algo.strategy.picker import OptionPicker
    m = classify(normalise(fractional_contracts(), NSE))
    p = OptionPicker(m, RsiOptionsConfig.stocks(), lambda t: None)
    exp, today = pd.Timestamp("2030-01-30"), pd.Timestamp("2030-01-01")
    assert m.symbol_of[p.pick("PENNY", exp, 16.3, "CALL", today)[0]] == "PENNY 30JAN30 16.25 CE"
    assert m.symbol_of[p.pick("PENNY", exp, 16.4, "PUT", today)[0]] == "PENNY 30JAN30 16.5 PE"


def test_a_fractional_strike_sizes_on_the_tick():
    from algo.strategy.config import RsiOptionsConfig
    from algo.strategy.sizing import option_params
    params, why = option_params(0.85, 0.05, RsiOptionsConfig.stocks())
    assert params is not None, why
    assert all(int(params[k] * 100) % 5 == 0 for k in ("ival", "trp", "lowerrange", "upperrange"))
