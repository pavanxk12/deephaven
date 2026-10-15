import pandas as pd

from algo.strategy.config import RsiOptionsConfig
from algo.strategy.picker import OptionPicker

TODAY = pd.Timestamp("2030-01-01")
E1, E2 = pd.Timestamp("2030-01-30"), pd.Timestamp("2030-02-27")


def picker(master, cfg, prices=None):
    prices = prices or {}
    return OptionPicker(master, cfg, 100.0, prices.get)


def symbol(master, tok):
    return master.symbol_of[tok]


def test_nearest_strike_to_future_close(master):
    p = picker(master, RsiOptionsConfig.stocks())
    tok, why = p.pick("NIFTY", E1, 2_501_000.0, "CALL", TODAY)          # 25010.00 rupees
    assert symbol(master, tok) == "NIFTY 30JAN30 25000 CE" and "25000" in why
    tok, _ = p.pick("NIFTY", E1, 2_507_000.0, "PUT", TODAY)             # 25070 -> 25100
    assert symbol(master, tok) == "NIFTY 30JAN30 25100 PE"


def test_same_as_fut_falls_back_to_nearest_expiry(master):
    p = picker(master, RsiOptionsConfig.stocks())
    tok, _ = p.pick("NIFTY", E2, 2_500_000.0, "CALL", TODAY)            # no options on E2 -> nearest (E1)
    assert symbol(master, tok).startswith("NIFTY 30JAN30")


def test_no_expiry_left_returns_none(master):
    p = picker(master, RsiOptionsConfig.stocks())
    tok, why = p.pick("NIFTY", E2, 2_500_000.0, "CALL", pd.Timestamp("2031-01-01"))      # future expiry has no options
    assert tok is None and "no CE expiry" in why


def test_index_rule_uses_nth_expiry(master):
    p = picker(master, RsiOptionsConfig.nifty(expiry_index=1))
    tok, why = p.pick("NIFTY", E1, 2_500_000.0, "CALL", TODAY)
    assert tok is None                                                  # only one expiry exists


def test_synthetic_future_moves_the_strike(master):
    # K = 25000: CE 300, PE 100 -> synthetic = 25000 + 200 = 25200 -> nearest listed strike is 25100
    ce, pe = 100 + 2, 100 + 3                                           # tokens of the 25000 CE / PE
    assert symbol(master, ce) == "NIFTY 30JAN30 25000 CE" and symbol(master, pe) == "NIFTY 30JAN30 25000 PE"
    p = picker(master, RsiOptionsConfig.nifty(), {ce: 30000.0, pe: 10000.0})
    tok, why = p.pick("NIFTY", E1, 2_500_000.0, "CALL", TODAY)
    assert symbol(master, tok) == "NIFTY 30JAN30 25100 CE" and "synthetic" in why


def test_synthetic_falls_back_to_future_close_without_prices(master):
    p = picker(master, RsiOptionsConfig.nifty())
    tok, why = p.pick("NIFTY", E1, 2_500_000.0, "PUT", TODAY)
    assert symbol(master, tok) == "NIFTY 30JAN30 25000 PE" and "no CE/PE price" in why
