import pandas as pd
import pytest

from algo.strategy.universe import assign_baskets, middle_set, parse_stocks, resolve_single, resolve_stocks

TEXT = """
BASKETS=2
# comment
RELIANCE 30JAN30 FUT      # trailing comment
NIFTY 30JAN30 FUT,B9
NIFTY 30JAN30 FUT
"""


def test_parse_stocks_dedups_and_reads_baskets():
    rows, n = parse_stocks(TEXT)
    assert n == 2 and rows == [("RELIANCE 30JAN30 FUT", None), ("NIFTY 30JAN30 FUT", "B9")]


def test_parse_requires_basket_count_when_a_stock_has_none():
    with pytest.raises(ValueError):
        parse_stocks("RELIANCE 30JAN30 FUT")
    rows, n = parse_stocks("A,B1\nB,B2")
    assert n is None and rows == [("A", "B1"), ("B", "B2")]


def test_auto_baskets_are_equal_parts_in_file_order():
    rows = [(f"S{i}", None) for i in range(8)]
    b = assign_baskets(rows, 4)
    assert [b[f"S{i}"] for i in range(8)] == ["B1", "B1", "B2", "B2", "B3", "B3", "B4", "B4"]


def test_middle_set_drops_outer_quartiles_per_basket():
    basket = {i: "B1" for i in range(8)}
    pcts = {i: i * 10 for i in range(8)}
    assert middle_set(pcts, basket) == {2, 3, 4, 5}


def test_middle_set_small_basket_and_missing_pct():
    assert middle_set({1: 5, 2: 6, 3: 7}, {1: "B1", 2: "B1", 3: "B1"}) == set()       # < 4 stocks: nobody ranked
    basket = {i: "B1" for i in range(5)}
    pcts = {0: 1, 1: 2, 2: 3, 3: 4, 4: None}
    assert middle_set(pcts, basket) == {1, 2}                                           # 4 ranked, None ignored


def test_baskets_are_ranked_independently():
    basket = {1: "B1", 2: "B1", 3: "B1", 4: "B1", 5: "B2", 6: "B2", 7: "B2", 8: "B2"}
    pcts = {1: 1, 2: 2, 3: 3, 4: 4, 5: 100, 6: 200, 7: 300, 8: 400}
    assert middle_set(pcts, basket) == {2, 3, 6, 7}


def test_resolve_stocks_skips_non_futures(master):
    inst = resolve_stocks("BASKETS=1\nRELIANCE 30JAN30 FUT\nRELIANCE\nNOPE 30JAN30 FUT", master)
    assert list(inst) == [3] and inst[3].underlying == "RELIANCE" and inst[3].basket == "B1"


def test_resolve_single_picks_nearest_future(master):
    inst = resolve_single("NIFTY", master, pd.Timestamp("2030-01-01"))
    assert list(inst) == [1]
    with pytest.raises(RuntimeError):
        resolve_single("NOPE", master, pd.Timestamp("2030-01-01"))
