"""The console's expense and currency settings (the queries themselves were run in a real Deephaven session: see tech.md "Expense")."""
import re

from algo.core.config import NSE, US
from algo.ui.tables import expense_formula


def test_nse_rates_are_the_ones_the_owner_gave_per_crore():
    assert NSE.expense_per_crore == {"FUT": (600.0, 5400.0), "OPT": (5625.0, 20328.0), "EQ": (200.0, 200.0)}
    assert US.expense_per_crore == {}                                          # no formula given for the US: no expense shown


def test_the_formula_charges_buys_and_sells_per_crore_by_kind():
    f = expense_formula(NSE.expense_per_crore)
    numbers = [float(x) for x in re.findall(r"\* ([0-9.]+)", f)]
    assert numbers == [600 / 1e7, 5400 / 1e7, 5625 / 1e7, 20328 / 1e7, 200 / 1e7, 200 / 1e7]
    assert f.startswith("Kind == `FUT` ?") and f.endswith(": 0.0")             # a kind without a rate costs nothing
    assert expense_formula(None) == "0.0" and expense_formula({}) == "0.0"


def test_every_money_figure_is_the_actual_amount_with_the_markets_currency_symbol():
    assert (NSE.currency, US.currency) == ("₹", "$")
    assert not hasattr(NSE, "turnover_unit")                                    # turnover is the actual value traded, no crore / million scaling


def test_the_kind_of_every_token_comes_from_the_masters_own_lists():
    import types

    import pandas as pd

    from algo.data.symbols import kind_of_tokens
    master = types.SimpleNamespace(
        opt=pd.DataFrame({"Token": [10, 11]}), futs=pd.DataFrame({"fut_token": [1]}),
        eqs=pd.DataFrame({"eq_token": [5, 6]}))                               # 6 is a stock whose name ends in CE: it is a stock because the master says so
    assert kind_of_tokens(master) == {5: "EQ", 6: "EQ", 1: "FUT", 10: "OPT", 11: "OPT"}
    empty = types.SimpleNamespace(opt=pd.DataFrame(), futs=None, eqs=pd.DataFrame({"eq_token": [7]}))
    assert kind_of_tokens(empty) == {7: "EQ"}                                  # a missing list is just empty


def test_the_divisor_of_every_token_comes_from_the_master_so_a_us_stock_and_a_us_option_differ():
    import types

    from algo.data.symbols import divisors_of_tokens
    master = types.SimpleNamespace(divisor_of={1: 100, 3426: 10000, 5: 0, 6: None})            # AAPL stock 100, an AAPL option 10000; a missing or zero divisor is left out
    assert divisors_of_tokens(master) == {1: 100.0, 3426: 10000.0}
