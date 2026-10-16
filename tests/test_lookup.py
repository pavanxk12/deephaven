"""The plain console functions for looking things up: contracts from the master, and the latest price and candles."""
import re
import types

import pandas as pd
import pytest

import algo
from algo import api, lookup
from algo.core import app as app_module


@pytest.fixture
def session(monkeypatch, master_weekly):
    """A console session with the contract master built (NIFTY weeklies and monthlies, RELIANCE)."""
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    app = algo.install(ns, "NSE")
    app.plugins["master"] = types.SimpleNamespace(data=master_weekly)
    return ns, app


@pytest.fixture
def bare(monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    algo.install(ns, "NSE")
    return ns


class FakeTable:
    """Enough of a Deephaven table for the lookups: where("token == N") and tail(n)."""

    def __init__(self, df):
        self.df = df

    def where(self, expr):
        token = int(re.fullmatch(r"token == (\d+)", expr).group(1))
        return FakeTable(self.df[self.df["token"] == token])

    def tail(self, n):
        return FakeTable(self.df.tail(n))


@pytest.fixture
def tables(session, monkeypatch):
    ns, app = session
    monkeypatch.setattr(lookup, "_frame", lambda t: t.df.reset_index(drop=True))
    app.tables["candles"] = FakeTable(pd.DataFrame({
        "Minute": ["09:15", "09:16", "09:17", "09:15", "09:16"], "token": [1, 1, 1, 3, 3],
        "Open": [25000.0, 25010.0, 25020.0, 3000.0, 3001.0], "Close": [25010.0, 25020.0, 25030.5, 3001.0, 3002.5],
        "Volume": [10, 20, 30, 5, 6]}))
    return ns, app


# ---- one contract ------------------------------------------------------------------------------------------------------
def test_a_contract_from_a_token_a_symbol_in_any_case_or_an_underlying_name(session, capsys):
    ns, app = session
    opt = ns["contract"]("nifty 30jan30 25000 ce")                                  # any case, extra spaces are fine
    assert opt == ns["contract"](" NIFTY  30JAN30 25000 CE ") == ns["contract"](opt["token"]) == ns["contract"](str(opt["token"]))
    assert (opt["kind"], opt["underlying"], opt["expiry"], opt["strike"], opt["type"], opt["lot"]) == (
        "OPT", "NIFTY", "2030-01-30", 25000.0, "CE", 50)
    assert opt["tick"] == pytest.approx(0.05) and opt["symbol"] == "NIFTY 30JAN30 25000 CE"
    fut = ns["contract"]("NIFTY")                                                    # an underlying means its nearest future
    assert (fut["kind"], fut["token"], fut["expiry"], fut["strike"], fut["type"]) == ("FUT", 1, "2030-01-30", None, None)
    assert ns["contract"]("RELIANCE")["token"] == 3                                  # the future, not the cash instrument
    cash = ns["contract"](900)
    assert (cash["kind"], cash["token"], cash["expiry"], cash["lot"]) == ("EQ", 900, None, None)     # the cash instrument, by its token
    assert ns["contracts"]("RELIANCE", kind="EQ")["token"].tolist() == [900]
    assert "token 1" in capsys.readouterr().out


def test_an_unknown_contract_is_said_not_raised(session, capsys):
    ns, _ = session
    for bad in ("NOPE", 99999, True, "", "NIFTY 99JAN30 1 CE", None):
        assert ns["contract"](bad) is None
    assert capsys.readouterr().out.count("unknown contract") == 6


def test_every_lookup_says_when_the_master_is_not_built(bare, capsys):
    for call in (lambda: bare["contract"]("NIFTY"), lambda: bare["contracts"]("NIFTY"), lambda: bare["expiries"]("NIFTY"),
                 lambda: bare["strikes"]("NIFTY"), lambda: bare["nearest_strikes"]("NIFTY", 25000), lambda: bare["last_price"]("NIFTY"),
                 lambda: bare["last_candles"]("NIFTY")):
        assert call() is None
    assert capsys.readouterr().out.count("master is not built") == 7


# ---- lists of contracts ------------------------------------------------------------------------------------------------------
def test_contracts_are_filtered_by_whatever_you_give(session, capsys):
    ns, _ = session
    every_25000 = ns["contracts"]("NIFTY", kind="OPT", type="CE", strike=25000)
    assert list(every_25000["expiry"]) == sorted(every_25000["expiry"]) and len(every_25000) == 5         # 3 weeklies, the monthly, next month
    assert set(every_25000["type"]) == {"CE"} and set(every_25000["strike"]) == {25000.0}
    assert list(every_25000.columns) == ["token", "symbol", "kind", "underlying", "expiry", "strike", "type", "tick", "lot"]
    assert len(ns["contracts"]("NIFTY", expiry=0, type="PE")) == 3                                         # the nearest expiry, 3 strikes
    assert len(ns["contracts"]("NIFTY", expiry="2030-01-30", kind="OPT")) == 6
    assert list(ns["contracts"]("NIFTY", kind="FUT")["token"]) == [1, 2]
    assert list(ns["contracts"](kind="EQ")["token"]) == [900]
    assert list(ns["contracts"]("reliance", kind="FUT", expiry=0)["token"]) == [3]                         # the underlying in any case
    assert len(ns["contracts"]("NIFTY", kind="FUT", type="CE")) == 0                                       # a futures list has no CE
    assert "no contracts match" in capsys.readouterr().out


def test_contracts_with_no_filter_shows_a_page_and_says_how_to_see_the_rest(session, capsys):
    ns, _ = session
    out = ns["contracts"](limit=5)
    assert len(out) > 5
    text = capsys.readouterr().out
    assert f"{len(out) - 5} more ({len(out)} in all)" in text
    assert len(ns["contracts"](limit=None)) == len(out)


def test_contracts_asks_for_an_underlying_when_the_expiry_is_a_number_and_checks_dates(session, capsys):
    ns, _ = session
    assert ns["contracts"](expiry=0) is None and ns["contracts"]("NIFTY", expiry="not a date") is None
    out = capsys.readouterr().out
    assert "needs an underlying" in out and "is not a date" in out
    assert len(ns["contracts"]("NIFTY", expiry=99)) == 0                                                    # no 100th expiry


def test_expiries_that_have_not_passed_as_dates(session, capsys):
    ns, _ = session
    assert ns["expiries"]("NIFTY") == ["2030-01-09", "2030-01-16", "2030-01-23", "2030-01-30", "2030-02-27"]
    assert ns["expiries"]("nifty", "FUT") == ["2030-01-30", "2030-02-27"]
    assert ns["expiries"]("RELIANCE") == ["2030-01-30"]
    assert ns["expiries"]("NOPE") == []
    assert "NOPE OPT expiries: none" in capsys.readouterr().out


def test_strikes_of_one_expiry_by_number_or_date(session, capsys):
    ns, _ = session
    nearest = ns["strikes"]("NIFTY")                                                 # expiry 0 = the nearest, a weekly
    assert list(nearest["strike"]) == [24900.0, 25000.0, 25100.0] and len(set(nearest["token"])) == 3
    assert list(ns["strikes"]("NIFTY", expiry="2030-01-30", type="pe")["strike"]) == [24900.0, 25000.0, 25100.0]
    assert ns["strikes"]("NIFTY", 1)["symbol"].iloc[0].startswith("NIFTY 16JAN30")
    for bad in (dict(expiry=9), dict(expiry="2031-01-01"), dict(expiry="garbage")):
        assert ns["strikes"]("NIFTY", **bad) is None
    assert ns["strikes"]("NOPE") is None
    assert capsys.readouterr().out.count("see expiries(") == 4                          # said once for each of the four that found nothing


def test_the_strikes_nearest_a_price(session, tables):
    ns, _ = tables
    near = ns["nearest_strikes"]("NIFTY", 25010, "CE", n=2)
    assert list(near["strike"]) == [25000.0, 25100.0]                                # closest first
    assert list(ns["nearest_strikes"]("NIFTY", 24000, n=1)["strike"]) == [24900.0]   # beyond the chain: the end strike
    assert list(ns["nearest_strikes"]("NIFTY", 25090, "PE", expiry=1)["strike"]) == [25100.0]
    got = ns["nearest_strikes"]("NIFTY")                                             # no price: the future's last candle close, 25030.5
    assert list(got["strike"]) == [25000.0]


def test_nearest_strikes_without_a_price_says_so(session, capsys):
    ns, _ = session
    assert ns["nearest_strikes"]("NIFTY") is None
    out = capsys.readouterr().out
    assert "candles table is not there" in out and "no price given" in out


# ---- the live tables ------------------------------------------------------------------------------------------------------------
def test_the_latest_price_and_candles_of_a_contract(tables, capsys):
    ns, _ = tables
    assert ns["last_price"]("NIFTY") == 25030.5 and ns["last_price"](3) == 3002.5 and ns["last_price"]("RELIANCE") == 3002.5
    two = ns["last_candles"]("NIFTY", 2)
    assert list(two["Minute"]) == ["09:16", "09:17"] and list(two["Close"]) == [25020.0, 25030.5]        # only this token, the last two
    assert len(ns["last_candles"]("NIFTY")) == 3
    capsys.readouterr()
    assert ns["last_price"]("NIFTY", show=False) == 25030.5 and len(ns["last_candles"]("NIFTY", 2, show=False)) == 2
    assert capsys.readouterr().out == ""                                                              # show=False prints nothing


def test_no_candle_yet_an_unknown_name_or_a_missing_table_is_said_not_raised(tables, session, capsys):
    ns, app = tables
    assert ns["last_price"]("NIFTY NEXT") is None and ns["last_price"](2) is None             # token 2 has no candle
    assert len(ns["last_candles"](2)) == 0
    assert "no candle yet" in capsys.readouterr().out
    del app.tables["candles"]
    assert ns["last_price"]("NIFTY") is None and ns["last_candles"]("NIFTY") is None
    assert capsys.readouterr().out.count("candles_start()") == 2


def test_a_failing_table_read_is_said_not_raised(tables, monkeypatch, capsys):
    ns, _ = tables
    monkeypatch.setattr(lookup, "_frame", lambda t: (_ for _ in ()).throw(RuntimeError("deephaven says no")))
    assert ns["last_price"]("NIFTY") is None
    assert "could not read candles" in capsys.readouterr().out


# ---- the help ---------------------------------------------------------------------------------------------------------------------
def test_every_function_is_in_exactly_one_group_and_has_a_description():
    names = [f.__name__ for f in api.FUNCTIONS]
    grouped = [n for names_ in api.GROUPS.values() for n in names_]
    assert sorted(names) == sorted(grouped) and len(set(grouped)) == len(grouped)
    for f in api.FUNCTIONS:
        assert (f.__doc__ or "").strip(), f"{f.__name__} has no description"


def test_algo_help_lists_every_function_by_service(bare, capsys):
    bare["algo_help"]()
    out = capsys.readouterr().out
    for group in ("MASTER (contracts)", "CANDLES", "PORTFOLIOS (the book)", "NATS (commands)", "AUTOMATIONS (a script you write)"):
        assert group in out
    for f in api.FUNCTIONS:
        assert f"{f.__name__}()" in out, f.__name__
    bare["algo_help"]("pf_create")
    one = capsys.readouterr().out
    assert one.startswith("pf_create(user") and "strategy" in one.splitlines()[0] and "next free one" in one
    bare["algo_help"]("nope")
    assert "unknown function" in capsys.readouterr().out


def test_install_points_to_the_help_and_puts_the_lookups_in_the_console(monkeypatch, capsys):
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    algo.install(ns, "NSE")
    assert "algo_help()" in capsys.readouterr().out
    for name in ("contract", "contracts", "expiries", "strikes", "nearest_strikes", "last_price", "last_candles",
                 "pf_create", "nats_send", "algo_help"):
        assert callable(ns[name])
