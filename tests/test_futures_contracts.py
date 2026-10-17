"""Which instrument a name means: RELIANCE is the cash stock, NIFTY FUT the nearest future of each day (rolling), NIFTY APR FUT / NIFTY 28APR26 FUT one contract; and a reference is
always measured on the same contract as the day it belongs to (no calendar spread across a roll)."""
import sys
import types

import numpy as np
import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow")

from algo.data.store import Store  # noqa: E402
from algo.series.absolute import (AbsoluteService, Baskets, Clock, ParquetSource, deephaven_readers, is_future, label_of, month_contract,  # noqa: E402
                                  parse_symbol, underlying_of)
from tests.test_absolute import DAYS, IST, NSE_SESSION, TODAY, epoch, write_candles  # noqa: E402
from tests.test_absolute_today import Table  # noqa: E402
from tests.test_relative import Reader, arrays  # noqa: E402

A, B, C = "NIFTY 10MAR26 FUT", "NIFTY 28APR26 FUT", "NIFTY 26MAY26 FUT"          # A expires on the 10th: nearest on the 9th and 10th, B from the 11th


@pytest.fixture
def folder(tmp_path):
    """Mon 9 to Fri 13 March 2026. Each contract opens at its own price and moves a little; the three are a few points apart (the calendar spread)."""
    plan = {  # day: {contract: (price at slot 0, price at slot 10)}
        "2026-03-09": {A: (100, 105), B: (110, 110), C: (120, 120), "NIFTY": (99, 99)},
        "2026-03-10": {A: (105, 105), B: (110, 110), C: (120, 121), "NIFTY": (99, 99)},
        "2026-03-11": {B: (111, 111), C: (121, 122), "NIFTY": (99, 99)},
        "2026-03-12": {B: (111, 112), C: (122, 123), "NIFTY": (99, 99)},
        "2026-03-13": {B: (112, 113), C: (123, 123), "NIFTY": (99, 99)},
    }
    for day, contracts in plan.items():
        rows = []
        for name, (p0, p10) in contracts.items():
            rows += [(name, 0, p0, 10), (name, 10, p10, 10)]
        write_candles(tmp_path, day, rows)
    return tmp_path


def svc(folder, now_slot=11, day=TODAY, **kw):
    return AbsoluteService(Clock(IST, NSE_SESSION), ParquetSource(Store(str(folder))), now=lambda: epoch(day, now_slot), baskets=Baskets(), **kw)


# ---- what a name means ----------------------------------------------------------------------------------------------------------------------------------
def test_the_names_and_what_they_stand_for():
    assert parse_symbol("sbin") == ("cash", "SBIN") and parse_symbol("SBIN FUT") == ("fut", "SBIN")
    assert parse_symbol("SBIN 30JUN26 FUT") == ("exact", "SBIN 30JUN26 FUT")
    assert parse_symbol("sbin jun fut") == ("month", "SBIN JUN") and parse_symbol("SBIN JUN26 FUT") == ("month", "SBIN JUN26")
    assert [underlying_of(parse_symbol(x)) for x in ("SBIN", "SBIN FUT", "SBIN 30JUN26 FUT", "SBIN JUN FUT")] == ["SBIN"] * 4
    assert [label_of(parse_symbol(x)) for x in ("sbin", "sbin fut", "sbin jun fut", "SBIN 30JUN26 FUT")] == ["SBIN", "SBIN FUT", "SBIN JUN FUT", "SBIN 30JUN26 FUT"]
    assert [is_future(parse_symbol(x)) for x in ("SBIN", "SBIN FUT", "SBIN JUN FUT", "SBIN 30JUN26 FUT")] == [False, True, True, True]


def test_a_month_means_the_contract_expiring_in_it_on_or_after_the_day():
    names = [A, B, C, "NIFTY", "BANKNIFTY 28APR26 FUT"]
    assert month_contract(("month", "NIFTY APR"), names, "2026-03-09") == B
    assert month_contract(("month", "NIFTY APR26"), names, "2026-03-09") == B and month_contract(("month", "NIFTY APR25"), names, "2026-03-09") is None
    assert month_contract(("month", "NIFTY MAR"), names, "2026-03-10") == A and month_contract(("month", "NIFTY MAR"), names, "2026-03-11") is None      # expired
    assert month_contract(("month", "NIFTY MAY"), names, "2026-03-09") == C and month_contract(("month", "NIFTY JUN"), names, "2026-03-09") is None
    assert month_contract(("fut", "NIFTY"), names, "2026-03-09") == A and month_contract(("fut", "NIFTY"), names, "2026-03-11") == B
    assert month_contract(("fut", "BANKNIFTY"), names, "2026-03-09") == "BANKNIFTY 28APR26 FUT" and month_contract(("fut", "NOSUCH"), names, "2026-03-09") is None


# ---- the history is the instrument that was named, nothing else ------------------------------------------------------------------------------------------
def test_the_cash_the_rolling_future_and_one_contract_each_read_their_own_rows(folder):
    src = ParquetSource(Store(str(folder)))
    close = lambda day, spec: src.bars(day, spec)[4].tolist()          # noqa: E731
    assert close("2026-03-09", ("cash", "NIFTY")) == [99.0, 99.0]                                  # the cash index
    assert close("2026-03-09", ("fut", "NIFTY")) == [100.0, 105.0] and close("2026-03-11", ("fut", "NIFTY")) == [111.0, 111.0]           # the nearest future of each day: A, then B
    assert close("2026-03-09", ("month", "NIFTY APR")) == [110.0, 110.0] and close("2026-03-11", ("month", "NIFTY APR")) == [111.0, 111.0]    # B on every day it traded
    assert close("2026-03-09", ("exact", C)) == [120.0, 120.0]
    assert src.bars("2026-03-09", ("month", "NIFTY JUN")) is None and src.bars("2026-03-11", ("month", "NIFTY MAR")) is None      # no such contract that day: nothing, not the cash
    assert src.bars("2026-03-09", ("cash", "NOSUCH")) is None


def test_asking_for_a_future_never_falls_back_to_the_equity_and_the_other_way_round(tmp_path):
    write_candles(tmp_path, "2026-03-09", [("SBIN", 0, 800, 10), ("SBIN", 10, 808, 10)])           # only cash files exist
    src = ParquetSource(Store(str(tmp_path)))
    assert src.bars("2026-03-09", ("cash", "SBIN")) is not None
    assert src.bars("2026-03-09", ("fut", "SBIN")) is None and src.bars("2026-03-09", ("month", "SBIN JUN")) is None
    s = svc(tmp_path, day="2026-03-10")
    assert s.absolute("pct", "intraday", "SBIN", 1, ref="open", now=1.0).n_valid == 1               # the cash has history
    r = s.absolute("pct", "intraday", "SBIN FUT", 1, ref="open", now=1.0)
    assert np.isnan(r[0]).all() and r.n_valid == 0 and r[1] == -1                                   # the future has none: NaN, not the equity's numbers


def test_a_month_contract_compares_with_its_own_earlier_days_not_with_earlier_contracts(folder):
    s = svc(folder)
    rolling = s.absolute("pct", "intraday", "NIFTY FUT", 5, ref="open", now=0.0)
    april = s.absolute("pct", "intraday", "NIFTY APR FUT", 5, ref="open", now=0.0)
    assert rolling[0].tolist() == pytest.approx([5.0, 0.0, 0.0, 1 / 111 * 100, 1 / 112 * 100])       # A's 5% on the 9th (A was the nearest), then B
    assert april[0].tolist() == pytest.approx([0.0, 0.0, 0.0, 1 / 111 * 100, 1 / 112 * 100])        # always B
    assert april.dates == DAYS and april.symbol == "NIFTY APR FUT"


# ---- no calendar spread across a roll -----------------------------------------------------------------------------------------------------------------------
def test_the_previous_close_is_the_same_contracts_not_the_old_nearest_ones(folder):
    s = svc(folder)
    r = s.absolute("pct", "intraday", "NIFTY FUT", 5, ref="prev_close", now=0.0)
    # 9th: no earlier file; 10th: A vs A's close (105); 11th is the first day of B: B's 111 against B's own close of 110 on the 10th, NOT against A's 105 (that would be 5.7%)
    assert np.isnan(r[0][0]) and r[0][1] == pytest.approx(0.0) and r[0][2] == pytest.approx((111 - 110) / 110 * 100)
    assert r[0][3] == pytest.approx((112 - 111) / 111 * 100) and r[0][4] == pytest.approx((113 - 112) / 112 * 100)


def test_a_weeks_open_is_the_open_of_the_same_contract_as_the_day_measured(folder):
    s = svc(folder, day="2026-03-18")                                                              # a Wednesday: compare with Wednesday 11 March of the week that began on the 9th
    r = s.absolute("pct", "intraweek", "NIFTY FUT", 1, ref="open", now=0.0)
    assert r.dates == ["2026-03-11"]
    assert r[0][0] == pytest.approx((111 - 110) / 110 * 100)                                       # B's price on the 11th against B's open on Monday the 9th (110), not A's 100
    v = s.absolute("pct", "intraweek", "NIFTY FUT", 1, ref="vwap", now=0.0)
    vwap = (110 * 40 + 111 * 20) / 60                                                              # B on Monday and Tuesday (20 shares each) and Wednesday up to now (20)
    assert v[0][0] == pytest.approx((111 - vwap) / vwap * 100, rel=1e-4)                           # B's volume weighted price over its own days, not A's


def test_a_cash_symbol_is_not_pinned_to_anything(folder):
    s = svc(folder, day="2026-03-18")
    assert s.absolute("pct", "intraweek", "NIFTY", 1, ref="open", now=0.0)[0][0] == pytest.approx(0.0)           # the cash index: 99 and 99


def test_today_s_contract_comes_from_the_master_and_pins_the_previous_close_for_a_basket(folder):
    """On 11 March (B's first day as the nearest) a basket symbol "NIFTY FUT" is measured from B's previous close."""
    reader = Reader({"S": arrays((0, 100, 100, 100, 100, 5), (10, 101, 101, 101, 101, 5)), "NIFTY": arrays((0, 111, 111, 111, 111, 5), (10, 111, 111, 111, 111, 5))})
    for name in reader.data:                                                                         # today's rows carry the 11th
        ts, *rest = reader.data[name]
        reader.data[name] = (np.array([epoch("2026-03-11", 0), epoch("2026-03-11", 10)]), *rest)
    s = svc(folder, day="2026-03-11", today_bars_many=reader, contract_of_today=lambda spec, day: ("exact", B) if spec == ("fut", "NIFTY") else spec)
    r = s.relative("pct", "intraday", "S", ["NIFTY FUT"], ref="prev_close", now=0.0)
    assert r.symbols == ["NIFTY FUT"]
    assert r[0][0] == pytest.approx((111 - 110) / 110 * 100)                                  # B against B's close on the 10th (110): 0.909, not 5.7 against A


# ---- today, from the master ---------------------------------------------------------------------------------------------------------------------------------
@pytest.fixture
def dh(monkeypatch):
    futs = pd.DataFrame({"fut_token": [11, 12, 13], "TckrSymb": ["NIFTY"] * 3, "expiry": [pd.Timestamp("2026-03-10"), pd.Timestamp("2026-04-28"), pd.Timestamp("2026-05-26")]})
    symbol_of = {11: A, 12: B, 13: C}
    master = types.SimpleNamespace(spot_of={"SBIN": 7}, token_of={A: 11, B: 12, C: 13}, futs=futs, symbol_of=symbol_of)
    candles = Table(pd.DataFrame({"token": [12], "Ts": [epoch(TODAY, 0)], "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [1]}))
    app = types.SimpleNamespace(master=master, tables={"candles": candles})
    dhmod, dhp = types.ModuleType("deephaven"), types.ModuleType("deephaven.pandas")
    dhp.to_pandas = lambda t: t.df
    dhp.to_table = lambda df: types.SimpleNamespace(df=df)
    monkeypatch.setitem(sys.modules, "deephaven", dhmod)
    monkeypatch.setitem(sys.modules, "deephaven.pandas", dhp)
    return app, candles


def test_the_master_picks_the_cash_the_rolling_future_or_the_month(dh):
    app, candles = dh
    today_bars, _, _, _, contract_of = deephaven_readers(app)
    today_bars(("cash", "SBIN"), TODAY)
    assert candles.where_expr == "token = 7"
    today_bars(("fut", "NIFTY"), "2026-03-09")
    assert candles.where_expr == "token = 11"                                                       # A is the nearest on the 9th
    today_bars(("fut", "NIFTY"), "2026-03-16")
    assert candles.where_expr == "token = 12"                                                       # B on the 16th
    today_bars(("month", "NIFTY MAY"), "2026-03-16")
    assert candles.where_expr == "token = 13"
    assert today_bars(("month", "NIFTY MAR"), "2026-03-16") is None and today_bars(("month", "NIFTY APR25"), "2026-03-16") is None
    assert contract_of(("fut", "NIFTY"), "2026-03-16") == ("exact", B) and contract_of(("month", "NIFTY MAY"), "2026-03-16") == ("exact", C)
    assert contract_of(("cash", "SBIN"), TODAY) == ("cash", "SBIN") and contract_of(("fut", "NOSUCH"), TODAY) is None


# ---- the names in a basket ------------------------------------------------------------------------------------------------------------------------------------
def test_a_basket_holds_cash_futures_and_months_and_finds_the_basket_of_each(tmp_path):
    parsed = Baskets.parse("[mix]\nSBIN\nsbin fut\nSBIN JUN FUT\nSBIN 30JUN26 FUT\n")
    assert parsed["mix"] == ["SBIN", "SBIN FUT", "SBIN JUN FUT", "SBIN 30JUN26 FUT"]
    path = tmp_path / "baskets.txt"
    path.write_text("[mix]\nSBIN\nSBIN FUT\nSBIN JUN FUT\n", encoding="utf-8")
    b = Baskets([str(path)])
    assert b.containing(parse_symbol("sbin jun fut")) == ["mix"] and b.containing(parse_symbol("SBIN")) == ["mix"] and b.containing(parse_symbol("SBIN JUL FUT")) == []
