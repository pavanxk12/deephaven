"""relative(series, level, symbol, basket): the value now of a symbol against the value now of each symbol of a basket; and the named baskets."""
import sys
import types

import numpy as np
import pandas as pd
import pytest

from algo.series.absolute import AbsoluteService, Baskets, Clock, deephaven_readers
from tests.test_absolute import IST, NSE_SESSION, TODAY, MemorySource, epoch, flat

NOW = epoch(TODAY, 11)                                                                      # the bar of session minute 10 has just closed


def arrays(*rows):
    """(slot, o, h, l, c, v) rows of TODAY -> the arrays a reader returns."""
    cols = list(zip(*rows))
    return (np.array([epoch(TODAY, s) for s in cols[0]], dtype=np.int64), *(np.array(c, dtype=np.float64) for c in cols[1:]))


def opens_at(price, vol=10):
    """A symbol that opened at 100 and trades at `price` at minute 10."""
    return arrays((0, 100, 100, 100, 100, vol), (10, price, price, price, price, vol))


class Reader:
    """today_bars_many over a dict {name: arrays}; counts the reads."""

    def __init__(self, data):
        self.data, self.calls = data, []

    def __call__(self, specs, day):
        self.calls.append(list(specs))
        return {sp: self.data[sp[1]] for sp in specs if sp[1] in self.data}


def svc(src, reader=None, **kw):
    return AbsoluteService(Clock(IST, NSE_SESSION), src, now=lambda: NOW, today_bars_many=reader, baskets=Baskets(), **kw)


def test_the_values_now_of_the_basket_and_where_the_symbol_stands():
    reader = Reader({"S": opens_at(102), "A": opens_at(101), "B": opens_at(103), "C": opens_at(99), "D": opens_at(104)})
    s = svc(MemorySource(), reader)
    values, idx = s.relative("pct_open", "intraday", "S", ["A", "B", "C", "D"])
    assert values.tolist() == pytest.approx([1.0, 3.0, -1.0, 4.0]) and idx == 2                  # 1 and -1 are below the 2.0 of S
    r = s.relative("pct_open", "intraday", "S", ["A", "B", "C", "D"])
    assert r.now == pytest.approx(2.0) and r.n_valid == 4 and r.pctile == 50.0 and r.symbols == ["A", "B", "C", "D"] and r.minute == 10 and r.today == TODAY


def test_the_symbol_itself_repeats_and_names_in_any_case_are_handled():
    reader = Reader({"S": opens_at(102), "A": opens_at(101), "B": opens_at(103)})
    s = svc(MemorySource(), reader)
    r = s.relative("pct_open", "intraday", "s", ["a", "S", "A", "B", "b"])
    assert r.symbols == ["A", "B"] and r[0].tolist() == pytest.approx([1.0, 3.0]) and r[1] == 1
    assert len(reader.calls) == 1 and sorted(sp[1] for sp in reader.calls[0]) == ["A", "B", "S"]   # ONE read for the symbol and the whole basket
    with pytest.raises(ValueError, match="no symbol other than"):
        s.relative("pct_open", "intraday", "S", ["S", "s"])


def test_futures_and_contracts_in_a_basket_keep_their_names():
    reader = Reader({"S": opens_at(102), "NIFTY": opens_at(101), "NIFTY 27OCT26 FUT": opens_at(103)})
    s = svc(MemorySource(), reader)
    r = s.relative("pct_open", "intraday", "S", ["NIFTY FUT", "NIFTY 27OCT26 FUT"])
    assert r.symbols == ["NIFTY FUT", "NIFTY 27OCT26 FUT"] and r[0].tolist() == pytest.approx([1.0, 3.0])


def test_pct_vwap_is_measured_from_each_symbols_own_vwap():
    reader = Reader({"S": arrays((0, 100, 100, 100, 100, 100), (10, 110, 110, 110, 110, 300)), "A": arrays((0, 50, 50, 50, 50, 10), (10, 50, 50, 50, 50, 10))})
    r = svc(MemorySource(), reader).relative("pct_vwap", "intraday", "S", ["A"])
    vwap = (100 * 100 + 110 * 300) / 400
    assert r.now == pytest.approx((110 - vwap) / vwap * 100) and r[0].tolist() == [0.0] and r[1] == 1


def test_a_symbol_with_no_rows_is_nan_and_left_out_and_too_few_valid_gives_no_position():
    reader = Reader({"S": opens_at(102), "A": opens_at(101)})
    s = svc(MemorySource(), reader)
    r = s.relative("pct_open", "intraday", "S", ["A", "NOBODY"])
    assert np.isnan(r[0][1]) and r.n_valid == 1 and r[1] == 1                                    # half of the two is enough
    r = svc(MemorySource(), reader).relative("pct_open", "intraday", "S", ["A", "N1", "N2", "N3"])
    assert r.n_valid == 1 and r[1] == -1                                                         # one of four is fewer than half
    assert svc(MemorySource(), Reader({"A": opens_at(101)})).relative("pct_open", "intraday", "S", ["A"])[1] == -1      # the symbol itself has no value
    assert svc(MemorySource(), reader).relative("pct_open", "intraday", "S", ["A"], now=0.5)[1] == 0                   # you can give its value


def test_today_s_saved_file_fills_in_what_the_live_table_does_not_have():
    src = MemorySource()
    src.add(TODAY, "B", {0: flat(100), 10: flat(103)})                                           # after the close: B is only in the file
    reader = Reader({"S": opens_at(102), "A": opens_at(101)})
    r = svc(src, reader).relative("pct_open", "intraday", "S", ["A", "B"])
    assert r[0].tolist() == pytest.approx([1.0, 3.0]) and r[1] == 1


def test_a_weekly_level_measures_from_the_week_open_and_reads_the_earlier_days_once_for_the_whole_basket():
    class Counting(MemorySource):
        def __init__(self):
            super().__init__()
            self.many, self.single = [], 0

        def bars(self, day, spec):
            self.single += 1
            return super().bars(day, spec)

        def bars_many(self, day, specs):
            self.many.append(day)
            return {sp: super(Counting, self).bars(day, sp) for sp in specs if super(Counting, self).bars(day, sp) is not None}

    src = Counting()
    for name, monday_open in (("S", 100), ("A", 200), ("B", 50)):
        src.add("2026-03-09", name, {0: flat(monday_open), 10: flat(monday_open)})
        src.add("2026-03-10", name, {0: flat(monday_open), 10: flat(monday_open)})
    now = epoch("2026-03-11", 11)                                                                # Wednesday of that week
    reader = Reader({n: arrays(*[(0, o, o, o, o, 5), (10, c, c, c, c, 5)]) for n, o, c in (("S", 100, 110), ("A", 200, 210), ("B", 50, 55))})
    for sp_name in reader.data:                                                                    # today's rows carry the Wednesday date
        ts, *rest = reader.data[sp_name]
        reader.data[sp_name] = (np.array([epoch("2026-03-11", 0), epoch("2026-03-11", 10)]), *rest)
    s = AbsoluteService(Clock(IST, NSE_SESSION), src, now=lambda: now, today_bars_many=reader, baskets=Baskets())
    r = s.relative("pct_open", "intraweek", "S", ["A", "B"])
    assert r.now == pytest.approx(10.0) and r[0].tolist() == pytest.approx([5.0, 10.0]) and r[1] == 1                # each against its Monday open
    assert src.many == ["2026-03-09", "2026-03-10"] and src.single == 0                              # two day files for the whole basket, not two per symbol
    d = s.relative("pct_open", "intraday", "S", ["A", "B"])                                         # the day level uses today's open instead
    assert d.now == pytest.approx(10.0) and src.many == ["2026-03-09", "2026-03-10"]


def test_iv_compares_the_underlyings_now():
    class IvReader:
        def __init__(self):
            self.calls = []

        def __call__(self, unders, day):
            self.calls.append(list(unders))
            return {u: (np.array([555 + 10]), np.array([v])) for u, v in {"NIFTY": 2.5, "BANKNIFTY": 3.5, "FINNIFTY": 1.5}.items() if u in unders}

    iv = IvReader()
    s = AbsoluteService(Clock(IST, NSE_SESSION), MemorySource(), now=lambda: NOW, today_iv_many=iv, baskets=Baskets())
    r = s.relative("iv", "intraday", "NIFTY", ["BANKNIFTY", "FINNIFTY", "NIFTY FUT"])             # a future's IV is its underlying's, so NIFTY FUT is the symbol again
    assert r.symbols == ["BANKNIFTY", "FINNIFTY", "NIFTY FUT"] and r[0].tolist() == [3.5, 1.5, 2.5] and r[1] == 1 and len(iv.calls) == 1                  # only 1.5 is below 2.5; the equal value is not


def test_at_evaluates_the_basket_as_of_another_moment():
    reader = Reader({"S": opens_at(102), "A": opens_at(101)})
    s = svc(MemorySource(), reader)
    r = s.relative("pct_open", "intraday", "S", ["A"], at="09:21")                               # session minute 6: the latest closed bar is minute 5, the price is carried from minute 0
    assert r.minute == 5 and r.now == 0.0 and r[0].tolist() == [0.0] and r[1] == 0
    assert s.relative("pct_open", "intraday", "S", ["A"], at="09:36")[1] == -1                      # 10 minutes after the last bar of the table: too old to use


def test_wrong_arguments():
    s = svc(MemorySource(), Reader({}))
    for args in (("rsi", "intraday", "S", ["A"]), ("iv", "daily", "S", ["A"]), ("iv", "intraday", "S", 5)):
        with pytest.raises((ValueError, KeyError)):
            s.relative(*args)
    with pytest.raises(KeyError, match="no basket called 'nope'"):
        s.relative("pct_open", "intraday", "S", "nope")


# ---- the console functions and the Deephaven reader -----------------------------------------------------------------------------------------------------
class Table:
    def __init__(self, df):
        self.df, self.keys, self.cols = df, None, None

    def where_in(self, keys, col):
        self.keys = (keys.df, col)
        return self

    def view(self, cols):
        self.cols = cols
        return self


@pytest.fixture
def dh(monkeypatch):
    futs = pd.DataFrame({"fut_token": [11], "TckrSymb": ["NIFTY"], "expiry": [pd.Timestamp("2026-03-17")]})
    master = types.SimpleNamespace(spot_of={"ABC": 7, "XYZ": 8}, token_of={}, futs=futs)
    candles = Table(pd.DataFrame({"token": [7, 7, 8, 11], "Ts": [epoch(TODAY, 0), epoch(TODAY, 1), epoch(TODAY, 0), epoch(TODAY, 0)], "Open": [1.0, 2.0, 5.0, 9.0],
                                  "High": [1.0, 2.0, 5.0, 9.0], "Low": [1.0, 2.0, 5.0, 9.0], "Close": [1.5, 2.5, 5.5, 9.5], "Volume": [1, 2, 3, 4]}))
    straddle = Table(pd.DataFrame({"Symbol": ["NIFTY", "NIFTY", "BANKNIFTY"], "Expiry": ["2026-03-24", "2026-03-17", "2026-03-17"],
                                   "Time": ["09:25:02", "09:25:02", "09:25:02"], "IV": [9.0, 2.0, 3.0]}))
    app = types.SimpleNamespace(master=master, tables={"candles": candles, "straddle_series": straddle})
    dhmod, dhp = types.ModuleType("deephaven"), types.ModuleType("deephaven.pandas")
    dhp.to_pandas = lambda t: t.df
    dhp.to_table = lambda df: types.SimpleNamespace(df=df)
    monkeypatch.setitem(sys.modules, "deephaven", dhmod)
    monkeypatch.setitem(sys.modules, "deephaven.pandas", dhp)
    return app, candles, straddle


def test_one_read_of_the_table_gives_the_rows_of_all_the_symbols(dh):
    app, candles, straddle = dh
    _, _, bars_many, iv_many = deephaven_readers(app)
    got = bars_many([("cash", "ABC"), ("cash", "XYZ"), ("fut", "NIFTY"), ("cash", "NOPE")], TODAY)
    assert candles.keys[1] == "token" and candles.keys[0]["token"].tolist() == [7, 8, 11]                  # one filter with every token
    assert set(got) == {("cash", "ABC"), ("cash", "XYZ"), ("fut", "NIFTY")} and got[("cash", "ABC")][4].tolist() == [1.5, 2.5] and got[("cash", "XYZ")][5].tolist() == [3.0]
    assert bars_many([("cash", "NOPE")], TODAY) == {}
    ivs = iv_many(["NIFTY", "BANKNIFTY", "NIFTY`; x"], TODAY)
    assert straddle.keys[1] == "Symbol" and sorted(straddle.keys[0]["Symbol"]) == ["BANKNIFTY", "NIFTY"]
    assert ivs["NIFTY"][1].tolist() == [2.0] and ivs["NIFTY"][0].tolist() == [565] and ivs["BANKNIFTY"][1].tolist() == [3.0]               # the nearest expiry only
    app.tables.clear()
    assert bars_many([("cash", "ABC")], TODAY) == {} and iv_many(["NIFTY"], TODAY) == {}


