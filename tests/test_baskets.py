"""baskets.txt: the baskets relative() refers to, written by hand or changed from the terminal."""
import os
import sys
import time
import types

import pandas as pd
import pytest

from algo.series.absolute import AbsoluteService, Baskets, Clock
from tests.test_absolute import IST, NSE_SESSION, TODAY, MemorySource, epoch
from tests.test_relative import Table, opens_at

TEXT = """# my baskets
[banks]
HDFCBANK
icicibank, SBIN      # two on one line
AXISBANK

[nifty_complex]
NIFTY FUT
BANKNIFTY FUT, NIFTY 27OCT26 FUT
"""


def baskets(tmp_path, text=TEXT, name="baskets.txt"):
    path = tmp_path / name
    if text is not None:
        path.write_text(text, encoding="utf-8")
    return Baskets([str(path)]), path


# ---- reading the file --------------------------------------------------------------------------------------------------------------------------------
def test_the_file_is_read_with_comments_commas_and_new_lines(tmp_path):
    b, _ = baskets(tmp_path)
    assert b.names() == ["banks", "nifty_complex"]
    assert b.get("banks") == ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK"]
    assert b.get("NIFTY_complex") == ["NIFTY FUT", "BANKNIFTY FUT", "NIFTY 27OCT26 FUT"]                 # a name in any case; spaces inside a symbol are part of it
    assert Baskets.parse("[a]\nTCS,TCS\n")["a"] == ["TCS"]                                                  # a repeat is dropped


@pytest.mark.parametrize("text, line, word", [
    ("TCS\n[a]\nINFY\n", 1, "before any [name] heading"),
    ("[a b]\nTCS\n", 1, "not a basket heading"),
    ("[a\nTCS\n", 1, "not a basket heading"),
    ("[a]\nTCS\n[A]\nINFY\n", 3, "defined twice"),
    ("[a]\n[b]\nTCS\n", None, "has no symbols"),
    ("[a]\n   \n", None, "has no symbols"),
])
def test_a_mistake_in_the_file_is_reported_with_its_line(text, line, word):
    with pytest.raises(ValueError) as e:
        Baskets.parse(text, "baskets.txt")
    assert word in str(e.value) and (line is None or f"line {line}" in str(e.value))


def test_a_basket_too_big_is_refused():
    with pytest.raises(ValueError, match="more than 5000"):
        Baskets.parse("[a]\n" + "\n".join(f"S{i}" for i in range(5001)))


def test_the_first_existing_file_in_the_list_is_used_and_a_missing_one_says_where_to_put_it(tmp_path):
    first, second = tmp_path / "md" / "baskets.txt", tmp_path / "state" / "baskets.txt"
    os.makedirs(second.parent)
    second.write_text("[x]\nTCS\n")
    b = Baskets([str(first), str(second)])
    assert b.path == str(second) and b.get("x") == ["TCS"]
    os.makedirs(first.parent)
    first.write_text("[x]\nINFY\n")
    assert b.path == str(first) and b.get("x") == ["INFY"]                                                  # the earlier place wins as soon as the file exists
    none = Baskets([str(tmp_path / "nowhere" / "baskets.txt")])
    assert none.names() == []
    with pytest.raises(KeyError, match="none of"):
        none.get("x")
    with pytest.raises(KeyError, match="Baskets there"):
        b.get("nope")


def test_the_file_is_read_again_only_when_it_changes(tmp_path, monkeypatch):
    b, path = baskets(tmp_path)
    parses = []
    real = Baskets.parse
    monkeypatch.setattr(Baskets, "parse", staticmethod(lambda t, w="baskets.txt": (parses.append(1), real(t, w))[1]))
    for _ in range(5):
        b.get("banks")
    assert len(parses) == 1                                                                                 # five calls, one read
    time.sleep(0.02)
    path.write_text(TEXT.replace("SBIN", "SBIN, PNB"), encoding="utf-8")
    assert b.get("banks")[-2:] == ["PNB", "AXISBANK"] and len(parses) == 2


def test_a_file_with_a_mistake_makes_the_basket_calls_say_so_until_it_is_fixed(tmp_path):
    b, path = baskets(tmp_path)
    assert b.get("banks")
    time.sleep(0.02)
    path.write_text("[banks]\nTCS\n[bad name]\nINFY\n", encoding="utf-8")
    with pytest.raises(ValueError, match="line 3"):
        b.get("banks")
    time.sleep(0.02)
    path.write_text("[banks]\nTCS\n", encoding="utf-8")
    assert b.get("banks") == ["TCS"]


# ---- changing it from the terminal -------------------------------------------------------------------------------------------------------------------
def test_defining_a_basket_changes_only_its_own_lines(tmp_path):
    b, path = baskets(tmp_path)
    assert b.define("banks", ["pnb", "SBIN"]) == ["PNB", "SBIN"]
    text = path.read_text()
    assert text.startswith("# my baskets\n[banks]\nPNB\nSBIN\n") and "[nifty_complex]\nNIFTY FUT\n" in text            # the comment on top and the other basket are as they were
    assert b.get("banks") == ["PNB", "SBIN"] and b.get("nifty_complex")[0] == "NIFTY FUT"
    b.define("it", ["TCS", "INFY"])
    assert path.read_text().rstrip().endswith("[it]\nTCS\nINFY") and b.names() == ["banks", "it", "nifty_complex"]


def test_add_remove_and_delete(tmp_path):
    b, path = baskets(tmp_path)
    assert b.edit("banks", add=["PNB", "sbin"], remove=["hdfcbank"]) == ["ICICIBANK", "SBIN", "AXISBANK", "PNB"]
    assert b.edit("nifty_complex", remove=["banknifty fut"]) == ["NIFTY FUT", "NIFTY 27OCT26 FUT"]
    b.delete("banks")
    assert b.names() == ["nifty_complex"] and "[banks]" not in path.read_text() and path.read_text().startswith("# my baskets\n")
    with pytest.raises(KeyError):
        b.delete("banks")
    with pytest.raises(KeyError):
        b.edit("banks", add=["A"])


def test_the_first_basket_creates_the_file_in_the_first_writable_place(tmp_path):
    md, state = tmp_path / "md", tmp_path / "state"
    os.makedirs(md)
    b = Baskets([str(md / "baskets.txt"), str(state / "baskets.txt")])
    b.define("banks", ["HDFCBANK"])
    assert (md / "baskets.txt").read_text() == "[banks]\nHDFCBANK\n" and not state.exists()
    b2 = Baskets([str(tmp_path / "gone" / "a" / "baskets.txt")])                                           # a folder that does not exist yet is made
    b2.define("x", ["TCS"])
    assert b2.get("x") == ["TCS"]


def test_what_is_wrong_is_refused_before_anything_is_written(tmp_path):
    b, path = baskets(tmp_path)
    before = path.read_text()
    for name, symbols in (("a b", ["TCS"]), ("", ["TCS"]), ("x", "TCS"), ("x", []), ("x", 5), ("x", [f"S{i}" for i in range(5001)])):
        with pytest.raises(ValueError):
            b.define(name, symbols)
    assert path.read_text() == before and not [p for p in os.listdir(tmp_path) if ".tmp" in p]
    path.write_text("TCS\n[a]\nINFY\n")                                                                     # a file that already has a mistake is not touched
    with pytest.raises(ValueError, match="before any"):
        b.define("x", ["A"])
    assert path.read_text() == "TCS\n[a]\nINFY\n"


def test_a_read_only_file_says_so(tmp_path, monkeypatch):
    b, path = baskets(tmp_path)
    monkeypatch.setattr(os, "access", lambda p, mode: False if str(p) == str(path) else True)
    with pytest.raises(PermissionError, match="read-only"):
        b.define("banks", ["A"])
    assert path.read_text() == TEXT


# ---- relative() and the console functions ---------------------------------------------------------------------------------------------------------------
def test_relative_takes_a_basket_by_name_from_the_file(tmp_path):
    b, _ = baskets(tmp_path, "[pair]\nS\nA\nB\n")
    from tests.test_relative import Reader
    reader = Reader({"S": opens_at(102), "A": opens_at(101), "B": opens_at(103)})
    s = AbsoluteService(Clock(IST, NSE_SESSION), MemorySource(), now=lambda: epoch(TODAY, 11), today_bars_many=reader, baskets=b)
    r = s.relative("pct_open", "intraday", "S", "pair")                                                    # S is in its own basket: left out
    assert r.symbols == ["A", "B"] and r[1] == 1


@pytest.fixture
def console(tmp_path, monkeypatch):
    import algo
    from algo.core import app as app_module
    from algo.core.config import Paths
    futs = pd.DataFrame({"fut_token": [11], "TckrSymb": ["NIFTY"], "expiry": [pd.Timestamp("2026-03-17")]})
    master = types.SimpleNamespace(spot_of={"ABC": 7, "XYZ": 8}, token_of={}, futs=futs)
    candles = Table(pd.DataFrame({"token": [7, 7, 8], "Ts": [epoch(TODAY, 0), epoch(TODAY, 1), epoch(TODAY, 0)], "Open": [1.0, 2.0, 5.0], "High": [1.0, 2.0, 5.0],
                                  "Low": [1.0, 2.0, 5.0], "Close": [1.5, 2.5, 5.5], "Volume": [1, 2, 3]}))
    dhmod, dhp = types.ModuleType("deephaven"), types.ModuleType("deephaven.pandas")
    dhp.to_pandas = lambda t: t.df
    dhp.to_table = lambda df: types.SimpleNamespace(df=df)
    monkeypatch.setitem(sys.modules, "deephaven", dhmod)
    monkeypatch.setitem(sys.modules, "deephaven.pandas", dhp)
    monkeypatch.setattr(app_module, "_APP", None)
    md, work = tmp_path / "md", tmp_path / "work"
    os.makedirs(md)
    ns = {}
    app = algo.install(ns, "NSE", paths=Paths(market_data=str(md), work=str(work)))
    app.plugins["master"] = types.SimpleNamespace(data=master)
    app.tables["candles"] = candles
    return ns, app, md, work


def test_the_console_creates_edits_and_deletes_baskets_in_the_file(console, capsys):
    ns, app, md, work = console
    assert {"relative", "basket"} <= set(ns)
    assert ns["basket"]() == [] and "none" in capsys.readouterr().out
    assert ns["basket"]("pair", ["xyz"]) == ["XYZ"]
    path = md / "baskets.txt"
    assert path.read_text() == "[pair]\nXYZ\n"                                                              # the market-data folder is writable here, so the file starts there
    assert ns["basket"]("pair", add=["ABC"]) == ["XYZ", "ABC"] and ns["basket"]("pair") == ["XYZ", "ABC"] and ns["basket"]() == ["pair"]
    path.write_text("# by hand\n" + path.read_text() + "\n[more]\nTCS\n")                                   # edited by hand: the console sees it
    assert ns["basket"]() == ["more", "pair"]
    assert ns["basket"]("pair", remove=["xyz"]) == ["ABC"] and "# by hand" in path.read_text() and "[more]" in path.read_text()
    r = ns["relative"]("pct_open", "intraday", "ABC", ["XYZ"], at="2026-03-16 09:19")
    assert r.symbols == ["XYZ"] and r.n_valid == 1
    ns["basket"]("duo", ["XYZ", "ABC"])
    r = ns["relative"]("pct_open", "intraday", "ABC", "duo", at="2026-03-16 09:19")                          # by name; ABC is in it and is left out
    assert r.symbols == ["XYZ"] and r.today == "2026-03-16" and r.n_valid == 1
    assert ns["basket"]("duo", delete=True) == [] and ns["basket"]() == ["more", "pair"]
    with pytest.raises(KeyError):
        ns["relative"]("pct_open", "intraday", "ABC", "duo")


def test_another_file_can_be_named_and_the_change_keeps_working(console):
    ns, app, md, work = console
    other = work / "mine" / "b.txt"
    ns["absolute_settings"](basket_file=str(other))
    ns["basket"]("x", ["TCS"])
    assert other.read_text() == "[x]\nTCS\n" and not (md / "baskets.txt").exists()
    assert ns["basket"]("x") == ["TCS"]


# ---- relative(series, level, symbol): the basket the symbol is defined in ------------------------------------------------------------------------------------------
def own_service(tmp_path, text, data):
    from tests.test_relative import Reader
    b, _ = baskets(tmp_path, text)
    return AbsoluteService(Clock(IST, NSE_SESSION), MemorySource(), now=lambda: epoch(TODAY, 11), today_bars_many=Reader(data), baskets=b)


def test_without_a_basket_the_symbol_is_ranked_in_the_basket_it_is_defined_in(tmp_path):
    s = own_service(tmp_path, "[banks]\nHDFC\nICICI\nSBIN\n[it]\nTCS\nINFY\n",
                    {"HDFC": opens_at(102), "ICICI": opens_at(101), "SBIN": opens_at(103), "TCS": opens_at(150), "INFY": opens_at(90)})
    r = s.relative("pct_open", "intraday", "hdfc")
    assert r.basket == "banks" and r.symbols == ["ICICI", "SBIN"] and r[0].tolist() == pytest.approx([1.0, 3.0]) and r[1] == 1                 # not ranked against the IT basket
    assert s.relative("pct_open", "intraday", "TCS").symbols == ["INFY"]


def test_an_explicit_basket_ranks_the_symbol_there_whether_or_not_it_is_in_it(tmp_path):
    s = own_service(tmp_path, "[banks]\nHDFC\nICICI\nSBIN\n[it]\nTCS\nINFY\n",
                    {"HDFC": opens_at(102), "ICICI": opens_at(101), "SBIN": opens_at(103), "TCS": opens_at(150), "INFY": opens_at(90)})
    inside = s.relative("pct_open", "intraday", "HDFC", "banks")
    assert inside.basket == "banks" and inside.symbols == ["ICICI", "SBIN"]                                                                   # it is in the basket: left out of the values
    outside = s.relative("pct_open", "intraday", "HDFC", "it")                                                                                # it is not: ranked against the other basket
    assert outside.basket == "it" and outside.symbols == ["TCS", "INFY"] and outside[0].tolist() == pytest.approx([50.0, -10.0]) and outside[1] == 1
    assert s.relative("pct_open", "intraday", "HDFC", ["ICICI", "TCS"]).basket is None                                                         # a list has no name


def test_a_symbol_in_no_basket_or_in_several_says_what_to_do(tmp_path):
    s = own_service(tmp_path, "[banks]\nHDFC\nICICI\n[big]\nHDFC\nTCS\n", {"HDFC": opens_at(102)})
    with pytest.raises(ValueError, match="in no basket") as e:
        s.relative("pct_open", "intraday", "NOBODY")
    assert "relative('pct_open', 'intraday', 'NOBODY', 'banks')" in str(e.value) and "banks, big" in str(e.value)
    with pytest.raises(ValueError, match="in several baskets: banks, big") as e:
        s.relative("pct_open", "intraday", "HDFC")
    assert "Say which" in str(e.value)
    assert s.relative("pct_open", "intraday", "HDFC", "big").symbols == ["TCS"]


def test_futures_and_cash_are_different_symbols_when_finding_the_basket(tmp_path):
    s = own_service(tmp_path, "[idx]\nNIFTY FUT\nBANKNIFTY FUT\n", {"NIFTY": opens_at(101), "BANKNIFTY": opens_at(102)})
    assert s.relative("pct_open", "intraday", "nifty fut").basket == "idx"
    with pytest.raises(ValueError, match="in no basket"):
        s.relative("pct_open", "intraday", "NIFTY")                                                                                           # the cash index is not the future


def test_the_console_form_without_a_basket(console):
    ns, app, md, work = console
    ns["basket"]("pair", ["ABC", "XYZ"])
    r = ns["relative"]("pct_open", "intraday", "ABC", at="2026-03-16 09:19")
    assert r.basket == "pair" and r.symbols == ["XYZ"] and r.n_valid == 1
