"""Picking the right contract file, and refusing the other market's file."""
import pandas as pd
import pytest

import algo
from algo.core import app as app_module
from algo.core.config import NSE, US, Paths
from algo.data.master import check_exchange, db_candidates, find_db


def frame(*codes):
    return pd.DataFrame({"Token": range(len(codes)), "Exch": list(codes)})


# ---- the exchange guard --------------------------------------------------------------------------------
def test_each_market_accepts_its_own_file():
    check_exchange(frame("NSE", "NSE"), NSE)
    check_exchange(frame("XCBO", "XNAS", "XCBO"), US)
    check_exchange(frame("XNYS"), US)


def test_each_market_refuses_the_others_file():
    with pytest.raises(RuntimeError, match=r"\['XCBO', 'XNAS'\].*not NSE"):
        check_exchange(frame("XCBO", "XNAS"), NSE, "20261014DB.db3")
    with pytest.raises(RuntimeError, match=r"\['NSE'\].*not US.*file="):
        check_exchange(frame("NSE"), US, "20261015DB.db3")
    with pytest.raises(RuntimeError):
        check_exchange(frame("NSE", "XCBO"), NSE)                         # a file mixing both is no good for either
    with pytest.raises(RuntimeError):
        check_exchange(frame("NSE", "XCBO"), US)


def test_spelling_and_blanks_do_not_matter_and_a_file_without_the_column_is_not_checked():
    check_exchange(frame(" nse ", "NSE", None), NSE)
    check_exchange(pd.DataFrame({"Token": [1]}), NSE)
    check_exchange(pd.DataFrame({"Token": [1]}), US)


# ---- choosing between files -----------------------------------------------------------------------------
def touch(folder, *names):
    for n in names:
        (folder / n).write_bytes(b"x")


def test_the_exact_name_wins_and_otherwise_the_last_match(tmp_path):
    touch(tmp_path, "20261015DB.db3", "20261015.db3", "20261014DB.db3")
    assert find_db(str(tmp_path), "20261015").endswith("20261015.db3")
    assert [p.split("\\")[-1].split("/")[-1] for p in db_candidates(str(tmp_path), "20261015")] == ["20261015.db3", "20261015DB.db3"]


def test_without_an_exact_name_the_last_sorted_match_is_used(tmp_path):
    touch(tmp_path, "20261015DB.db3", "20261015DB_US.db3")
    assert find_db(str(tmp_path), "20261015").endswith("20261015DB_US.db3")
    assert len(db_candidates(str(tmp_path), "20261015")) == 2


def test_no_file_says_which_day_and_where(tmp_path):
    with pytest.raises(FileNotFoundError, match="no .db3 for 20261015"):
        find_db(str(tmp_path), "20261015")
    assert db_candidates(str(tmp_path), "20261015") == []


# ---- master_build(file=...) ------------------------------------------------------------------------------
def test_master_build_can_name_the_file(tmp_path, monkeypatch):
    from algo import api
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    algo.install(ns, "US", Paths(market_data=str(tmp_path / "md"), work=str(tmp_path / "w")))
    seen = []
    monkeypatch.setattr(api, "_run", lambda plugin: seen.append(plugin))
    ns["master_build"]("US", file="20261015DB_US.db3")
    assert seen[-1].db_path == str(tmp_path / "md" / "20261015DB_US.db3")
    absolute = str(tmp_path / "elsewhere" / "x.db3")
    ns["master_build"](file=absolute)
    assert seen[-1].db_path == absolute
    ns["master_build"]()
    assert seen[-1].db_path is None
