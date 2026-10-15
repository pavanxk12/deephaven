"""End-of-day files: whole tables to Parquet and back, safe against crashes, in a folder you choose.

The Deephaven table is a stand-in backed by pandas; the Parquet files are real (pyarrow)."""
import json
import os
import re
import sys
import types
from datetime import datetime, time as dtime

import pandas as pd
import pytest

pytest.importorskip("pyarrow")

import algo
from algo.core import app as app_module
from algo.core.config import Paths
from algo.data.store import Eod, History, Store, normalise_day, save_tables


class FT:
    """Table stand-in: a DataFrame with the few methods the store uses."""

    def __init__(self, df):
        self.df = df.reset_index(drop=True)
        self.snapshots = 0

    @property
    def size(self):
        return len(self.df)

    @property
    def column_names(self):
        return list(self.df.columns)

    def snapshot(self):
        self.snapshots += 1
        return FT(self.df.copy())

    def update_view(self, expr):
        name, value = re.fullmatch(r"(\w+) = `(.*)`", expr).groups()
        return FT(self.df.assign(**{name: value}))


@pytest.fixture
def fake_parquet(monkeypatch):
    mod = types.ModuleType("deephaven.parquet")
    mod.write = lambda table, path: table.df.to_parquet(path)
    mod.read = lambda path: FT(pd.read_parquet(path))
    dh = types.ModuleType("deephaven")
    dh.parquet = mod
    dh.merge = lambda tables: FT(pd.concat([t.df for t in tables], ignore_index=True))
    monkeypatch.setitem(sys.modules, "deephaven", dh)
    monkeypatch.setitem(sys.modules, "deephaven.parquet", mod)
    return mod


@pytest.fixture
def session(tmp_path, fake_parquet, monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    paths = Paths(market_data=str(tmp_path / "md"), work=str(tmp_path / "work"), eod_dir=str(tmp_path / "eod"))
    ns = {}
    app = algo.install(ns, "NSE", paths)
    app.tables["candles"] = FT(pd.DataFrame({"Minute": [1, 2, 3], "token": [10, 10, 11], "Close": [1.5, 2.5, 3.5]}))
    app.tables["straddle_series"] = FT(pd.DataFrame({"Time": ["09:16:02", "09:17:02"], "Symbol": ["NIFTY"] * 2, "V": [190.0, 188.5]}))
    return ns, app, tmp_path


DAY = "2026-10-15"


# ---- saving ---------------------------------------------------------------------------------------------
def test_candles_and_straddle_are_saved_whole_with_a_date_and_a_meta_file(session):
    ns, app, tmp = session
    ns["eod_save"](day=DAY)
    eod = tmp / "eod"
    c = pd.read_parquet(eod / f"candles_{DAY}.parquet")
    s = pd.read_parquet(eod / f"straddle_{DAY}.parquet")
    assert list(c.columns) == ["Minute", "token", "Close", "Date"] and len(c) == 3 and set(c.Date) == {DAY}
    assert list(s.V) == [190.0, 188.5] and set(s.Date) == {DAY}
    meta = json.loads((eod / f"candles_{DAY}.meta.json").read_text())
    assert meta["rows"] == 3 and meta["market"] == "NSE" and meta["day"] == DAY and meta["columns"] == ["Minute", "token", "Close"]
    assert meta["file"] == f"candles_{DAY}.parquet" and meta["saved_at"].startswith("20")


def test_the_default_folder_is_paths_saved_and_the_day_defaults_to_today(tmp_path, fake_parquet, monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    app = algo.install({}, "NSE", Paths(market_data=str(tmp_path / "md"), work=str(tmp_path / "work")))
    app.tables["candles"] = FT(pd.DataFrame({"a": [1]}))
    save_tables(app, ("candles",))
    today = datetime.now(app.market.zone).strftime("%Y-%m-%d")
    assert (tmp_path / "work" / "fo_market_data" / f"candles_{today}.parquet").exists()


def test_a_folder_given_to_the_call_wins(session):
    ns, app, tmp = session
    ns["eod_save"](day=DAY, folder=str(tmp / "elsewhere"), tables=["candles"])
    assert (tmp / "elsewhere" / f"candles_{DAY}.parquet").exists() and not (tmp / "eod").exists()


def test_saving_again_replaces_the_file(session):
    ns, app, tmp = session
    ns["eod_save"](day=DAY)
    app.tables["candles"] = FT(pd.DataFrame({"Minute": [1], "token": [9], "Close": [9.9]}))
    ns["eod_save"](day=DAY)
    assert len(pd.read_parquet(tmp / "eod" / f"candles_{DAY}.parquet")) == 1
    assert not list((tmp / "eod").glob("*.tmp*"))


def test_snapshot_is_taken_unless_you_say_no(session):
    ns, app, _ = session
    ns["eod_save"](day=DAY, tables=["candles"])
    assert app.tables["candles"].snapshots == 1
    ns["eod_save"](day=DAY, tables=["candles"], snapshot=False)
    assert app.tables["candles"].snapshots == 1


def test_a_missing_or_empty_table_is_skipped_and_the_others_still_saved(session, capsys):
    ns, app, tmp = session
    app.tables["candles"] = FT(pd.DataFrame({"a": []}))
    del app.tables["straddle_series"]
    ns["eod_save"](day=DAY)
    out = capsys.readouterr().out
    assert "empty" in out and "missing" in out and not list((tmp / "eod").glob("*.parquet"))
    app.tables["straddle_series"] = FT(pd.DataFrame({"V": [1.0]}))
    ns["eod_save"](day=DAY)
    assert (tmp / "eod" / f"straddle_{DAY}.parquet").exists() and not (tmp / "eod" / f"candles_{DAY}.parquet").exists()


def test_a_crash_while_writing_leaves_the_old_file_untouched_and_no_temp_file(session, fake_parquet):
    ns, app, tmp = session
    ns["eod_save"](day=DAY, tables=["candles"])
    good = (tmp / "eod" / f"candles_{DAY}.parquet").read_bytes()

    def crash(table, path):
        with open(path, "wb") as f:
            f.write(b"half a file")
        raise OSError("disk full")
    fake_parquet.write = crash
    app.tables["candles"] = FT(pd.DataFrame({"Minute": [7]}))
    results = save_tables(app, ("candles",), DAY)
    assert results[0]["status"] == "failed"
    assert (tmp / "eod" / f"candles_{DAY}.parquet").read_bytes() == good
    assert not list((tmp / "eod").glob("*.tmp*"))


def test_an_unwritable_folder_says_so(session, monkeypatch):
    ns, app, tmp = session
    monkeypatch.setattr(os, "access", lambda *a, **k: False)
    with pytest.raises(PermissionError, match="not writable"):
        ns["eod_save"](day=DAY)


def test_only_known_tables_can_be_named(session):
    ns, _, _ = session
    with pytest.raises(ValueError, match="cannot save"):
        ns["eod_save"](tables=["orders"])


@pytest.mark.parametrize("given,expected", [("2026-10-15", "2026-10-15"), ("20261015", "2026-10-15")])
def test_both_day_spellings(given, expected):
    assert normalise_day(given, None) == expected


@pytest.mark.parametrize("bad", ["15-10-2026", "2026-13-01", "today", 20261015.5, ""])
def test_a_bad_day_is_refused(bad):
    with pytest.raises(ValueError, match="day must look like"):
        normalise_day(bad, None)


# ---- loading ----------------------------------------------------------------------------------------------
def test_a_saved_day_loads_back_into_named_tables(session):
    ns, app, _ = session
    ns["eod_save"](day=DAY)
    ns["eod_load"](DAY, tables=["candles", "straddle_series"])
    assert list(ns["candles_20261015"].df.Close) == [1.5, 2.5, 3.5] and list(ns["straddle_20261015"].df.V) == [190.0, 188.5]
    ns["eod_load"]("20261015", tables=["candles"])                            # the other spelling gives the same name


def test_all_saved_days_merge_into_one_table(session):
    ns, app, _ = session
    ns["eod_save"](day="2026-10-14")
    ns["eod_save"](day="2026-10-15")
    ns["eod_load"]()
    assert len(ns["candles_hist"].df) == 6 and set(ns["candles_hist"].df.Date) == {"2026-10-14", "2026-10-15"}
    assert len(ns["straddle_hist"].df) == 4


def test_loading_a_day_that_was_never_saved_just_says_so(session, capsys):
    ns, _, _ = session
    ns["eod_load"]("2026-01-01")
    assert "no candles_2026-01-01.parquet" in capsys.readouterr().out and "candles_20260101" not in ns


def test_loading_also_looks_in_the_read_only_history_folder(session):
    ns, app, tmp = session
    ns["eod_save"](day=DAY, folder=str(tmp / "md"))                         # files copied to the host folder mounted read-only
    ns["eod_load"](DAY, tables=["candles"])
    assert "candles_20261015" in ns


def test_temp_files_are_never_loaded(session):
    ns, app, tmp = session
    ns["eod_save"](day=DAY)
    (tmp / "eod" / "candles_2026-10-16.tmp.parquet").write_bytes(b"junk")
    ns["eod_load"]()
    assert len(ns["candles_hist"].df) == 3


def test_the_history_plugin_loads_every_day(session):
    ns, app, _ = session
    ns["eod_save"](day=DAY)
    app.run(History())
    assert "candles_hist" in ns and "straddle_hist" in ns


# ---- automatic daily save ---------------------------------------------------------------------------------------
def test_eod_auto_defaults_to_five_minutes_after_the_session_ends(session):
    ns, app, _ = session
    ns["eod_auto"]()
    plugin = app["eod"]
    assert plugin.auto_at == dtime(15, 35) and "15:35" in plugin.describe()
    ns["eod_auto"](stop=True)
    assert "eod" not in app.plugins


def test_eod_auto_takes_a_time_and_refuses_nonsense(session):
    ns, app, _ = session
    ns["eod_auto"]("16:10")
    assert app["eod"].auto_at == dtime(16, 10)
    for bad in ("25:00", "1535", "soon", 1535):
        with pytest.raises(ValueError, match="at must look like"):
            ns["eod_auto"](bad)
    ns["eod_auto"](stop=True)


def test_the_daily_job_saves_and_survives_a_failure(session):
    ns, app, tmp = session
    plugin = Eod(("candles",), auto_at=dtime(15, 35), folder=str(tmp / "eod"))
    plugin._app = app
    plugin.save()
    assert (tmp / "eod").exists() and plugin.last[0]["status"] == "saved"
    assert "candles 3" in plugin.describe()


def test_the_store_survives_an_empty_folder_listing(tmp_path, fake_parquet):
    store = Store(str(tmp_path / "none"))
    with pytest.raises(FileNotFoundError):
        store.load_all("candles_")
    with pytest.raises(FileNotFoundError):
        store.load_day("candles_", DAY)
