"""The algo's state (registry, books, limits) in Postgres instead of JSON files. The tests use SQLite as the database (same SQL, the `?` placeholder), so no server is needed;
the same flows were also run once against the real Postgres of the India server, in a scratch schema that was dropped afterwards."""
import json
import os
import sqlite3

import pytest

from algo.core import statedb
from algo.core.limits import Limits, limits_of
from algo.core.statedb import StateDB, backend, state_db
from algo.strategy.book import Portfolios
from algo.strategy.registry import Registry


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "state.sqlite")
    return StateDB(connect=lambda: sqlite3.connect(path, check_same_thread=False), paramstyle="?"), path


def reopen(path):
    return StateDB(connect=lambda: sqlite3.connect(path, check_same_thread=False), paramstyle="?")


def test_the_backend_is_the_files_unless_the_environment_says_postgres(monkeypatch):
    monkeypatch.delenv("ALGO_STATE_STORE", raising=False)
    assert backend() == "file" and state_db({}) is None
    for value in ("postgres", "PG", " Postgres "):
        monkeypatch.setenv("ALGO_STATE_STORE", value)
        assert backend() == "postgres"
    monkeypatch.setenv("ALGO_STATE_STORE", "files")
    assert backend() == "file"


def test_state_db_is_made_once_and_kept(monkeypatch, db):
    made = []
    monkeypatch.setenv("ALGO_STATE_STORE", "postgres")
    monkeypatch.setattr(statedb, "StateDB", lambda: made.append(1) or db[0])
    shared = {}
    assert state_db(shared) is db[0] and state_db(shared) is db[0] and made == [1]


def test_postgres_that_cannot_be_used_stops_the_start_with_the_reason():
    def broken():
        raise OSError("connection refused")
    with pytest.raises(RuntimeError, match="cannot be used.*connection refused.*must not be handed out from nothing"):
        StateDB(connect=broken, paramstyle="?")


def test_a_dropped_connection_is_retried_once_on_a_new_one(tmp_path):
    path = str(tmp_path / "s.sqlite")
    opened = []

    class Flaky:
        """The first connection dies on its first statement after the schema is made."""

        def __init__(self):
            self.con = sqlite3.connect(path, check_same_thread=False)
            self.n = len(opened)
            opened.append(self)
            self.calls = 0

        def cursor(self):
            self.calls += 1
            if self.n == 1 and self.calls == 1:
                raise sqlite3.OperationalError("server closed the connection")
            return self.con.cursor()

        def commit(self): self.con.commit()
        def rollback(self): self.con.rollback()
        def close(self): self.con.close()

    d = StateDB(connect=Flaky, paramstyle="?")
    d._drop()                                                        # the schema call used connection 0; the next call opens connection 1, which dies once
    d.save_portfolio(3, 1001, {"owner": "book", "name": "A", "algo": "x", "created": "t", "note": ""})
    assert len(opened) == 3 and d.load_portfolios()[0][:3] == (3, 1001, "book")


# =============================== the registry ===============================
REC = dict(owner="book_a", name="RELIANCE", algo="rsi", created="2026-10-21T09:00:00", note="")


def test_the_registry_keeps_every_number_in_the_table_and_a_new_one_sees_them(tmp_path, db):
    d, path = db
    r = Registry(str(tmp_path / "unused.json"), db=d)
    assert r.allocate(3, "book_a", start=1001, algo="rsi", name="A") == 1001
    assert r.allocate(3, "book_a", start=1001, algo="rsi", name="B") == 1002
    assert r.register(4, 1001, "console").status == "registered"
    assert len(open(tmp_path / "unused.json").read().splitlines()) == 3          # the backup copy: one line per change, same format as the file store
    again = Registry(str(tmp_path / "unused.json"), db=reopen(path))             # a restart
    assert again.portfolios() == r.portfolios() and again.count() == (2, 3)
    assert again.allocate(3, "book_a", start=1001) == 1003                       # numbers are never reused
    claim = again.register(3, 1001, "someone_else")
    assert not claim and claim.held_by == "book_a"
    assert again.register(3, 1001, "book_a", note="hi").status == "updated"
    assert Registry(str(tmp_path / "x.json"), db=reopen(path)).get(3, 1001)["note"] == "hi"


def test_an_old_registry_file_is_imported_once_and_left_alone(tmp_path, db):
    d, path = db
    f = tmp_path / "portfolio_registry.json"
    old = Registry(str(f))                                                       # the file store, as it was
    old.allocate(3, "book_a", start=1001, algo="rsi", name="A")
    old.allocate(3, "book_a", start=1001, algo="rsi", name="B")
    before = f.read_bytes()
    r = Registry(str(f), db=d)
    assert [row[:2] for row in r.portfolios()] == [(3, 1001), (3, 1002)] and f.read_bytes() == before
    assert len(d.load_portfolios()) == 2                                         # now in the table
    old.allocate(3, "book_a", start=1001)                                        # the file moves on without the table (a second store): the table is the source
    assert Registry(str(f), db=reopen(path)).count() == (1, 2)


def test_a_failed_write_is_raised_and_the_number_is_not_kept(tmp_path, db):
    d, path = db
    r = Registry(str(tmp_path / "x.json"), db=d)
    r.allocate(3, "book_a", start=1001)
    d._connect = lambda: (_ for _ in ()).throw(OSError("postgres is down"))
    d._drop()
    with pytest.raises(OSError):
        r.allocate(3, "book_a", start=1001)
    assert r.count() == (1, 1)                                                   # the same number is handed out again when Postgres is back
    d._connect = lambda: sqlite3.connect(path, check_same_thread=False)
    assert r.allocate(3, "book_a", start=1001) == 1002


# =============================== a book ===============================
def test_a_book_keeps_its_two_maps_in_the_tables_by_file_name(tmp_path, db):
    d, path = db
    a = Portfolios(str(tmp_path / "pf_map_a.json"), str(tmp_path / "pf_sub_a.json"), db=d)
    b = Portfolios(str(tmp_path / "pf_map_b.json"), str(tmp_path / "pf_sub_b.json"), db=d)
    a.pf_map[111], a.pf_map[222] = 1001, 1002
    a.save_map()
    a.subscribe(1001, 9001)
    b.pf_map[111] = 2001
    b.save_map()
    a2 = Portfolios(str(tmp_path / "pf_map_a.json"), str(tmp_path / "pf_sub_a.json"), db=reopen(path))
    b2 = Portfolios(str(tmp_path / "pf_map_b.json"), str(tmp_path / "pf_sub_b.json"), db=reopen(path))
    assert a2.pf_map == {111: 1001, 222: 1002} and a2.subscribed == {1001: 9001}
    assert b2.pf_map == {111: 2001} and b2.subscribed == {}                      # books do not mix
    a2.pf_map[222] = 1005                                                        # a changed entry replaces the old rows
    a2.save_map()
    assert Portfolios(str(tmp_path / "pf_map_a.json"), str(tmp_path / "pf_sub_a.json"), db=reopen(path)).pf_map == {111: 1001, 222: 1005}
    assert json.loads((tmp_path / "pf_map_a.json").read_text()) == {"111": 1001, "222": 1005}      # the backup copy follows
    assert json.loads((tmp_path / "pf_sub_a.json").read_text()) == {"1001": 9001}


def test_old_book_files_are_imported_once(tmp_path, db):
    d, path = db
    (tmp_path / "pf_map_a.json").write_text(json.dumps({"111": 1001}))
    (tmp_path / "pf_sub_a.json").write_text(json.dumps({"1001": 9001}))
    p = Portfolios(str(tmp_path / "pf_map_a.json"), str(tmp_path / "pf_sub_a.json"), db=d)
    assert p.pf_map == {111: 1001} and p.subscribed == {1001: 9001}
    (tmp_path / "pf_map_a.json").write_text(json.dumps({"111": 1001, "222": 1002}))   # the file changes afterwards: the table is the source now
    assert Portfolios(str(tmp_path / "pf_map_a.json"), str(tmp_path / "pf_sub_a.json"), db=reopen(path)).pf_map == {111: 1001}


# =============================== the limits ===============================
def test_limits_and_their_latches_survive_a_restart_in_the_table(tmp_path, db):
    d, path = db
    lim = Limits(str(tmp_path / "limits.json"), db=d)
    lim.set_limit("nifty_rsi", "turnover", 50000)
    lim.set_limit("nifty_rsi", "m2m", -5000)
    lim.update("nifty_rsi", 60000.0, -100.0)                                     # the turnover limit is reached: latched
    assert lim.get("nifty_rsi")["turnover_hit"]
    again = Limits(str(tmp_path / "limits.json"), db=reopen(path))
    r = again.get("nifty_rsi")
    assert (r["turnover"], r["m2m"], r["turnover_hit"], r["m2m_hit"]) == (50000.0, -5000.0, True, False)
    assert json.loads((tmp_path / "limits.json").read_text())["nifty_rsi"]["turnover_hit"] is True    # the backup copy


def test_an_old_limits_file_is_imported_once(tmp_path, db):
    d, path = db
    f = tmp_path / "limits.json"
    f.write_text(json.dumps({"s1": {"turnover": 1000, "m2m": None, "turnover_hit": True, "m2m_hit": False}}))
    lim = Limits(str(f), db=d)
    assert lim.get("s1")["turnover"] == 1000.0 and lim.get("s1")["turnover_hit"]
    f.write_text("{}")
    assert Limits(str(f), db=reopen(path)).get("s1")["turnover"] == 1000.0


def test_limits_of_uses_the_state_db_when_postgres_is_chosen(monkeypatch, tmp_path, db):
    monkeypatch.setenv("ALGO_STATE_STORE", "postgres")
    monkeypatch.setattr(statedb, "StateDB", lambda: db[0])
    shared = {}
    assert limits_of(shared, str(tmp_path / "limits.json")).db is db[0]


def test_the_backup_files_are_a_mirror_whose_failure_never_stops_the_database_write(tmp_path, db):
    d, path = db
    blocked = tmp_path / "not_a_folder"
    blocked.write_text("a file where the folder should be")
    bad = str(blocked / "portfolio_registry.json")                                # cannot be written: its parent is a file
    r = Registry(bad, db=d)
    assert r.allocate(3, "book_a", start=1001) == 1001                            # still handed out and kept in Postgres
    assert Registry(bad, db=reopen(path)).get(3, 1001)["owner"] == "book_a"
    p = Portfolios(str(blocked / "pf_map_a.json"), str(blocked / "pf_sub_a.json"), db=d)
    p.pf_map[1] = 2
    p.save_map()
    p.subscribe(2, 3)
    assert Portfolios(str(blocked / "pf_map_a.json"), str(blocked / "pf_sub_a.json"), db=reopen(path)).pf_map == {1: 2}
    lim = Limits(str(blocked / "limits.json"), db=d)
    lim.set_limit("s", "turnover", 5)
    assert Limits(str(blocked / "limits.json"), db=reopen(path)).get("s")["turnover"] == 5.0


def test_at_start_the_backup_file_is_made_to_say_what_the_table_says(tmp_path, db):
    d, path = db
    r = Registry(str(tmp_path / "r.json"), db=d)
    r.allocate(3, "book_a", start=1001)
    r.allocate(3, "book_a", start=1001)
    (tmp_path / "r.json").write_text("")                                          # the backup was lost or emptied
    Registry(str(tmp_path / "r.json"), db=reopen(path))                           # a restart rewrites it from the table
    assert len((tmp_path / "r.json").read_text().splitlines()) == 2
