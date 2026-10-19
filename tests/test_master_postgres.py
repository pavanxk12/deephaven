"""The contract master read from Postgres (table resultset_<yyyymmdd>) instead of the .db3 file. The pg8000 driver is faked."""
import sqlite3
import sys
import types

import pandas as pd
import pytest

import algo
from algo.core import app as app_module
from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.data.master import Master, pg_row_count, read_contracts, read_contracts_pg
from tests.conftest import raw_contracts

DAY = "20261016"


class FakePg:
    """A Postgres with the tables you give it. Columns are kept in lower case, as the real table has them."""

    def __init__(self, tables, fail=None):
        self.tables, self.fail, self.sql = tables, fail, []

    def connect(self, **kw):
        if self.fail:
            raise self.fail
        self.sql.append(("connect", kw))
        return self.Conn(self)

    class Conn:
        def __init__(self, pg):
            self.pg = pg

        def cursor(self):
            return FakePg.Cur(self.pg)

        def close(self):
            pass


class _Cur:
    pass


class Cur:
    def __init__(self, pg):
        self.pg, self.rows = pg, []

    def execute(self, sql, params=None):
        self.pg.sql.append((sql, params))
        if "information_schema.columns" in sql:
            frame = self.pg.tables.get(params[0])
            self.rows = [] if frame is None else [(c,) for c in frame.columns]
        elif sql.startswith("SELECT count(*)"):
            name = sql.split('"')[1]
            if name not in self.pg.tables:
                raise RuntimeError(f'relation "{name}" does not exist')
            self.rows = [(len(self.pg.tables[name]),)]
        else:
            name = sql.split(' FROM "')[1].rstrip('"')
            frame = self.pg.tables[name]
            wanted = [part.split(" AS ") for part in sql[len("SELECT "):sql.index(" FROM ")].split(", ")]
            cols = [(src.strip('"'), dst.strip('"')) for src, dst in wanted]
            self.rows = list(frame[[s for s, _ in cols]].itertuples(index=False, name=None))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


FakePg.Cur = Cur


def pg_table(raw):
    """The raw contract rows the way Postgres keeps them: lower-case column names."""
    return raw.rename(columns=str.lower).assign(exch="NSE", divisor=100)


@pytest.fixture
def pg(monkeypatch):
    fake = FakePg({f"resultset_{DAY}": pg_table(raw_contracts()), "resultset_20261017": pg_table(raw_contracts()).iloc[0:0]})
    dbapi = types.SimpleNamespace(connect=fake.connect)
    monkeypatch.setitem(sys.modules, "pg8000", types.SimpleNamespace(dbapi=dbapi))
    monkeypatch.setitem(sys.modules, "pg8000.dbapi", dbapi)
    return fake


def test_the_columns_come_back_with_the_names_the_db3_file_gives(pg):
    raw, table = read_contracts_pg(DAY, NSE)
    assert table == f"resultset_{DAY}"
    assert {"Token", "Symbol", "ExpiryDate", "InstType", "OptionType", "StrikePrice", "LotSize", "TickSize", "Divisor", "Exch"} <= set(raw.columns)
    assert len(raw) == len(raw_contracts()) and set(raw["Token"]) == set(raw_contracts()["Token"])


def test_it_builds_the_same_master_as_the_db3_file_with_the_same_rows(pg, tmp_path):
    from algo.data.master import classify, normalise
    db = tmp_path / "x.db3"
    con = sqlite3.connect(db)
    raw_contracts().assign(Exch="NSE", Divisor=100).to_sql("ResultSet", con, index=False)
    con.close()
    from_file = classify(normalise(read_contracts(str(db), NSE)[0], NSE))
    from_pg = classify(normalise(read_contracts_pg(DAY, NSE)[0], NSE))
    assert from_pg.symbol_of == from_file.symbol_of and from_pg.tick_of == from_file.tick_of
    assert len(from_pg.opt) == len(from_file.opt) and len(from_pg.futs) == len(from_file.futs)


def test_the_day_is_checked_and_a_holiday_or_missing_table_says_so(pg):
    for bad in ("2026-10-16", "x", "20261016; drop table users"):
        with pytest.raises(ValueError, match="yyyymmdd"):
            read_contracts_pg(bad, NSE)
    with pytest.raises(RuntimeError, match="no table resultset_20260101"):
        read_contracts_pg("20260101", NSE)
    with pytest.raises(RuntimeError, match="empty"):
        read_contracts_pg("20261017", NSE)
    assert pg_row_count("20261017") == 0 and pg_row_count(DAY) == len(raw_contracts())
    with pytest.raises(RuntimeError, match="does not exist"):
        pg_row_count("20260101")


def test_a_table_without_the_needed_columns_is_refused(pg):
    pg.tables["resultset_20261018"] = pd.DataFrame({"token": [1], "symbol": ["X"]})
    with pytest.raises(RuntimeError, match="lacks columns"):
        read_contracts_pg("20261018", NSE)


def test_the_connection_uses_the_docker_service_and_the_password_from_the_environment(pg, monkeypatch):
    monkeypatch.setenv("ALGO_PG_PASSWORD", "from-the-environment")
    read_contracts_pg(DAY, NSE)
    kw = pg.sql[0][1]
    assert (kw["host"], kw["port"], kw["database"], kw["user"], kw["password"]) == ("postgres", 5432, "algofusion", "algofusion", "from-the-environment")
    read_contracts_pg(DAY, NSE, pg={"host": "10.0.0.5", "password": "given"})
    kw = [c[1] for c in pg.sql if c[0] == "connect"][-1]
    assert kw["host"] == "10.0.0.5" and kw["password"] == "given"


def test_without_any_driver_it_says_what_to_install(monkeypatch):
    for name in ("pg8000", "psycopg", "psycopg2"):
        monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(RuntimeError, match="pg8000, psycopg or psycopg2"):
        read_contracts_pg(DAY, NSE)


def test_psycopg_is_used_when_pg8000_is_not_there_and_the_container_url_gives_the_login(pg, monkeypatch):
    seen = []
    monkeypatch.setitem(sys.modules, "pg8000", None)                                  # no pg8000: the volume's psycopg
    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=lambda **kw: seen.append(kw) or pg.connect(**kw)))
    monkeypatch.delenv("ALGO_PG_PASSWORD", raising=False)
    monkeypatch.setenv("AUTOMATION_DATABASE_URL", "postgresql://robot:p%40ss@db.internal:5544/algofusion")
    read_contracts_pg(DAY, NSE)
    assert seen[0] == {"host": "db.internal", "port": 5544, "dbname": "algofusion", "user": "robot", "password": "p@ss", "connect_timeout": 15}


def test_the_settings_are_layered_defaults_then_the_container_url_then_the_password_variable_then_what_you_pass():
    from algo.data.master import pg_settings
    assert pg_settings(environ={}) == {"host": "postgres", "port": 5432, "database": "algofusion", "user": "algofusion"}
    env = {"AUTOMATION_DATABASE_URL": "postgresql://u:secret@h:1/d"}
    assert pg_settings(environ=env) == {"host": "h", "port": 1, "database": "d", "user": "u", "password": "secret"}
    assert pg_settings(environ={**env, "ALGO_PG_PASSWORD": "other"})["password"] == "other"
    assert pg_settings({"host": "x", "password": "mine"}, environ=env) == {"host": "x", "port": 1, "database": "d", "user": "u", "password": "mine"}


# ---- the Master plugin ----------------------------------------------------------------------------------------------------------------
def master_app(tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "_APP", None)
    return App({}, NSE, Paths(market_data=str(tmp_path / "md"), work=str(tmp_path / "w")))


def test_the_plugin_reads_postgres_and_caches_by_row_count(pg, tmp_path, monkeypatch):
    app = master_app(tmp_path, monkeypatch)
    app.run(Master(day=DAY, source="postgres"))
    assert app["master"].data.symbol_of[1] == "NIFTY 30JAN30 FUT" and "master" in app.ns
    reads = [s for s, _ in pg.sql if isinstance(s, str) and s.startswith("SELECT ") and " FROM \"resultset" in s and "count" not in s]
    assert len(reads) == 1
    app.run(Master(day=DAY, source="postgres"))                                  # again: the cache is used, the table is not read a second time
    reads = [s for s, _ in pg.sql if isinstance(s, str) and s.startswith("SELECT ") and " FROM \"resultset" in s and "count" not in s]
    assert len(reads) == 1
    pg.tables[f"resultset_{DAY}"] = pg.tables[f"resultset_{DAY}"].iloc[:-2]      # the table changed: a new row count is a new cache
    app.run(Master(day=DAY, source="postgres"))
    assert len([s for s, _ in pg.sql if isinstance(s, str) and " FROM \"resultset" in s and "count" not in s and s.startswith("SELECT ")]) == 2


def test_postgres_alone_fails_loudly_but_auto_falls_back_to_the_file(pg, tmp_path, monkeypatch):
    app = master_app(tmp_path, monkeypatch)
    pg.fail = ConnectionError("postgres is down")
    with pytest.raises(ConnectionError):
        app.run(Master(day=DAY, source="postgres"))
    md = tmp_path / "md"
    md.mkdir()
    con = sqlite3.connect(md / f"{DAY}DB.db3")
    raw_contracts().assign(Exch="NSE", Divisor=100).to_sql("ResultSet", con, index=False)
    con.close()
    app.run(Master(day=DAY, source="auto"))
    assert app["master"].data.symbol_of[1] == "NIFTY 30JAN30 FUT"               # read from the file


def test_a_holiday_in_postgres_falls_back_in_auto_mode(pg, tmp_path, monkeypatch):
    app = master_app(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="no contracts for 20261017"):
        app.run(Master(day="20261017", source="postgres"))


def test_the_default_is_still_the_file_and_a_named_file_wins(pg, tmp_path, monkeypatch):
    assert Master().source == "file"
    with pytest.raises(ValueError, match="source must be"):
        Master(source="mysql")
    app = master_app(tmp_path, monkeypatch)
    with pytest.raises(FileNotFoundError):
        app.run(Master(day=DAY, db_path=str(tmp_path / "nope.db3"), source="postgres"))     # file= means that file, Postgres is not asked
    assert not [c for c in pg.sql if c[0] == "connect"]


def test_master_build_passes_the_source(tmp_path, monkeypatch):
    from algo import api
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    algo.install(ns, "NSE", Paths(market_data=str(tmp_path / "md"), work=str(tmp_path / "w")))
    seen = []
    monkeypatch.setattr(api, "_run", lambda plugin: seen.append(plugin))
    ns["master_build"]("NSE", source="postgres", pg={"host": "h"})
    assert seen[-1].source == "postgres" and seen[-1].pg == {"host": "h"}
    ns["master_build"]()
    assert seen[-1].source == "file" and seen[-1].pg is None
