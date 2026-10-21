"""The algo's own state in Postgres instead of JSON files: the portfolio registry, each strategy's book (PF map and subscriptions) and the limits.

Switched on by the environment variable ALGO_STATE_STORE=postgres (the compose file of each server sets it); without it everything stays in the JSON files as before.
The connection is the one the contract master uses (algo.core.pg.pg_settings: AUTOMATION_DATABASE_URL / ALGO_PG_PASSWORD, host `postgres`, database `algofusion`).

Four small tables, created if they are missing (nothing existing is touched):

    algo_portfolios   user_id, pf, owner, name, algo, created, note, updated_at          primary key (user_id, pf)   the registry: every number ever handed out
    algo_pf_map       book, token, pf                                                    primary key (book, token)   a book's future token -> PF
    algo_pf_sub       book, pf, option_token                                             primary key (book, pf)      a book's PF -> option token it is subscribed to
    algo_limits       strategy, turnover, m2m, turnover_hit, m2m_hit, updated_at         primary key (strategy)      the turnover / M2M limits and their latches

`book` is the name of the book's file (`pf_map_nifty_rsi_book.json`), so the old files and the rows line up. The first time a store is opened empty and its old JSON file
exists, the file is IMPORTED into the table (the file is left as it is, as a backup); after that the database is the only source and the file is not read or written.
If Postgres cannot be reached the store refuses to start, with the reason: starting from nothing would hand out numbers that are already in use.
"""
from __future__ import annotations

import os
import threading

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS algo_portfolios (
        user_id integer NOT NULL, pf bigint NOT NULL, owner text NOT NULL, name text, algo text, created text, note text,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, pf))""",
    """CREATE TABLE IF NOT EXISTS algo_pf_map (
        book text NOT NULL, token bigint NOT NULL, pf bigint NOT NULL, PRIMARY KEY (book, token))""",
    """CREATE TABLE IF NOT EXISTS algo_pf_sub (
        book text NOT NULL, pf bigint NOT NULL, option_token bigint NOT NULL, PRIMARY KEY (book, pf))""",
    """CREATE TABLE IF NOT EXISTS algo_limits (
        strategy text NOT NULL, turnover double precision, m2m double precision, turnover_hit boolean NOT NULL DEFAULT false,
        m2m_hit boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (strategy))""",
)

UPSERT_PORTFOLIO = ("INSERT INTO algo_portfolios (user_id, pf, owner, name, algo, created, note) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (user_id, pf) DO UPDATE SET owner = EXCLUDED.owner, name = EXCLUDED.name, algo = EXCLUDED.algo, "
                    "created = EXCLUDED.created, note = EXCLUDED.note, updated_at = CURRENT_TIMESTAMP")
UPSERT_LIMIT = ("INSERT INTO algo_limits (strategy, turnover, m2m, turnover_hit, m2m_hit) VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (strategy) DO UPDATE SET turnover = EXCLUDED.turnover, m2m = EXCLUDED.m2m, turnover_hit = EXCLUDED.turnover_hit, "
                "m2m_hit = EXCLUDED.m2m_hit, updated_at = CURRENT_TIMESTAMP")


def backend() -> str:
    """"postgres" when ALGO_STATE_STORE says so, else "file"."""
    value = os.environ.get("ALGO_STATE_STORE", "").strip().lower()
    return "postgres" if value in ("postgres", "pg", "postgresql") else "file"


class StateDB:
    """A small, thread-safe wrapper over one DB-API connection: every call commits, and a call that fails (a dropped connection) is tried once more on a new one.
    The statements are all idempotent (upserts, replace-all), so trying twice is safe. `connect` and `paramstyle` exist so a test can use another database."""

    def __init__(self, connect=None, paramstyle: str = "%s"):
        if connect is None:
            from algo.core.pg import pg_connect
            connect = lambda: pg_connect(None)                       # noqa: E731
        self._connect, self._style = connect, paramstyle
        self._lock = threading.RLock()
        self._con = None
        try:
            self.tx([(ddl, ()) for ddl in SCHEMA])
        except Exception as e:
            raise RuntimeError(f"the algo state is set to live in Postgres (ALGO_STATE_STORE=postgres) but Postgres cannot be used: {e!r}. "
                               f"Nothing was started: the portfolio numbers must not be handed out from nothing") from None

    def _sql(self, sql: str) -> str:
        return sql if self._style == "%s" else sql.replace("%s", self._style)

    def _drop(self) -> None:
        try:
            if self._con is not None:
                self._con.close()
        except Exception:
            pass
        self._con = None

    def tx(self, statements, fetch: bool = False):
        """Run [(sql, params)] in ONE transaction and commit. A params that is a list of tuples (not a tuple) is run as executemany. Returns the rows of the last
        statement when fetch=True."""
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._con is None:
                        self._con = self._connect()
                    cur, rows = self._con.cursor(), None
                    try:
                        for sql, params in statements:
                            if isinstance(params, list):
                                if params:
                                    cur.executemany(self._sql(sql), params)
                            else:
                                cur.execute(self._sql(sql), params)
                        if fetch:
                            rows = cur.fetchall()
                    finally:
                        cur.close()
                    self._con.commit()
                    return rows
                except Exception:
                    try:
                        self._con.rollback()
                    except Exception:
                        pass
                    self._drop()
                    if attempt == 2:
                        raise

    def run(self, sql: str, params=(), fetch: bool = False):
        return self.tx([(sql, params)], fetch)

    # ---- the registry --------------------------------------------------------------------------------------------------
    def load_portfolios(self) -> list[tuple]:
        return self.run("SELECT user_id, pf, owner, name, algo, created, note FROM algo_portfolios ORDER BY user_id, pf", fetch=True) or []

    def save_portfolio(self, user: int, pf: int, rec: dict) -> None:
        self.run(UPSERT_PORTFOLIO, (int(user), int(pf), rec["owner"], rec.get("name", ""), rec.get("algo", ""), rec.get("created", ""), rec.get("note", "")))

    def save_portfolios(self, rows: list[tuple]) -> None:
        """Import many: (user, pf, owner, name, algo, created, note)."""
        self.tx([(UPSERT_PORTFOLIO, [(int(u), int(p), o, n, a, c, t) for u, p, o, n, a, c, t in rows])])

    # ---- a book --------------------------------------------------------------------------------------------------------
    def load_ints(self, table: str, book: str) -> "dict[int, int] | None":
        """A book's map (`pf_map`: token -> pf, or `pf_sub`: pf -> option token); None when the book has no rows at all."""
        cols = {"algo_pf_map": ("token", "pf"), "algo_pf_sub": ("pf", "option_token")}[table]
        rows = self.run(f"SELECT {cols[0]}, {cols[1]} FROM {table} WHERE book = %s", (book,), fetch=True)
        return {int(k): int(v) for k, v in rows} if rows else None

    def save_ints(self, table: str, book: str, data: dict) -> None:
        """Replace the book's rows with `data`, in one transaction."""
        cols = {"algo_pf_map": ("token", "pf"), "algo_pf_sub": ("pf", "option_token")}[table]
        self.tx([(f"DELETE FROM {table} WHERE book = %s", (book,)),
                 (f"INSERT INTO {table} (book, {cols[0]}, {cols[1]}) VALUES (%s, %s, %s)", [(book, int(k), int(v)) for k, v in data.items()])])

    # ---- the limits ----------------------------------------------------------------------------------------------------
    def load_limits(self) -> list[tuple]:
        return self.run("SELECT strategy, turnover, m2m, turnover_hit, m2m_hit FROM algo_limits", fetch=True) or []

    def save_limits(self, data: dict) -> None:
        self.tx([(UPSERT_LIMIT, [(name, r.get("turnover"), r.get("m2m"), bool(r.get("turnover_hit")), bool(r.get("m2m_hit"))) for name, r in data.items()])])


def state_db(shared: dict) -> "StateDB | None":
    """The one StateDB of an app (kept in `app.shared`), or None when the state lives in files. Raises (with the reason) if Postgres was asked for and cannot be used."""
    if backend() != "postgres":
        return None
    db = shared.get("state_db")
    if db is None:
        db = shared["state_db"] = StateDB()
    return db
