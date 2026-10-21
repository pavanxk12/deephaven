"""The Postgres connection every algo part uses (the contract master, the state store): where it is, the login, and whichever driver is installed."""
from __future__ import annotations

import os

PG_DEFAULTS = {"host": "postgres", "port": 5432, "database": "algofusion", "user": "algofusion"}      # the Docker service names; the password is never kept here
PG_LIBS = "/data/deephaven/operator/pylibs"                                                          # where the Deephaven volume keeps psycopg (the image has no Postgres driver)


def pg_settings(pg: dict | None = None, environ=None) -> dict:
    """Where and how to connect, in this order: the defaults, then the Postgres URL the container already has (AUTOMATION_DATABASE_URL or ALGO_DATABASE_URL),
    then ALGO_PG_PASSWORD, then what you pass in `pg`. The password is only ever held in memory."""
    from urllib.parse import unquote, urlparse
    env = os.environ if environ is None else environ
    cfg = dict(PG_DEFAULTS)
    url = env.get("ALGO_DATABASE_URL") or env.get("AUTOMATION_DATABASE_URL")
    if url:
        u = urlparse(url)
        for key, value in (("host", u.hostname), ("port", u.port), ("database", (u.path or "").lstrip("/") or None),
                           ("user", unquote(u.username) if u.username else None), ("password", unquote(u.password) if u.password else None)):
            if value:
                cfg[key] = value
    if env.get("ALGO_PG_PASSWORD"):
        cfg["password"] = env["ALGO_PG_PASSWORD"]
    cfg.update({k: v for k, v in (pg or {}).items() if v is not None})
    return cfg


def pg_connect(pg: dict | None):
    """A Postgres connection from whichever driver is there: pg8000, else psycopg (3) / psycopg2 (also looked for in the Deephaven volume's pylibs folder)."""
    import sys
    cfg = pg_settings(pg)
    host, port, db, user, password = cfg["host"], int(cfg["port"]), cfg["database"], cfg["user"], cfg.get("password")
    try:
        from pg8000 import dbapi
        return dbapi.connect(host=host, port=port, database=db, user=user, password=password, timeout=15)
    except ImportError:
        pass
    if os.path.isdir(PG_LIBS) and PG_LIBS not in sys.path:
        sys.path.append(PG_LIBS)
    for name in ("psycopg", "psycopg2"):
        try:
            driver = __import__(name)
        except ImportError:
            continue
        return driver.connect(host=host, port=port, dbname=db, user=user, password=password, connect_timeout=15)
    raise RuntimeError("reading the contracts from Postgres needs a driver: pg8000, psycopg or psycopg2 (pip install pg8000)")
