"""End of day: save whole tables (candles, straddle series) to Parquet files, and load them back.

    eod_save()                        # candles_<day>.parquet, straddle_<day>.parquet (+ a small .meta.json each)
    eod_save(folder="/data/eod")      # somewhere else, e.g. a folder mounted from the host
    eod_load("2026-10-15")            # -> tables candles_20261015, straddle_20261015
    eod_auto()                        # save by itself, 5 minutes after the session ends

Files are written under a temporary name and renamed when complete, so a crash never leaves a half-written file and
never damages the previous one. Every file has a `Date` column (the tables only carry a time of day).
"""
from __future__ import annotations

import glob
import json
import os
from datetime import datetime, time as dtime, timedelta

from algo.core import log as logmod
from algo.core.files import replace_with_retry
from algo.core.plugin import Plugin

_log = logmod.get("eod")

# table in the session -> file prefix, and which running plugin built it (for the .meta.json)
PREFIX = {"candles": "candles_", "straddle_series": "straddle_"}
PLUGIN = {"candles": "candles", "straddle_series": "straddle"}
DEFAULT_TABLES = ("candles", "straddle_series")


def normalise_day(day: "str | None", zone) -> str:
    """"2026-10-15" or "20261015" -> "2026-10-15"; None = today in the exchange timezone."""
    if day is None:
        return datetime.now(zone).strftime("%Y-%m-%d")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(str(day), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"day must look like 2026-10-15 or 20261015, got {day!r}")


class Store:
    """Parquet files in one folder, read back from that folder and optional extra folders."""

    def __init__(self, folder: str, extra_read_dirs: tuple[str, ...] = ()):
        self.folder, self.read_dirs = folder, (folder, *extra_read_dirs)

    def path(self, prefix: str, day: str) -> str:
        return os.path.join(self.folder, f"{prefix}{day}.parquet")

    def save(self, table, prefix: str, day: str, meta: dict | None = None, snapshot: bool = True) -> tuple[str, int] | None:
        """Write `table` (plus a Date column) to `<prefix><day>.parquet`. Returns (path, rows), or None for an empty table.

        snapshot=True first takes a frozen copy so the table can keep ticking while it is written (it needs the memory of
        one copy for a moment); snapshot=False writes the live table directly."""
        from deephaven import parquet

        os.makedirs(self.folder, exist_ok=True)
        if not os.access(self.folder, os.W_OK):
            raise PermissionError(f"{self.folder} is not writable (a read-only mount?). Use eod_save(folder=...) with a writable "
                                  f"folder, for example one mounted from the host")
        src = table.snapshot() if snapshot else table
        if src.size == 0:
            return None
        out = src.update_view(f"Date = `{day}`")                   # a lazy column: no copy of the data
        final = self.path(prefix, day)
        tmp = final[:-len(".parquet")] + ".tmp.parquet"
        try:
            parquet.write(out, tmp)
            replace_with_retry(tmp, final)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        if meta is not None:
            meta_path = final[:-len(".parquet")] + ".meta.json"
            with open(meta_path + ".tmp", "w") as f:
                json.dump({**meta, "rows": int(src.size), "file": os.path.basename(final)}, f, indent=1)
            replace_with_retry(meta_path + ".tmp", meta_path)
        return final, int(src.size)

    def find(self, prefix: str, day: str) -> str | None:
        for d in self.read_dirs:
            p = os.path.join(d, f"{prefix}{day}.parquet")
            if os.path.exists(p):
                return p
        return None

    def load_day(self, prefix: str, day: str):
        from deephaven import parquet
        p = self.find(prefix, day)
        if p is None:
            raise FileNotFoundError(f"no {prefix}{day}.parquet in {', '.join(self.read_dirs)}")
        return parquet.read(p)

    def load_all(self, prefix: str):
        """Merge every saved day with this prefix into one table. Returns (table, number of files)."""
        from deephaven import merge, parquet
        files = sorted({f for d in self.read_dirs for f in glob.glob(os.path.join(d, f"{prefix}*.parquet"))
                        if ".tmp." not in f})
        if not files:
            raise FileNotFoundError(f"no {prefix}*.parquet in {', '.join(self.read_dirs)}")
        return merge([parquet.read(f) for f in files]), len(files)


def make_store(app, folder: str | None = None) -> Store:
    return Store(folder or app.paths.saved, (app.paths.market_data,))


def save_tables(app, tables=DEFAULT_TABLES, day: str | None = None, folder: str | None = None,
                snapshot: bool = True) -> list[dict]:
    """Save the named session tables for `day`. A missing or empty table is skipped (and said so); a failing one does not
    stop the others. Returns one dict per table: name, status, rows, path."""
    store = make_store(app, folder)
    day = normalise_day(day, app.market.zone)
    results = []
    for name in tables:
        if name not in PREFIX:
            raise ValueError(f"cannot save {name!r}; choose from {sorted(PREFIX)}")
        try:
            table = app.table(name)
        except Exception:
            _log.warning("%s is not in the session, skipped", name)
            results.append({"table": name, "status": "missing", "rows": 0, "path": None})
            continue
        plugin = app.get(PLUGIN[name])
        meta = {"table": name, "day": day, "market": app.market.code, "timezone": app.market.tz,
                "saved_at": datetime.now(app.market.zone).isoformat(timespec="seconds"),
                "columns": list(getattr(table, "column_names", [])),
                "built_by": plugin.describe() if plugin is not None else None}
        try:
            out = store.save(table, PREFIX[name], day, meta, snapshot)
        except PermissionError:
            raise
        except Exception:
            _log.exception("%s save failed", name)
            results.append({"table": name, "status": "failed", "rows": 0, "path": None})
            continue
        if out is None:
            _log.info("%s is empty, nothing saved", name)
            results.append({"table": name, "status": "empty", "rows": 0, "path": None})
        else:
            _log.info("saved %s: %d rows -> %s", name, out[1], out[0])
            results.append({"table": name, "status": "saved", "rows": out[1], "path": out[0]})
    return results


class Eod(Plugin):
    """Saves the chosen tables once a day at `auto_at` (exchange time). `eod_auto()` starts it, `eod_save()` saves right now."""
    name = "eod"

    def __init__(self, tables=DEFAULT_TABLES, auto_at: dtime | None = None, folder: str | None = None,
                 snapshot: bool = True):
        super().__init__()
        self.tables, self.auto_at, self.folder, self.snapshot = tuple(tables), auto_at, folder, snapshot
        self.last: list[dict] = []

    def start(self) -> None:
        if self.auto_at is None:
            raise ValueError("Eod needs auto_at; use eod_save() to save right now")
        self.spawn(self._loop)
        self.log.info("will save %s every day at %s (%s)", ", ".join(self.tables), self.auto_at.strftime("%H:%M"),
                      self.app.market.tz)

    def describe(self) -> str:
        saved = ", ".join(f"{r['table']} {r['rows']:,}" for r in self.last if r["status"] == "saved")
        return f"{', '.join(self.tables)} at {self.auto_at:%H:%M}" + (f"; last: {saved}" if saved else "")

    def save(self) -> list[dict]:
        self.last = save_tables(self.app, self.tables, folder=self.folder, snapshot=self.snapshot)
        return self.last

    def _loop(self) -> None:
        zone = self.app.market.zone
        while not self.halted.is_set():
            now = datetime.now(zone)
            nxt = now.replace(hour=self.auto_at.hour, minute=self.auto_at.minute, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            if self.halted.wait((nxt - now).total_seconds()):
                return
            try:
                self.save()
            except Exception:
                self.log.exception("daily save failed; will try again tomorrow")


class History(Plugin):
    """Loads every saved day back as tables `<prefix>hist` (`candles_hist`, `straddle_hist`)."""
    name = "history"

    def __init__(self, prefixes: tuple[str, ...] = ("straddle_", "candles_"), folder: str | None = None):
        super().__init__()
        self.prefixes, self.folder = prefixes, folder

    def start(self) -> None:
        store = make_store(self.app, self.folder)
        for prefix in self.prefixes:
            try:
                table, n = store.load_all(prefix)
            except FileNotFoundError as e:
                self.log.warning("%s", e)
                continue
            self.app.publish(f"{prefix}hist", table)
            self.log.info("%s: %d files -> %shist", prefix, n, prefix)
