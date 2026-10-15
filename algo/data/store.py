"""Saving tables at end of day and loading saved history back (parquet or csv).

Writes go to `paths.saved` (the writable volume); history is read from `paths.market_data`, where the saved
files are copied after `docker cp`.
"""
from __future__ import annotations

import glob
import os
from datetime import datetime, time as dtime, timedelta

from algo.core.plugin import Plugin

DEFAULT_TABLES = {"straddle_series": "straddle_", "candles": "candles_"}       # table name -> file prefix


class Store:
    def __init__(self, write_dir: str, read_dir: str, fmt: str = "parquet"):
        if fmt not in ("parquet", "csv"):
            raise ValueError("fmt must be 'parquet' or 'csv'")
        self.write_dir, self.read_dir, self.fmt = write_dir, read_dir, fmt

    def path(self, prefix: str, day: str) -> str:
        return os.path.join(self.write_dir, f"{prefix}{day}.{self.fmt}")

    def save(self, table, prefix: str, day: str) -> tuple[str, int] | None:
        """Snapshot `table`, add a Date column, write it. None when the table is empty."""
        snap = table.snapshot()
        if snap.size == 0:
            return None
        snap = snap.update(f"Date = `{day}`")                 # Minute / Time carry no date
        os.makedirs(self.write_dir, exist_ok=True)
        path = self.path(prefix, day)
        if self.fmt == "csv":
            from deephaven import csv as dhcsv
            dhcsv.write(snap, path)
        else:
            from deephaven import parquet
            parquet.write(snap, path)
        return path, snap.size

    def load_all(self, prefix: str):
        """Merge every saved file with this prefix into one table."""
        from deephaven import merge, parquet
        files = sorted(glob.glob(os.path.join(self.read_dir, f"{prefix}*.parquet")))
        if not files:
            raise FileNotFoundError(f"no {prefix}*.parquet in {self.read_dir}")
        return merge([parquet.read(f) for f in files]), len(files)


class Eod(Plugin):
    """Saves the chosen tables for today. `app["eod"].save()` any time; `auto_at=time(15, 35)` saves once a day."""
    name = "eod"

    def __init__(self, tables: dict | None = None, fmt: str = "parquet", auto_at: dtime | None = None):
        super().__init__()
        self.tables, self.fmt, self.auto_at = dict(tables or DEFAULT_TABLES), fmt, auto_at
        self.store: Store | None = None

    def start(self) -> None:
        self.store = Store(self.app.paths.saved, self.app.paths.market_data, self.fmt)
        if self.auto_at is not None:
            self.spawn(self._loop)
            self.log.info("auto-save at %s (%s). Manual: app['eod'].save()", self.auto_at, self.app.market.tz)
        else:
            self.log.info("ready. Call app['eod'].save() to save")

    def describe(self) -> str:
        return f"{', '.join(self.tables)} -> {self.fmt}" + (f", auto {self.auto_at}" if self.auto_at else "")

    def save(self) -> None:
        day = datetime.now(self.app.market.zone).strftime("%Y-%m-%d")
        for name, prefix in self.tables.items():
            try:
                table = self.app.table(name)
            except Exception:
                self.log.warning("%s not found, skipped", name)
                continue
            try:
                out = self.store.save(table, prefix, day)
            except Exception:
                self.log.exception("%s save failed", name)
                continue
            self.log.info("%s is empty, nothing saved" % name if out is None else "saved %s: %d rows -> %s" % (name, out[1], out[0]))

    def _loop(self) -> None:
        zone = self.app.market.zone
        while not self.halted.is_set():
            now = datetime.now(zone)
            nxt = now.replace(hour=self.auto_at.hour, minute=self.auto_at.minute, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            if self.halted.wait((nxt - now).total_seconds()):
                return
            self.save()


class History(Plugin):
    """Loads saved days back as tables `<prefix>hist` (e.g. `straddle_hist`, `candles_hist`)."""
    name = "history"

    def __init__(self, prefixes: tuple[str, ...] = ("straddle_", "candles_")):
        super().__init__()
        self.prefixes = prefixes

    def start(self) -> None:
        store = Store(self.app.paths.saved, self.app.paths.market_data)
        for prefix in self.prefixes:
            try:
                table, n = store.load_all(prefix)
            except FileNotFoundError as e:
                self.log.warning("%s", e)
                continue
            self.app.publish(f"{prefix}hist", table)
            self.log.info("%s: %d files -> %shist", prefix, n, prefix)
