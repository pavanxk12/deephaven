"""The App: one object per Deephaven session that owns the running plugins and the published tables.

    import algo
    app = algo.connect(globals(), market="NSE")
    app.run(algo.Master())
    app.run(algo.Candles())

Running a plugin whose name is already running stops the old one first, so a re-paste never leaves
duplicate listeners behind.
"""
from __future__ import annotations

import contextlib
import logging
import sys
from typing import Any, Iterable

from algo.core import log
from algo.core.config import Market, Paths, market_for
from algo.core.plugin import Plugin

_APP: "App | None" = None


class MissingDependency(RuntimeError):
    pass


class App:
    def __init__(self, ns: dict, market: Market, paths: Paths):
        self.ns = ns                                  # the session globals: published tables land here
        self.market = market
        self.paths = paths
        self.plugins: dict[str, Plugin] = {}
        self.tables: dict[str, Any] = {}
        self.shared: dict[str, Any] = {}              # objects that must survive a plugin restart
        self._publish_cbs: dict[str, list] = {}
        self.ctx = self._capture_context()

    # ---- plugins ---------------------------------------------------------------------
    def run(self, plugin: Plugin) -> Plugin:
        missing = [d for d in plugin.requires if not self.has(d)]
        if missing:
            raise MissingDependency(f"{plugin.name} needs {missing}: run those first")
        old = self.plugins.pop(plugin.name, None)
        if old is not None:
            old._teardown()
        plugin._bind(self)
        try:
            plugin.start()
        except Exception:
            plugin._teardown()
            raise
        self.plugins[plugin.name] = plugin
        return plugin

    def run_all(self, plugins: Iterable[Plugin]) -> None:
        for p in plugins:
            self.run(p)

    def stop(self, name: str) -> None:
        plugin = self.plugins.pop(name, None)
        if plugin is not None:
            plugin._teardown()

    def stop_all(self) -> None:
        for name in reversed(list(self.plugins)):
            self.stop(name)

    def __getitem__(self, name: str) -> Plugin:
        try:
            return self.plugins[name]
        except KeyError:
            raise KeyError(f"plugin {name!r} is not running (running: {list(self.plugins)})") from None

    def get(self, name: str) -> "Plugin | None":
        return self.plugins.get(name)

    # ---- tables ------------------------------------------------------------------------
    def publish(self, name: str, table, ns_name: str | None = None) -> None:
        """Make a table visible to other plugins (as `name`) and to the Deephaven IDE (as a top-level variable,
        `ns_name` or `name`). Republishing an existing name tells the subscribers, so listeners can move to the new table."""
        replaced = name in self.tables
        self.tables[name] = table
        self.ns[ns_name or name] = table
        if replaced:
            for cb in list(self._publish_cbs.get(name, [])):
                try:
                    cb(table)
                except Exception:
                    log.get("app").exception("republish callback for %s failed", name)

    def on_publish(self, name: str, callback):
        """Call `callback(new_table)` whenever table `name` is republished. Returns an unregister function."""
        self._publish_cbs.setdefault(name, []).append(callback)
        return lambda: self._publish_cbs[name].remove(callback) if callback in self._publish_cbs.get(name, []) else None

    def table(self, name: str):
        for source in (self.tables, self.ns):
            if name in source:
                return source[name]
        raise MissingDependency(f"table {name!r} not found in the session")

    def has(self, name: str) -> bool:
        return name in self.plugins or name in self.tables or name in self.ns

    @property
    def master(self):
        """The MasterData of the running master plugin."""
        return self["master"].data

    # ---- helpers -----------------------------------------------------------------------
    def in_ctx(self):
        """Use around Deephaven queries issued from listener / timer threads: `with app.in_ctx(): ...`"""
        return self.ctx

    def status(self) -> list[tuple[str, str]]:
        rows = [(name, p.describe()) for name, p in self.plugins.items()]
        for name, detail in rows:
            print(f"  {name:<14} {detail}")
        if not rows:
            print("  no plugins running")
        return rows

    @staticmethod
    def _capture_context():
        try:
            from deephaven.execution_context import get_exec_ctx
            return get_exec_ctx()
        except Exception:
            return contextlib.nullcontext()


def connect(ns: dict, market: "str | Market | None" = None, paths: Paths | None = None,
            level: int = logging.INFO) -> App:
    """Return the session's App, creating it on first use. Call once at the top of every script."""
    global _APP
    log.setup(level)
    mk = market_for(market)
    if _APP is not None and _APP.market != mk:
        _APP.stop_all()
        _APP = None
    if _APP is None:
        _APP = App(ns, mk, paths or Paths())
    else:
        _APP.ns = ns
        if paths is not None:
            _APP.paths = paths
        _APP.ctx = App._capture_context()
    return _APP


def current() -> App:
    if _APP is None:
        raise RuntimeError("call algo.connect(globals(), market=...) first")
    return _APP


def reload() -> None:
    """Stop everything and forget the algo modules, so the next `import algo` loads the files again.

        algo.reload(); import algo; app = algo.connect(globals(), "NSE")
    """
    global _APP
    if _APP is not None:
        _APP.stop_all()
        _APP = None
    for name in [m for m in sys.modules if m == "algo" or m.startswith("algo.")]:
        del sys.modules[name]
