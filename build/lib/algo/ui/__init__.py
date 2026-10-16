"""Operator console (imports deephaven.ui, so it is loaded only when asked for)."""


def __getattr__(name):
    if name in ("Console", "ConsoleConfig"):
        from algo.ui import console
        return getattr(console, name)
    raise AttributeError(name)
