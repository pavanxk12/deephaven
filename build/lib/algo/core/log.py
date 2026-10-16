"""Logging for the algo package: one `algo` logger that writes to the *current* sys.stdout.

Deephaven swaps sys.stdout per console session, so the handler looks it up on every record
instead of binding it once.
"""
from __future__ import annotations

import logging
import sys

ROOT = "algo"


class _StdoutHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            print(self.format(record), file=sys.stdout, flush=True)
        except Exception:
            self.handleError(record)


class _Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        short = record.name[len(ROOT) + 1:] if record.name.startswith(ROOT + ".") else record.name
        text = f"{self.formatTime(record, '%H:%M:%S')} [{short}] {record.getMessage()}"
        if record.exc_info:
            text += "\n" + self.formatException(record.exc_info)
        return text


def setup(level: int = logging.INFO) -> logging.Logger:
    """Attach the stdout handler once; safe to call on every connect()."""
    log = logging.getLogger(ROOT)
    if not any(isinstance(h, _StdoutHandler) for h in log.handlers):
        handler = _StdoutHandler()
        handler.setFormatter(_Formatter())
        log.addHandler(handler)
    log.setLevel(level)
    log.propagate = False
    return log


def get(name: str) -> logging.Logger:
    return logging.getLogger(f"{ROOT}.{name}")
