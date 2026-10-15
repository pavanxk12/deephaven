"""Which underlyings a strategy tracks, and the basket ranking filter (pure functions + two small resolvers)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import pandas as pd

from algo.core import log

_log = log.get("universe")


@dataclass(frozen=True)
class Instrument:
    token: int              # the future that is the signal source
    name: str               # master symbol, e.g. "RELIANCE 27OCT26 FUT"
    underlying: str         # "RELIANCE"
    expiry: pd.Timestamp    # the future's expiry
    basket: str


def parse_stocks(text: str) -> tuple[list[tuple[str, str | None]], int | None]:
    """`BASKETS=n` line plus one symbol per line, `SYMBOL` or `SYMBOL,basket`. Blank lines and `#` comments are ignored.

    Returns ([(symbol, basket or None)], n_baskets). Duplicates keep the first.
    """
    rows, seen, nbaskets = [], set(), None
    for line in text.splitlines():
        parts = [p.strip() for p in line.split("#")[0].split(",")]
        if not parts[0]:
            continue
        if parts[0].upper().startswith("BASKETS"):
            nbaskets = int(parts[0].split("=")[1])
            continue
        name = " ".join(parts[0].upper().split())
        if name in seen:
            _log.warning("%s is listed twice, using the first", name)
            continue
        seen.add(name)
        rows.append((name, parts[1].upper() if len(parts) > 1 and parts[1] else None))
    if nbaskets is None and any(b is None for _, b in rows):
        raise ValueError("add a line `BASKETS=4` (or give every stock its basket)")
    return rows, nbaskets


def assign_baskets(rows: list[tuple[str, str | None]], nbaskets: int | None) -> dict[str, str]:
    """Stocks without a basket are split into `nbaskets` equal parts in file order (B1, B2, ...)."""
    auto = [n for n, b in rows if b is None]
    return {n: (b if b is not None else f"B{auto.index(n) * nbaskets // len(auto) + 1}") for n, b in rows}


def middle_set(pcts: Mapping[int, int | None], basket_of: Mapping[int, str]) -> set[int]:
    """Q2 + Q3 by Pct-from-open, ranked inside each basket: drop the lowest and the highest quarter of every basket.

    A basket with fewer than 4 ranked stocks has no filter and contributes nobody (nothing is eligible).
    """
    out: set[int] = set()
    for b in set(basket_of.values()):
        ranked = sorted((pcts[t], t) for t in basket_of if basket_of[t] == b and pcts.get(t) is not None)
        n = len(ranked)
        if n < 4:
            continue
        k = n // 4
        out |= {t for _, t in ranked[k:n - k]}
    return out


def resolve_stocks(text: str, master) -> dict[int, Instrument]:
    """stocks.txt content -> {future token: Instrument}; symbols that are not futures with a tick are skipped."""
    rows, nbaskets = parse_stocks(text)
    basket = assign_baskets(rows, nbaskets) if rows else {}
    out: dict[int, Instrument] = {}
    for name, _ in rows:
        tok = master.token_of.get(name)
        if tok is None:
            _log.warning("%s: not a symbol in master, skipped", name)
        elif tok not in master.fut_info:
            _log.warning("%s: not a future in master, skipped (the signal source must be a future)", name)
        elif tok not in master.tick_of:
            _log.warning("%s: no tick in master, skipped", name)
        else:
            sym, exp = master.fut_info[tok]
            out[tok] = Instrument(tok, master.symbol_of.get(tok, name), sym, exp, basket[name])
    return out


def resolve_single(underlying: str, master, today: pd.Timestamp) -> dict[int, Instrument]:
    """The nearest non-expired future of one underlying (e.g. NIFTY) as the only instrument."""
    tok = master.nearest_future(underlying, today)
    if tok is None:
        raise RuntimeError(f"no {underlying} future found in master")
    if tok not in master.tick_of:
        raise RuntimeError(f"{underlying} future {tok} has no tick in master")
    sym, exp = master.fut_info[tok]
    return {tok: Instrument(tok, master.symbol_of.get(tok, underlying), sym, exp, "B1")}
