"""token -> Symbol lookup as a small static table, joined into big ticking tables instead of storing a text per row."""
from __future__ import annotations

import pandas as pd


def symbol_table(symbol_of: dict, tokens=None):
    """Static table (token, Symbol). With `tokens`, only those instruments, so the join table stays small."""
    from deephaven.pandas import to_table
    if tokens is None:
        keys, names = list(symbol_of), list(symbol_of.values())
    else:
        keys = [int(t) for t in tokens if int(t) in symbol_of]
        names = [symbol_of[t] for t in keys]
    return to_table(pd.DataFrame({"token": pd.Series(keys, dtype="int64"), "Symbol": pd.Series(names, dtype="object")}))


def kind_of_tokens(master) -> dict:
    """token -> "OPT" / "FUT" / "EQ", from the contract master's own option, future and equity lists (not from the symbol text)."""
    out: dict = {}
    for frame, col, kind in ((master.eqs, "eq_token", "EQ"), (master.futs, "fut_token", "FUT"), (master.opt, "Token", "OPT")):
        if frame is not None and col in frame.columns:
            for tok in frame[col].dropna().astype("int64"):
                out[int(tok)] = kind
    return out


def kind_table(master):
    """Static table (token, Kind) for the console's expense and its Futures / Options / Equity filter."""
    from deephaven.pandas import to_table
    kinds = kind_of_tokens(master)
    return to_table(pd.DataFrame({"token": pd.Series(list(kinds), dtype="int64"), "Kind": pd.Series(list(kinds.values()), dtype="object")}))


def divisors_of_tokens(master) -> dict:
    """token -> the contract file's own Divisor (US equities 100, US options 10000, NSE 100): how many raw price units make one currency unit."""
    return {int(t): float(d) for t, d in master.divisor_of.items() if d}


def divisor_table(master):
    """Static table (token, Divisor) so the console divides every price by ITS contract's divisor, not one number for the whole market."""
    from deephaven.pandas import to_table
    d = divisors_of_tokens(master)
    return to_table(pd.DataFrame({"token": pd.Series(list(d), dtype="int64"), "Divisor": pd.Series(list(d.values()), dtype="float64")}))
