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
