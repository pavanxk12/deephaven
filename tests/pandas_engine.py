"""A small pandas-backed stand-in for the Deephaven table operations the indicator pipeline uses, so the REAL pipeline code
(`build_indicator_table`) can be executed and its numbers compared with textbook formulas.

It implements the documented behaviour of: view / update_view / update, update_by (cum_sum, delta, ema_tick,
rolling_avg_tick, forward_fill, grouped by a key, row order preserved), agg_by (first, last, max, min), natural_join and
where_in; and translates the Java-flavoured query expressions (`a ? b : c`, `Math.max`, `isNull`, `NULL_DOUBLE`, `lowerBin`).
Division is plain Python division: an unguarded division by zero raises here, where the engine would hand back inf / NaN.
"""
import math
import re
import sys
import types

import numpy as np
import pandas as pd

NAN = float("nan")
_LEX = re.compile(r"\s*(?:(\d+\.\d*(?:[eE][+-]?\d+)?|\.\d+|\d+)|([A-Za-z_][A-Za-z_0-9.]*)|(\|\||&&|==|!=|<=|>=|[-+*/<>?:(),]))")


def _isnull(x):
    return x is None or (isinstance(x, float) and x != x)


def _lower_bin(value, size):
    return value - value % size


_NS = {"_max": max, "_abs": abs, "_isnull": _isnull, "_lower_bin": _lower_bin, "_minus": lambda a, b: a - b, "NAN": NAN}
_FUNCS = {"Math.max": "_max", "Math.abs": "_abs", "isNull": "_isnull", "lowerBin": "_lower_bin", "minus": "_minus"}
_CONSTS = {"NULL_DOUBLE": "NAN", "NULL_LONG": "NAN", "SECOND": "1", "MINUTE": "60"}


class _Translate:
    def __init__(self, text):
        self.t, pos = [], 0
        text = text.strip()
        while pos < len(text):
            m = _LEX.match(text, pos)
            if not m or m.end() == pos:
                raise ValueError(f"cannot read {text!r} at {pos}")
            self.t.append(next(g for g in m.groups() if g is not None))
            pos = m.end()
        self.i = 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else None

    def take(self, want=None):
        tok = self.peek()
        if want is not None and tok != want:
            raise ValueError(f"expected {want!r}, got {tok!r} in {self.t}")
        self.i += 1
        return tok

    def expr(self):
        cond = self.or_()
        if self.peek() == "?":
            self.take("?")
            a = self.expr()
            self.take(":")
            b = self.expr()
            return f"(({a}) if ({cond}) else ({b}))"
        return cond

    def or_(self):
        out = self.and_()
        while self.peek() == "||":
            self.take()
            out = f"({out} or {self.and_()})"
        return out

    def and_(self):
        out = self.cmp()
        while self.peek() == "&&":
            self.take()
            out = f"({out} and {self.cmp()})"
        return out

    def cmp(self):
        left = self.add()
        if self.peek() in ("==", "!=", "<", ">", "<=", ">="):
            op = self.take()
            return f"({left} {op} {self.add()})"
        return left

    def add(self):
        out = self.mul()
        while self.peek() in ("+", "-"):
            op = self.take()
            out = f"({out} {op} {self.mul()})"
        return out

    def mul(self):
        out = self.unary()
        while self.peek() in ("*", "/"):
            op = self.take()
            out = f"({out} {op} {self.unary()})"
        return out

    def unary(self):
        if self.peek() == "-":
            self.take()
            return f"(-{self.unary()})"
        return self.primary()

    def primary(self):
        tok = self.take()
        if tok == "(":
            out = self.expr()
            self.take(")")
            return f"({out})"
        if re.fullmatch(r"\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+", tok):
            return tok
        if self.peek() == "(":
            self.take("(")
            args = []
            if self.peek() != ")":
                args.append(self.expr())
                while self.peek() == ",":
                    self.take()
                    args.append(self.expr())
            self.take(")")
            return f"{_FUNCS[tok]}({', '.join(args)})"
        if tok in _CONSTS:
            return _CONSTS[tok]
        return f"v[{tok!r}]"


def compile_expr(text):
    code = _Translate(text).expr()
    return eval("lambda v: " + code, dict(_NS))      # noqa: S307  (test helper, own strings)


def _spec(s):
    """"Name = expr" -> (Name, expr); "Name" -> (Name, None)."""
    if "=" in s and not s.strip().startswith("="):
        left, right = s.split("=", 1)
        if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", left.strip()):
            return left.strip(), right.strip()
    return s.strip(), None


def _pairs(cols):
    out = []
    for c in cols:
        name, src = _spec(c)
        out.append((name, src or name))
    return out


def _f(x):
    return NAN if x is None else float(x)


class PT:
    """A table: a DataFrame with Deephaven-like methods."""

    def __init__(self, df):
        self.df = df.reset_index(drop=True)

    # ---- column expressions -------------------------------------------------------------------
    def _add_columns(self, specs, keep_all):
        specs = [specs] if isinstance(specs, str) else list(specs)
        df = self.df.copy() if keep_all else self.df
        out = {} if not keep_all else None
        cur = df
        for s in specs:
            name, expr = _spec(s)
            if expr is None:
                col = cur[name].tolist()
            else:
                fn = compile_expr(expr)
                col = [fn(r) for r in cur.to_dict("records")]
            if keep_all:
                cur = cur.assign(**{name: col})
            else:
                out[name] = col
                cur = cur.assign(**{name: col})              # later columns may use earlier ones
        return cur if keep_all else pd.DataFrame(out)

    def view(self, specs):
        return PT(self._add_columns(specs, keep_all=False))

    def update_view(self, specs):
        return PT(self._add_columns(specs, keep_all=True))

    update = update_view

    # ---- update_by ----------------------------------------------------------------------------
    def update_by(self, ops, by=None):
        by = [by] if isinstance(by, str) else list(by or [])
        df = self.df.copy()
        groups = df.groupby(by, sort=False).indices if by else {(): np.arange(len(df))}
        for op in ops:
            kind, params, pairs = op
            for out_name, in_name in pairs:
                result = np.full(len(df), NAN)
                for idx in groups.values():
                    result[idx] = _grouped(kind, params, df[in_name].to_numpy(dtype=float)[idx])
                df[out_name] = result
        return PT(df)

    # ---- aggregations and joins ---------------------------------------------------------------
    def agg_by(self, aggs, by):
        by = [by] if isinstance(by, str) else list(by)
        fn = {"first": lambda s: s.iloc[0], "last": lambda s: s.iloc[-1], "max_": "max", "min_": "min"}
        rows = []
        for key, g in self.df.groupby(by, sort=False):
            key = key if isinstance(key, tuple) else (key,)
            rows.append(dict(zip(by, key)) | {col: (fn[k](g[col]) if callable(fn[k]) else getattr(g[col], fn[k])())
                                              for k, col in aggs})
        return PT(pd.DataFrame(rows))

    def natural_join(self, other, on, joins):
        on = [on] if isinstance(on, str) else list(on)
        joins = [joins] if isinstance(joins, str) else list(joins)
        keys = [(_spec(o)[0], _spec(o)[1] or _spec(o)[0]) for o in on]
        right = {tuple(r[rk] for _, rk in keys): r for r in other.df.to_dict("records")}
        cols = {j: [] for j in joins}
        for r in self.df.to_dict("records"):
            hit = right.get(tuple(r[lk] for lk, _ in keys))
            for j in joins:
                cols[j].append(hit[j] if hit is not None else NAN)
        return PT(self.df.assign(**cols))

    def aj(self, other, on, joins):
        """As-of join: on = [exact keys..., "left >= right"]; each left row takes the right row with the greatest `right`
        that is <= `left` among rows with equal exact keys (NaN when there is none)."""
        on = [on] if isinstance(on, str) else list(on)
        joins = [joins] if isinstance(joins, str) else list(joins)
        exact = [(_spec(o)[0], _spec(o)[1] or _spec(o)[0]) for o in on[:-1]]
        left_col, right_col = re.fullmatch(r"\s*(\w+)\s*>=\s*(\w+)\s*", on[-1]).groups()
        out = [(_spec(j)[0], _spec(j)[1] or _spec(j)[0]) for j in joins]
        by_key = {}
        for r in other.df.to_dict("records"):
            by_key.setdefault(tuple(r[rk] for _, rk in exact), []).append(r)
        for rows in by_key.values():
            rows.sort(key=lambda r: r[right_col])
        cols = {name: [] for name, _ in out}
        for r in self.df.to_dict("records"):
            best = None
            for cand in by_key.get(tuple(r[lk] for lk, _ in exact), []):
                if cand[right_col] <= r[left_col]:
                    best = cand
                else:
                    break
            for name, src in out:
                cols[name].append(best[src] if best is not None else NAN)
        return PT(self.df.assign(**cols))

    def where_in(self, other, col):
        return PT(self.df[self.df[col].isin(set(other.df[col]))])


def _grouped(kind, params, x):
    n = len(x)
    out = np.full(n, NAN)
    if kind == "cum_sum":
        out[:] = np.cumsum(np.nan_to_num(x))
    elif kind == "delta":
        out[1:] = x[1:] - x[:-1]
    elif kind == "ema":
        alpha_keep = math.exp(-1.0 / params)               # weight of the previous value
        cur = NAN
        for i, v in enumerate(x):
            if v == v:
                cur = v if cur != cur else alpha_keep * cur + (1.0 - alpha_keep) * v
            out[i] = cur
    elif kind == "ravg":
        for i in range(n):
            w = x[max(0, i - params + 1): i + 1]
            w = w[~np.isnan(w)]
            out[i] = w.mean() if len(w) else NAN
    elif kind == "ffill":
        cur = NAN
        for i, v in enumerate(x):
            if v == v:
                cur = v
            out[i] = cur
    else:
        raise ValueError(kind)
    return out


def install(monkeypatch):
    """Register fake `deephaven.updateby` / `deephaven.agg` that describe operations for PT.update_by / agg_by."""
    uby = types.ModuleType("deephaven.updateby")
    uby.cum_sum = lambda cols: ("cum_sum", None, _pairs(cols))
    uby.delta = lambda cols: ("delta", None, _pairs(cols))
    uby.forward_fill = lambda cols: ("ffill", None, _pairs(cols))
    uby.ema_tick = lambda decay, cols: ("ema", decay, _pairs(cols))
    uby.rolling_avg_tick = lambda cols, rev_ticks: ("ravg", rev_ticks, _pairs(cols))
    agg = types.ModuleType("deephaven.agg")
    for name in ("first", "last", "max_", "min_"):
        setattr(agg, name, lambda col, _n=name: (_n, col))
    dh = types.ModuleType("deephaven")
    dh.updateby, dh.agg = uby, agg
    monkeypatch.setitem(sys.modules, "deephaven", dh)
    monkeypatch.setitem(sys.modules, "deephaven.updateby", uby)
    monkeypatch.setitem(sys.modules, "deephaven.agg", agg)
