"""Plain functions to look things up from the Deephaven terminal. No classes, nothing to set up: they read the services that are running.

    contract("NIFTY 27OCT26 25000 CE")     one contract: token, symbol, kind, underlying, expiry, strike, type, tick, lot
    contract(47162)                        the same, from a token
    contract("NIFTY")                      an underlying name means its nearest future
    contracts("NIFTY", type="CE", strike=25000)       a list of contracts (any filters you like), as a table
    expiries("NIFTY")                      the expiry dates that have not passed
    strikes("NIFTY", expiry=0, type="CE")  the strikes (and tokens) of one expiry; expiry 0 = the nearest, or "2026-10-27"
    nearest_strikes("NIFTY", n=3)          the 3 strikes nearest the price (default: the future's last price)

    last_price("NIFTY")                    the latest candle close
    last_candles("NIFTY", 10)              the last 10 candles (to start an indicator from, compute it in your script)

Each one prints what it found and also returns it, so you can use the result in your own code. An unknown name or a service that is not
running prints a plain message and returns None; nothing raises.
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from algo.core.app import current

COLUMNS = ["token", "symbol", "kind", "underlying", "expiry", "strike", "type", "tick", "lot"]


def _master():
    try:
        return current().master
    except (KeyError, RuntimeError):
        print("The contract master is not built. Call master_build() first.")
        return None


def _today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(current().market.zone).date())


def _token(m, x) -> int | None:
    """A token number, a contract symbol (exact, any case), or an underlying name (its nearest future, else its cash instrument)."""
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, np.integer)) or str(x).strip().isdigit():
        token = int(x)
        return token if token in m.symbol_of else None
    text = " ".join(str(x).upper().split())
    token = m.token_of.get(text) if " " in text else None      # a full contract symbol has spaces ("NIFTY 27OCT26 25000 CE")
    if token is None:
        token = m.nearest_future(text, _today())                  # a bare name is an underlying: its nearest future ...
    if token is None:
        token = m.token_of.get(text)                              # ... else the instrument of that name (a cash equity)
    if token is None:
        token = m.spot_of.get(text)
    return None if token is None else int(token)


def _date(ts) -> str | None:
    return None if ts is None or pd.isna(ts) else pd.Timestamp(ts).strftime("%Y-%m-%d")


def _info(m, token: int) -> dict | None:
    for frame, col, kind in ((m.opt, "Token", "OPT"), (m.futs, "fut_token", "FUT"), (m.eqs, "eq_token", "EQ")):
        hit = frame[frame[col] == token]
        if len(hit):
            r = hit.iloc[0]
            opt = kind == "OPT"
            return {"token": token, "symbol": m.symbol_of.get(token), "kind": kind, "underlying": r["TckrSymb"],
                    "expiry": _date(r["expiry"]) if "expiry" in hit else None,
                    "strike": float(r["strike"]) if opt else None, "type": r["OptionType"] if opt else None,
                    "tick": m.tick_of.get(token), "lot": int(r["lot"]) if "lot" in hit and pd.notna(r["lot"]) else None}
    return None


def contract(x, show: bool = True) -> dict | None:
    """One contract, from a token, a symbol ("NIFTY 27OCT26 25000 CE") or an underlying name ("NIFTY" = its nearest future).
    show=False returns it without printing (for use inside a listener)."""
    m = _master()
    if m is None:
        return None
    token = _token(m, x)
    info = None if token is None else _info(m, token)
    if info is None:
        print(f"unknown contract {x!r}: give a token, an exact symbol such as 'NIFTY 27OCT26 25000 CE', or an underlying such as 'NIFTY'")
        return None
    if show:
        print("  ".join(f"{k} {v}" for k, v in info.items() if v is not None))
    return info


def _expiry(m, underlying: str, ot: str, expiry):
    """An expiry date from 0 (the nearest that has not passed), another index, or a date text / Timestamp."""
    if isinstance(expiry, (int, np.integer)) and not isinstance(expiry, bool):
        later = [e for e in m.expiries(str(underlying).upper(), ot) if e >= _today()]
        return later[expiry] if 0 <= expiry < len(later) else None
    try:
        return pd.Timestamp(expiry)
    except (ValueError, TypeError):
        return None


def expiries(underlying: str, kind: str = "OPT", show: bool = True) -> list[str] | None:
    """The expiry dates of an underlying that have not passed, as 'YYYY-MM-DD': expiries("NIFTY"), expiries("NIFTY", "FUT")."""
    m = _master()
    if m is None:
        return None
    frame = m.futs if str(kind).upper() == "FUT" else m.opt
    dates = sorted(e for e in frame.loc[frame["TckrSymb"] == str(underlying).upper(), "expiry"].dropna().unique() if e >= _today())
    out = [_date(d) for d in dates]
    if show:
        print(f"{str(underlying).upper()} {str(kind).upper()} expiries: " + (", ".join(out) if out else "none"))
    return out


def contracts(underlying: str | None = None, kind: str | None = None, expiry=None, type: str | None = None, strike: float | None = None,
              limit: int | None = 40) -> pd.DataFrame | None:
    """A table of contracts, filtered by whatever you give: underlying ("NIFTY"), kind ("FUT", "OPT" or "EQ"), expiry (a date text, or
    0/1/.. = the nearest/next one of that underlying), type ("CE" or "PE"), strike. limit=None shows them all."""
    m = _master()
    if m is None:
        return None
    parts = []
    for k, frame, col in (("FUT", m.futs, "fut_token"), ("OPT", m.opt, "Token"), ("EQ", m.eqs, "eq_token")):
        if kind is not None and str(kind).upper() != k:
            continue
        if (type is not None or strike is not None) and k != "OPT":
            continue
        f = frame
        if underlying is not None:
            f = f[f["TckrSymb"] == str(underlying).upper()]
        if expiry is not None:
            if "expiry" not in f:
                continue
            if isinstance(expiry, (int, np.integer)) and not isinstance(expiry, bool):
                if underlying is None:
                    print("an expiry number (0 = nearest) needs an underlying; or give a date such as '2026-10-27'")
                    return None
                ts = _expiry(m, underlying, "CE" if k == "OPT" else "XX", expiry) if k == "OPT" else _fut_expiry(m, underlying, expiry)
            else:
                try:
                    ts = pd.Timestamp(expiry)
                except (ValueError, TypeError):
                    print(f"expiry {expiry!r} is not a date; use '2026-10-27' or a number (0 = the nearest)")
                    return None
            f = f[f["expiry"] == ts] if ts is not None else f.iloc[0:0]
        if type is not None:
            f = f[f["OptionType"] == str(type).upper()]
        if strike is not None:
            f = f[np.isclose(f["strike"].astype(float), float(strike))]
        if len(f):
            parts.append(pd.DataFrame({
                "token": f[col].astype("int64").to_numpy(), "kind": k, "underlying": f["TckrSymb"].to_numpy(),
                "expiry": [_date(e) for e in f["expiry"]] if "expiry" in f else None,
                "strike": f["strike"].astype(float).to_numpy() if k == "OPT" else np.nan,
                "type": f["OptionType"].to_numpy() if k == "OPT" else None,
                "tick": [m.tick_of.get(int(t)) for t in f[col]], "lot": f["lot"].to_numpy() if "lot" in f else None}))
    if not parts:
        print("no contracts match")
        return pd.DataFrame(columns=COLUMNS)
    out = pd.concat(parts, ignore_index=True)
    out["symbol"] = [m.symbol_of.get(int(t)) for t in out["token"]]
    out = out.sort_values(["underlying", "expiry", "strike", "type"], na_position="first").reset_index(drop=True)[COLUMNS]
    total = len(out)
    shown = out if limit is None else out.head(int(limit))
    print(shown.to_string(index=False))
    if total > len(shown):
        print(f"... {total - len(shown)} more ({total} in all); contracts(..., limit=None) shows every one")
    return out


def _fut_expiry(m, underlying: str, index: int):
    later = sorted(e for e in m.futs.loc[m.futs["TckrSymb"] == str(underlying).upper(), "expiry"].dropna().unique() if e >= _today())
    return later[index] if 0 <= index < len(later) else None


def strikes(underlying: str, expiry=0, type: str = "CE", show: bool = True) -> pd.DataFrame | None:
    """The strikes and tokens of one expiry: strikes("NIFTY"), strikes("NIFTY", expiry=1, type="PE"), strikes("NIFTY", "2026-10-27")."""
    m = _master()
    if m is None:
        return None
    name, ot = str(underlying).upper(), str(type).upper()
    exp = _expiry(m, name, ot, expiry)
    try:
        values, tokens = m.strikes(name, exp, ot)
    except KeyError:
        print(f"no {ot} options of {name} for expiry {expiry!r}; see expiries({name!r})")
        return None
    out = pd.DataFrame({"strike": values, "token": tokens.astype("int64"), "symbol": [m.symbol_of.get(int(t)) for t in tokens]})
    if show:
        print(f"{name} {ot} {_date(exp)}: {len(out)} strikes, {out['strike'].min():g} to {out['strike'].max():g}")
    return out


def nearest_strikes(underlying: str, price: float | None = None, type: str = "CE", expiry=0, n: int = 1,
                    show: bool = True) -> pd.DataFrame | None:
    """The `n` strikes nearest a price: nearest_strikes("NIFTY", 25010, "CE", n=3). Without a price it uses the underlying's last candle close.
    With a price it reads only the master (cheap, and show=False keeps it quiet for use inside a listener)."""
    if price is None:
        price = last_price(underlying)
        if price is None:
            print("no price given and no candle price found for it")
            return None
    table = strikes(underlying, expiry, type, show=show)
    if table is None:
        return None
    out = table.iloc[np.argsort(np.abs(table["strike"].to_numpy() - float(price)), kind="stable")[:max(int(n), 1)]].reset_index(drop=True)
    if show:
        print(f"nearest to {price:g}:\n" + out.to_string(index=False))
    return out


# ---- the live tables ---------------------------------------------------------------------------------------------------------
def _frame(table) -> pd.DataFrame:
    from deephaven.pandas import to_pandas
    return to_pandas(table)


def _rows(name: str, x, n: int) -> pd.DataFrame | None:
    m = _master()
    if m is None:
        return None
    token = _token(m, x)
    if token is None:
        print(f"unknown contract {x!r}")
        return None
    table = current().tables.get(name)                              # only what a service has published (not a console function of that name)
    if table is None:
        print(f"the {name} table is not there: start it first (candles_start())")
        return None
    try:
        return _frame(table.where(f"token == {token}").tail(int(n)))
    except Exception as e:
        print(f"could not read {name}: {e}")
        return None


def last_price(x, show: bool = True) -> float | None:
    """The latest candle close of a token, a symbol or an underlying name (None if there is no candle yet). show=False prints nothing."""
    df = _rows("candles", x, 1)
    if df is None or not len(df):
        if df is not None:
            print("no candle yet for it")
        return None
    price = float(df["Close"].iloc[-1])
    if show:
        print(f"{x}: {price:g}")
    return price


def last_candles(x, n: int = 10, show: bool = True) -> pd.DataFrame | None:
    """The last `n` candles of a token, a symbol or an underlying name (to start an indicator from). show=False prints nothing."""
    df = _rows("candles", x, n)
    if df is not None and show:
        print(df.to_string(index=False) if len(df) else "no candle yet for it")
    return df
