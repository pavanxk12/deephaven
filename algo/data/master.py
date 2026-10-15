"""Contract master: parsed once per contract file from the dated SQLite (.db3) and cached as a pickle.

`MasterData` is plain pandas/numpy with the lookups the other plugins need, so it can be tested without Deephaven.
`Master` is the plugin that builds it and publishes `master` (a dict, for notebooks that used the old script).
"""
from __future__ import annotations

import glob
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property

import numpy as np
import pandas as pd

from algo.core.config import Market
from algo.core.plugin import Plugin

CACHE_VERSION = "v3"


# =================================================================================================
# data
# =================================================================================================
@dataclass
class MasterData:
    chain: pd.DataFrame      # ce_token, pe_token, TckrSymb, expiry, strike
    opt: pd.DataFrame        # Token, TckrSymb, expiry, strike, OptionType, tick, lot ...
    futs: pd.DataFrame       # fut_token, TckrSymb, expiry, tick, lot
    eqs: pd.DataFrame        # eq_token, TckrSymb
    symbol_of: dict          # token -> "NIFTY 27OCT26 25000 CE" / "NIFTY 27OCT26 FUT" / "AAPL"

    # ---- lookups ------------------------------------------------------------------------
    @cached_property
    def token_of(self) -> dict:
        """Exact upper-case symbol -> token (equity, future or option)."""
        return {s.upper(): t for t, s in self.symbol_of.items()}

    @cached_property
    def tick_of(self) -> dict:
        """token -> tick in currency units, for futures and options; an equity uses its own future's tick."""
        out = {int(t): float(k) for frame, col in ((self.futs, "fut_token"), (self.opt, "Token"))
               for t, k in zip(frame[col], frame["tick"]) if pd.notna(k) and k > 0}
        fut_tick = self.futs.dropna(subset=["tick"]).drop_duplicates("TckrSymb").set_index("TckrSymb")["tick"]
        for tok, sym in zip(self.eqs["eq_token"], self.eqs["TckrSymb"]):
            if sym in fut_tick.index and fut_tick[sym] > 0:
                out[int(tok)] = float(fut_tick[sym])
        return out

    @cached_property
    def fut_info(self) -> dict:
        """future token -> (underlying symbol, expiry)."""
        return {int(t): (s, e) for t, s, e in zip(self.futs["fut_token"], self.futs["TckrSymb"], self.futs["expiry"])}

    @cached_property
    def _option_index(self) -> tuple[dict, dict]:
        index, expiries = {}, {}
        for (sym, exp, ot), g in self.opt.groupby(["TckrSymb", "expiry", "OptionType"]):
            g = g.sort_values("strike")
            index[(sym, exp, ot)] = (g["strike"].to_numpy(dtype=float), g["Token"].to_numpy())
            expiries.setdefault((sym, ot), []).append(exp)
        for lst in expiries.values():
            lst.sort()
        return index, expiries

    def expiries(self, symbol: str, option_type: str) -> list:
        """Sorted expiries that have options of this underlying and type ("CE"/"PE")."""
        return self._option_index[1].get((symbol, option_type), [])

    def strikes(self, symbol: str, expiry, option_type: str) -> tuple[np.ndarray, np.ndarray]:
        """(sorted strikes, matching option tokens)."""
        return self._option_index[0][(symbol, expiry, option_type)]

    def nearest_future(self, symbol: str, today: pd.Timestamp) -> int | None:
        """Token of the nearest non-expired future of an underlying."""
        f = self.futs[(self.futs["TckrSymb"] == symbol) & (self.futs["expiry"] >= today)].sort_values("expiry")
        return int(f["fut_token"].iloc[0]) if len(f) else None

    # ---- (de)serialisation ------------------------------------------------------------------
    def as_dict(self) -> dict:
        return {"chain": self.chain, "opt": self.opt, "futs": self.futs, "eqs": self.eqs,
                "symbol_of": self.symbol_of}

    @classmethod
    def from_dict(cls, d: dict) -> "MasterData":
        return cls(chain=d["chain"], opt=d["opt"], futs=d["futs"], eqs=d["eqs"], symbol_of=d["symbol_of"])


# =================================================================================================
# parsing (pure)
# =================================================================================================
def parse_expiry(series: pd.Series, epoch: str) -> pd.Series:
    """Expiry column -> normalised timestamps. Accepts yyyymmdd, epoch seconds / ms, or date strings."""
    n = pd.to_numeric(series, errors="coerce")
    if n.notna().mean() > 0.5:
        med = n.median()
        if 19000101 <= med <= 21001231:
            out = pd.to_datetime(n.astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
        else:
            unit = "ms" if med > 1e11 else "s"
            out = pd.Timestamp(epoch) + pd.to_timedelta(n, unit=unit)
    else:
        out = pd.to_datetime(series, errors="coerce")
    return out.dt.normalize()


def _col(raw: pd.DataFrame, name: str, default):
    return raw[name] if name in raw else default


def normalise(raw: pd.DataFrame, market: Market) -> pd.DataFrame:
    """Raw contract rows -> Token, TckrSymb, InstType, OptionType, strike, tick, lot, expiry (currency units)."""
    c = market.contract_columns
    return pd.DataFrame({
        "Token": pd.to_numeric(raw[c["token"]], errors="coerce"),
        "TckrSymb": raw[c["symbol"]].astype(str).str.strip().str.upper(),
        "InstType": raw[c["inst"]].astype(str).str.strip().str.upper(),
        "OptionType": (raw[c["opt"]].astype(str).str.strip().str.upper() if c["opt"] in raw else ""),
        "strike": (pd.to_numeric(raw[c["strike"]], errors="coerce") / market.strike_div
                   if c["strike"] in raw else float("nan")),
        "tick": (pd.to_numeric(raw[c["tick"]], errors="coerce") / market.tick_div
                 if c["tick"] in raw else float("nan")),
        "lot": pd.to_numeric(raw[c["lot"]], errors="coerce") if c["lot"] in raw else float("nan"),
        "expiry": parse_expiry(raw[c["expiry"]], market.expiry_epoch),
    })


def classify(df: pd.DataFrame) -> MasterData:
    """Split normalised rows into option chain / options / futures / equities and build the symbol map.

    Options and futures need an expiry; equities do not (they have none), so they are not dropped with it.
    """
    df = df[df["Token"].notna()].copy()
    df["Token"] = df["Token"].astype("int64")
    df["OptionType"] = df["OptionType"].str[:1].map({"C": "CE", "P": "PE"})        # C / CALL / CE, P / PUT / PE
    dated = df["expiry"].notna()

    itype = df["InstType"]
    is_opt = (itype.str.startswith("OPT") | df["OptionType"].notna()) & dated
    is_fut = itype.str.startswith("FUT") & ~is_opt & dated
    is_eq = itype.isin(["EQUITY", "EQ", "STK", "ETF", "INDEX"]) & ~(itype.str.startswith("OPT") | df["OptionType"].notna()) \
        & ~itype.str.startswith("FUT")

    opt = df[is_opt & df["OptionType"].notna() & (df["strike"] > 0)]
    ce = opt[opt["OptionType"] == "CE"][["Token", "TckrSymb", "expiry", "strike"]].rename(columns={"Token": "ce_token"})
    pe = opt[opt["OptionType"] == "PE"][["Token", "TckrSymb", "expiry", "strike"]].rename(columns={"Token": "pe_token"})
    chain = (ce.merge(pe, on=["TckrSymb", "expiry", "strike"], how="inner")
               .drop_duplicates(["TckrSymb", "expiry", "strike"]).reset_index(drop=True))
    futs = df[is_fut][["Token", "TckrSymb", "expiry", "tick", "lot"]].rename(columns={"Token": "fut_token"}).reset_index(drop=True)
    eqs = df[is_eq][["Token", "TckrSymb"]].rename(columns={"Token": "eq_token"}).reset_index(drop=True)
    opt = opt.reset_index(drop=True)

    data = MasterData(chain=chain, opt=opt, futs=futs, eqs=eqs, symbol_of={})
    data.symbol_of = build_symbol_map(data)
    return data


def build_symbol_map(m: MasterData) -> dict:
    sym = {}
    for r in m.opt.itertuples(index=False):
        sym[int(r.Token)] = f"{r.TckrSymb} {r.expiry:%d%b%y}".upper() + f" {r.strike:.10g} {r.OptionType}"
    for r in m.futs.itertuples(index=False):
        sym[int(r.fut_token)] = f"{r.TckrSymb} {r.expiry:%d%b%y}".upper() + " FUT"
    for r in m.eqs.itertuples(index=False):
        sym[int(r.eq_token)] = r.TckrSymb
    return sym


# =================================================================================================
# files
# =================================================================================================
def find_db(directory: str, day: str) -> str:
    """`<dir>/<yyyymmdd>.db3`, else the last `*<yyyymmdd>*.db3`."""
    exact = os.path.join(directory, f"{day}.db3")
    if os.path.exists(exact):
        return exact
    hits = sorted(glob.glob(os.path.join(directory, f"*{day}*.db3")))
    if not hits:
        raise FileNotFoundError(f"no .db3 for {day} in {directory}")
    return hits[-1]


def read_contracts(db_path: str, market: Market, table: str | None = None) -> tuple[pd.DataFrame, str]:
    """All rows of the contract table (auto-detected when `table` is None)."""
    c = market.contract_columns
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if table is None:
            need = {c["token"], c["symbol"], c["expiry"], c["inst"]}
            names = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            for n in names:
                if need <= {r[1] for r in con.execute(f'PRAGMA table_info("{n}")')}:
                    table = n
                    break
            if table is None:
                raise RuntimeError(f"no table with columns {need}; tables: {names}")
        return pd.read_sql_query(f'SELECT * FROM "{table}"', con), table
    finally:
        con.close()


def pick_cache_dir(preferred: str | None) -> str | None:
    """First writable directory, else None (caching disabled)."""
    for d in (preferred, os.path.join(tempfile.gettempdir(), "fo_master_cache"), os.path.expanduser("~/.cache/fo_master")):
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            if os.access(d, os.W_OK):
                return d
        except OSError:
            pass
    return None


# =================================================================================================
# plugin
# =================================================================================================
class Master(Plugin):
    """Builds `MasterData` for today (or `day`) and publishes it. Other plugins read `app.master`."""
    name = "master"

    def __init__(self, day: str | None = None, db_path: str | None = None, table: str | None = None,
                 force_rebuild: bool = False):
        super().__init__()
        self.day, self.db_path, self.table, self.force = day, db_path, table, force_rebuild
        self.data: MasterData | None = None

    def start(self) -> None:
        mk, paths = self.app.market, self.app.paths
        day = self.day or datetime.now(mk.zone).strftime("%Y%m%d")
        db = self.db_path or find_db(paths.market_data, day)
        st = os.stat(db)
        cache_dir = pick_cache_dir(paths.cache)
        cache = (os.path.join(cache_dir, f"master_{CACHE_VERSION}_{mk.code}_{day}_{int(st.st_mtime)}_{st.st_size}.pkl")
                 if cache_dir else None)

        data = self._load(cache)
        if data is None:
            raw, table = read_contracts(db, mk, self.table)
            df = normalise(raw, mk)
            if df["expiry"].notna().sum() == 0:
                raise RuntimeError("could not parse the expiry column (all NaT) - check Market.expiry_epoch")
            data = classify(df)
            self.log.info("built from %s (table '%s')", db, table)
            self._save(cache, data)

        self.data = data
        self.app.ns["master"] = data.as_dict()           # the dict old notebooks expect
        self.log.info("%d chain rows, %d option contracts, %d futures, %d equities, %d tokens mapped",
                      len(data.chain), len(data.opt), len(data.futs), len(data.eqs), len(data.symbol_of))

    def describe(self) -> str:
        d = self.data
        return f"{len(d.symbol_of)} tokens" if d else "not built"

    # ---- cache ---------------------------------------------------------------------------
    def _load(self, path: str | None) -> MasterData | None:
        if not path or self.force or not os.path.exists(path):
            return None
        try:
            data = MasterData.from_dict(pd.read_pickle(path))
            self.log.info("loaded cache %s", path)
            return data
        except Exception as e:
            self.log.warning("cache unreadable (%s), rebuilding", e)
            return None

    def _save(self, path: str | None, data: MasterData) -> None:
        if not path:
            self.log.warning("no writable cache directory, not caching")
            return
        tmp = path + ".tmp"
        try:
            pd.to_pickle(data.as_dict(), tmp)
            os.replace(tmp, path)
            self.log.info("saved cache %s", path)
        except OSError as e:
            self.log.warning("cache not saved (%s)", e)
            try:
                os.remove(tmp)
            except OSError:
                pass
