# build_master.py -- parse today's contract master from the dated .db3 (SQLite), done ONCE per file
# Output: `master` dict
#   master["chain"]     : option chain rows (ce_token, pe_token, TckrSymb, expiry, strike)
#   master["opt"]       : every option contract (Token, TckrSymb, expiry, strike, OptionType)
#   master["futs"]      : futures rows (fut_token, TckrSymb, expiry, tick, lot)
#   master["eqs"]       : equities/ETFs (eq_token, TckrSymb)
#   master["symbol_of"] : {token: "NIFTY 27OCT26 25000 CE" / "SPY 19DEC26 FUT" / "AAPL"}
import os
import glob
import sqlite3
import tempfile
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

# =============================== USER INPUT ===============================
MARKET = "NSE"             # "NSE" or "US"
DB_DATE = None            # None = today in market tz (yyyymmdd) or e.g. "20261012"
DB_PATH = None            # None = auto: <db_dir>/<yyyymmdd>.db3 (or *yyyymmdd*.db3); or full path
TABLE = None              # None = auto-detect the contract table
CACHE_DIR = None          # None = auto: first writable of [/tmp/fo_master_cache, ~/.cache/fo_master, db_dir]
FORCE_REBUILD = False

CONFIGS = {
    "NSE": dict(
        tz="Asia/Kolkata", db_dir="/data/fo_market_data",
        expiry_epoch="1980-01-01",
        strike_div=100.0, tick_div=100.0,           # paise -> rupees
        db_cols=dict(token="Token", symbol="Symbol", expiry="ExpiryDate", inst="InstType",
                     opt="OptionType", strike="StrikePrice", lot="LotSize", tick="TickSize"),
    ),
    "US": dict(
        tz="America/New_York", db_dir="/data/fo_market_data",
        expiry_epoch="1970-01-01",
        strike_div=1.0, tick_div=1.0,               # 1000.0 for OCC-style strikes
        db_cols=dict(token="Token", symbol="Symbol", expiry="ExpiryDate", inst="InstType",
                     opt="OptionType", strike="StrikePrice", lot="LotSize", tick="TickSize"),
    ),
}
# ==========================================================================

cfg = CONFIGS[MARKET]
DC = cfg["db_cols"]
_day = DB_DATE or datetime.now(ZoneInfo(cfg["tz"])).strftime("%Y%m%d")

if DB_PATH is None:
    _exact = os.path.join(cfg["db_dir"], f"{_day}.db3")
    if os.path.exists(_exact):
        DB_PATH = _exact
    else:
        _hits = sorted(glob.glob(os.path.join(cfg["db_dir"], f"*{_day}*.db3")))
        if not _hits:
            raise FileNotFoundError(f"no .db3 for {_day} in {cfg['db_dir']}")
        DB_PATH = _hits[-1]


def _pick_cache_dir():
    """First writable directory from the candidates, else None (caching disabled)."""
    cands = [CACHE_DIR] if CACHE_DIR else []
    cands += [os.path.join(tempfile.gettempdir(), "fo_master_cache"),
              os.path.expanduser("~/.cache/fo_master"),
              cfg["db_dir"]]
    for d in cands:
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            if os.access(d, os.W_OK):
                return d
        except OSError:
            pass
    return None


_cache_dir = _pick_cache_dir()
_st = os.stat(DB_PATH)
MASTER_FILE = (os.path.join(_cache_dir,
                            f"master_v3_{MARKET}_{_day}_{int(_st.st_mtime)}_{_st.st_size}.pkl")
               if _cache_dir else None)


def save_cache(obj):
    """Best-effort atomic pickle write; never raises."""
    if not MASTER_FILE:
        print("[master] no writable cache dir, skipping cache")
        return
    tmp = MASTER_FILE + ".tmp"
    try:
        pd.to_pickle(obj, tmp)
        os.replace(tmp, MASTER_FILE)
        print(f"[master] saved cache {MASTER_FILE}")
    except OSError as e:
        print(f"[master] cache not saved ({e})")
        try:
            os.remove(tmp)
        except OSError:
            pass


def load_cache():
    """Return cached master or None (missing/corrupt/disabled)."""
    if not MASTER_FILE or FORCE_REBUILD or not os.path.exists(MASTER_FILE):
        return None
    try:
        m = pd.read_pickle(MASTER_FILE)
        print(f"[master] loaded {MASTER_FILE}")
        return m
    except Exception as e:
        print(f"[master] cache unreadable ({e}); rebuilding")
        return None


def parse_expiry(s):
    n = pd.to_numeric(s, errors="coerce")
    if n.notna().mean() > 0.5:                                  # numeric column
        med = n.median()
        if 19000101 <= med <= 21001231:                         # yyyymmdd
            out = pd.to_datetime(n.astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
        else:                                                   # epoch seconds / ms
            unit = "ms" if med > 1e11 else "s"
            out = pd.Timestamp(cfg["expiry_epoch"]) + pd.to_timedelta(n, unit=unit)
    else:                                                       # date strings (ISO etc.)
        out = pd.to_datetime(s, errors="coerce")
    return out.dt.normalize()


def build_symbol_map(m):
    sym = {}
    for r in m["opt"].itertuples(index=False):
        sym[int(r.Token)] = f"{r.TckrSymb} {r.expiry:%d%b%y}".upper() + f" {r.strike:g} {r.OptionType}"
    for r in m["futs"].itertuples(index=False):
        sym[int(r.fut_token)] = f"{r.TckrSymb} {r.expiry:%d%b%y}".upper() + " FUT"
    for r in m["eqs"].itertuples(index=False):
        sym[int(r.eq_token)] = r.TckrSymb
    return sym


master = load_cache()

if master is None:
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        tbl = TABLE
        if tbl is None:
            need = {DC["token"], DC["symbol"], DC["expiry"], DC["inst"]}
            names = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            for n in names:
                if need <= {r[1] for r in con.execute(f'PRAGMA table_info("{n}")')}:
                    tbl = n
                    break
            if tbl is None:
                raise RuntimeError(f"no table with columns {need}; tables: {names}")
        raw = pd.read_sql_query(f'SELECT * FROM "{tbl}"', con)
    finally:
        con.close()

    df = pd.DataFrame({
        "Token": pd.to_numeric(raw[DC["token"]], errors="coerce"),
        "TckrSymb": raw[DC["symbol"]].astype(str).str.strip().str.upper(),
        "InstType": raw[DC["inst"]].astype(str).str.strip().str.upper(),
        "OptionType": (raw[DC["opt"]].astype(str).str.strip().str.upper()
                       if DC["opt"] in raw else ""),
        "strike": (pd.to_numeric(raw[DC["strike"]], errors="coerce") / cfg["strike_div"]
                   if DC["strike"] in raw else float("nan")),
        "tick": (pd.to_numeric(raw[DC["tick"]], errors="coerce") / cfg["tick_div"]
                 if DC["tick"] in raw else float("nan")),
        "lot": (pd.to_numeric(raw[DC["lot"]], errors="coerce")
                if DC["lot"] in raw else float("nan")),
        "expiry": parse_expiry(raw[DC["expiry"]]),
    })
    print(f"[master] table '{tbl}': {len(df)} rows; ExpiryDate sample = "
          f"{raw[DC['expiry']].head(3).tolist()} -> {df['expiry'].head(3).tolist()}")

    if df["expiry"].notna().sum() == 0:
        raise RuntimeError("could not parse ExpiryDate (all NaT) - check expiry_epoch / format in CONFIGS")

    df = df[df["Token"].notna() & df["expiry"].notna()].copy()
    df["Token"] = df["Token"].astype("int64")
    df["OptionType"] = df["OptionType"].str[:1].map({"C": "CE", "P": "PE"})   # C/CALL/CE, P/PUT/PE

    print("[master] InstType counts:\n", df["InstType"].value_counts().to_string())

    itype = df["InstType"]
    is_opt = itype.str.startswith("OPT") | df["OptionType"].notna()
    is_fut = itype.str.startswith("FUT") & ~is_opt
    is_eq = itype.isin(["EQUITY", "EQ", "STK", "ETF", "INDEX"]) & ~is_opt & ~is_fut

    opt = df[is_opt & df["OptionType"].notna() & (df["strike"] > 0)]
    ce = (opt[opt["OptionType"] == "CE"][["Token", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"Token": "ce_token"}))
    pe = (opt[opt["OptionType"] == "PE"][["Token", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"Token": "pe_token"}))
    chain = (ce.merge(pe, on=["TckrSymb", "expiry", "strike"], how="inner")
               .drop_duplicates(["TckrSymb", "expiry", "strike"]).reset_index(drop=True))
    futs = (df[is_fut][["Token", "TckrSymb", "expiry", "tick", "lot"]]
            .rename(columns={"Token": "fut_token"}).reset_index(drop=True))
    eqs = (df[is_eq][["Token", "TckrSymb"]]
           .rename(columns={"Token": "eq_token"}).reset_index(drop=True))

    master = {"chain": chain, "opt": opt.reset_index(drop=True), "futs": futs, "eqs": eqs}
    master["symbol_of"] = build_symbol_map(master)
    print(f"[master] built from {DB_PATH} (table '{tbl}')")
    save_cache(master)

print(f"[master] {len(master['chain'])} chain rows, {len(master['opt'])} option contracts, "
      f"{len(master['futs'])} futures, {len(master['eqs'])} equities, "
      f"{len(master['symbol_of'])} tokens mapped")

# quick sanity check
for k in ("opt", "futs", "eqs"):
    print(f"[master] sample {k}:\n", master[k].head(3).to_string(index=False))