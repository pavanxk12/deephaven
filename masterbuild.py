# build_master.py -- parse today's contract master from the dated .db3 (SQLite), done ONCE per file
# Output: `master` dict with
#   master["chain"] : option chain rows (ce_token, pe_token, TckrSymb, expiry, strike)
#   master["futs"]  : futures rows (fut_token, TckrSymb, expiry, tick, lot)   tick in RUPEES, lot in qty
import os
import glob
import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta

# =============================== USER INPUT ===============================
DB_DIR = "/data/fo_market_data"
DB_DATE = None                    # None = today (IST) -> yyyymmdd ; or e.g. "20261012"
DB_PATH = None                    # None = auto: DB_DIR/<yyyymmdd>.db3 (or any *yyyymmdd*.db3); or full path
TABLE = None                      # None = auto-detect the table that has Token + Symbol columns
CACHE_DIR = "/data/fo_market_data"
FORCE_REBUILD = False
# ==========================================================================

IST = timezone(timedelta(hours=5, minutes=30))
_day = DB_DATE or datetime.now(IST).strftime("%Y%m%d")

if DB_PATH is None:
    _exact = os.path.join(DB_DIR, f"{_day}.db3")
    if os.path.exists(_exact):
        DB_PATH = _exact
    else:
        _hits = sorted(glob.glob(os.path.join(DB_DIR, f"*{_day}*.db3")))
        if not _hits:
            raise FileNotFoundError(f"no .db3 for {_day} in {DB_DIR}")
        DB_PATH = _hits[-1]

_st = os.stat(DB_PATH)
_key = f"{_day}_{int(_st.st_mtime)}_{_st.st_size}"
MASTER_FILE = os.path.join(CACHE_DIR, f"master_v3_{_key}.pkl")      # v3 = built from db3

if os.path.exists(MASTER_FILE) and not FORCE_REBUILD:
    master = pd.read_pickle(MASTER_FILE)
    print(f"[master] loaded {MASTER_FILE}")
else:
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        tbl = TABLE
        if tbl is None:
            names = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            for n in names:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{n}")')}
                if {"Token", "Symbol", "ExpiryDate", "InstType"} <= cols:
                    tbl = n
                    break
            if tbl is None:
                raise RuntimeError(f"no table with Token/Symbol/ExpiryDate/InstType found; tables: {names}")
        df = pd.read_sql_query(
            f'SELECT Token, Symbol, ExpiryDate, InstType, OptionType, StrikePrice, LotSize, TickSize '
            f'FROM "{tbl}"', con)
    finally:
        con.close()

    df = df[df["Symbol"].notna() & df["InstType"].notna()]
    for c in ("Symbol", "InstType", "OptionType"):
        df[c] = df[c].astype(str).str.strip().str.upper()

    # NSE epoch: seconds since 1980-01-01
    df["expiry"] = (pd.Timestamp("1980-01-01")
                    + pd.to_timedelta(pd.to_numeric(df["ExpiryDate"], errors="coerce"), unit="s")).dt.normalize()
    df["strike"] = pd.to_numeric(df["StrikePrice"], errors="coerce") / 100.0
    df["tick"] = pd.to_numeric(df["TickSize"], errors="coerce") / 100.0     # paise -> rupees
    df["lot"] = pd.to_numeric(df["LotSize"], errors="coerce")
    df["Token"] = pd.to_numeric(df["Token"], errors="coerce").astype("int64")
    df = df.rename(columns={"Symbol": "TckrSymb"})

    opt = df[df["InstType"].str.startswith("OPT")]
    ce = (opt[opt["OptionType"] == "CE"][["Token", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"Token": "ce_token"}))
    pe = (opt[opt["OptionType"] == "PE"][["Token", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"Token": "pe_token"}))
    chain_all = (ce.merge(pe, on=["TckrSymb", "expiry", "strike"], how="inner")
                   .drop_duplicates(["TckrSymb", "expiry", "strike"])
                   .reset_index(drop=True))

    futs_all = (df[df["InstType"].str.startswith("FUT")]
                [["Token", "TckrSymb", "expiry", "tick", "lot"]]
                .rename(columns={"Token": "fut_token"}).reset_index(drop=True))

    master = {"chain": chain_all, "futs": futs_all}
    pd.to_pickle(master, MASTER_FILE)
    print(f"[master] built from {DB_PATH} (table '{tbl}') and saved {MASTER_FILE}")

print(f"[master] {len(master['chain'])} chain rows, {len(master['futs'])} futures, "
      f"{master['chain']['TckrSymb'].nunique()} symbols")

# quick sanity check: show nearest future of a few symbols
_f = master["futs"].sort_values("expiry").groupby("TckrSymb").first().reset_index()
print("[master] sample futures:\n",
      _f[_f["TckrSymb"].isin(["NIFTY", "BANKNIFTY", "RELIANCE"])]
      [["fut_token", "TckrSymb", "expiry", "tick", "lot"]].to_string(index=False))