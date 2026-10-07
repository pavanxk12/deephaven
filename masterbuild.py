# build_master.py -- heavy parse of NSE contract master, done ONCE per CSV file
import os
import pandas as pd

# =============================== USER INPUT ===============================
CSV_PATH = "/data/fo_market_data/NSE_FO_contract.csv"
CACHE_DIR = "/data/fo_market_data"
FORCE_REBUILD = False
# ==========================================================================

_st = os.stat(CSV_PATH)
_key = f"{int(_st.st_mtime)}_{_st.st_size}"
MASTER_FILE = os.path.join(CACHE_DIR, f"master_{_key}.pkl")

if os.path.exists(MASTER_FILE) and not FORCE_REBUILD:
    master = pd.read_pickle(MASTER_FILE)
    print(f"[master] loaded {MASTER_FILE}")
else:
    df = pd.read_csv(
        CSV_PATH, skipinitialspace=True,
        usecols=["FinInstrmId", "FinInstrmNm", "TckrSymb", "XpryDt", "StrkPric", "OptnTp", "DelFlg"],
    )
    df = df[df["TckrSymb"].notna() & df["FinInstrmNm"].notna()]
    df = df[df["DelFlg"].astype(str).str.strip() != "Y"]
    for c in ("TckrSymb", "FinInstrmNm", "OptnTp"):
        df[c] = df[c].astype(str).str.strip().str.upper()

    # NSE epoch: seconds since 1980-01-01
    df["expiry"] = (pd.Timestamp("1980-01-01")
                    + pd.to_timedelta(pd.to_numeric(df["XpryDt"], errors="coerce"), unit="s")).dt.normalize()
    df["strike"] = pd.to_numeric(df["StrkPric"], errors="coerce") / 100.0

    opt = df[df["FinInstrmNm"].str.startswith("OPT")]
    ce = (opt[opt["OptnTp"] == "CE"][["FinInstrmId", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"FinInstrmId": "ce_token"}))
    pe = (opt[opt["OptnTp"] == "PE"][["FinInstrmId", "TckrSymb", "expiry", "strike"]]
          .rename(columns={"FinInstrmId": "pe_token"}))
    chain_all = ce.merge(pe, on=["TckrSymb", "expiry", "strike"], how="inner").reset_index(drop=True)

    futs_all = (df[df["FinInstrmNm"].str.startswith("FUT")][["FinInstrmId", "TckrSymb", "expiry"]]
                .rename(columns={"FinInstrmId": "fut_token"}).reset_index(drop=True))

    master = {"chain": chain_all, "futs": futs_all}
    pd.to_pickle(master, MASTER_FILE)
    print(f"[master] built and saved {MASTER_FILE}")

print(f"[master] {len(master['chain'])} chain rows, {len(master['futs'])} futures, "
      f"{master['chain']['TckrSymb'].nunique()} symbols")