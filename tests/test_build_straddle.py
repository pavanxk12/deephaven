"""Straddle history from GDFL F&O 1-minute files (tools/build_straddle_history.py): the same engine, columns and IV as straddle_start()."""
import importlib.util
import json
import math
import os
import tarfile
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from algo.series.straddle import StraddleConfig, StraddleEngine  # noqa: E402

TOOLS = os.path.join(os.path.dirname(__file__), "..", "tools")
spec = importlib.util.spec_from_file_location("build_straddle_history", os.path.join(TOOLS, "build_straddle_history.py"))
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)

HEADER = "Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest\n"
STRIKES = [24500 + 100 * i for i in range(11)]
CFG = StraddleConfig(expiries=1)


def bars(day_ddmmyyyy, expiry="27JAN26", strikes=STRIKES, fut=25000.0, minutes=range(15, 26), fut_expiry="27JAN26", skip=()):
    """One bar a minute for the future and for both options of every strike. The 25000 strike costs 100 + 90, the others more."""
    rows = []
    for m in minutes:
        t = f"09:{m:02d}:59"
        rows.append(f"NIFTY{fut_expiry}FUT.NFO,{day_ddmmyyyy},{t},{fut},{fut},{fut},{fut},50,1000")
        for k in strikes:
            if f"{k}" in skip:
                continue
            extra = abs(k - 25000) * 0.3
            ce, pe = (100 if k == 25000 else 100 + extra), (90 if k == 25000 else 90 + extra)
            rows.append(f"NIFTY{expiry}{k}CE.NFO,{day_ddmmyyyy},{t},{ce},{ce},{ce},{ce},10,0")
            rows.append(f"NIFTY{expiry}{k}PE.NFO,{day_ddmmyyyy},{t},{pe},{pe},{pe},{pe},10,0")
    return rows


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(HEADER + "\n".join(rows) + "\n")
    return str(path)


@pytest.fixture
def root(tmp_path):
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv", bars("05/01/2026"))
    return tmp_path


def test_a_day_gives_the_straddle_table_columns_with_iv_and_a_date(tmp_path, root):
    out = tmp_path / "out"
    r = tool.convert_file(tool.find_files(str(root), 2026)[0][0], str(out), tool.Contracts(), CFG)
    assert r["status"] == "converted" and r["series"] == 1
    t = pq.read_table(str(out / "straddle_2026-01-05.parquet"))
    assert t.column_names == list(StraddleEngine.COLUMNS) + ["Date"] and t.column_names[-2] == "IV"
    assert [str(t.schema.field(c).type) for c in ("Time", "Symbol", "Expiry", "V", "IV", "Date")] == ["string", "string", "string", "double", "double", "string"]
    d = t.slice(0, 1).to_pylist()[0]
    assert (d["Symbol"], d["Expiry"], d["Date"], d["Fut"], d["ATMStrike"], d["MinStrike"], d["V"]) == ("NIFTY", "2026-01-27", "2026-01-05", 25000.0, 25000.0, 25000.0, 190.0)
    assert d["Synthetic"] == 25000 + 100 - 90 and d["Time"] == "09:16:02"                   # the 09:15 bar closes at 09:16; sampled 2 seconds after
    assert d["IV"] == 190.0 / 25010.0 * math.sqrt(22) * 100                                   # 5 Jan to 27 Jan is 22 days
    assert t.num_rows == 11                                                                   # one point a minute: the bars 09:15 to 09:25 close at 09:16 to 09:26


def test_the_rows_are_the_ones_the_live_engine_makes_for_the_same_bars(tmp_path, root):
    """The tool adds nothing of its own: feed the same closes to StraddleEngine and the rows are identical."""
    out = tmp_path / "out"
    tool.convert_file(tool.find_files(str(root), 2026)[0][0], str(out), tool.Contracts(), CFG)
    got = pq.read_table(str(out / "straddle_2026-01-05.parquet")).to_pylist()
    from datetime import datetime
    from zoneinfo import ZoneInfo

    import numpy as np

    from algo.series.straddle import SeriesDef
    strikes = np.array(STRIKES, dtype=float)
    eng = StraddleEngine({("NIFTY", "2026-01-27"): SeriesDef(ref=0, strikes=strikes, ce=tuple(range(1, 12)), pe=tuple(range(12, 23)))}, CFG)
    eng.cfg = StraddleConfig(expiries=1, max_age_s=180)
    ist = ZoneInfo("Asia/Kolkata")
    want = []
    for m in range(16, 27):
        when = datetime(2026, 1, 5, 9, m, 2, tzinfo=ist)
        eng.set_price(0, 25000.0, when.timestamp())
        for i, k in enumerate(STRIKES):
            extra = abs(k - 25000) * 0.3
            eng.set_price(1 + i, 100 if k == 25000 else 100 + extra, when.timestamp())
            eng.set_price(12 + i, 90 if k == 25000 else 90 + extra, when.timestamp())
        want += [dict(zip(StraddleEngine.COLUMNS, r)) for r in eng.sample(when, when.timestamp())]
    assert len(got) == len(want) == 11
    for g, w in zip(got, want):
        assert {k: v for k, v in g.items() if k != "Date"} == w


def test_a_zip_gives_the_same_table_as_the_csv(tmp_path, root):
    csv = tool.find_files(str(root), 2026)[0][0]
    z = tmp_path / "ZIPS" / "GFDLNFO_CONTRACT_05012026.zip"
    os.makedirs(z.parent)
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv, "GFDLNFO_CONTRACT_05012026.csv")
    a, b = tmp_path / "a", tmp_path / "b"
    tool.convert_file(csv, str(a), tool.Contracts(), CFG)
    tool.convert_file(str(z), str(b), tool.Contracts(), CFG)
    assert pq.read_table(str(a / "straddle_2026-01-05.parquet")).equals(pq.read_table(str(b / "straddle_2026-01-05.parquet")))


def test_weekly_options_use_the_monthly_future_and_the_nearest_expiry_is_chosen(tmp_path):
    rows = bars("05/01/2026", expiry="13JAN26", fut_expiry="27JAN26") + bars("05/01/2026", expiry="20JAN26", fut_expiry="27JAN26", minutes=range(15, 17))[:0]
    rows += [r.replace("NIFTY13JAN26", "NIFTY20JAN26") for r in bars("05/01/2026", expiry="13JAN26", fut_expiry="27JAN26") if "FUT" not in r]
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv", rows)
    out = tmp_path / "out"
    r = tool.convert_file(tool.find_files(str(tmp_path), 2026)[0][0], str(out), tool.Contracts(), StraddleConfig(expiries=1))
    t = pq.read_table(str(out / "straddle_2026-01-05.parquet")).to_pylist()
    assert r["series"] == 1 and {x["Expiry"] for x in t} == {"2026-01-13"} and t[0]["Fut"] == 25000.0       # nearest expiry only, on the monthly future
    out2 = tmp_path / "out2"
    r2 = tool.convert_file(tool.find_files(str(tmp_path), 2026)[0][0], str(out2), tool.Contracts(), StraddleConfig(expiries=2))     # straddle_start(1)
    assert r2["series"] == 2 and {x["Expiry"] for x in pq.read_table(str(out2 / "straddle_2026-01-05.parquet")).to_pylist()} == {"2026-01-13", "2026-01-20"}


def test_a_day_with_too_few_strikes_gives_no_rows_and_is_called_empty(tmp_path):
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv", bars("05/01/2026", strikes=STRIKES[:6]))
    r = tool.convert_file(tool.find_files(str(tmp_path), 2026)[0][0], str(tmp_path / "out"), tool.Contracts(), CFG)
    assert r["status"] == "empty" and not os.path.exists(tmp_path / "out" / "straddle_2026-01-05.parquet")


def test_a_contract_that_traded_on_one_of_the_last_days_is_still_known_and_an_old_one_is_forgotten(tmp_path):
    c = tool.Contracts()
    c.add_day(["A", "B"])
    for i in range(tool.KNOWN_DAYS):
        known = c.add_day(["C"])
    assert "A" in known and "B" in known                                                      # five days later it is still listed
    assert "A" not in c.add_day(["C"])                                                        # six days later it is forgotten
    jan = tmp_path / "JAN_2026"
    write_csv(jan / "GFDLNFO_CONTRACT_05012026.csv", bars("05/01/2026"))
    write_csv(jan / "GFDLNFO_CONTRACT_06012026.csv", bars("06/01/2026", skip=("24500", "24600", "24700", "24800")))   # only 7 strikes trade today
    m = tool.build_folder(str(tmp_path), str(tmp_path / "out"), CFG, 2026, log=lambda *_: None)
    assert m["files"] == 2 and not m["failed"] and m["empty_days"] == []                      # the other strikes were known from yesterday: 11 in the chain


def test_a_folder_run_picks_the_year_prefers_csv_to_zip_resumes_and_reports_a_bad_file(tmp_path):
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv", bars("05/01/2026"))
    write_csv(tmp_path / "JAN_2025" / "GFDLNFO_CONTRACT_06012025.csv", bars("06/01/2025"))                        # another year
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_07012026.csv", bars("08/01/2026"))                        # the name and the data disagree
    with zipfile.ZipFile(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.zip", "w") as zf:
        zf.writestr("x.csv", "not used")
    files, extra = tool.find_files(str(tmp_path), 2026)
    assert [os.path.basename(f) for f in files] == ["GFDLNFO_CONTRACT_05012026.csv", "GFDLNFO_CONTRACT_07012026.csv"] and len(extra) == 1
    out = tmp_path / "out"
    m = tool.build_folder(str(tmp_path), str(out), CFG, 2026, log=lambda *_: None)
    assert m["files"] == 1 and [r["day"] for r in m["failed"]] == ["2026-01-07"] and "Date column" in m["failed"][0]["error"]
    assert (m["first_day"], m["last_day"]) == ("2026-01-05", "2026-01-05") and len(m["extra_copies_left_out"]) == 1
    meta = json.loads((out / "straddle_2026-01-05.meta.json").read_text())
    assert meta["table"] == "straddle_series" and meta["columns"][-2:] == ["IV", "Date"] and "expiries=1" in meta["built_by"]
    again = tool.build_folder(str(tmp_path), str(out), CFG, 2026, log=lambda *_: None)
    assert again["files"] == 1 and again["rows"] == m["rows"] and not [p for p in os.listdir(out) if ".tmp" in p]


def test_tickers_are_read_the_way_the_exchange_writes_them():
    c = tool.Contracts()
    sym, exp, strike, typ = c.parse("NIFTY06JAN2625700CE.NFO")
    assert (sym, exp.strftime("%Y-%m-%d"), strike, typ) == ("NIFTY", "2026-01-06", 25700.0, "CE")
    assert c.parse("EXIDEIND27JAN26377.5PE.NFO")[2:] == (377.5, "PE") and c.parse("360ONE24FEB26FUT.NFO")[0] == "360ONE" and c.parse("360ONE24FEB26FUT.NFO")[3] == "FUT"
    assert c.parse("NOT A TICKER") is None and c.parse("NIFTY31FEB26FUT.NFO") is None                          # 31 February is not a date


def test_the_tar_holds_the_straddle_files_and_the_manifest(tmp_path, root):
    out = tmp_path / "out"
    tool.build_folder(str(root), str(out), CFG, 2026, log=lambda *_: None)
    n = tool.make_tar_straddle(str(out), str(tmp_path / "s.tar"))
    with tarfile.open(tmp_path / "s.tar") as tf:
        assert n == len(tf.getnames()) and {"straddle_2026-01-05.parquet", "manifest.json"} <= set(tf.getnames())
