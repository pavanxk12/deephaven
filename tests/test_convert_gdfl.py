"""GDFL cash 1-minute CSV -> the candles file that eod_load reads (tools/convert_gdfl_csv.py)."""
import importlib.util
import json
import os
import tarfile
from datetime import datetime, time as dtime, timezone

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

TOOLS = os.path.join(os.path.dirname(__file__), "..", "tools")
spec = importlib.util.spec_from_file_location("convert_gdfl_csv", os.path.join(TOOLS, "convert_gdfl_csv.py"))
conv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conv)

HEADER = "Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest\n"
ROWS = [
    "TCS.NSE,12/03/2026,09:16:59,3500.5,3502,3499,3501,200,0",
    "RELIANCE.NSE,12/03/2026,09:15:59,1390,1390,1381.1,1383,419349,0",
    "RELIANCE.NSE,12/03/2026,09:16:59,1382.3,1385.6,1381.7,1384.7,107345,0",
    "RELIANCE.NSE,12/03/2026,15:29:59,1390,1391,1389,1390.5,5000,0",
    "ABC.BE.NSE,12/03/2026,09:15:59,10,11,9.5,10.5,10,0",
    "0SCL26.YR.NSE,12/03/2026,10:30:59,996.05,996.05,996.05,996.05,10,0",
]


def write_csv(path, rows=ROWS, header=HEADER):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(header + "\n".join(rows) + "\n")
    return str(path)


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "MAR_2026" / "MAR_2026"
    write_csv(r / "GFDLCM_STOCK_12032026.CSV")                                       # the capital .CSV exists in the real data
    return tmp_path


def test_the_converted_file_has_the_columns_and_types_of_the_candles_table(tmp_path, root):
    out = tmp_path / "out"
    r = conv.convert_file(conv.find_files(str(root))[0], str(out), {})
    assert r["status"] == "converted" and r["rows"] == 6
    t = pq.read_table(str(out / "candles_2026-03-12.parquet"))
    assert t.column_names == ["Minute", "Symbol", "token", "Open", "High", "Low", "Close", "Volume", "Date"]
    assert [str(f.type) for f in t.schema] == ["timestamp[ns, tz=UTC]", "string", "int64", "double", "double", "double", "double", "int64", "string"]
    assert set(t["Date"].to_pylist()) == {"2026-03-12"}
    assert pq.ParquetFile(str(out / "candles_2026-03-12.parquet")).metadata.row_group(0).column(0).compression == "ZSTD"


def test_the_minute_is_the_start_of_the_bar_in_utc_and_the_symbol_has_no_dot_nse(tmp_path, root):
    out = tmp_path / "out"
    conv.convert_file(conv.find_files(str(root))[0], str(out), {})
    t = pq.read_table(str(out / "candles_2026-03-12.parquet"))
    rel = [(m, c) for m, s, c in zip(t["Minute"].to_pylist(), t["Symbol"].to_pylist(), t["Close"].to_pylist()) if s == "RELIANCE"]
    assert rel[0][0] == datetime(2026, 3, 12, 3, 45, tzinfo=timezone.utc) and rel[0][1] == 1383.0         # 09:15:59 India = the 09:15 bar = 03:45 UTC
    assert rel[-1][0] == datetime(2026, 3, 12, 9, 59, tzinfo=timezone.utc)                                # 15:29:59 India = the 15:29 bar
    assert t["Symbol"].to_pylist() == sorted(t["Symbol"].to_pylist())                                     # sorted by Symbol, then Minute
    assert set(t["Symbol"].to_pylist()) == {"0SCL26.YR", "ABC.BE", "RELIANCE", "TCS"}                     # another series keeps its series: unique names


def test_tokens_are_made_up_stable_and_never_reused(tmp_path, root):
    out = tmp_path / "out"
    tokens = {}
    conv.convert_file(conv.find_files(str(root))[0], str(out), tokens)
    first = dict(tokens)
    assert sorted(first) == ["0SCL26.YR", "ABC.BE", "RELIANCE", "TCS"] and sorted(first.values()) == [10_000_000 + i for i in range(4)]
    t = pq.read_table(str(out / "candles_2026-03-12.parquet"))
    assert {s: tk for s, tk in zip(t["Symbol"].to_pylist(), t["token"].to_pylist())} == first
    second = root / "MAR_2026" / "MAR_2026" / "GFDLCM_STOCK_13032026.csv"
    write_csv(second, [r.replace("12/03", "13/03") for r in ROWS] + ["INFY.NSE,13/03/2026,09:15:59,1,2,1,2,5,0"])
    conv.convert_file(str(second), str(out), tokens)
    assert all(tokens[k] == v for k, v in first.items()) and tokens["INFY"] == 10_000_004                # old ones unchanged, the new one next in line


def test_only_the_wanted_symbols_and_session_are_kept(tmp_path, root):
    out = tmp_path / "out"
    conv.convert_file(conv.find_files(str(root))[0], str(out), {}, symbols={"RELIANCE"}, session=conv.parse_session("09:15-15:00"))
    t = pq.read_table(str(out / "candles_2026-03-12.parquet"))
    assert t["Symbol"].to_pylist() == ["RELIANCE"] * 2 and pc.sum(t["Volume"]).as_py() == 419349 + 107345        # the 15:29 bar is after 15:00


def test_a_folder_run_finds_files_at_any_depth_resumes_and_writes_a_manifest(tmp_path, root):
    write_csv(root / "APR_2026" / "APR_2026" / "GFDLCM_STOCK_01042026.csv", [r.replace("12/03", "01/04") for r in ROWS])
    out = tmp_path / "out"
    m = conv.convert_folder(str(root), str(out), log=lambda *_: None)
    assert (m["files"], m["rows"], m["failed"], m["tickers"]) == (2, 12, [], 4) and (m["first_day"], m["last_day"]) == ("2026-03-12", "2026-04-01")
    assert set(m["sha256"]) == {"candles_2026-03-12.parquet", "candles_2026-04-01.parquet"} and (out / "tickers.json").exists()
    again = conv.convert_folder(str(root), str(out), log=lambda *_: None)
    assert again["files"] == 2 and again["rows"] == 12                                                         # nothing redone, nothing lost
    assert not [p for p in os.listdir(out) if ".tmp" in p]


def test_a_wrong_date_a_missing_column_a_bad_price_or_a_duplicate_day_is_reported_and_the_rest_carry_on(tmp_path, root):
    base = root / "MAR_2026" / "MAR_2026"
    write_csv(base / "GFDLCM_STOCK_13032026.csv")                                                              # the name says 13, the data says 12
    write_csv(base / "GFDLCM_STOCK_16032026.csv", ["TCS.NSE,16/03/2026,09:16:59,1,2,1,2,0"], header="Ticker,Date,Time,Open,High,Low,Close,Open Interest\n")
    write_csv(base / "GFDLCM_STOCK_17032026.csv", ["X.NSE,17/03/2026,09:15:59,0,1,0,1,5,0"])                   # a price of zero
    write_csv(root / "copy" / "GFDLCM_STOCK_12032026.csv")                                                      # the same day twice
    m = conv.convert_folder(str(root), str(tmp_path / "out"), log=lambda *_: None)
    errors = sorted((r["day"], r["error"]) for r in m["failed"])
    assert m["files"] == 1 and [d for d, _ in errors] == ["2026-03-12", "2026-03-13", "2026-03-16", "2026-03-17"]
    text = dict(errors)
    assert "second file" in text["2026-03-12"] and "Date column" in text["2026-03-13"]
    assert "missing columns" in text["2026-03-16"] and "zero or negative" in text["2026-03-17"]


def test_names_and_sessions_are_checked():
    assert conv.day_of("D:/x/GFDLCM_STOCK_01012026.csv") == "2026-01-01" and conv.day_of("a/GFDLCM_STOCK_31122026.CSV") == "2026-12-31"
    for bad in ("data.csv", "GFDLCM_STOCK_31022026.csv", "GFDLCM_STOCK_1012026.csv"):
        with pytest.raises(ValueError):
            conv.day_of(bad)
    assert conv.parse_session("09:15-15:30") == (dtime(9, 15), dtime(15, 30)) and conv.parse_session(None) is None
    with pytest.raises(ValueError):
        conv.parse_session("15:30-09:15")


def test_the_tar_holds_the_candles_the_manifest_and_the_ticker_numbers(tmp_path, root):
    out = tmp_path / "out"
    conv.convert_folder(str(root), str(out), log=lambda *_: None)
    conv.make_tar(str(out), str(tmp_path / "in.tar"))
    with tarfile.open(tmp_path / "in.tar") as tf:
        assert {"candles_2026-03-12.parquet", "manifest.json", "tickers.json"} <= set(tf.getnames())
    json.loads((out / "tickers.json").read_text())
