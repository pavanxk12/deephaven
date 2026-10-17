"""The vendor 1-minute OHLCV -> the candles file that eod_load reads (tools/convert_us_ohlcv.py)."""
import importlib.util
import json
import os
import tarfile
from datetime import datetime, time as dtime, timezone

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

spec = importlib.util.spec_from_file_location("convert_us_ohlcv", os.path.join(os.path.dirname(__file__), "..", "tools", "convert_us_ohlcv.py"))
conv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conv)


def utc(h, m, day=(2026, 5, 15)):
    return datetime(*day, h, m, tzinfo=timezone.utc)


def vendor(rows, path):
    """rows: (ts, symbol, open, high, low, close, volume, instrument_id)"""
    cols = list(zip(*rows))
    t = pa.table({"ts_event": pa.array(cols[0], pa.timestamp("ns", "UTC")), "symbol": pa.array(cols[1], pa.large_string()),
                  "open": cols[2], "high": cols[3], "low": cols[4], "close": cols[5], "volume": pa.array(cols[6], pa.int64()),
                  "instrument_id": pa.array(cols[7], pa.uint32())})
    pq.write_table(t, path, compression="zstd")
    return path


ROWS = [  # 04:00 ET is 08:00 UTC; 09:30 ET is 13:30 UTC; 16:00 ET is 20:00 UTC
    (utc(13, 31), "SPY", 700.0, 701.0, 699.5, 700.5, 1000, 11), (utc(13, 30), "SPY", 699.0, 700.0, 698.0, 699.5, 500, 11),
    (utc(8, 0), "AAPL", 200.0, 200.5, 199.9, 200.2, 40, 7), (utc(13, 30), "AAPL", 201.0, 201.5, 200.9, 201.2, 60, 7),
    (utc(20, 0), "AAPL", 205.0, 205.1, 204.9, 205.0, 10, 7), (utc(20, 1), "AAPL", 205.0, 205.2, 205.0, 205.1, 0, 7),
]


@pytest.fixture
def folder(tmp_path):
    src = tmp_path / "vendor"
    src.mkdir()
    vendor(ROWS, str(src / "2026-05-15.parquet"))
    (src / "2026-05-25.empty").write_bytes(b"")
    return src


def test_the_converted_file_has_the_columns_and_types_of_the_candles_table_sorted_by_symbol_and_minute(tmp_path, folder):
    out = tmp_path / "out"
    r = conv.convert_file(str(folder / "2026-05-15.parquet"), str(out))
    assert r["status"] == "converted" and r["rows"] == 6 and r["source_rows"] == 6
    t = pq.read_table(str(out / "candles_2026-05-15.parquet"))
    assert t.column_names == ["Minute", "Symbol", "token", "Open", "High", "Low", "Close", "Volume", "Date"]
    assert [str(f.type) for f in t.schema] == ["timestamp[ns, tz=UTC]", "string", "int64", "double", "double", "double", "double", "int64", "string"]
    assert t["Symbol"].to_pylist() == ["AAPL"] * 4 + ["SPY"] * 2
    minutes = t["Minute"].to_pylist()
    assert minutes[:4] == sorted(minutes[:4]) and minutes[4:] == sorted(minutes[4:])
    assert set(t["Date"].to_pylist()) == {"2026-05-15"} and set(t["token"].to_pylist()) == {7, 11}
    assert pq.ParquetFile(str(out / "candles_2026-05-15.parquet")).metadata.row_group(0).column(0).compression == "ZSTD"


def test_a_meta_file_like_eod_saves_is_written_beside_it(tmp_path, folder):
    out = tmp_path / "out"
    conv.convert_file(str(folder / "2026-05-15.parquet"), str(out))
    meta = json.loads((out / "candles_2026-05-15.meta.json").read_text())
    assert (meta["table"], meta["day"], meta["market"], meta["timezone"], meta["rows"]) == ("candles", "2026-05-15", "US", "America/New_York", 6)
    assert meta["columns"] == conv.COLUMNS and meta["symbols"] == 2 and "instrument_id" in meta["built_by"]


def test_only_the_wanted_symbols_and_session_are_kept(tmp_path, folder):
    out = tmp_path / "out"
    conv.convert_file(str(folder / "2026-05-15.parquet"), str(out), symbols={"AAPL"}, session=conv.parse_session("09:30-16:00"))
    t = pq.read_table(str(out / "candles_2026-05-15.parquet"))
    assert t["Symbol"].to_pylist() == ["AAPL", "AAPL"]                               # 08:00 UTC (04:00 ET) and 20:01 UTC (16:01 ET) are outside
    assert [m.hour for m in t["Minute"].to_pylist()] == [13, 20]                     # 09:30 and 16:00 ET are inside, both ends included
    assert pc.sum(t["Volume"]).as_py() == 70


def test_a_day_that_is_converted_is_left_alone_unless_forced(tmp_path, folder):
    out = tmp_path / "out"
    src = str(folder / "2026-05-15.parquet")
    conv.convert_file(src, str(out))
    first = (out / "candles_2026-05-15.parquet").stat().st_mtime_ns
    assert conv.convert_file(src, str(out))["status"] == "exists" and (out / "candles_2026-05-15.parquet").stat().st_mtime_ns == first
    assert conv.convert_file(src, str(out), force=True)["status"] == "converted"
    assert not [p for p in os.listdir(out) if ".tmp" in p]                           # nothing half-written is left behind


def test_a_bad_file_is_reported_and_the_others_still_convert(tmp_path, folder):
    (folder / "2026-05-18.parquet").write_bytes(b"this is not parquet")
    vendor(ROWS, str(folder / "2026-05-19.parquet"))
    m = conv.convert_folder(str(folder), str(tmp_path / "out"), log=lambda *_: None)
    assert [r["day"] for r in m["failed"]] == ["2026-05-18"] and m["files"] == 2 and m["rows"] == 12
    assert m["holidays"] == ["2026-05-25"] and (m["first_day"], m["last_day"]) == ("2026-05-15", "2026-05-19")
    assert set(m["sha256"]) == {"candles_2026-05-15.parquet", "candles_2026-05-19.parquet"}
    assert os.path.exists(tmp_path / "out" / "manifest.json")


def test_a_file_with_missing_columns_or_a_naive_time_is_refused(tmp_path):
    t = pa.table({"ts_event": pa.array([datetime(2026, 5, 15, 13, 30)], pa.timestamp("ns")), "symbol": ["A"], "open": [1.0], "high": [1.0],
                  "low": [1.0], "close": [1.0], "volume": [1], "instrument_id": [1]})
    with pytest.raises(ValueError, match="time zone"):
        conv.to_candles(t, "2026-05-15")
    with pytest.raises(ValueError, match="missing columns"):
        conv.to_candles(t.drop(["volume"]), "2026-05-15")
    with pytest.raises(ValueError, match="yyyy-mm-dd"):
        conv.day_of("whatever.parquet")


def test_session_and_symbol_arguments_are_checked():
    assert conv.parse_session("09:30-16:00") == (dtime(9, 30), dtime(16, 0)) and conv.parse_session(None) is None
    for bad in ("9:30", "16:00-09:30", "ab-cd"):
        with pytest.raises(ValueError):
            conv.parse_session(bad)


def test_the_tar_holds_the_files_and_the_manifest_without_compression(tmp_path, folder):
    out = tmp_path / "out"
    conv.convert_folder(str(folder), str(out), log=lambda *_: None)
    n = conv.make_tar(str(out), str(tmp_path / "us.tar"))
    with tarfile.open(tmp_path / "us.tar") as tf:
        names = tf.getnames()
    assert n == len(names) and "candles_2026-05-15.parquet" in names and "manifest.json" in names
