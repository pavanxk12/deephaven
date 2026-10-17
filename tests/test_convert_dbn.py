"""Databento .dbn.zst days -> candles files, added to a folder that already holds other days (tools/convert_databento_dbn.py). The databento library is faked."""
import importlib.util
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")
import pyarrow.parquet as pq  # noqa: E402

TOOLS = os.path.join(os.path.dirname(__file__), "..", "tools")
sys.path.insert(0, TOOLS)
spec = importlib.util.spec_from_file_location("convert_databento_dbn", os.path.join(TOOLS, "convert_databento_dbn.py"))
conv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conv)
import convert_us_ohlcv as us  # noqa: E402


def frame(day, rows):
    """rows: (hour, minute, symbol, id, o, h, l, c, v) in UTC, the way store.to_df(price_type='float', pretty_ts=True, map_symbols=True) returns them."""
    idx = pd.DatetimeIndex([pd.Timestamp(f"{day} {h:02d}:{m:02d}", tz="UTC") for h, m, *_ in rows], name="ts_event")
    return pd.DataFrame({"rtype": 33, "publisher_id": 2, "instrument_id": [r[3] for r in rows], "open": [r[4] for r in rows], "high": [r[5] for r in rows],
                         "low": [r[6] for r in rows], "close": [r[7] for r in rows], "volume": pd.array([r[8] for r in rows], dtype="uint64"),
                         "symbol": [r[2] for r in rows]}, index=idx)


@pytest.fixture
def fake_databento(monkeypatch):
    """A databento module whose DBNStore.from_file returns a store whose to_df gives the frame registered for that file name."""
    frames = {}

    class Store:
        def __init__(self, path):
            self.path = path

        @classmethod
        def from_file(cls, path):
            return cls(path)

        def to_df(self, price_type="float", pretty_ts=True, map_symbols=True):
            assert (price_type, pretty_ts, map_symbols) == ("float", True, True)         # prices as numbers, real timestamps, the tickers from the file's own mappings
            return frames[os.path.basename(self.path)].copy()

    mod = types.ModuleType("databento")
    mod.DBNStore = Store
    monkeypatch.setitem(sys.modules, "databento", mod)
    return frames


ROWS = [(13, 30, "SPY", 11, 700.0, 701.0, 699.5, 700.5, 1000), (13, 31, "SPY", 11, 700.5, 701.0, 700.0, 700.8, 500), (8, 0, "AAPL", 7, 200.0, 200.5, 199.9, 200.2, 40)]


def dbn(tmp_path, day, rows=ROWS, name=None):
    path = tmp_path / "dl" / (name or f"xnas-itch-{day.replace('-', '')}.ohlcv-1m.dbn.zst")
    os.makedirs(path.parent, exist_ok=True)
    path.write_bytes(b"x")
    return str(path)


def test_the_day_comes_from_the_file_name():
    assert conv.day_of("D:/x/xnas-itch-20260914.ohlcv-1m.dbn.zst") == "2026-09-14" and conv.day_of("a/xnas-itch-20261016.ohlcv-1m.dbn") == "2026-10-16"
    for bad in ("manifest.json", "xnas-itch-2026.ohlcv-1m.dbn.zst", "xnas-itch-20260914.trades.dbn.zst"):
        with pytest.raises(ValueError, match="must end in"):
            conv.day_of(bad)


def test_the_table_has_what_the_us_converter_expects_with_the_ticker_from_the_files_own_mappings(tmp_path, fake_databento):
    fake_databento["xnas-itch-20260914.ohlcv-1m.dbn.zst"] = frame("2026-09-14", ROWS)
    t = conv.read_dbn(dbn(tmp_path, "2026-09-14"))
    assert t.column_names == ["ts_event", "symbol", "open", "high", "low", "close", "volume", "instrument_id"]
    assert [str(f.type) for f in t.schema] == ["timestamp[ns, tz=UTC]", "large_string", "double", "double", "double", "double", "int64", "uint32"]
    assert t["symbol"].to_pylist() == ["SPY", "SPY", "AAPL"] and t["instrument_id"].to_pylist() == [11, 11, 7] and t["volume"].to_pylist() == [1000, 500, 40]


def test_bars_with_an_undefined_price_are_dropped_and_said(tmp_path, fake_databento, capsys):
    df = frame("2026-09-14", ROWS)
    df.loc[df.index[1], "close"] = float("nan")
    fake_databento["xnas-itch-20260914.ohlcv-1m.dbn.zst"] = df
    t = conv.read_dbn(dbn(tmp_path, "2026-09-14"))
    assert t.num_rows == 2 and "1 bar(s) with an undefined price were dropped" in capsys.readouterr().out


def test_a_download_is_converted_to_candles_and_added_to_a_folder_that_has_other_days(tmp_path, fake_databento):
    out = tmp_path / "out"
    # the folder already holds 2026-09-14 from an earlier feed
    old = tmp_path / "old_src"
    old.mkdir()
    import pyarrow.compute as pc  # noqa: F401
    vendor = pa.table({"ts_event": pa.array([pd.Timestamp("2026-09-14 13:30", tz="UTC").value], pa.timestamp("ns", "UTC")), "symbol": pa.array(["SPY"], pa.large_string()),
                       "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": pa.array([7], pa.int64()), "instrument_id": pa.array([11], pa.uint32())})
    pq.write_table(vendor, str(old / "2026-09-14.parquet"))
    us.convert_folder(str(old), str(out), log=lambda *_: None)
    before = (out / "candles_2026-09-14.parquet").read_bytes()
    for day in ("2026-09-14", "2026-09-15"):
        fake_databento[f"xnas-itch-{day.replace('-', '')}.ohlcv-1m.dbn.zst"] = frame(day, ROWS)
        dbn(tmp_path, day)
    m = us.convert_folder(str(tmp_path / "dl"), str(out), log=lambda *_: None, pattern="*.ohlcv-1m.dbn*", read=conv.read_dbn, dayfn=conv.day_of, label=conv.LABEL)
    assert m["this_run"] == {"converted": ["2026-09-15"], "already_there": ["2026-09-14"], "empty": []}                  # the day that was there is left alone
    assert (out / "candles_2026-09-14.parquet").read_bytes() == before
    assert m["files"] == 2 and (m["first_day"], m["last_day"]) == ("2026-09-14", "2026-09-15") and not m["failed"]
    t = pq.read_table(str(out / "candles_2026-09-15.parquet"))
    assert t.column_names == ["Minute", "Symbol", "token", "Open", "High", "Low", "Close", "Volume", "Date"] and set(t["Date"].to_pylist()) == {"2026-09-15"}
    assert t["Symbol"].to_pylist() == ["AAPL", "SPY", "SPY"] and t["token"].to_pylist() == [7, 11, 11]
    meta = json.loads((out / "candles_2026-09-15.meta.json").read_text())
    assert "Databento XNAS.ITCH" in meta["built_by"] and meta["source"] == "xnas-itch-20260915.ohlcv-1m.dbn.zst"


def test_the_manifest_lists_every_file_of_the_folder_and_says_when_two_feeds_are_mixed(tmp_path, fake_databento):
    out = tmp_path / "out"
    for day in ("2026-09-14", "2026-09-15"):
        fake_databento[f"xnas-itch-{day.replace('-', '')}.ohlcv-1m.dbn.zst"] = frame(day, ROWS)
        dbn(tmp_path, day)
    m = us.convert_folder(str(tmp_path / "dl"), str(out), log=lambda *_: None, pattern="*.ohlcv-1m.dbn*", read=conv.read_dbn, dayfn=conv.day_of, label=conv.LABEL)
    assert m["files"] == 2 and m["mixed_sources"] is False and list(m["sources"]) == [f"converted from {conv.LABEL}"]
    fake_databento["xnas-itch-20260916.ohlcv-1m.dbn.zst"] = frame("2026-09-16", ROWS)
    dbn(tmp_path, "2026-09-16")
    src2 = tmp_path / "dl2"
    os.makedirs(src2)
    os.replace(tmp_path / "dl" / "xnas-itch-20260916.ohlcv-1m.dbn.zst", src2 / "xnas-itch-20260916.ohlcv-1m.dbn.zst")
    m = us.convert_folder(str(src2), str(out), log=lambda *_: None, pattern="*.ohlcv-1m.dbn*", read=conv.read_dbn, dayfn=conv.day_of, label="another feed")
    assert m["files"] == 3 and len(m["sha256"]) == 3 and m["mixed_sources"] is True                                         # the earlier two days are still listed
    assert {k: v["days"] for k, v in m["sources"].items()} == {f"converted from {conv.LABEL}": 2, "converted from another feed": 1}


def test_a_bad_file_is_reported_and_the_rest_carry_on(tmp_path, fake_databento):
    out = tmp_path / "out"
    fake_databento["xnas-itch-20260914.ohlcv-1m.dbn.zst"] = frame("2026-09-14", ROWS)
    dbn(tmp_path, "2026-09-14")
    dbn(tmp_path, "2026-09-15")                                                                                              # no frame registered: reading it fails
    m = us.convert_folder(str(tmp_path / "dl"), str(out), log=lambda *_: None, pattern="*.ohlcv-1m.dbn*", read=conv.read_dbn, dayfn=conv.day_of, label=conv.LABEL)
    assert m["files"] == 1 and [r["day"] for r in m["failed"]] == ["2026-09-15"] and m["this_run"]["converted"] == ["2026-09-14"]
