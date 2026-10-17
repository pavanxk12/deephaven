"""The futures of the GDFL F&O files -> candles_<day>.parquet (tools/convert_gdfl_futures.py)."""
import importlib.util
import os
import sys
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

TOOLS = os.path.join(os.path.dirname(__file__), "..", "tools")
sys.path.insert(0, TOOLS)
spec = importlib.util.spec_from_file_location("convert_gdfl_futures", os.path.join(TOOLS, "convert_gdfl_futures.py"))
conv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conv)
from convert_gdfl_csv import convert_folder  # noqa: E402

HEADER = "Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest\n"
ROWS = [
    "NIFTY27JAN26FUT.NFO,05/01/2026,09:15:59,25000,25010,24990,25005,100,5000",
    "NIFTY27JAN26FUT.NFO,05/01/2026,09:16:59,25005,25020,25000,25015,200,5100",
    "NIFTY24FEB26FUT.NFO,05/01/2026,09:15:59,25100,25110,25090,25105,10,900",
    "360ONE24FEB26FUT.NFO,05/01/2026,09:15:59,1183,1184,1182,1183.5,5,41000",
    "NIFTY06JAN2625700CE.NFO,05/01/2026,09:15:59,50,51,49,50.5,75,0",
    "RELIANCE27JAN261400PE.NFO,05/01/2026,09:15:59,20,21,19,20.5,500,0",
]


def write_csv(path, rows=ROWS):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(HEADER + "\n".join(rows) + "\n")
    return str(path)


def test_only_the_futures_are_kept_with_the_masters_symbol_text(tmp_path):
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv")
    m = convert_folder(str(tmp_path), str(tmp_path / "out"), log=lambda *_: None, fmt=conv.futures_format(2026))
    assert m["files"] == 1 and m["rows"] == 4 and not m["failed"]
    t = pq.read_table(str(tmp_path / "out" / "candles_2026-01-05.parquet"))
    assert t.column_names == ["Minute", "Symbol", "token", "Open", "High", "Low", "Close", "Volume", "Date"]
    assert sorted(set(t["Symbol"].to_pylist())) == ["360ONE 24FEB26 FUT", "NIFTY 24FEB26 FUT", "NIFTY 27JAN26 FUT"]            # no options, no ".NFO"
    nifty = [c for s, c in zip(t["Symbol"].to_pylist(), t["Close"].to_pylist()) if s == "NIFTY 27JAN26 FUT"]
    assert nifty == [25005.0, 25015.0] and set(t["Date"].to_pylist()) == {"2026-01-05"}
    assert t["Minute"].to_pylist()[0].hour == 3 and t["Minute"].to_pylist()[0].minute == 45                                     # 09:15 India = 03:45 UTC


def test_a_zip_and_other_years_are_handled_like_the_straddle_builder_does(tmp_path):
    csv = write_csv(tmp_path / "STAGE" / "GFDLNFO_CONTRACT_06012026.csv", [r.replace("05/01", "06/01") for r in ROWS])
    z = tmp_path / "APR_2026" / "GFDLNFO_CONTRACT_06012026.zip"
    os.makedirs(z.parent)
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv, "GFDLNFO_CONTRACT_06012026.csv")
    os.remove(csv)
    write_csv(tmp_path / "JAN_2025" / "GFDLNFO_CONTRACT_07012025.csv", [r.replace("05/01/2026", "07/01/2025") for r in ROWS])
    m = convert_folder(str(tmp_path), str(tmp_path / "out"), log=lambda *_: None, fmt=conv.futures_format(2026))
    assert m["files"] == 1 and m["first_day"] == "2026-01-06" and os.path.exists(tmp_path / "out" / "candles_2026-01-06.parquet")


def test_the_converted_futures_roll_to_the_nearest_unexpired_contract_in_absolute(tmp_path):
    """The reader the absolute() function uses picks the January contract before it expires and the February one after."""
    from algo.data.store import Store
    from algo.series.absolute import ParquetSource
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_05012026.csv")
    write_csv(tmp_path / "JAN_2026" / "GFDLNFO_CONTRACT_28012026.csv", [r.replace("05/01", "28/01") for r in ROWS])         # 28 Jan: the 27 Jan contract has expired
    convert_folder(str(tmp_path), str(tmp_path / "out"), log=lambda *_: None, fmt=conv.futures_format(2026))
    src = ParquetSource(Store(str(tmp_path / "out")))
    assert src.bars("2026-01-05", ("fut", "NIFTY"))[4].tolist() == [25005.0, 25015.0]                                          # the January contract
    assert src.bars("2026-01-28", ("fut", "NIFTY"))[4].tolist() == [25105.0]                                                    # the February contract
    assert src.bars("2026-01-05", ("exact", "NIFTY 24FEB26 FUT"))[4].tolist() == [25105.0]
