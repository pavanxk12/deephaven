import glob
from deephaven import parquet, merge

def load_all(prefix):
    files = sorted(glob.glob(f"/data/fo_market_data/{prefix}*.parquet"))
    print(f"{prefix}: {len(files)} files")
    return merge([parquet.read(f) for f in files])

straddle_hist = load_all("straddle_")
candles_hist = load_all("candles_")