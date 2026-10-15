"""The batch (numpy) path of the candle builder must give exactly what the row-by-row rules give."""
import numpy as np
import pytest
from zoneinfo import ZoneInfo

from algo.core.nulls import NULL_DOUBLE
from algo.data.candles import CandleAggregator, Candles, LttParser

TZ = ZoneInfo("Asia/Kolkata")
FMT = "%d-%m-%Y %H:%M"
SCALES = {1: 0.01, 2: 0.01, 3: 0.0001, 4: 0.01}


def build(allowed=None, bucket="1min"):
    c = Candles(bucket, build_from=0)
    c.scale = 0.01
    c.scale_of = dict(SCALES)
    c.tokens = None if allowed is None else np.array(sorted(allowed), dtype=np.int64)
    c._build_lookups(10)
    c._parse = LttParser(FMT, TZ)
    out = []
    c.agg = CandleAggregator(lambda *r: out.append(r), 0, c.bucket)
    return c, out


class Update:
    def __init__(self, added=None, modified=None):
        self._a, self._m = added, modified

    def added(self, cols):
        return self._a

    def modified(self, cols):
        return self._m


def batch(rows):
    return {"token": np.array([r[0] for r in rows], dtype=np.int64),
            "ltp": np.array([r[1] for r in rows], dtype=np.float64),
            "vol": np.array([r[2] for r in rows], dtype=np.float64),
            "ltt": np.array([r[3] for r in rows], dtype=object)}


def random_rows(n, seed):
    rng = np.random.default_rng(seed)
    bad_price = [float("nan"), float("inf"), -float("inf"), 0.0, -5.0, NULL_DOUBLE]
    bad_vol = [float("nan"), -1.0, float("inf")]
    rows = []
    for i in range(n):
        tok = int(rng.integers(0, 6))                                  # tokens 0 and 5 are not in SCALES
        price = float(rng.integers(100, 100000)) if rng.random() > 0.15 else bad_price[int(rng.integers(0, len(bad_price)))]
        vol = float(i + 1) if rng.random() > 0.1 else bad_vol[int(rng.integers(0, len(bad_vol)))]
        r = rng.random()
        sec = int(rng.integers(0, 600))
        lt = f"14-10-2030 10:{sec // 60:02d}:{sec % 60:02d}" if r > 0.1 else [None, "", "garbage", "14-10-2030 10:99:99"][int(r * 40) % 4]
        rows.append((tok, price, vol, lt))
    return rows


def run_batch(rows, **kw):
    c, out = build(**kw)
    half = len(rows) // 2
    c._on_update(Update(added=batch(rows[:half]), modified=batch(rows[half:])), False)
    c.agg.flush(10 ** 12)
    return out


def run_rows(rows, **kw):
    c, out = build(**kw)
    for r in rows:
        c._tick(c.agg, *r)
    c.agg.flush(10 ** 12)
    return out


@pytest.mark.parametrize("seed", range(8))
def test_batch_path_equals_row_path_on_dirty_data(seed):
    rows = random_rows(400, seed)
    # added rows come before modified rows in a batch, and so they do in the row-by-row run
    assert run_batch(rows) == run_rows(rows)


@pytest.mark.parametrize("seed", range(3))
def test_the_instrument_filter_agrees_between_the_paths(seed):
    rows = random_rows(400, seed)
    a, b = run_batch(rows, allowed={1, 3}), run_rows(rows, allowed={1, 3})
    assert a == b and {r[0] for r in a} <= {1, 3} and a


def test_only_wanted_instruments_get_candles():
    rows = [(t, 10000.0, 5.0, "14-10-2030 10:15:05") for t in (1, 2, 3, 4)]
    assert {r[0] for r in run_batch(rows, allowed={2, 4})} == {2, 4}


def test_each_contract_is_scaled_by_its_own_divisor_in_the_batch_path():
    rows = [(1, 24567.0, 5.0, "14-10-2030 10:15:05"), (3, 2456700.0, 5.0, "14-10-2030 10:15:05"),
            (5, 24567.0, 5.0, "14-10-2030 10:15:05")]              # token 5 is not in the table: the default scale
    got = {r[0]: r[2] for r in run_batch(rows)}
    assert got == {1: 245.67, 3: 245.67, 5: 245.67}


def test_tokens_outside_the_lookup_range_cannot_index_out_of_bounds():
    rows = [(10 ** 9, 100.0, 1.0, "14-10-2030 10:15:05"), (-5, 100.0, 1.0, "14-10-2030 10:15:05"),
            (1, 100.0, 1.0, "14-10-2030 10:15:05")]
    assert {r[0] for r in run_batch(rows)} == {10 ** 9, -5, 1}         # unknown tokens use the default scale
    assert {r[0] for r in run_batch(rows, allowed={1})} == {1}         # and are dropped when a filter is set


def test_an_empty_or_all_bad_batch_is_harmless():
    c, out = build()
    c._on_update(Update(added=batch([])), False)
    c._on_update(Update(added=batch([(1, float("nan"), 1.0, None)])), False)
    c._on_update(Update(added=None, modified=None), False)
    assert out == [] and c.skipped == 0


def test_the_parser_cache_cannot_grow_without_limit():
    p = LttParser(FMT, TZ)
    p.MAX_CACHED_MINUTES = 50
    for m in range(60):
        assert p(f"14-10-2030 10:{m:02d}:05") is not None and len(p._minutes) <= 50
