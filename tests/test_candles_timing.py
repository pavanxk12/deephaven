"""The candle builder reports how long its feed batches take, and says so when one is slow."""
import logging

from tests.test_candles_vector import Update, batch, build


def one_batch(c):
    c._on_update(Update(added=batch([(1, 10000.0, 5.0, "14-10-2030 10:15:05")])), False)


def test_batch_times_are_recorded_and_shown():
    c, _ = build()
    for _ in range(3):
        one_batch(c)
    assert c.batches == 3 and 0 <= c.last_ms <= c.max_ms
    assert "feed batch" in c.describe() and "3 batches" in c.describe()


def test_a_slow_batch_is_reported_once_a_minute(caplog):
    c, _ = build()
    c.SLOW_BATCH_MS = -1                                    # everything counts as slow
    with caplog.at_level(logging.WARNING, logger="algo.candles"):
        one_batch(c)
        one_batch(c)
    slow = [r for r in caplog.records if "feed batch took" in r.getMessage()]
    assert len(slow) == 1 and "symbols=" in slow[0].getMessage()


def test_a_normal_batch_is_silent(caplog):
    c, _ = build()
    with caplog.at_level(logging.WARNING, logger="algo.candles"):
        one_batch(c)
    assert not [r for r in caplog.records if "feed batch took" in r.getMessage()]
