from zoneinfo import ZoneInfo

from algo.data.candles import CandleAggregator, LttParser

IST = ZoneInfo("Asia/Kolkata")


def test_ltt_nse_format():
    p = LttParser("%d-%m-%Y %H:%M", IST)
    a = p("14-10-2026 10:15:30")
    assert a is not None and p("14-10-2026 10:15:31") == a + 1
    assert p("14-10-2026 10:16:00") == a + 30


def test_ltt_us_format_with_milliseconds():
    p = LttParser("%Y-%m-%dT%H:%M", IST)
    assert p("2026-10-14T10:15:30.123") == p("2026-10-14T10:15:30")
    assert p("2026-10-14T10:15:30.123") is not None


def test_ltt_rejects_garbage():
    p = LttParser("%d-%m-%Y %H:%M", IST)
    for bad in ("", "abc", "14-10-2026 10:15", "xx-yy-zzzz 10:15:30", "14-10-2026 10:15:ab"):
        assert p(bad) is None


def collect():
    out = []
    return out, (lambda *c: out.append(c))


def test_candle_ohlc_and_volume_delta():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0)
    for sec, price, vol in ((60, 10, 100), (70, 12, 110), (80, 9, 130), (119, 11, 140), (125, 20, 150)):
        agg.tick(1, price, vol, sec)
    # first tick of the minute starts the candle; the next minute closes it
    assert out == [(1, 60, 10, 12, 9, 11, 40)]          # volume 140 - first_vol 100
    agg.flush(180)
    assert out[1] == (1, 120, 20, 20, 20, 20, 10)       # 150 - 140 carried from the previous candle


def test_ticks_before_start_build_nothing_but_track_volume():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=120)
    agg.tick(1, 10, 500, 61)
    agg.tick(1, 11, 520, 130)
    agg.flush(1000)
    assert out == [(1, 120, 11, 11, 11, 11, 20)]        # 520 - 500 seen before the start


def test_late_tick_is_ignored():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0)
    agg.tick(1, 10, 100, 130)
    agg.tick(1, 99, 100, 70)                            # older minute
    agg.flush(1000)
    assert out[0][2:6] == (10, 10, 10, 10)


def test_flush_only_closes_finished_minutes():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0)
    agg.tick(1, 10, 1, 60)
    agg.tick(2, 10, 1, 125)
    agg.flush(120)
    assert [c[0] for c in out] == [1]


def test_duplicate_trades_are_detected():
    agg = CandleAggregator(lambda *c: None, 0)
    assert agg.is_new(1, "t", 5)
    agg.mark(1, "t", 5)
    assert not agg.is_new(1, "t", 5) and agg.is_new(1, "t", 6)


def test_30_second_candles():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0, bucket=30)
    for sec, price, vol in ((60, 10, 100), (75, 12, 110), (91, 9, 130), (130, 11, 140)):
        agg.tick(1, price, vol, sec)
    # bars start at 60, 90, 120: the first two are closed by the arrival of later bars
    assert [(c[1], c[2], c[3], c[4], c[5]) for c in out] == [(60, 10, 12, 10, 12), (90, 9, 9, 9, 9)]
    agg.flush(150)
    assert out[2][1] == 120


def test_5_minute_candles_align_to_the_epoch():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0, bucket=300)
    for sec, price in ((299, 1), (300, 2), (450, 5), (599, 3), (600, 7)):
        agg.tick(1, price, 10, sec)
    assert [(c[1], c[2], c[3], c[4], c[5]) for c in out] == [(0, 1, 1, 1, 1), (300, 2, 5, 2, 3)]


def test_flush_uses_the_bar_start():
    out, emit = collect()
    agg = CandleAggregator(emit, start_min=0, bucket=300)
    agg.tick(1, 1, 1, 310)
    agg.flush(300)                                     # the bar starting at 300 is not before 300
    assert out == []
    agg.flush(600)
    assert len(out) == 1
