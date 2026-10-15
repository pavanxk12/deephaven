import pytest

from algo.core.timeframe import format_timeframe, parse_timeframe


@pytest.mark.parametrize("text,seconds", [("30s", 30), ("1min", 60), ("5min", 300), ("15min", 900), ("5 MIN", 300),
                                          ("1m", 60), ("30 sec", 30), ("2minutes", 120), ("30min", 1800)])
def test_parse(text, seconds):
    assert parse_timeframe(text) == seconds


@pytest.mark.parametrize("bad", [30, 5, None, "", "min", "5", "5x", "45s", "0s", "7min2", "-1min", "1h", "2h", "90s"])
def test_rejects(bad):
    with pytest.raises(ValueError):
        parse_timeframe(bad)


def test_format_roundtrip():
    assert [format_timeframe(s) for s in (30, 60, 300, 900)] == ["30s", "1min", "5min", "15min"]
    assert all(parse_timeframe(format_timeframe(s)) == s for s in (10, 15, 20, 30, 60, 120, 300, 900, 1800))
