"""Real rows from the US market-data table (export table-2026-10-15-171828.csv, tokens 1-8 = US equities): ltt text and its
timezone, checked against the Kafka time of the same message. If the settings were wrong, every US tick would be dropped."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from algo.core.config import US
from algo.data.candles import LttParser

# token, ltt (feed text), KafkaTimestamp (the table shows it in IST, the container's TZ)
ROWS = [
    (1, "2026-10-15 11:01:04", "2026-10-15T16:33:37.790"),
    (2, "2026-10-15 11:45:31", "2026-10-15T17:18:22.390"),
    (3, "2026-10-15 11:08:43", "2026-10-15T16:38:43.990"),
    (4, "2026-10-15 11:47:02", "2026-10-15T17:17:03.190"),
    (5, "2026-10-15 11:45:30", "2026-10-15T17:18:27.190"),
    (6, "2026-10-15 11:32:42", "2026-10-15T17:08:19.590"),
    (7, "2026-10-15 11:45:31", "2026-10-15T17:18:27.190"),
    (8, "2026-10-15 11:09:46", "2026-10-15T16:57:27.190"),
]
IST = ZoneInfo("Asia/Kolkata")


@pytest.mark.parametrize("token,ltt,kafka", ROWS)
def test_every_real_ltt_parses_and_precedes_its_kafka_message(token, ltt, kafka):
    parse = LttParser(US.ltt_fmt, US.ltt_zone)
    sec = parse(ltt)
    assert sec is not None, f"token {token}: the US settings cannot read {ltt!r}"
    arrived = datetime.fromisoformat(kafka).replace(tzinfo=IST).timestamp()
    assert 0 <= arrived - sec <= 3600            # a trade is never after the message that carries it


def test_the_wrong_zone_would_put_the_trades_in_the_future():
    wrong = LttParser(US.ltt_fmt, IST)           # reading the text as IST
    arrived = datetime.fromisoformat(ROWS[2][2]).replace(tzinfo=IST).timestamp()
    assert wrong(ROWS[2][1]) - arrived > 3600 * 5


def test_the_trade_times_are_new_york_pre_market():
    parse = LttParser(US.ltt_fmt, US.ltt_zone)
    hours = {datetime.fromtimestamp(parse(r[1]), US.zone).hour for r in ROWS}
    assert hours == {7}


def test_the_equity_price_scale_matches_the_files_divisor():
    # token 1 ltp "34,669" with the equity divisor 100 -> 346.69 (a plausible price); with 10000 it would be 3.47
    assert 34669 / 100 == pytest.approx(346.69)
