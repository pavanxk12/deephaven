"""1-minute candles built from the live tick table (`fo_market_data`).

`LttParser` and `CandleAggregator` are pure Python (unit tested); `Candles` is the Deephaven plugin around them.
Output table `candles`: Minute, Symbol, token, Open, High, Low, Close, Volume (one row per token per closed minute).
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

from algo.core.plugin import Plugin

CandleSink = Callable[[int, int, float, float, float, float, int], None]
# (token, minute_epoch, open, high, low, close, volume)


class LttParser:
    """`ltt` string (e.g. `dd-mm-yyyy HH:MM:SS` or `yyyy-mm-ddTHH:MM:SS.mmm`) -> epoch seconds.

    strptime runs once per minute; every other tick is a slice, a dict lookup and an int().
    Fractional seconds are ignored.
    """

    def __init__(self, minute_fmt: str, tz: ZoneInfo):
        self.fmt, self.tz = minute_fmt, tz
        self._minutes: dict[str, int] = {}

    def __call__(self, s: str) -> int | None:
        s = s.strip()
        dot = s.find(".")
        if dot > 0:
            s = s[:dot]
        if len(s) < 5 or s[-3] != ":":                  # must end with ":SS"
            return None
        key = s[:-3]
        base = self._minutes.get(key)
        if base is None:
            try:
                base = int(datetime.strptime(key, self.fmt).replace(tzinfo=self.tz).timestamp())
            except ValueError:
                return None
            self._minutes[key] = base
        try:
            return base + int(s[-2:])
        except ValueError:
            return None


class CandleAggregator:
    """Ticks -> closed 1-minute candles.

    A candle is emitted once a later minute arrives for the token, or via `flush(cutoff)` once the clock has moved
    past it. Volume per candle is the difference of the feed's cumulative volume. Late ticks are ignored.
    Ticks before `start_min` build no candle (only cumulative volume is tracked).
    """

    def __init__(self, emit: CandleSink, start_min: int):
        self.emit, self.start_min = emit, start_min
        self._cur: dict[int, list] = {}        # token -> [minute, o, h, l, c, cum_vol, first_vol]
        self._prev_cum: dict[int, int] = {}    # token -> cumulative volume at the end of the last closed candle
        self._seen: dict[int, tuple] = {}      # token -> (ltt, vol) of the last trade seen

    def is_new(self, token: int, ltt: str, vol: int) -> bool:
        return self._seen.get(token) != (ltt, vol)

    def mark(self, token: int, ltt: str, vol: int) -> None:
        self._seen[token] = (ltt, vol)

    def tick(self, token: int, price: float, vol: int, ltt_sec: int) -> None:
        minute = int(ltt_sec // 60 * 60)
        if minute < self.start_min:
            self._prev_cum[token] = vol
            return
        c = self._cur.get(token)
        if c is not None and minute > c[0]:
            self._close(token, self._cur.pop(token))
            c = None
        if c is None:
            self._cur[token] = [minute, price, price, price, price, vol, vol]
        elif minute == c[0]:
            if price > c[2]:
                c[2] = price
            if price < c[3]:
                c[3] = price
            c[4] = price
            c[5] = vol
        # minute < c[0]: late / out-of-order tick, ignored

    def flush(self, cutoff_min: int) -> None:
        """Close every candle whose minute is before `cutoff_min`."""
        for token in [t for t, c in self._cur.items() if c[0] < cutoff_min]:
            self._close(token, self._cur.pop(token))

    def _close(self, token: int, c: list) -> None:
        minute, o, h, l, cl, cum, first_vol = c
        base = self._prev_cum.get(token, first_vol)
        self.emit(token, minute, o, h, l, cl, max(cum - base, 0))
        self._prev_cum[token] = cum


class Candles(Plugin):
    name = "candles"
    requires = ("master",)

    COLS = ["token", "ltp", "vol", "ltt"]
    GRACE_S = 10          # a minute is closed this many seconds after its end

    def __init__(self, build_from: int | None = None):
        """build_from: epoch second of the first minute to build; None = the next full minute."""
        super().__init__()
        self.build_from = build_from
        self.agg: CandleAggregator | None = None
        self.rows = 0

    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht

        app, mk = self.app, self.app.market
        feed = app.table(mk.feed)
        symbol_of = app.master.symbol_of
        price_t = dht.int64 if mk.int_prices else dht.double
        writer = DynamicTableWriter({
            "MinuteEpoch": dht.int64, "Symbol": dht.string, "token": dht.int64,
            "Open": price_t, "High": price_t, "Low": price_t, "Close": price_t, "Volume": dht.int64,
        })
        app.publish("candles", writer.table.view([
            "Minute = epochSecondsToInstant(MinuteEpoch)",
            "Symbol", "token", "Open", "High", "Low", "Close", "Volume",
        ]))

        px = (lambda v: round(v)) if mk.int_prices else (lambda v: float(v))

        def emit(token, minute, o, h, l, c, vol):
            writer.write_row(minute, symbol_of.get(token), token, px(o), px(h), px(l), px(c), vol)
            self.rows += 1

        start = self.build_from if self.build_from is not None else int(time.time() // 60 * 60) + 60
        self.agg = CandleAggregator(emit, start)
        self._parse = LttParser(mk.ltt_fmt, mk.ltt_zone)
        self.listen(feed, self._on_update, replay=True)
        self.log.info("market=%s ltt_tz=%s fmt=%s feed=%s tokens mapped=%d",
                      mk.code, mk.ltt_tz, mk.ltt_fmt, mk.feed, len(symbol_of))

    def describe(self) -> str:
        return f"{self.rows} candles written"

    def _on_update(self, update, is_replay) -> None:
        agg, latest = self.agg, 0
        for ch in (update.added(self.COLS), update.modified(self.COLS)):
            if ch is None:
                continue
            for tok, ltp, vol, lt in zip(ch["token"], ch["ltp"], ch["vol"], ch["ltt"]):
                if lt is None or vol < 0 or not (ltp == ltp and ltp > 0):
                    continue
                tok, vol = int(tok), int(vol)
                if not agg.is_new(tok, lt, vol):
                    continue
                try:
                    sec = self._parse(lt)
                    if sec is None:
                        continue
                    agg.mark(tok, lt, vol)
                    agg.tick(tok, float(ltp), vol, sec)
                except Exception as e:             # one bad tick must never stop the listener
                    self.log.warning("tick skipped token=%s: %r", tok, e)
                    continue
                if sec > latest:
                    latest = sec
        if latest:
            agg.flush(int((latest - self.GRACE_S) // 60 * 60))
