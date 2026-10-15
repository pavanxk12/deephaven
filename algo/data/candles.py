"""Candles of 30s / 1min / 5min / 15min built from the live tick table (`fo_market_data`).

`LttParser` and `CandleAggregator` are pure Python (unit tested); `Candles` is the Deephaven plugin around them.
Output table `candles`: Minute (start of the bar), Symbol, token, Open, High, Low, Close (double, in proper currency
units), Volume (int64); one row per token per closed bar.

Cheap on purpose: the feed batch is cleaned and scaled with numpy in one pass, only the surviving rows reach the
Python state machine; the stored table has numbers only (the Symbol text is joined from a small static table, not stored
per row); `symbols=` / `kind=` keep the table to the instruments you need, which is the biggest saving of all.

Prices are `feed price x multiplier`, rounded to `decimals` places so binary-float noise (245.67000000000002) never reaches
a table. A feed in paise needs multiplier 0.01. Anything that is not a finite price above zero is dropped, never stored.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import numpy as np

from algo.core.nulls import good_price, good_volume
from algo.core.plugin import Plugin
from algo.core.timeframe import format_timeframe, parse_timeframe

CandleSink = Callable[[int, int, float, float, float, float, int], None]
# (token, minute_epoch, open, high, low, close, volume)


class LttParser:
    """`ltt` string (e.g. `dd-mm-yyyy HH:MM:SS` or `yyyy-mm-ddTHH:MM:SS.mmm`) -> epoch seconds.

    strptime runs once per minute; every other tick is a slice, a dict lookup and an int().
    Fractional seconds are ignored.
    """

    MAX_CACHED_MINUTES = 5000

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
            if len(self._minutes) >= self.MAX_CACHED_MINUTES:      # a session that runs for days must not grow forever
                self._minutes.clear()
            self._minutes[key] = base
        try:
            return base + int(s[-2:])
        except ValueError:
            return None


class CandleAggregator:
    """Ticks -> closed candles of `bucket` seconds (30, 60, 300, ...), aligned to the epoch.

    A candle is emitted once a later bar arrives for the token, or via `flush(cutoff)` once the clock has moved
    past it. Volume per candle is the difference of the feed's cumulative volume. Late ticks are ignored.
    Ticks before `start_min` (epoch seconds of the first bar) build no candle (only cumulative volume is tracked).
    """

    def __init__(self, emit: CandleSink, start_min: int, bucket: int = 60):
        self.emit, self.start_min, self.bucket = emit, start_min, bucket
        self._cur: dict[int, list] = {}        # token -> [minute, o, h, l, c, cum_vol, first_vol]
        self._prev_cum: dict[int, int] = {}    # token -> cumulative volume at the end of the last closed candle
        self._seen: dict[int, tuple] = {}      # token -> (ltt, vol) of the last trade seen

    def is_new(self, token: int, ltt: str, vol: int) -> bool:
        return self._seen.get(token) != (ltt, vol)

    def mark(self, token: int, ltt: str, vol: int) -> None:
        self._seen[token] = (ltt, vol)

    def tick(self, token: int, price: float, vol: int, ltt_sec: int) -> None:
        minute = int(ltt_sec // self.bucket * self.bucket)
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
        """Close every candle whose bar starts before `cutoff_min`."""
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
    SLOW_BATCH_MS = 250   # a feed batch that takes longer than this holds up every table in the update cycle: say so

    def __init__(self, timeframe: str = "1min", multiplier: float | None = None, *, symbols=None, kind: str | None = None,
                 build_from: int | None = None, decimals: int = 6):
        """timeframe: "30s", "1min", "5min", "15min". multiplier: applied to every feed price to get proper rates
        (feed in paise -> 0.01); None = each contract's own Divisor from the contract file. symbols / kind: build only
        these underlyings (["NIFTY"]) and kinds ("FUT", "OPT", "EQ"); None = everything in the feed. build_from: epoch
        second of the first bar to build; None = the next full bar."""
        super().__init__()
        self.symbols, self.kind = symbols, kind
        if multiplier is not None and (isinstance(multiplier, (bool, str)) or not good_price(multiplier)):
            raise ValueError(f"multiplier must be a number above zero (e.g. 0.01 for a feed in paise), got {multiplier!r}")
        if not isinstance(decimals, int) or isinstance(decimals, bool) or not 0 <= decimals <= 12:
            raise ValueError("decimals must be a whole number from 0 to 12")
        self.multiplier, self.decimals, self.skipped = multiplier, decimals, 0
        self.scale_of: dict[int, float] = {}               # token -> multiplier, from the contract file (when none is given)
        self.timeframe = format_timeframe(parse_timeframe(timeframe))
        self.bucket = parse_timeframe(timeframe)
        self.build_from = build_from
        self.agg: CandleAggregator | None = None
        self.rows = 0
        self.batches, self.last_ms, self.max_ms, self._last_slow_log = 0, 0.0, 0.0, 0.0
        self.tokens: np.ndarray | None = None
        self._scale_arr = np.empty(0)
        self._allowed: np.ndarray | None = None

    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht
        from algo.data.symbols import symbol_table

        app, mk = self.app, self.app.market
        feed = app.table(mk.feed)
        master = app.master
        symbol_of = master.symbol_of
        self.scale = self.multiplier if self.multiplier is not None else 1.0 / mk.price_div
        if self.multiplier is None:                         # no multiplier given: each contract's own Divisor from the file
            self.scale_of = {t: 1.0 / d for t, d in master.divisor_of.items()}
        self.tokens = master.tokens_for(self.symbols, self.kind)
        self._build_lookups(max(symbol_of, default=0))

        # numbers only: a text per row would cost a Java String per candle, the Symbol is joined from a static table
        writer = DynamicTableWriter({
            "MinuteEpoch": dht.int64, "token": dht.int64,
            "Open": dht.double, "High": dht.double, "Low": dht.double, "Close": dht.double, "Volume": dht.int64,
        })
        app.shared["candle_seconds"] = self.bucket          # read by plugins that follow `candles`, before they are told
        app.publish("candles", writer.table
                    .natural_join(symbol_table(symbol_of, self.tokens), on="token", joins="Symbol")
                    .view(["Minute = epochSecondsToInstant(MinuteEpoch)", "Symbol", "token",
                           "Open", "High", "Low", "Close", "Volume"]))

        def emit(token, minute, o, h, l, c, vol):
            writer.write_row(minute, token, float(o), float(h), float(l), float(c), int(vol))
            self.rows += 1

        b = self.bucket
        start = self.build_from if self.build_from is not None else int(time.time() // b * b) + b
        self.agg = CandleAggregator(emit, start, b)
        self._parse = LttParser(mk.ltt_fmt, mk.ltt_zone)
        self.listen(feed, self._on_update, replay=True)
        scales = sorted({round(v, 12) for v in self.scale_of.values()}) or [self.scale]
        self.log.info("%s candles, feed price x %s%s, market=%s ltt_tz=%s fmt=%s feed=%s, %s",
                      self.timeframe, " / ".join(f"{v:g}" for v in scales),
                      "" if self.multiplier is not None else " (the contract file's Divisor)",
                      mk.code, mk.ltt_tz, mk.ltt_fmt, mk.feed,
                      "all %d instruments" % len(symbol_of) if self.tokens is None else "%d instruments" % len(self.tokens))

    def describe(self) -> str:
        scope = "all instruments" if self.tokens is None else f"{len(self.tokens)} instruments"
        return (f"{self.timeframe}, {scope}, {self.rows} candles, {self.skipped} bad ticks skipped, "
                f"feed batch {self.last_ms:.1f} ms (max {self.max_ms:.1f}, {self.batches} batches)")

    def _build_lookups(self, max_token: int) -> None:
        """Dense arrays indexed by token: the scale of each contract and (when restricted) which tokens are wanted."""
        size = max_token + 1
        arr = np.full(size, self.scale, dtype=np.float64)
        for t, s in self.scale_of.items():
            if 0 <= t < size:
                arr[t] = s
        self._scale_arr = arr
        if self.tokens is None:
            self._allowed = None
        else:
            allowed = np.zeros(size, dtype=bool)
            allowed[self.tokens[(self.tokens >= 0) & (self.tokens < size)]] = True
            self._allowed = allowed

    def _clean(self, ch: dict):
        """One feed batch -> (tokens, prices, volumes, ltt strings) of the usable rows, as plain Python lists.

        Everything numeric is done on whole arrays: null / NaN / infinite / non-positive prices, negative volumes,
        missing times, tokens outside the wanted set, the per-contract scale and the rounding. Only survivors are
        turned into Python objects."""
        tok = np.asarray(ch["token"]).astype(np.int64, copy=False)
        ltp = np.asarray(ch["ltp"], dtype=np.float64)
        vol = np.asarray(ch["vol"], dtype=np.float64)
        lt = np.asarray(ch["ltt"], dtype=object)
        size = len(self._scale_arr)
        inside = (tok >= 0) & (tok < size)
        ok = np.isfinite(ltp) & (ltp > 0) & np.isfinite(vol) & (vol >= 0) & np.not_equal(lt, None)
        if self._allowed is not None:
            ok &= inside
            ok[ok] &= self._allowed[tok[ok]]
        scale = np.full(len(tok), self.scale)
        np.copyto(scale, self._scale_arr[np.clip(tok, 0, size - 1)], where=inside)
        price = np.round(ltp * scale, self.decimals)
        ok &= price > 0                                   # the multiplier must not round a price to zero
        idx = np.flatnonzero(ok)
        return (tok[idx].tolist(), price[idx].tolist(), vol[idx].astype(np.int64).tolist(), lt[idx].tolist())

    def _feed(self, agg: CandleAggregator, tok: int, price: float, vol: int, lt) -> int | None:
        """One clean row -> the aggregator. Returns the tick's time, or None if it was a repeat / unreadable time."""
        try:
            if not agg.is_new(tok, lt, vol):
                return None
            sec = self._parse(lt)
            if sec is None:
                return None
            agg.mark(tok, lt, vol)
            agg.tick(tok, price, vol, sec)
            return sec
        except Exception as e:                            # one bad row must never stop the listener
            self.skipped += 1
            if self.skipped <= 10 or self.skipped % 1000 == 0:
                self.log.warning("tick skipped (%d so far) token=%r: %r", self.skipped, tok, e)
            return None

    def _tick(self, agg: CandleAggregator, tok, ltp, vol, lt) -> int | None:
        """One raw feed row (scalars) -> the aggregator; the same rules as `_clean`, for single rows and tests.
        Nothing a feed row contains can raise out of here: a bad row is counted and skipped."""
        try:
            if lt is None or not good_price(ltp) or not good_volume(vol):
                return None
            tok, vol = int(tok), int(vol)
            if self._allowed is not None and not (0 <= tok < len(self._allowed) and self._allowed[tok]):
                return None
            price = round(float(ltp) * self.scale_of.get(tok, self.scale), self.decimals)
            if not good_price(price):                  # the multiplier rounded it to zero
                return None
            return self._feed(agg, tok, price, vol, lt)
        except Exception as e:
            self.skipped += 1
            if self.skipped <= 10 or self.skipped % 1000 == 0:
                self.log.warning("tick skipped (%d so far) token=%r: %r", self.skipped, tok, e)
            return None

    def _on_update(self, update, is_replay) -> None:
        started = time.perf_counter()
        agg, latest = self.agg, 0
        for ch in (update.added(self.COLS), update.modified(self.COLS)):
            if ch is None:
                continue
            for tok, price, vol, lt in zip(*self._clean(ch)):
                sec = self._feed(agg, tok, price, vol, lt)
                if sec is not None and sec > latest:
                    latest = sec
        if latest:
            agg.flush(int((latest - self.GRACE_S) // self.bucket * self.bucket))
        self._time_batch((time.perf_counter() - started) * 1000.0)

    def _time_batch(self, ms: float) -> None:
        """Keep the last / slowest batch time for `algo_status()`; warn (at most once a minute) when one is slow."""
        self.batches += 1
        self.last_ms = ms
        if ms > self.max_ms:
            self.max_ms = ms
        if ms > self.SLOW_BATCH_MS and time.monotonic() - self._last_slow_log > 60:
            self._last_slow_log = time.monotonic()
            self.log.warning("a feed batch took %.0f ms (limit %d ms): it holds up every table in the update cycle. "
                             "Narrow the instruments (symbols= / kind=) or move the candle loop to a vectorised / compiled version",
                             ms, self.SLOW_BATCH_MS)
