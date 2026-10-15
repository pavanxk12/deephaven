"""One `indicators` table computed from `candles` with Deephaven's incremental `update_by`.

Table `indicator_values` — columns: Minute, Symbol, token, Pct, then one int64 column per indicator:
    RSI_<n>  RSI x 100 (Wilder smoothing)        EMA_<n>  EMA of the close, same unit as Close
Pct = percent change from the first bar seen for the token, x 100 (+1.25% -> 125).

Indicators run on the candles as they are, or on larger bars rolled up from them (a whole multiple of the candle size). To add an indicator, write
an `Indicator` subclass (two small methods) and pass an instance to `Indicators([...])`.
"""
from __future__ import annotations

import math

from algo.core.plugin import Plugin
from algo.core.timeframe import format_timeframe, parse_timeframe


def decay_ticks(alpha: float) -> float:
    """`ema_tick` weight per row is exp(-1/decay_ticks); solve for the smoothing factor `alpha`."""
    return -1.0 / math.log(1.0 - alpha)


class Indicator:
    """One output column. Subclasses add `update_by` operations and a final expression."""
    needs_delta = False            # True: the base table provides Gain / Loss (price change split in two)

    @property
    def column(self) -> str:
        raise NotImplementedError

    def ops(self) -> list:
        """`update_by` operations (EMA / smoothing) producing this indicator's helper columns."""
        raise NotImplementedError

    def final(self) -> str:
        """Query-language expression `Name = ...` for the output column (int64)."""
        raise NotImplementedError


class EMA(Indicator):
    """EMA with alpha 2/(n+1). While fewer than n bars exist it is the running average, so it never starts blank."""

    def __init__(self, n: int):
        if n < 1:
            raise ValueError("EMA length must be >= 1")
        self.n = n

    @property
    def column(self) -> str:
        return f"EMA_{self.n}"

    def ops(self) -> list:
        from deephaven import updateby as uby
        return [uby.ema_tick(decay_ticks(2.0 / (self.n + 1)), [f"EMA_{self.n}_raw = Px"])]

    def final(self) -> str:
        n = self.n
        return f"EMA_{n} = Cnt < {n} ? Math.round(PxSum / Cnt) : Math.round(EMA_{n}_raw)"


class RSI(Indicator):
    """Wilder RSI (EMA with alpha 1/n of gains and losses). Blank until n price changes have been seen."""
    needs_delta = True

    def __init__(self, n: int):
        if n < 2:
            raise ValueError("RSI length must be >= 2")
        self.n = n

    @property
    def column(self) -> str:
        return f"RSI_{self.n}"

    def ops(self) -> list:
        from deephaven import updateby as uby
        n = self.n
        return [uby.ema_tick(decay_ticks(1.0 / n), [f"AvgG_{n} = Gain", f"AvgL_{n} = Loss"])]

    def final(self) -> str:
        n = self.n
        return (f"RSI_{n} = Cnt <= {n} ? NULL_LONG : Math.round(100.0 * "
                f"(AvgL_{n} == 0 ? (AvgG_{n} > 0 ? 100.0 : 50.0) : 100.0 - 100.0 / (1.0 + AvgG_{n} / AvgL_{n})))")


# Every indicator listed here is also exposed as a function in the session by `algo.install` (EMA(9), RSI(14), ...).
# To add one: write the class above and add it to this dict.
LIBRARY: dict[str, type] = {"EMA": EMA, "RSI": RSI}


def resolve_timeframe(requested: str | None, candle_seconds: int) -> int:
    """Seconds of the bars the indicators run on: the candle size by default, else a whole multiple of it."""
    if requested is None:
        return candle_seconds
    seconds = parse_timeframe(requested)
    if seconds < candle_seconds or seconds % candle_seconds:
        raise ValueError(f"indicator bars of {requested} cannot be built from {format_timeframe(candle_seconds)} candles: "
                         f"use a whole multiple of {format_timeframe(candle_seconds)}")
    return seconds


def build_indicator_table(candles, bar_seconds: int, candle_seconds: int, indicators):
    """candles table -> indicators table (pure Deephaven query; no side effects)."""
    from deephaven import agg
    from deephaven import updateby as uby

    # 1) resample (skipped when the bars are the candles): Minute becomes the bucket start
    if bar_seconds == candle_seconds:
        t = candles.view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"])
    else:
        t = (candles.view([f"Minute = lowerBin(Minute, {bar_seconds} * SECOND)", "Symbol", "token", "Open", "Close"])
             .agg_by([agg.first("Open"), agg.last("Close")], by=["Minute", "Symbol", "token"])
             .view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"]))

    need_delta = any(i.needs_delta for i in indicators)

    # 2) bar count + running price sum per token (+ price change for RSI)
    first = [uby.cum_sum(["Cnt = One", "PxSum = Px"])]
    if need_delta:
        first.append(uby.delta(["D = Px"]))
    t = t.update_by(first, by="token")

    # 3) helper columns
    helpers = ["OpenFirst = Cnt == 1 ? Op : NULL_DOUBLE"]
    if need_delta:
        helpers += ["Gain = isNull(D) ? NULL_DOUBLE : Math.max(D, 0.0)",
                    "Loss = isNull(D) ? NULL_DOUBLE : Math.max(-D, 0.0)"]
    t = t.update(helpers)

    # 4) day-open carry-forward and every smoothing operation in one pass
    ops = [uby.forward_fill(["DayOpen = OpenFirst"])]
    for ind in indicators:
        ops += ind.ops()
    t = t.update_by(ops, by="token")

    # 5) output columns
    pct = "Pct = (isNull(DayOpen) || DayOpen == 0) ? NULL_LONG : Math.round(10000.0 * (Px - DayOpen) / DayOpen)"
    return t.view(["Minute", "Symbol", "token", pct] + [i.final() for i in indicators])


class Indicators(Plugin):
    """Computes every registered indicator in one table, republished when the set changes.

    The Deephaven variable is `indicator_values` (the name `indicators` is the session function that adds to it).
    `add()` / `remove()` rebuild the table over the whole day; plugins that listen to it follow through
    `app.on_publish("indicators", ...)`. If `candles` is restarted (new bar size, say) the table is rebuilt on it.
    """
    name = "indicators"
    requires = ("candles",)
    TABLE_VAR = "indicator_values"

    def __init__(self, indicators=(), timeframe: str | None = None):
        """timeframe: bar size the indicators run on ("5min"); None = the candle size."""
        super().__init__()
        if timeframe is not None:
            parse_timeframe(timeframe)
        self.timeframe = timeframe
        self.indicators: list[Indicator] = []
        self._merge(indicators)

    def _merge(self, specs) -> list[str]:
        """Add the specs that are not already computed; returns the new column names."""
        added = []
        have = {i.column for i in self.indicators}
        for spec in specs:
            if not isinstance(spec, Indicator):
                raise TypeError(f"{spec!r} is not an indicator: write EMA(9) or RSI(14), not the bare number")
            if spec.column in have:
                self.log.info("%s is already computed", spec.column)
                continue
            have.add(spec.column)
            self.indicators.append(spec)
            added.append(spec.column)
        return added

    def start(self) -> None:
        self.follow("candles", lambda _t: self._build())
        self._build()

    def add(self, *specs, timeframe: str | None = None) -> list[str]:
        """Start computing more indicators alongside the existing ones. Returns the new columns."""
        before = (list(self.indicators), self.timeframe)
        try:
            if timeframe is not None and timeframe != self.timeframe:
                resolve_timeframe(timeframe, self.app.shared["candle_seconds"])          # validate before changing anything
                self.log.warning("bar size changes from %s to %s: every indicator is recomputed on the new bars",
                                 self.timeframe or "the candle size", timeframe)
                self.timeframe = timeframe
            added = self._merge(specs)
            if added or self.timeframe != before[1]:
                self._build()
        except Exception:
            self.indicators, self.timeframe = before
            raise
        return added

    def remove(self, *columns: str) -> None:
        gone = [c for c in columns if any(i.column == c for i in self.indicators)]
        for c in columns:
            if c not in gone:
                self.log.warning("%s is not computed", c)
        if gone:
            self.indicators = [i for i in self.indicators if i.column not in gone]
            self._build()

    def _build(self) -> None:
        candle_s = self.app.shared["candle_seconds"]
        bar_s = resolve_timeframe(self.timeframe, candle_s)
        table = build_indicator_table(self.app.table("candles"), bar_s, candle_s, self.indicators)
        self.app.publish("indicators", table, ns_name=self.TABLE_VAR)
        self.bar_seconds = bar_s
        self.log.info("`%s` on %s bars: Pct%s", self.TABLE_VAR, format_timeframe(bar_s),
                      "".join(", " + i.column for i in self.indicators))

    def columns(self) -> list[str]:
        return [i.column for i in self.indicators]

    def describe(self) -> str:
        return (f"{format_timeframe(getattr(self, 'bar_seconds', 0) or 60)} bars: Pct"
                + "".join(", " + i.column for i in self.indicators))
