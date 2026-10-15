"""One `indicator_values` table computed from `candles` with Deephaven's incremental `update_by`.

Columns: Minute (start of the bar), Symbol, token, Pct, then one int64 column per indicator:
    RSI_<n>  RSI x 100 (Wilder smoothing)        EMA_<n>  EMA of the close, same unit as Close
Pct = percent change from the first bar seen for the token, x 100 (+1.25% -> 125).

Every indicator can have its own bar size: EMA(9) runs on the table's default bars, EMA(9, "5min") on 5-minute bars and
is called EMA_9_5min. Bars are rolled up from the candles, so a size must be a whole multiple of the candle size.
The table has one row per bar of the FINEST size in use; a coarser indicator shows the value of the bar that row falls in
(the bar still forming at that moment, and the final value for bars that have closed).

To add an indicator, write an `Indicator` subclass and add it to `LIBRARY`.
"""
from __future__ import annotations

import math

from algo.core.plugin import Plugin
from algo.core.timeframe import format_timeframe, parse_timeframe


def decay_ticks(alpha: float) -> float:
    """`ema_tick` weight per row is exp(-1/decay_ticks); solve for the smoothing factor `alpha`."""
    return -1.0 / math.log(1.0 - alpha)


class Indicator:
    """One output column. Subclasses set `base` ("EMA_9") and add `update_by` operations and a final expression."""
    needs_delta = False            # True: the bar table provides Gain / Loss (price change split in two)
    base = ""                      # column name without the bar size

    def __init__(self, timeframe: str | None = None):
        # normalise "5 MIN" -> "5min"; None = the bar size the whole set runs on
        self.timeframe = format_timeframe(parse_timeframe(timeframe)) if timeframe is not None else None

    @property
    def column(self) -> str:
        return self.base + (f"_{self.timeframe}" if self.timeframe else "")

    def ops(self) -> list:
        """`update_by` operations (EMA / smoothing) producing this indicator's helper columns."""
        raise NotImplementedError

    def final(self) -> str:
        """Query-language expression `Name = ...` for the output column (int64)."""
        raise NotImplementedError


class EMA(Indicator):
    """EMA with alpha 2/(n+1). While fewer than n bars exist it is the running average, so it never starts blank."""

    def __init__(self, n: int, timeframe: str | None = None):
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise ValueError("EMA length must be a whole number >= 1")
        super().__init__(timeframe)
        self.n = n
        self.base = f"EMA_{n}"

    def ops(self) -> list:
        from deephaven import updateby as uby
        return [uby.ema_tick(decay_ticks(2.0 / (self.n + 1)), [f"{self.column}_raw = Px"])]

    def final(self) -> str:
        c, n = self.column, self.n
        return f"{c} = Cnt < {n} ? Math.round(PxSum / Cnt) : Math.round({c}_raw)"


class RSI(Indicator):
    """Wilder RSI (EMA with alpha 1/n of gains and losses). Blank until n price changes have been seen."""
    needs_delta = True

    def __init__(self, n: int, timeframe: str | None = None):
        if not isinstance(n, int) or isinstance(n, bool) or n < 2:
            raise ValueError("RSI length must be a whole number >= 2")
        super().__init__(timeframe)
        self.n = n
        self.base = f"RSI_{n}"

    def ops(self) -> list:
        from deephaven import updateby as uby
        c = self.column
        return [uby.ema_tick(decay_ticks(1.0 / self.n), [f"AvgG_{c} = Gain", f"AvgL_{c} = Loss"])]

    def final(self) -> str:
        c, n = self.column, self.n
        return (f"{c} = Cnt <= {n} ? NULL_LONG : Math.round(100.0 * "
                f"(AvgL_{c} == 0 ? (AvgG_{c} > 0 ? 100.0 : 50.0) : 100.0 - 100.0 / (1.0 + AvgG_{c} / AvgL_{c})))")


# Every indicator listed here is also exposed as a function in the session by `algo.install` (EMA(9), RSI(14), ...).
LIBRARY: dict[str, type] = {"EMA": EMA, "RSI": RSI}


# =================================================================================================
# bar sizes (pure)
# =================================================================================================
def resolve_timeframe(requested: str | None, fallback_seconds: int, candle_seconds: int) -> int:
    """Seconds of one indicator's bars: its own size, else `fallback_seconds`. Must be a whole multiple of the candle size."""
    seconds = parse_timeframe(requested) if requested is not None else fallback_seconds
    if seconds < candle_seconds or seconds % candle_seconds:
        raise ValueError(f"indicator bars of {format_timeframe(seconds)} cannot be built from "
                         f"{format_timeframe(candle_seconds)} candles: use a whole multiple of {format_timeframe(candle_seconds)}")
    return seconds


def default_bars(requested: str | None, candle_seconds: int) -> int:
    """Bar size of indicators that name none: the set-wide size if given, else the candle size."""
    return resolve_timeframe(requested, candle_seconds, candle_seconds)


def group_by_bar(indicators, default_seconds: int, candle_seconds: int) -> dict[int, list]:
    """{bar seconds: [indicators]}, finest first. With no indicators there is still one (empty) group at the default size."""
    groups: dict[int, list] = {}
    for ind in indicators:
        groups.setdefault(resolve_timeframe(ind.timeframe, default_seconds, candle_seconds), []).append(ind)
    if not groups:
        groups[default_seconds] = []
    return dict(sorted(groups.items()))


# =================================================================================================
# the query
# =================================================================================================
def _bars(candles, bar_seconds: int, candle_seconds: int):
    """Minute (bar start), Symbol, token, Px (close), Op (open), One: the candles, or rolled up into larger bars."""
    if bar_seconds == candle_seconds:
        return candles.view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"])
    from deephaven import agg
    return (candles.view([f"Minute = lowerBin(Minute, {bar_seconds} * SECOND)", "Symbol", "token", "Open", "Close"])
            .agg_by([agg.first("Open"), agg.last("Close")], by=["Minute", "Symbol", "token"])
            .view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"]))


def _compute(bars, indicators, with_pct: bool):
    """Add the indicator columns (and Pct) to a bar table."""
    from deephaven import updateby as uby
    need_delta = any(i.needs_delta for i in indicators)

    # bar count + running price sum per token (+ price change for RSI)
    first = [uby.cum_sum(["Cnt = One", "PxSum = Px"])]
    if need_delta:
        first.append(uby.delta(["D = Px"]))
    t = bars.update_by(first, by="token")

    helpers = []
    if with_pct:
        helpers.append("OpenFirst = Cnt == 1 ? Op : NULL_DOUBLE")
    if need_delta:
        helpers += ["Gain = isNull(D) ? NULL_DOUBLE : Math.max(D, 0.0)",
                    "Loss = isNull(D) ? NULL_DOUBLE : Math.max(-D, 0.0)"]
    if helpers:
        t = t.update(helpers)

    # day-open carry-forward and every smoothing operation in one pass
    ops = [uby.forward_fill(["DayOpen = OpenFirst"])] if with_pct else []
    for ind in indicators:
        ops += ind.ops()
    if ops:
        t = t.update_by(ops, by="token")

    out = ["Minute", "Symbol", "token"]
    if with_pct:
        out.append("Pct = (isNull(DayOpen) || DayOpen == 0) ? NULL_LONG : Math.round(10000.0 * (Px - DayOpen) / DayOpen)")
    return t.view(out + [i.final() for i in indicators])


def build_indicator_table(candles, candle_seconds: int, default_seconds: int, indicators):
    """candles table -> indicator table (pure Deephaven query; no side effects)."""
    groups = group_by_bar(indicators, default_seconds, candle_seconds)
    (driver_s, driver_inds), *coarser = groups.items()
    table = _compute(_bars(candles, driver_s, candle_seconds), driver_inds, with_pct=True)
    if not coarser:
        return table
    bins = []
    for bar_s, inds in coarser:
        cols = [i.column for i in inds]
        g = _compute(_bars(candles, bar_s, candle_seconds), inds, with_pct=False).view(["Minute", "token"] + cols)
        bins.append(f"Bin_{bar_s}")
        table = (table.update_view(f"Bin_{bar_s} = lowerBin(Minute, {bar_s} * SECOND)")
                 .natural_join(g, on=["token", f"Bin_{bar_s} = Minute"], joins=cols))
    names = ["Minute", "Symbol", "token", "Pct"] + [i.column for i in indicators]
    return table.view(names)


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
        """timeframe: bar size for indicators that name none ("5min"); None = the candle size."""
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
                parse_timeframe(timeframe)
                self.log.warning("default bar size changes from %s to %s for indicators that name none",
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
        default_s = default_bars(self.timeframe, candle_s)
        table = build_indicator_table(self.app.table("candles"), candle_s, default_s, self.indicators)
        self.app.publish("indicators", table, ns_name=self.TABLE_VAR)
        self.default_seconds = default_s
        self.log.info("`%s`: Pct%s (default bars %s)", self.TABLE_VAR, "".join(", " + i.column for i in self.indicators),
                      format_timeframe(default_s))

    def columns(self) -> list[str]:
        return [i.column for i in self.indicators]

    def describe(self) -> str:
        d = format_timeframe(getattr(self, "default_seconds", 0) or 60)
        return f"default bars {d}: Pct" + "".join(", " + i.column for i in self.indicators)
