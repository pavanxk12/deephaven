"""One `indicators` table computed from `candles` with Deephaven's incremental `update_by`.

Columns: Minute, Symbol, token, Pct, then one int64 column per indicator:
    RSI_<n>  RSI x 100 (Wilder smoothing)        EMA_<n>  EMA of the close, same unit as Close
Pct = percent change from the first bar seen for the token, x 100 (+1.25% -> 125).

Indicators are computed on `timeframe` -minute bars rolled up from the 1-minute candles. To add an indicator, write
an `Indicator` subclass (two small methods) and pass an instance to `Indicators([...])`.
"""
from __future__ import annotations

import math

from algo.core.plugin import Plugin


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


DEFAULT = (RSI(9), EMA(9), EMA(18))


def build_indicator_table(candles, timeframe: int, indicators):
    """candles table -> indicators table (pure Deephaven query; no side effects)."""
    from deephaven import agg
    from deephaven import updateby as uby

    # 1) resample (skipped for 1-minute bars): Minute becomes the bucket start
    if timeframe == 1:
        t = candles.view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"])
    else:
        t = (candles.view([f"Minute = lowerBin(Minute, {timeframe} * MINUTE)", "Symbol", "token", "Open", "Close"])
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
    name = "indicators"
    requires = ("candles",)

    def __init__(self, indicators=DEFAULT, timeframe: int = 1):
        super().__init__()
        if not isinstance(timeframe, int) or timeframe < 1:
            raise ValueError("timeframe must be a positive integer (minutes)")
        cols = [i.column for i in indicators]
        if len(set(cols)) != len(cols):
            raise ValueError(f"duplicate indicator columns: {cols}")
        self.indicators, self.timeframe = list(indicators), timeframe

    def start(self) -> None:
        table = build_indicator_table(self.app.table("candles"), self.timeframe, self.indicators)
        self.app.publish("indicators", table)
        self.log.info("built on %d-min bars with Pct, %s", self.timeframe,
                      ", ".join(i.column for i in self.indicators))

    def describe(self) -> str:
        return f"{self.timeframe}-min bars: " + ", ".join(i.column for i in self.indicators)
