"""One `indicator_values` table computed from `candles` with Deephaven's incremental `update_by`.

Columns: Minute (start of the bar), Symbol, token, Pct, then one double column per indicator:
    SMA_<n>   simple average of the last n closes          EMA_<n>   exponential average of the close
    RSI_<n>   RSI, 0 to 100 (Wilder smoothing)             ATR_<n>   average true range (Wilder smoothing)
    MACD_<f>_<s>  MACD line     MACD_SIGNAL_<f>_<s>_<g>  its signal line     MACD_HIST_<f>_<s>_<g>  line - signal
Every indicator makes exactly one column.
Prices come out in the unit of the candles (proper currency units). Pct = percent change from the first bar seen for
the token (+1.25 = +1.25%). A blank is a Deephaven null, never a number. Smoothed averages start from the first bar, as the EMA does; each is blank or a partial average
until it has enough bars (see the class docs).

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
    """One output column. Subclasses set `base` ("EMA_9") and describe the calculation as `steps()`."""
    needs_delta = False            # True: the bar table provides D (price change)
    needs_gain_loss = False        # True: and Gain / Loss (D split in two)
    needs_pxsum = False            # True: and PxSum (running sum of closes, for the early running average)
    needs_hilo = False             # True: and Hi / Lo (high and low of the bar)
    base = ""                      # column name without the bar size

    def __init__(self, timeframe: str | None = None):
        # normalise "5 MIN" -> "5min"; None = the bar size the whole set runs on
        self.timeframe = format_timeframe(parse_timeframe(timeframe)) if timeframe is not None else None

    @property
    def column(self) -> str:
        return self.base + (f"_{self.timeframe}" if self.timeframe else "")

    def steps(self) -> list[tuple[str, list]]:
        """The calculation as ordered steps: ("ops", [update_by operations]) or ("update", ["Col = expr", ...]).
        Step i of every indicator runs together, so a step may only use what earlier steps produced."""
        return [("ops", self.ops())]

    def ops(self) -> list:
        """`update_by` operations (EMA / smoothing) producing this indicator's helper columns."""
        raise NotImplementedError

    def final(self) -> str:
        """Query-language expression `Name = ...` for the output column (int64)."""
        raise NotImplementedError


class SMA(Indicator):
    """Simple average of the last n closes. With fewer than n bars it is the average of the bars there are."""

    def __init__(self, n: int, timeframe: str | None = None):
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise ValueError("SMA length must be a whole number >= 1")
        super().__init__(timeframe)
        self.n = n
        self.base = f"SMA_{n}"

    def ops(self) -> list:
        from deephaven import updateby as uby
        return [uby.rolling_avg_tick(cols=[f"{self.column}_raw = Px"], rev_ticks=self.n)]

    def final(self) -> str:
        return f"{self.column} = {self.column}_raw"


class EMA(Indicator):
    """EMA with alpha 2/(n+1). While fewer than n bars exist it is the running average, so it never starts blank."""
    needs_pxsum = True

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
        return f"{c} = Cnt < {n} ? PxSum / Cnt : {c}_raw"


class RSI(Indicator):
    """Wilder RSI (EMA with alpha 1/n of gains and losses). Blank until n price changes have been seen."""
    needs_delta = True
    needs_gain_loss = True

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
        return (f"{c} = Cnt <= {n} ? NULL_DOUBLE : "
                f"(AvgL_{c} == 0 ? (AvgG_{c} > 0 ? 100.0 : 50.0) : 100.0 - 100.0 / (1.0 + AvgG_{c} / AvgL_{c}))")


class _MACD(Indicator):
    """Shared by the three MACD lines. Line = EMA(fast) - EMA(slow); signal = EMA(signal) of the line; hist = line - signal."""
    kind = ""

    def __init__(self, fast: int, slow: int, signal: int | None, timeframe: str | None):
        names = (("fast", fast), ("slow", slow)) + ((("signal", signal),) if signal is not None else ())
        for name, v in names:
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ValueError(f"MACD {name} must be a whole number >= 1")
        if fast >= slow:
            raise ValueError("MACD fast must be shorter than slow")
        super().__init__(timeframe)
        self.fast, self.slow, self.signal = fast, slow, signal
        tail = f"_{signal}" if signal is not None else ""
        self.base = f"{self.kind}_{fast}_{slow}{tail}"

    def _fast_slow(self) -> list:
        from deephaven import updateby as uby
        c = self.column
        return [uby.ema_tick(decay_ticks(2.0 / (self.fast + 1)), [f"{c}_f = Px"]),
                uby.ema_tick(decay_ticks(2.0 / (self.slow + 1)), [f"{c}_s = Px"])]

    def steps(self) -> list[tuple[str, list]]:
        if self.signal is None:
            return [("ops", self._fast_slow())]
        from deephaven import updateby as uby
        c = self.column
        return [("ops", self._fast_slow()),
                ("update", [f"{c}_m = {c}_f - {c}_s"]),
                ("ops", [uby.ema_tick(decay_ticks(2.0 / (self.signal + 1)), [f"{c}_g = {c}_m"])])]


class MACD(_MACD):
    """MACD line = EMA(fast) - EMA(slow). Blank until `slow` bars."""
    kind = "MACD"

    def __init__(self, fast: int = 12, slow: int = 26, timeframe: str | None = None):
        super().__init__(fast, slow, None, timeframe)

    def final(self) -> str:
        c = self.column
        return f"{c} = Cnt < {self.slow} ? NULL_DOUBLE : {c}_f - {c}_s"


class MACD_SIGNAL(_MACD):
    """The MACD signal line: EMA(signal) of the MACD line. Blank until `slow + signal - 1` bars."""
    kind = "MACD_SIGNAL"

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9, timeframe: str | None = None):
        super().__init__(fast, slow, signal, timeframe)

    def final(self) -> str:
        c = self.column
        return f"{c} = Cnt < {self.slow + self.signal - 1} ? NULL_DOUBLE : {c}_g"


class MACD_HIST(_MACD):
    """The MACD histogram: MACD line - signal line. Blank until `slow + signal - 1` bars."""
    kind = "MACD_HIST"

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9, timeframe: str | None = None):
        super().__init__(fast, slow, signal, timeframe)

    def final(self) -> str:
        c = self.column
        return f"{c} = Cnt < {self.slow + self.signal - 1} ? NULL_DOUBLE : {c}_m - {c}_g"


class ATR(Indicator):
    """Average true range: Wilder smoothing (alpha 1/n) of max(high-low, |high-prev close|, |low-prev close|).
    The first bar's true range is high-low. Blank until n bars."""
    needs_delta = True            # previous close = Px - D
    needs_hilo = True

    def __init__(self, n: int = 14, timeframe: str | None = None):
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise ValueError("ATR length must be a whole number >= 1")
        super().__init__(timeframe)
        self.n = n
        self.base = f"ATR_{n}"

    def steps(self) -> list[tuple[str, list]]:
        from deephaven import updateby as uby
        c = self.column
        tr = (f"{c}_tr = isNull(D) ? Hi - Lo : "
              f"Math.max(Hi - Lo, Math.max(Math.abs(Hi - (Px - D)), Math.abs(Lo - (Px - D))))")
        return [("update", [tr]),
                ("ops", [uby.ema_tick(decay_ticks(1.0 / self.n), [f"{c}_atr = {c}_tr"])])]

    def final(self) -> str:
        return f"{self.column} = Cnt < {self.n} ? NULL_DOUBLE : {self.column}_atr"


# Every indicator listed here is also exposed as a function in the session by `algo.install` (EMA(9), RSI(14), ...).
LIBRARY: dict[str, type] = {"SMA": SMA, "EMA": EMA, "RSI": RSI, "MACD": MACD, "MACD_SIGNAL": MACD_SIGNAL,
                            "MACD_HIST": MACD_HIST, "ATR": ATR}


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
def _bars(candles, bar_seconds: int, candle_seconds: int, hilo: bool = False):
    """Minute (bar start), Symbol, token, Px (close), Op (open), One, and Hi / Lo when asked: the candles, or rolled up
    into larger bars. Only the columns that are used are carried (a rolled-up high / low is a stored aggregate)."""
    cols = ["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"]
    if hilo:
        cols += ["Hi = High * 1.0", "Lo = Low * 1.0"]
    if bar_seconds == candle_seconds:
        return candles.view(cols)
    from deephaven import agg
    aggs = [agg.first("Open"), agg.last("Close")] + ([agg.max_("High"), agg.min_("Low")] if hilo else [])
    src = ["Open", "Close"] + (["High", "Low"] if hilo else [])
    return (candles.view([f"Minute = lowerBin(Minute, {bar_seconds} * SECOND)", "Symbol", "token", *src])
            .agg_by(aggs, by=["Minute", "Symbol", "token"])
            .view(cols))


def _compute(bars, indicators, with_pct: bool):
    """Add the indicator columns (and Pct) to a bar table."""
    from deephaven import updateby as uby
    need_delta = any(i.needs_delta for i in indicators)

    # bar count per token, plus only what some indicator uses: the running price sum (EMA), the price change (RSI, ATR)
    sums = ["Cnt = One"] + (["PxSum = Px"] if any(i.needs_pxsum for i in indicators) else [])
    first = [uby.cum_sum(sums)]
    if need_delta:
        first.append(uby.delta(["D = Px"]))
    t = bars.update_by(first, by="token")

    # plain formulas are lazy (update_view): they are evaluated when read and take no memory per row
    helpers = []
    if with_pct:
        helpers.append("OpenFirst = Cnt == 1 ? Op : NULL_DOUBLE")
    if any(i.needs_gain_loss for i in indicators):
        helpers += ["Gain = isNull(D) ? NULL_DOUBLE : Math.max(D, 0.0)",
                    "Loss = isNull(D) ? NULL_DOUBLE : Math.max(-D, 0.0)"]
    if helpers:
        t = t.update_view(helpers)

    # the indicators' steps, run together step by step; the day-open carry-forward rides along in step 0
    plans = [ind.steps() for ind in indicators]
    for k in range(max([len(p) for p in plans] + [1 if with_pct else 0])):
        ops = [uby.forward_fill(["DayOpen = OpenFirst"])] if (with_pct and k == 0) else []
        updates = []
        for plan in plans:
            if k < len(plan):
                kind, payload = plan[k]
                (ops if kind == "ops" else updates).extend(payload)
        if updates:
            t = t.update_view(updates)
        if ops:
            t = t.update_by(ops, by="token")

    out = ["Minute", "Symbol", "token"]
    if with_pct:
        out.append("Pct = (isNull(DayOpen) || DayOpen == 0) ? NULL_DOUBLE : 100.0 * (Px - DayOpen) / DayOpen")
    return t.view(out + [i.final() for i in indicators])


def build_indicator_table(candles, candle_seconds: int, default_seconds: int, indicators, token_table=None):
    """candles table -> indicator table (pure Deephaven query; no side effects).
    `token_table` (a table with a `token` column) limits the work to those instruments."""
    if token_table is not None:
        candles = candles.where_in(token_table, "token")
    groups = group_by_bar(indicators, default_seconds, candle_seconds)
    (driver_s, driver_inds), *coarser = groups.items()
    table = _compute(_bars(candles, driver_s, candle_seconds, any(i.needs_hilo for i in driver_inds)), driver_inds,
                     with_pct=True)
    if not coarser:
        return table
    bins = []
    for bar_s, inds in coarser:
        cols = [i.column for i in inds]
        g = (_compute(_bars(candles, bar_s, candle_seconds, any(i.needs_hilo for i in inds)), inds, with_pct=False)
             .view(["Minute", "token"] + cols))
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

    def __init__(self, indicators=(), timeframe: str | None = None, symbols=None, kind: str | None = None):
        """timeframe: bar size for indicators that name none ("5min"); None = the candle size.
        symbols / kind: compute only for these underlyings (["NIFTY"]) and kinds ("FUT", "OPT", "EQ"); None = every
        instrument in `candles`. Narrowing this is the main way to save memory and CPU."""
        super().__init__()
        if timeframe is not None:
            parse_timeframe(timeframe)
        self.timeframe = timeframe
        self.symbols, self.kind = symbols, kind
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

    def add(self, *specs, timeframe: str | None = None, symbols=None, kind: str | None = None) -> list[str]:
        """Start computing more indicators alongside the existing ones. Returns the new columns.
        Naming `symbols` / `kind` again changes which instruments every indicator is computed for."""
        before = (list(self.indicators), self.timeframe, self.symbols, self.kind)
        try:
            rescope = (symbols is not None or kind is not None) and (symbols, kind) != (self.symbols, self.kind)
            if rescope:
                self.log.warning("instruments change to symbols=%s kind=%s: every indicator is recomputed", symbols, kind)
                self.symbols, self.kind = symbols, kind
            if timeframe is not None and timeframe != self.timeframe:
                parse_timeframe(timeframe)
                self.log.warning("default bar size changes from %s to %s for indicators that name none",
                                 self.timeframe or "the candle size", timeframe)
                self.timeframe = timeframe
            added = self._merge(specs)
            if added or rescope or self.timeframe != before[1]:
                self._build()
        except Exception:
            self.indicators, self.timeframe, self.symbols, self.kind = before
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
        tokens = self.app.master.tokens_for(self.symbols, self.kind)
        token_table = None
        if tokens is not None:
            from deephaven.pandas import to_table
            import pandas as pd
            token_table = to_table(pd.DataFrame({"token": pd.Series(tokens, dtype="int64")}))
        table = build_indicator_table(self.app.table("candles"), candle_s, default_s, self.indicators, token_table)
        self.app.publish("indicators", table, ns_name=self.TABLE_VAR)
        self.default_seconds = default_s
        self.log.info("`%s`: Pct%s (default bars %s, %s)", self.TABLE_VAR, "".join(", " + c for c in self.columns()),
                      format_timeframe(default_s),
                      "all instruments" if tokens is None else f"{len(tokens)} instruments")

    def columns(self) -> list[str]:
        return [i.column for i in self.indicators]

    def describe(self) -> str:
        d = format_timeframe(getattr(self, "default_seconds", 0) or 60)
        return f"default bars {d}: Pct" + "".join(", " + c for c in self.columns())
