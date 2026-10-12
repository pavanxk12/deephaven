# indicators.py -- one `indicators` table built from `candles`: Minute, Symbol, token, Pct + one column per indicator.
# Indicators are computed on TIMEFRAME_MIN-minute bars rolled up from the 1-minute `candles`.
# `candles` is not modified, and `indicators` is the only table this script publishes.
# To add an indicator: add a line to INDICATORS and re-run this script.
import math
from deephaven import updateby as uby
from deephaven import agg

# =============================== USER INPUT ===============================
TIMEFRAME_MIN = 5     # 1 = use candles as-is, 5 = 5-minute bars, 15 = 15-minute bars, ...

INDICATORS = [
    ("RSI", 9),       # -> RSI_9   (Wilder smoothing, int64 = RSI x 100), 9 bars of TIMEFRAME_MIN
    ("EMA", 9),       # -> EMA_9   (int64, paise like Close; dynamic length min(bars, 9))
    ("EMA", 18),      # -> EMA_18  (int64, paise like Close; dynamic length min(bars, 18))
]
# ==========================================================================

if "candles" not in globals():
    raise RuntimeError("'candles' not found - run candles.py first")

if not isinstance(TIMEFRAME_MIN, int) or TIMEFRAME_MIN < 1:
    raise ValueError("TIMEFRAME_MIN must be a positive integer")


def _decay_ticks(alpha):
    # ema_tick weight per row is exp(-1/decay_ticks); solve for the smoothing factor alpha
    return -1.0 / math.log(1.0 - alpha)


def _build(src, mins, specs):
    # ---- 1) resample (skipped when mins == 1): Minute becomes the bucket start ----
    if mins == 1:
        t = src.view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"])
    else:
        t = (src.view([f"Minute = lowerBin(Minute, {mins} * MINUTE)", "Symbol", "token", "Open", "Close"])
                .agg_by([agg.first("Open"), agg.last("Close")], by=["Minute", "Symbol", "token"])
                .view(["Minute", "Symbol", "token", "Px = Close * 1.0", "Op = Open * 1.0", "One = 1"]))

    need_rsi = any(k == "RSI" for k, _ in specs)

    # ---- 2) bar count + running price sum per token (+ price change for RSI) ----
    first_ops = [uby.cum_sum(["Cnt = One", "PxSum = Px"])]
    if need_rsi:
        first_ops.append(uby.delta(["D = Px"]))
    t = t.update_by(first_ops, by="token")

    # ---- 3) helper columns ----
    helpers = ["OpenFirst = Cnt == 1 ? Op : NULL_DOUBLE"]
    if need_rsi:
        helpers += ["Gain = isNull(D) ? NULL_DOUBLE : Math.max(D, 0.0)",
                    "Loss = isNull(D) ? NULL_DOUBLE : Math.max(-D, 0.0)"]
    t = t.update(helpers)

    # ---- 4) DayOpen carry-forward + all EMA / Wilder smoothing in one update_by ----
    ops = [uby.forward_fill(["DayOpen = OpenFirst"])]
    for kind, n in specs:
        if kind == "EMA":
            ops.append(uby.ema_tick(_decay_ticks(2.0 / (n + 1)), [f"EMA_{n}_raw = Px"]))
        elif kind == "RSI":
            # Wilder smoothing = EMA with alpha 1/n
            ops.append(uby.ema_tick(_decay_ticks(1.0 / n), [f"AvgG_{n} = Gain", f"AvgL_{n} = Loss"]))
        else:
            raise ValueError(f"unknown indicator {kind}")
    t = t.update_by(ops, by="token")

    # ---- 5) final columns ----
    # Pct = (Close - day's open) / day's open, as int64 = percent x 100 (e.g. +1.25% -> 125)
    finals = ["Pct = (isNull(DayOpen) || DayOpen == 0) ? NULL_LONG : Math.round(10000.0 * (Px - DayOpen) / DayOpen)"]
    for kind, n in specs:
        if kind == "EMA":
            # dynamic length: running average of bars seen while Cnt < n, then true EMA
            finals.append(f"EMA_{n} = Cnt < {n} ? Math.round(PxSum / Cnt) : Math.round(EMA_{n}_raw)")
        else:
            finals.append(f"RSI_{n} = Cnt <= {n} ? NULL_LONG : Math.round(100.0 * "
                          f"(AvgL_{n} == 0 ? (AvgG_{n} > 0 ? 100.0 : 50.0) : 100.0 - 100.0 / (1.0 + AvgG_{n} / AvgL_{n})))")
    return t.view(["Minute", "Symbol", "token"] + finals)


indicators = _build(candles, TIMEFRAME_MIN, INDICATORS)
print(f"[indicators] built `indicators` on {TIMEFRAME_MIN}-min bars with Pct, "
      f"{', '.join(f'{k}_{n}' for k, n in INDICATORS)}")