# indicators.py -- one `indicators` table built from `candles`: Minute, Symbol, token + one column per indicator.
# To add an indicator: add a line to INDICATORS and re-run this script (recomputes over the whole candles table).
import math
from deephaven import updateby as uby

# =============================== USER INPUT ===============================
INDICATORS = [
    ("RSI", 14),      # -> RSI_14  (Wilder smoothing, int64 = RSI x 100)
    ("EMA", 9),       # -> EMA_9   (int64, paise like Close)
    ("EMA", 18),      # -> EMA_18
]
# ==========================================================================

if "candles" not in globals():
    raise RuntimeError("'candles' not found - run candles.py first")


def _decay_ticks(alpha):
    # ema_tick weight per row is exp(-1/decay_ticks); solve for the smoothing factor alpha
    return -1.0 / math.log(1.0 - alpha)


def _ema_cols(n):
    return f"EMA_{n}", uby.ema_tick(_decay_ticks(2.0 / (n + 1)), [f"EMA_{n}_raw=Px"])


def _build(candles_tbl, specs):
    t = candles_tbl.view([
        "Minute", "Symbol", "token",
        "Px = Close * 1.0",
        "One = 1",
    ])

    # row count per token, used to blank an indicator until it has enough history
    t = t.update_by(uby.cum_sum(["Cnt = One"]), by="token")

    ops = []
    need_delta = any(k == "RSI" for k, _ in specs)
    if need_delta:
        t = t.update_by(uby.delta(["D = Px"]), by="token")
        t = t.update(["Gain = isNull(D) ? NULL_DOUBLE : Math.max(D, 0.0)",
                      "Loss = isNull(D) ? NULL_DOUBLE : Math.max(-D, 0.0)"])

    for kind, n in specs:
        if kind == "EMA":
            ops.append(_ema_cols(n)[1])
        elif kind == "RSI":
            # Wilder smoothing = EMA with alpha 1/n
            ops.append(uby.ema_tick(_decay_ticks(1.0 / n), [f"AvgG_{n} = Gain", f"AvgL_{n} = Loss"]))
        else:
            raise ValueError(f"unknown indicator {kind}")

    t = t.update_by(ops, by="token")

    # Math.round(double) already returns long, so no casts; new columns are created in view (no type change on update)
    finals = []
    for kind, n in specs:
        if kind == "EMA":
            finals.append(f"EMA_{n} = Cnt < {n} ? NULL_LONG : Math.round(EMA_{n}_raw)")
        else:
            finals.append(f"RSI_{n} = Cnt <= {n} ? NULL_LONG : Math.round(100.0 * "
                          f"(AvgL_{n} == 0 ? (AvgG_{n} > 0 ? 100.0 : 50.0) : 100.0 - 100.0 / (1.0 + AvgG_{n} / AvgL_{n})))")
    return t.view(["Minute", "Symbol", "token"] + finals)


indicators = _build(candles, INDICATORS)
print(f"[indicators] built `indicators` with {', '.join(f'{k}_{n}' for k, n in INDICATORS)}")
