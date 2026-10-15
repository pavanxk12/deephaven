"""The real indicator pipeline, run on a pandas stand-in for the engine, against textbook formulas.

Several tokens at once, several indicators at once, several bar sizes at once. What this proves is that the queries are
right (formulas, warm-up, grouping per token, step order, joins); the engine's own operators (ema_tick, update_by...) are
assumed to behave as documented.
"""
import numpy as np
import pandas as pd
import pytest

from algo.data.indicators import ATR, EMA, MACD, MACD_HIST, MACD_SIGNAL, RSI, SMA, build_indicator_table
from tests import pandas_engine as eng

pytestmark = pytest.mark.usefixtures("fake_engine")


@pytest.fixture
def fake_engine(monkeypatch):
    eng.install(monkeypatch)


# ---- data -------------------------------------------------------------------------------------------
def candles(n_by_token, seed=7, start_by_token=None, step=60):
    """Random-walk candles: several tokens, different lengths and start times, rows ordered by time then token."""
    rng = np.random.default_rng(seed)
    rows = []
    for tok, n in n_by_token.items():
        start = (start_by_token or {}).get(tok, 0)
        px = 100.0 + 50 * tok
        for i in range(n):
            o = px
            px = max(1.0, px * (1 + rng.normal(0, 0.004)))
            hi = max(o, px) * (1 + abs(rng.normal(0, 0.001)))
            lo = min(o, px) * (1 - abs(rng.normal(0, 0.001)))
            rows.append(dict(Minute=36000 + start + i * step, Symbol=f"SYM{tok}", token=tok, Open=o, High=hi, Low=lo,
                             Close=px, Volume=int(rng.integers(1, 100))))
    return pd.DataFrame(rows).sort_values(["Minute", "token"], kind="stable").reset_index(drop=True)


def run(df, inds, default=60, candle=60, tokens=None):
    tt = None if tokens is None else eng.PT(pd.DataFrame({"token": list(tokens)}))
    return build_indicator_table(eng.PT(df), candle, default, inds, tt).df


def of(out, tok, col):
    return out.loc[out.token == tok, col].to_numpy(dtype=float)


def same(a, b, tol=1e-9):
    np.testing.assert_allclose(a, b, rtol=tol, atol=tol, equal_nan=True)


# ---- textbook references ---------------------------------------------------------------------------------
def ref_ema(c, n):
    ema = c.ewm(alpha=2 / (n + 1), adjust=False).mean().to_numpy()
    avg = c.expanding().mean().to_numpy()
    return np.where(np.arange(len(c)) < n - 1, avg, ema)                 # running average until n bars


def ref_rsi(c, n):
    d = c.diff()
    g = d.clip(lower=0).where(d.notna())
    l = (-d).clip(lower=0).where(d.notna())
    ag, al = g.ewm(alpha=1 / n, adjust=False).mean().to_numpy(), l.ewm(alpha=1 / n, adjust=False).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        rsi = np.where(al == 0, np.where(ag > 0, 100.0, 50.0), 100 - 100 / (1 + ag / al))
    rsi = np.array(rsi, dtype=float)
    rsi[: n] = np.nan                                                    # blank until n price changes
    return rsi


def ref_wilder_rsi(c, n):
    """Textbook: first averages are simple means of the first n changes, then Wilder smoothing."""
    d = c.diff().to_numpy()
    gain, loss = np.clip(d, 0, None), np.clip(-d, 0, None)
    ag, al = np.full(len(c), np.nan), np.full(len(c), np.nan)
    ag[n], al[n] = gain[1: n + 1].mean(), loss[1: n + 1].mean()
    for i in range(n + 1, len(c)):
        ag[i], al[i] = (ag[i - 1] * (n - 1) + gain[i]) / n, (al[i - 1] * (n - 1) + loss[i]) / n
    return 100 - 100 / (1 + ag / al)


def true_range(df):
    pc = df.Close.shift(1)
    tr = pd.concat([df.High - df.Low, (df.High - pc).abs(), (df.Low - pc).abs()], axis=1).max(axis=1, skipna=True)
    return tr


def ref_atr(df, n):
    atr = true_range(df).ewm(alpha=1 / n, adjust=False).mean().to_numpy().copy()
    atr[: n - 1] = np.nan
    return atr


def ref_wilder_atr(df, n):
    tr = true_range(df).to_numpy()
    atr = np.full(len(df), np.nan)
    atr[n - 1] = tr[:n].mean()
    for i in range(n, len(df)):
        atr[i] = (atr[i - 1] * (n - 1) + tr[i]) / n
    return atr


def tok_df(df, tok):
    return df[df.token == tok].reset_index(drop=True)


ALL = lambda: [SMA(5), EMA(9), RSI(14), MACD(12, 26), MACD_SIGNAL(12, 26, 9), MACD_HIST(12, 26, 9), ATR(14)]  # noqa: E731


# ---- every indicator, several tokens, all at once ------------------------------------------------------
def test_all_indicators_on_several_tokens_match_the_reference():
    df = candles({1: 120, 2: 90, 3: 150}, start_by_token={1: 0, 2: 600, 3: 1800})
    out = run(df, ALL())
    assert len(out) == len(df) and list(out.token) == list(df.token) and list(out.Minute) == list(df.Minute)
    for tok in (1, 2, 3):
        t = tok_df(df, tok)
        c = t.Close
        same(of(out, tok, "SMA_5"), c.rolling(5, min_periods=1).mean().to_numpy())
        same(of(out, tok, "EMA_9"), ref_ema(c, 9))
        same(of(out, tok, "RSI_14"), ref_rsi(c, 14))
        line = c.ewm(alpha=2 / 13, adjust=False).mean() - c.ewm(alpha=2 / 27, adjust=False).mean()
        sig = line.ewm(alpha=2 / 10, adjust=False).mean()
        m = np.where(np.arange(len(c)) < 25, np.nan, line)
        s = np.where(np.arange(len(c)) < 26 + 9 - 2, np.nan, sig)
        h = np.where(np.arange(len(c)) < 26 + 9 - 2, np.nan, line - sig)
        same(of(out, tok, "MACD_12_26"), m)
        same(of(out, tok, "MACD_SIGNAL_12_26_9"), s)
        same(of(out, tok, "MACD_HIST_12_26_9"), h)
        same(of(out, tok, "ATR_14"), ref_atr(t, 14))
        same(of(out, tok, "Pct"), 100 * (c - t.Open.iloc[0]) / t.Open.iloc[0])


def test_the_output_is_one_row_per_candle_with_the_expected_columns():
    df = candles({1: 30, 2: 30})
    out = run(df, [EMA(9), RSI(5)])
    assert list(out.columns) == ["Minute", "Symbol", "token", "Pct", "EMA_9", "RSI_5"]
    assert list(out.Symbol) == [f"SYM{t}" for t in df.token]


def test_a_token_is_not_affected_by_the_other_tokens():
    df = candles({1: 80, 2: 80, 3: 80})
    together = run(df, ALL())
    for tok in (1, 2, 3):
        alone = run(df[df.token == tok], ALL())
        for col in alone.columns:
            if col not in ("Symbol",):
                same(of(together, tok, col), alone[col].to_numpy(dtype=float))


def test_adding_an_indicator_does_not_change_the_others():
    df = candles({1: 70, 2: 70})
    few = run(df, [EMA(9), RSI(14)])
    many = run(df, ALL() + [EMA(21)])
    for col in ("EMA_9", "RSI_14", "Pct"):
        same(few[col].to_numpy(dtype=float), many[col].to_numpy(dtype=float))


def test_the_order_indicators_are_listed_in_does_not_matter():
    df = candles({1: 70, 2: 70})
    a, b = run(df, ALL()), run(df, list(reversed(ALL())))
    for col in a.columns:
        if col != "Symbol":
            same(a[col].to_numpy(dtype=float), b[col].to_numpy(dtype=float))


def test_same_indicator_twice_with_different_lengths_gets_separate_columns():
    df = candles({1: 60})
    out = run(df, [EMA(5), EMA(9), EMA(20), RSI(5), RSI(14), SMA(3), SMA(10)])
    for n in (5, 9, 20):
        same(of(out, 1, f"EMA_{n}"), ref_ema(df.Close, n))
    for n in (5, 14):
        same(of(out, 1, f"RSI_{n}"), ref_rsi(df.Close, n))


# ---- wilder convergence against the textbook seeding ---------------------------------------------------
def test_rsi_and_atr_converge_to_the_textbook_wilder_values():
    df = candles({1: 500})
    out = run(df, [RSI(14), ATR(14)])
    t = tok_df(df, 1)
    same(of(out, 1, "RSI_14")[-50:], ref_wilder_rsi(t.Close, 14)[-50:], tol=1e-6)
    same(of(out, 1, "ATR_14")[-50:], ref_wilder_atr(t, 14)[-50:], tol=1e-6)


# ---- numbers that used to be trouble -----------------------------------------------------------------
def test_a_flat_price_gives_rsi_50_and_zero_atr_without_dividing_by_zero():
    df = candles({1: 40})
    df["Open"] = df["High"] = df["Low"] = df["Close"] = 123.45
    out = run(df, [RSI(14), ATR(14), EMA(9), SMA(5), MACD(), MACD_SIGNAL(), MACD_HIST()])
    assert set(of(out, 1, "RSI_14")[14:]) == {50.0} and np.isnan(of(out, 1, "RSI_14")[:14]).all()
    assert set(of(out, 1, "ATR_14")[13:]) == {0.0}
    same(of(out, 1, "EMA_9"), np.full(40, 123.45))
    assert set(of(out, 1, "Pct")) == {0.0}
    assert np.nanmax(np.abs(of(out, 1, "MACD_12_26")[25:])) < 1e-9


def test_a_steady_rise_gives_rsi_100():
    df = candles({1: 40})
    df["Close"] = df["High"] = np.arange(1, 41) + 100.0
    df["Open"] = df["Low"] = df["Close"] - 1
    out = run(df, [RSI(14)])
    assert set(of(out, 1, "RSI_14")[14:]) == {100.0}


def test_a_single_bar_and_very_short_histories_are_blank_not_wrong():
    df = candles({1: 1, 2: 3})
    out = run(df, ALL())
    assert len(out) == 4
    assert np.isnan(of(out, 1, "RSI_14")).all() and np.isnan(of(out, 1, "MACD_12_26")).all() and np.isnan(of(out, 1, "ATR_14")).all()
    assert of(out, 1, "SMA_5")[0] == df.Close.iloc[0] and of(out, 1, "EMA_9")[0] == df.Close.iloc[0]
    assert of(out, 1, "Pct")[0] == pytest.approx(100 * (df.Close.iloc[0] - df.Open.iloc[0]) / df.Open.iloc[0])


def test_an_instrument_with_a_gap_still_lines_up():
    df = candles({1: 60})
    df = df.drop(index=range(20, 30)).reset_index(drop=True)             # ten missing minutes: the indicator sees consecutive bars
    out = run(df, [EMA(9), RSI(5)])
    same(of(out, 1, "EMA_9"), ref_ema(df.Close, 9))
    same(of(out, 1, "RSI_5"), ref_rsi(df.Close, 5))


# ---- bar sizes ---------------------------------------------------------------------------------------
def test_coarser_bars_are_rolled_up_and_joined_onto_the_finer_rows():
    df = candles({1: 61, 2: 47})
    out = run(df, [EMA(9), EMA(9, "5min"), RSI(5, "15min")])
    assert list(out.columns) == ["Minute", "Symbol", "token", "Pct", "EMA_9", "EMA_9_5min", "RSI_5_15min"]
    assert len(out) == len(df)
    for tok in (1, 2):
        t = tok_df(df, tok)
        same(of(out, tok, "EMA_9"), ref_ema(t.Close, 9))
        for size, col, ref in ((300, "EMA_9_5min", lambda c: ref_ema(c, 9)), (900, "RSI_5_15min", lambda c: ref_rsi(c, 5))):
            bins = (t.Minute // size) * size
            coarse_close = t.groupby(bins, sort=True).Close.last()
            coarse = pd.Series(ref(coarse_close.reset_index(drop=True)), index=coarse_close.index)
            expected = bins.map(coarse).to_numpy(dtype=float)           # each row shows the value of the bar it falls in
            same(of(out, tok, col), expected)


def test_atr_on_coarser_bars_uses_the_bars_high_and_low():
    df = candles({1: 90})
    out = run(df, [ATR(5, "5min")])
    t = df.copy()
    t["bin"] = (t.Minute // 300) * 300
    bars = t.groupby("bin").agg(Open=("Open", "first"), High=("High", "max"), Low=("Low", "min"), Close=("Close", "last"))
    assert len(out) == len(bars) == 18                                 # only 5-minute indicators: one row per 5-minute bar
    same(of(out, 1, "ATR_5_5min"), ref_atr(bars.reset_index(drop=True), 5))


def test_candles_larger_than_a_minute_work_as_the_base():
    df = candles({1: 40}, step=300)
    out = run(df, [EMA(9), EMA(9, "15min")], default=300, candle=300)
    same(of(out, 1, "EMA_9"), ref_ema(df.Close, 9))
    bins = (df.Minute // 900) * 900
    c = df.groupby(bins).Close.last()
    same(of(out, 1, "EMA_9_15min"), bins.map(pd.Series(ref_ema(c.reset_index(drop=True), 9), index=c.index)).to_numpy(dtype=float))


# ---- scope -------------------------------------------------------------------------------------------
def test_only_the_requested_tokens_are_computed():
    df = candles({1: 50, 2: 50, 3: 50})
    out = run(df, [EMA(9), RSI(5)], tokens=[2, 3])
    assert set(out.token) == {2, 3} and len(out) == 100
    full = run(df, [EMA(9), RSI(5)])
    for tok in (2, 3):
        same(of(out, tok, "EMA_9"), of(full, tok, "EMA_9"))
