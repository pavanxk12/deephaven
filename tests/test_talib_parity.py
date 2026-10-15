"""Our indicators against TA-Lib, an independent implementation. Skipped when TA-Lib is not installed (it is only a checker,
the runtime does not use it).

Both smooth the same way, but TA-Lib seeds EMA / RSI / ATR / MACD with a simple average of the first bars while the engine's
EMA starts from the first value. The difference decays by (1 - alpha) per bar, so the two agree once the series is long enough;
SMA has no seed and agrees as soon as the window is full.
"""
import numpy as np
import pytest

talib = pytest.importorskip("talib")

from algo.data.indicators import ATR, EMA, MACD, MACD_HIST, MACD_SIGNAL, RSI, SMA  # noqa: E402
from tests.test_indicator_math import candles, of, run, same, tok_df  # noqa: E402,F401
from tests.test_indicator_math import fake_engine  # noqa: E402,F401  (used through pytestmark)

pytestmark = pytest.mark.usefixtures("fake_engine")


@pytest.fixture(scope="module")
def data():
    return candles({1: 900, 2: 900}, seed=11)


def arrays(df, tok):
    t = tok_df(df, tok)
    return t.High.to_numpy(float), t.Low.to_numpy(float), t.Close.to_numpy(float)


def test_sma_is_identical_once_the_window_is_full(data):
    out = run(data, [SMA(20)])
    for tok in (1, 2):
        _, _, c = arrays(data, tok)
        same(of(out, tok, "SMA_20")[19:], talib.SMA(c, 20)[19:], tol=1e-12)


@pytest.mark.parametrize("n", [5, 14, 50])
def test_ema_agrees_after_the_seed_has_decayed(n, data):
    out = run(data, [EMA(n)])
    for tok in (1, 2):
        _, _, c = arrays(data, tok)
        same(of(out, tok, f"EMA_{n}")[-300:], talib.EMA(c, n)[-300:], tol=1e-9)


@pytest.mark.parametrize("n", [5, 14])
def test_rsi_agrees_with_talib(n, data):
    out = run(data, [RSI(n)])
    for tok in (1, 2):
        _, _, c = arrays(data, tok)
        same(of(out, tok, f"RSI_{n}")[-300:], talib.RSI(c, n)[-300:], tol=1e-7)


@pytest.mark.parametrize("n", [5, 14])
def test_atr_agrees_with_talib(n, data):
    out = run(data, [ATR(n)])
    for tok in (1, 2):
        h, l, c = arrays(data, tok)
        same(of(out, tok, f"ATR_{n}")[-300:], talib.ATR(h, l, c, n)[-300:], tol=1e-7)


def test_macd_line_signal_and_histogram_agree_with_talib(data):
    out = run(data, [MACD(12, 26), MACD_SIGNAL(12, 26, 9), MACD_HIST(12, 26, 9)])
    for tok in (1, 2):
        _, _, c = arrays(data, tok)
        line, signal, hist = talib.MACD(c, 12, 26, 9)
        same(of(out, tok, "MACD_12_26")[-300:], line[-300:], tol=1e-7)
        same(of(out, tok, "MACD_SIGNAL_12_26_9")[-300:], signal[-300:], tol=1e-7)
        same(of(out, tok, "MACD_HIST_12_26_9")[-300:], hist[-300:], tol=1e-7)


def test_the_early_bars_differ_only_by_the_seed(data):
    """Documented difference: the first bars of EMA / RSI differ from TA-Lib's SMA-seeded values, and the gap shrinks."""
    out = run(data, [EMA(14)])
    _, _, c = arrays(data, 1)
    ours, theirs = of(out, 1, "EMA_14"), talib.EMA(c, 14)
    gap = np.abs(ours - theirs)
    assert gap[14] > 1e-6 and gap[-1] < 1e-9 and gap[200] < gap[30]
