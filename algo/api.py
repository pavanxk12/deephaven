"""The functions you call in the Deephaven session. One line makes them available:

    import algo; algo.install(globals(), "NSE")

    master_build()                       contract master for today
    candles_start()                      1-minute candles start forming
    indicators(EMA(9), RSI(5))           start computing these; calling it again ADDS to what is already computed
    straddle_start(0)                    straddle series for the nearest option expiry (1 = two nearest, 2 = three, ...)

`EMA`, `RSI`, ... (everything in `algo.data.indicators.LIBRARY`) are plain functions that make an indicator;
only `indicators(...)` starts the computation.
"""
from __future__ import annotations

import logging

from algo.core.app import App, MissingDependency, connect, current
from algo.core.config import Paths, market_for
from algo.data.candles import Candles
from algo.data.indicators import LIBRARY, Indicators
from algo.data.master import Master
from algo.series.straddle import Straddle, StraddleConfig

HINTS = {"master": "master_build()", "candles": "candles_start()", "indicators": "indicators(EMA(9), RSI(14))"}


def _app() -> App:
    return current()


def _run(plugin) -> None:
    try:
        _app().run(plugin)
    except MissingDependency as e:
        needed = [d for d in plugin.requires if not _app().has(d)]
        raise RuntimeError(f"{e}. Call " + " then ".join(HINTS.get(d, d) for d in needed) + " first.") from None


def master_build(exchange: str | None = None, day: str | None = None, force: bool = False) -> None:
    """Build the contract master: master_build("NSE") or master_build("US"). The exchange sets the timezone, price scale,
    session and file formats for everything after it. Naming a different exchange than the running one stops the plugins
    that are running (they belong to the other market); no argument keeps the current exchange.
    Reads today's .db3 (or `day`, e.g. "20261015"); uses the cache unless force=True."""
    app = _app()
    if exchange is not None:
        market = market_for(exchange)
        if market != app.market:
            if app.plugins:
                print(f"switching {app.market.code} -> {market.code}: stopping {', '.join(app.plugins)}")
            app = connect(app.ns, market, app.paths)
    print(f"exchange: {app.market.code}")
    _run(Master(day=day, force_rebuild=force))


def candles_start(timeframe: str = "1min", build_from: int | None = None) -> None:
    """Start building candles from the live feed: candles_start("30s"), "1min" (default), "5min", "15min".
    Calling it again restarts the builder (the candles table starts empty); indicators, the straddle and the strategy
    follow the new table by themselves. `build_from`: epoch second of the first bar; default = the next full bar."""
    _run(Candles(timeframe, build_from=build_from))


def candles_stop() -> None:
    _app().stop("candles")


def indicators(*specs, timeframe: str | None = None) -> None:
    """Start computing indicators, e.g. indicators(EMA(9), RSI(5)). Calling it again adds to what is already running:
        indicators(EMA(9), RSI(5))   then   indicators(EMA(6))   ->   EMA_9, RSI_5 and EMA_6 are all computed.
    The result is the table `indicator_values`. They run on the candles as they are; `timeframe="5min"` rolls the candles
    up to larger bars first (a whole multiple of the candle size) and applies to the whole set.
    With no arguments it lists what is computed."""
    app = _app()
    plugin = app.get("indicators")
    if plugin is None:
        _run(Indicators(specs, timeframe))
    elif not specs and timeframe is None:
        print("computing:", ", ".join(plugin.columns()) or "(Pct only)", "on", plugin.describe().split(":")[0])
    else:
        plugin.add(*specs, timeframe=timeframe)


def indicators_remove(*columns: str) -> None:
    """Stop computing columns, e.g. indicators_remove("EMA_6")."""
    plugin = _app().get("indicators")
    if plugin is None:
        print("indicators are not running")
    else:
        plugin.remove(*columns)


def straddle_start(n: int = 0, symbols=None, **config) -> None:
    """Start the straddle series (V / D) -> `straddle_series`.
    n = how many option expiries beyond the nearest: 0 = nearest only, 1 = nearest + next, 2 = three nearest, ...
    Weeklies count as expiries; each expiry uses the future that matches it (weekly options use the monthly future).
    `symbols=["NIFTY", "BANKNIFTY"]` limits it; default is every symbol with a future and options."""
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise ValueError("n must be 0, 1, 2, ... (0 = nearest expiry only)")
    cfg = StraddleConfig(expiries=n + 1, symbols=tuple(symbols) if symbols else None, **config)
    _run(Straddle(cfg))


def straddle_stop() -> None:
    _app().stop("straddle")


def algo_status() -> None:
    """What is running."""
    _app().status()


FUNCTIONS = (master_build, candles_start, candles_stop, indicators, indicators_remove,
             straddle_start, straddle_stop, algo_status)


def install(ns: dict, market: str = "NSE", paths: Paths | None = None, level: int = logging.INFO) -> App:
    """Connect to the session and put the functions above (and EMA, RSI, ...) into `ns`."""
    app = connect(ns, market, paths, level)
    for fn in FUNCTIONS:
        ns[fn.__name__] = fn
    ns.update(LIBRARY)
    print("algo ready - call:", ", ".join(f"{f.__name__}()" for f in FUNCTIONS))
    print("indicator functions:", ", ".join(LIBRARY))
    return app
