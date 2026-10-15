"""algo - plugin framework for the Deephaven trading stack.

    import algo; algo.install(globals(), "NSE")      # once per session (or from init.py)
    master_build()
    candles_start()
    indicators(EMA(9), RSI(5))
    straddle_start(0)

Every component is a `Plugin` with start() / stop(); `app.run` replaces a running plugin of the same name.
Pure logic (aggregation, sizing, ranking, straddle maths) is separate from the Deephaven wiring and unit tested.
"""
from algo.api import install
from algo.core.app import App, MissingDependency, connect, current, reload
from algo.core.config import MARKETS, NSE, US, Market, Paths
from algo.core.plugin import Plugin
from algo.data.candles import Candles
from algo.data.indicators import ATR, EMA, MACD, MACD_HIST, MACD_SIGNAL, PCT, RSI, SMA, Indicator, Indicators
from algo.data.master import Master
from algo.data.store import Eod, History
from algo.series.straddle import Straddle, StraddleConfig
from algo.strategy.book import Book
from algo.strategy.bridge import Bridge
from algo.strategy.config import BridgeConfig, RsiOptionsConfig
from algo.strategy.rsi_options import RsiOptions

def __getattr__(name):                      # the console needs deephaven.ui, so it is imported on first use
    if name in ("Console", "ConsoleConfig"):
        from algo import ui
        return getattr(ui, name)
    raise AttributeError(f"module 'algo' has no attribute {name!r}")


__all__ = [
    "install",
    "App", "MissingDependency", "connect", "current", "reload", "MARKETS", "NSE", "US", "Market", "Paths", "Plugin",
    "Master", "Candles", "Indicators", "Indicator", "SMA", "EMA", "RSI", "MACD", "MACD_SIGNAL", "MACD_HIST", "ATR", "PCT", "Eod", "History", "Straddle", "StraddleConfig",
    "Book", "Bridge", "BridgeConfig", "RsiOptions", "RsiOptionsConfig", "Console", "ConsoleConfig",
]
