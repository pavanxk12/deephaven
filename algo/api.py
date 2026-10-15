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
import os
from datetime import datetime, timedelta

from algo.core.app import App, MissingDependency, connect, current
from algo.core.config import Paths, market_for
from algo.data.candles import Candles
from algo.data.indicators import LIBRARY, Indicators
from algo.data.master import Master
from algo.data.store import DEFAULT_TABLES, PREFIX, Eod, make_store, normalise_day, save_tables
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


def master_build(exchange: str | None = None, day: str | None = None, force: bool = False, file: str | None = None) -> None:
    """Build the contract master: master_build("NSE") or master_build("US"). The exchange sets the timezone, price scale,
    session and file formats for everything after it. Naming a different exchange than the running one stops the plugins
    that are running (they belong to the other market); no argument keeps the current exchange.
    Reads today's .db3 (or `day`, e.g. "20261015"); uses the cache unless force=True.
    `file="20261015DB_US.db3"` names the file when more than one in the folder matches the day (it is also refused if the file
    belongs to the other market)."""
    app = _app()
    if exchange is not None:
        market = market_for(exchange)
        if market != app.market:
            if app.plugins:
                print(f"switching {app.market.code} -> {market.code}: stopping {', '.join(app.plugins)}")
            app = connect(app.ns, market, app.paths)
    print(f"exchange: {app.market.code}")
    path = None if file is None else (file if os.path.isabs(file) else os.path.join(app.paths.market_data, file))
    _run(Master(day=day, db_path=path, force_rebuild=force))


def candles_start(timeframe: str = "1min", multiplier: float | None = None, *, symbols=None, kind: str | None = None,
                  build_from: int | None = None) -> None:
    """Start building candles from the live feed: candles_start("30s"), "1min" (default), "5min", "15min".
    `multiplier` is applied to every feed price to get proper rates: a feed in paise needs 0.01
    (candles_start("1min", 0.01)); the default is each contract's own Divisor from the contract file.
    `symbols=["NIFTY", "BANKNIFTY"]` and/or `kind="FUT"` ("FUT", "OPT" or "EQ") build candles only for those instruments:
    the candle table (and everything derived from it) shrinks in proportion, which is the main way to save memory.
    Calling it again restarts the builder (the candles table starts empty); indicators, the straddle and the strategy
    follow the new table by themselves. `build_from`: epoch second of the first bar; default = the next full bar."""
    _run(Candles(timeframe, multiplier=multiplier, symbols=symbols, kind=kind, build_from=build_from))


def candles_stop() -> None:
    _app().stop("candles")


def indicators(*specs, timeframe: str | None = None, symbols=None, kind: str | None = None) -> None:
    """Start computing indicators, e.g. indicators(EMA(9), RSI(5)). Calling it again adds to what is already running:
        indicators(EMA(9), RSI(5))   then   indicators(EMA(6))   ->   EMA_9, RSI_5 and EMA_6 are all computed.
    The result is the table `indicator_values`.
    Bar size: each indicator can have its own, EMA(9, "5min") -> column EMA_9_5min, on 5-minute bars rolled up from the
    candles (a whole multiple of the candle size). `timeframe="5min"` here is the default for the ones that name none.
    Instruments: `symbols=["NIFTY"]` and/or `kind="FUT"` compute only for those (everything in `candles` by default);
    each extra instrument costs memory for every indicator column, so narrow this to what you use.
    With no arguments it lists what is computed."""
    app = _app()
    plugin = app.get("indicators")
    if plugin is None:
        _run(Indicators(specs, timeframe, symbols=symbols, kind=kind))
    elif not specs and timeframe is None and symbols is None and kind is None:
        print("computing:", ", ".join(plugin.columns()) or "(none)", f"[{plugin.describe().split(':')[0]}]")
    else:
        plugin.add(*specs, timeframe=timeframe, symbols=symbols, kind=kind)


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


def eod_save(day: str | None = None, folder: str | None = None, tables=None, snapshot: bool = True) -> None:
    """Save the whole candles and straddle tables to Parquet files: candles_<day>.parquet, straddle_<day>.parquet (each with
    a Date column and a small .meta.json beside it). `tables=["candles", "straddle_series", "indicator_values"]` chooses.
    `folder` is where the files go; by default Paths.saved (set Paths(eod_dir=...) in algo.install to point it at a folder
    mounted from the host). `day` defaults to today in the exchange timezone ("2026-10-15" or "20261015").
    snapshot=True takes a frozen copy first (briefly needs the memory of one copy); False writes the live table."""
    results = save_tables(_app(), tuple(tables) if tables else DEFAULT_TABLES, day, folder, snapshot)
    for r in results:
        print(f"  {r['table']:<17} {r['status']:<8} {r['rows']:>12,} rows  {r['path'] or ''}")


def eod_load(day: str | None = None, folder: str | None = None, tables=None) -> None:
    """Load saved files back as tables. eod_load("2026-10-15") -> candles_20261015, straddle_20261015 (and indicators_...);
    eod_load() merges every saved day -> candles_hist, straddle_hist."""
    app = _app()
    store = make_store(app, folder)
    for prefix in [PREFIX[t] for t in (tables or DEFAULT_TABLES)]:
        try:
            if day is None:
                table, n = store.load_all(prefix)
                name, note = f"{prefix}hist", f"{n} file(s)"
            else:
                d = normalise_day(day, app.market.zone)
                table = store.load_day(prefix, d)
                name, note = f"{prefix}{d.replace('-', '')}", d
        except FileNotFoundError as e:
            print(f"  {prefix}: {e}")
            continue
        app.publish(name, table)
        print(f"  {name:<24} loaded ({note})")


def eod_auto(at: str | None = None, folder: str | None = None, stop: bool = False) -> None:
    """Save by itself every day at `at` ("15:35", exchange time). Default: 5 minutes after the session ends.
    eod_auto(stop=True) switches it off."""
    app = _app()
    if stop:
        app.stop("eod")
        return
    if at is None:
        end = datetime.combine(datetime.now(app.market.zone).date(), app.market.session[1]) + timedelta(minutes=5)
        when = end.time()
    else:
        try:
            when = datetime.strptime(at, "%H:%M").time()
        except (TypeError, ValueError):
            raise ValueError(f'at must look like "15:35", got {at!r}') from None
    _run(Eod(DEFAULT_TABLES, auto_at=when, folder=folder))


def algo_status() -> None:
    """What is running."""
    _app().status()


FUNCTIONS = (master_build, candles_start, candles_stop, indicators, indicators_remove,
             straddle_start, straddle_stop, eod_save, eod_load, eod_auto, algo_status)


def install(ns: dict, market: str = "NSE", paths: Paths | None = None, level: int = logging.INFO) -> App:
    """Connect to the session and put the functions above (and EMA, RSI, ...) into `ns`."""
    app = connect(ns, market, paths, level)
    for fn in FUNCTIONS:
        ns[fn.__name__] = fn
    ns.update(LIBRARY)
    print("algo ready - call:", ", ".join(f"{f.__name__}()" for f in FUNCTIONS))
    print("indicator functions:", ", ".join(LIBRARY))
    return app
