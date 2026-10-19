"""The functions you call in the Deephaven session. One line makes them available:

    import algo; algo.install(globals(), "NSE")

    master_build()                       contract master for today
    candles_start()                      1-minute candles start forming
    straddle_start(0)                    straddle series for the nearest option expiry (1 = two nearest, 2 = three, ...)

Indicators are not a service: an automation computes the ones it needs from the candles it reads (see examples/03_algo_template.py).
`algo_help()` lists every function by service.
"""
from __future__ import annotations

import inspect
import logging
import os
from dataclasses import replace
from datetime import datetime, timedelta

from algo.comms.nats_service import NatsConfig, NatsService
from algo.core.app import App, MissingDependency, connect, current
from algo.core.config import FIRST_PF, Paths, market_for
from algo.data.candles import Candles
from algo.lookup import (contract, contracts, expiries, last_candles, last_price, nearest_strikes,
                         strikes)
from algo.data.master import Master
from algo.data.store import DEFAULT_TABLES, PREFIX, Eod, make_store, normalise_day, save_tables
from algo.automation import (algo_exit, algo_pause, algo_portfolios, algo_sending, algo_start, algo_stop, algo_unpause,
                             algo_unsubscribe, pf_activate, pf_assign, pf_allowed, pf_cooling, pf_leg, pf_mark, pf_track, watch)
from algo.series.straddle import Straddle, StraddleConfig
from algo.strategy.registry import PortfolioRegistry

HINTS = {"master": "master_build()", "candles": "candles_start()",
         "nats": "nats_start()", "registry": "registry_start()"}


def _app() -> App:
    return current()


def _run(plugin) -> None:
    try:
        _app().run(plugin)
    except MissingDependency as e:
        needed = [d for d in plugin.requires if not _app().has(d)]
        raise RuntimeError(f"{e}. Call " + " then ".join(HINTS.get(d, d) for d in needed) + " first.") from None


def master_build(exchange: str | None = None, day: str | None = None, force: bool = False, file: str | None = None,
                 source: str = "file", pg: dict | None = None) -> None:
    """Build the contract master: master_build("NSE") or master_build("US"). The exchange sets the timezone, price scale,
    session and file formats for everything after it. Naming a different exchange than the running one stops the plugins
    that are running (they belong to the other market); no argument keeps the current exchange.
    Reads today's .db3 (or `day`, e.g. "20261015"); uses the cache unless force=True.
    `file="20261015DB_US.db3"` names the file when more than one in the folder matches the day (it is also refused if the file
    belongs to the other market).
    `source="postgres"` reads the same contracts from the Postgres table resultset_<day> (needs the pg8000 driver; the password in the ALGO_PG_PASSWORD environment
    variable, or pg={"password": ...}; host, port, database and user default to the Docker service: pg={"host": "postgres", "database": "algofusion", ...});
    `source="auto"` tries Postgres and uses the .db3 file when it cannot; the default "file" is the .db3 as before. Naming `file=` always means that file."""
    app = _app()
    if exchange is not None:
        market = market_for(exchange)
        if market != app.market:
            if app.plugins:
                print(f"switching {app.market.code} -> {market.code}: stopping {', '.join(app.plugins)}")
            app = connect(app.ns, market, app.paths)
    print(f"exchange: {app.market.code}")
    path = None if file is None else (file if os.path.isabs(file) else os.path.join(app.paths.market_data, file))
    _run(Master(day=day, db_path=path, force_rebuild=force, source=source, pg=pg))


def candles_start(timeframe: str = "1min", multiplier: float | None = None, *, symbols=None, kind: str | None = None,
                  expiries: int | None = None, build_from: int | None = None) -> None:
    """Start building candles from the live feed: candles_start("30s"), "1min" (default), "5min", "15min".
    `multiplier` is applied to every feed price to get proper rates: a feed in paise needs 0.01
    (candles_start("1min", 0.01)); the default is each contract's own Divisor from the contract file.
    `symbols=["NIFTY", "BANKNIFTY"]` and/or `kind="FUT"` ("FUT", "OPT" or "EQ") build candles only for those instruments:
    the candle table (and everything derived from it) shrinks in proportion, which is the main way to save memory.
    `expiries=1` keeps, for every underlying, only the options of its nearest expiry (2 = the two nearest...); futures and
    equities are untouched. Options are about 99% of all instruments, so this is the biggest single saving
    (NSE, all underlyings: everything is ~1.8 GB of 1-minute candles a day, expiries=1 about 0.7 GB, futures alone 14 MB).
    The straddle needs options of its n+1 nearest expiries, so keep `expiries` at n+1 or more if you use straddle_start(n).
    Calling it again restarts the builder (the candles table starts empty); the straddle follows the new table by itself, and a script
    that listens with watch("candles", fn) follows it too (a plain listener would be left on the old, silent table). `build_from`: epoch second of the first bar; default = the next full bar."""
    _run(Candles(timeframe, multiplier=multiplier, symbols=symbols, kind=kind, expiries=expiries, build_from=build_from))


def candles_stop() -> None:
    """Stop building candles."""
    _app().stop("candles")


def straddle_start(n: int = 0, symbols=None, **config) -> None:
    """Start the straddle series (V / D) -> `straddle_series`.
    n = how many option expiries beyond the nearest: 0 = nearest only, 1 = nearest + next, 2 = three nearest, ...
    Weeklies count as expiries; each expiry uses the future that matches it (weekly options use the monthly future).
    `symbols=["NIFTY", "BANKNIFTY"]` limits it; default is every symbol with a future and options."""
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise ValueError("n must be 0, 1, 2, ... (0 = nearest expiry only)")
    candles = _app().get("candles")
    if candles is not None and getattr(candles, "expiries", None) is not None and candles.expiries < n + 1:
        print(f"warning: candles were started with expiries={candles.expiries} but straddle_start({n}) needs the {n + 1} nearest "
              f"expiries; the missing ones will be empty. Restart candles with expiries={n + 1} or more.")
    cfg = StraddleConfig(expiries=n + 1, symbols=tuple(symbols) if symbols else None, **config)
    _run(Straddle(cfg))


def straddle_stop() -> None:
    """Stop the straddle series."""
    _app().stop("straddle")


def absolute(series: str, level: str, symbol: str, n: int = 22, ref=None, now: "float | None" = None, at=None):
    """Where does the value NOW stand among the values at the SAME POINT IN TIME of the previous n periods? A plain function: nothing runs in the background.
        hist, idx = absolute("iv", "intraday", "NIFTY", 22)
        hist, idx = absolute("pct", "intraday", "NIFTY FUT", 22, ref="10min")      % change from 10 minutes ago, now vs the same minute of the last 22 days
        hist = the n past values at this session minute (oldest first, NaN where missing); idx = how many of them are below the value now (0 = lowest ... n = highest, -1 = cannot tell).
        The result also has .now .n_valid .pctile .minute .dates .ref.
    series: "pct" (the percent change of the price from a reference point, chosen with ref=) | "iv" (the IV of straddle_series; with ref= its percent change) |
            "pct_open" and "pct_vwap" (shorthands for series "pct" with ref "open" / "vwap").
    ref (the reference point a percent is measured from; default "open"):
        "open"        the open of the period: day open (intraday), week open (intraweek), month open (intramonth)
        "vwap"        the VWAP since that open (price only)
        "prev_close"  the last value of the previous trading day
        "10min"       10 minutes ago (any number of minutes: "1min" ... "389min")
        "09:30"       a fixed time of the day (exchange time): the value at that minute; NaN before that time has come
    level: "intraday" (same minute, previous n days) | "intraweek" (same weekday and minute, previous n weeks) | "intramonth" (same trading day of the month and minute, previous n months).
    symbol (the instrument is exactly what you write; nothing falls back to another):
        "SBIN"               the cash stock, its own history
        "SBIN FUT"           the nearest future that has not expired, on each day (it rolls: last month's contract on the days it was the nearest)
        "SBIN JUN FUT"       the contract expiring in June ("SBIN JUN26 FUT" with the year): only that contract's own days, never an earlier contract's
        "SBIN 30JUN26 FUT"   one contract by its full name
        for "iv" the underlying (the straddle is built from the options; all four spellings give the same IV).
        A day on which the named instrument has no file or no rows is NaN. A reference (open, vwap, prev_close) of a future is always taken from the SAME contract as the
        day it belongs to, so a roll does not add the calendar spread between two contracts.
    now=: give the current value yourself.  at=: evaluate as of another moment, in exchange time: "HH:MM", "2026-03-20 11:30", a datetime or epoch seconds.
    How it works: history is read from the saved candles_<day> / straddle_<day> files (one symbol and one day at a time, kept in one size-limited cache);
    today so far is read from the candles / straddle_series table for this one symbol at the moment of the call. Time is the exchange's own session minute, so the
    US clock change needs no care. The first call for a symbol reads its history (about half a second for 22 days); absolute_warm() does many symbols ahead in one go."""
    return _absolute().absolute(series, level, symbol, n, ref=ref, now=now, at=at)


def relative(series: str, level: str, symbol: str, basket=None, ref=None, now: "float | None" = None, at=None):
    """Where does the value NOW of a symbol stand among the values now of the other symbols of a basket? A plain function, computed at the call.
        values, idx = relative("pct", "intraday", "HDFCBANK", ref="10min")            the basket HDFCBANK is defined in (baskets.txt); says so if it is in none or in several
        values, idx = relative("pct", "intraday", "HDFCBANK", "banks", ref="open")    explicitly that basket: it may or may not hold HDFCBANK
        values = the series now for each basket symbol (the symbol itself left out; NaN where a symbol has no value); idx = how many of them are below the symbol's value
        (0 = lowest ... n = highest, -1 = cannot tell). The result also has .now .n_valid .pctile .symbols (the names the values belong to) .basket .minute .ref.
    series and ref: as in absolute(): series "pct" measured from ref ("open" | "vwap" | "prev_close" | "10min" minutes ago | "09:30" a fixed time of the day; default "open"),
    "iv" (the IV of the underlying; with ref= its percent change), or the shorthands "pct_open" / "pct_vwap". level: "intraday" | "intraweek" | "intramonth": it sets which
    period the open / VWAP belongs to (today's, this week's, this month's); every symbol of the basket is measured the same way. basket: None (the symbol's own basket), a
    basket's NAME (see basket()) or a list of symbols.
    Today's rows of all the symbols are read in ONE read; the earlier days a reference needs (a week's or month's, the previous day) are read once for the whole basket.
    now=: give the symbol's value yourself. at=: evaluate as of another moment ("HH:MM", "2026-03-20 11:30", a datetime or epoch seconds, exchange time)."""
    return _absolute().relative(series, level, symbol, basket, ref=ref, now=now, at=at)


def basket(name: "str | None" = None, symbols=None, add=None, remove=None, delete: bool = False):
    """Baskets of symbols for relative(): a text file you edit, or this function. They live in baskets.txt:
        # baskets.txt   (in /data/fo_market_data/ next to stocks.txt, else in the state folder; absolute_settings(basket_file=...) names another file)
        [banks]
        HDFCBANK
        ICICIBANK, SBIN, AXISBANK          # symbols one per line or separated by commas; # starts a comment
        [nifty_complex]
        NIFTY FUT
        BANKNIFTY FUT
    and then  relative("pct_open", "intraday", "HDFCBANK", "banks").
    From the terminal:
        basket()                                    the baskets in the file, with their sizes
        basket("banks")                             the symbols of one
        basket("banks", ["HDFCBANK", "SBIN"])       create it, or replace it
        basket("banks", add=["AXISBANK"]);  basket("banks", remove=["SBIN"])
        basket("banks", delete=True)
    A change is written into the same baskets.txt; only that basket's lines change (your comments and the other baskets stay). The file is read again whenever it changes;
    a mistake in it is reported with its line number (and relative() refuses until it is fixed). A file in a read-only mounted folder cannot be changed from here."""
    store = _absolute().baskets
    if name is None:
        names = store.names()
        print(f"baskets in {store.path or store.paths}: " + (", ".join(f"{n} ({len(store.get(n))})" for n in names) if names else "none"))
        return names
    if delete:
        store.delete(name)
        print(f"basket {name!r} deleted from {store.path or store.paths}")
        return []
    if symbols is not None:
        store.define(name, symbols)
    elif add is not None or remove is not None:
        store.edit(name, add or (), remove or ())
    out = store.get(name)
    print(f"basket {name!r}: {len(out)} symbols: " + ", ".join(out[:12]) + (" ..." if len(out) > 12 else ""))
    return out


def absolute_warm(series: str, level: str, symbols, n: int = 22, ref=None, at=None) -> int:
    """Read the saved history for many symbols ahead of time, each day file once for all of them (300 symbols x 22 days in about 1.5 s), so the absolute() calls that
    follow are instant (about 0.1 ms plus the read of today's rows). Use the same series and ref as the calls that will follow. It warms as many symbols as the cache holds
    and says so. Returns how many were warmed."""
    return _absolute().warm(series, level, symbols, n, ref=ref, at=at)


def absolute_settings(memory_mb: "float | None" = None, max_age: "int | None" = None, iv_max_age: "int | None" = None, min_valid: "int | None" = None,
                      folder: "str | None" = None, basket_file: "str | None" = None, clear: bool = False) -> dict:
    """Show or change what absolute() uses: memory_mb (the limit of its cache, default 200; about 5 KB per symbol per day), max_age (minutes a price is carried, 5),
    iv_max_age (minutes an IV is carried, 3), min_valid (how many of the n past values must exist; default half of n), folder (where the saved files are; default the
    EOD folder and the market-data folder), basket_file (the baskets text file; default baskets.txt in the market-data folder, then in the state folder). Changing any of them, or clear=True, empties the cache. Returns the current cache numbers."""
    app = _app()
    cfg = app.shared.setdefault("absolute_settings", {"memory_mb": 200, "max_age": 5, "iv_max_age": 3, "min_valid": None, "folder": None, "basket_file": None})
    changed = clear
    for key, value in (("memory_mb", memory_mb), ("max_age", max_age), ("iv_max_age", iv_max_age), ("min_valid", min_valid), ("folder", folder), ("basket_file", basket_file)):
        if value is not None and cfg[key] != value:
            cfg[key], changed = value, True
    if changed:
        app.shared.pop("absolute_service", None)
    stats = _absolute().stats()
    print(f"absolute(): {cfg} | cache {stats['cache_mb']} MB in {stats['cache_entries']} entries")
    return stats


def _absolute():
    """The one shared AbsoluteService of the session (made on first use; only a cache, no thread, no listener)."""
    from algo.series.absolute import AbsoluteService, Baskets, Clock, ParquetSource, deephaven_readers
    app = _app()
    svc = app.shared.get("absolute_service")
    if svc is None:
        cfg = app.shared.setdefault("absolute_settings", {"memory_mb": 200, "max_age": 5, "iv_max_age": 3, "min_valid": None, "folder": None, "basket_file": None})
        today_bars, today_iv, today_bars_many, today_iv_many, contract_of = deephaven_readers(app)
        places = [cfg["basket_file"]] if cfg.get("basket_file") else [os.path.join(app.paths.market_data, "baskets.txt"), os.path.join(app.paths.state, "baskets.txt")]
        baskets = Baskets(places)
        svc = app.shared["absolute_service"] = AbsoluteService(
            Clock(app.market.zone, app.market.session), ParquetSource(make_store(app, cfg["folder"])), cfg["memory_mb"], cfg["max_age"], cfg["iv_max_age"],
            cfg["min_valid"], today_bars, today_iv, None, today_bars_many, today_iv_many, baskets, contract_of)
    return svc


def eod_save(day: str | None = None, folder: str | None = None, tables=None, snapshot: bool = True) -> None:
    """Save the whole candles and straddle tables to Parquet files: candles_<day>.parquet, straddle_<day>.parquet (each with
    a Date column and a small .meta.json beside it). `tables=["candles", "straddle_series"]` chooses.
    `folder` is where the files go; by default Paths.saved (set Paths(eod_dir=...) in algo.install to point it at a folder
    mounted from the host). `day` defaults to today in the exchange timezone ("2026-10-15" or "20261015").
    snapshot=True takes a frozen copy first (briefly needs the memory of one copy); False writes the live table."""
    results = save_tables(_app(), tuple(tables) if tables else DEFAULT_TABLES, day, folder, snapshot)
    for r in results:
        print(f"  {r['table']:<17} {r['status']:<8} {r['rows']:>12,} rows  {r['path'] or ''}")


def eod_load(day: "str | int | None" = None, folder: str | None = None, tables=None, days: int | None = None) -> None:
    """Load saved files back as tables. eod_load("2026-10-15") -> candles_20261015, straddle_20261015.
    eod_load(days=5) (or eod_load(5)) merges the LAST 5 saved days -> candles_hist, straddle_hist; eod_load() merges every saved day.
    `tables=["straddle_series"]` chooses. A day is looked for in the EOD folder and in the market-data folder."""
    app = _app()
    if isinstance(day, int) and not isinstance(day, bool):                    # eod_load(5) means the last 5 days
        day, days = None, (day if days is None else days)
    if day is not None and days is not None:
        raise ValueError("give a day (eod_load('2026-10-15')) or a number of days (eod_load(days=5)), not both")
    if days is not None and (isinstance(days, bool) or not isinstance(days, int) or days < 1):
        raise ValueError(f"days must be a whole number from 1, like 5, got {days!r}")
    store = make_store(app, folder)
    for prefix in [PREFIX[t] for t in (tables or DEFAULT_TABLES)]:
        try:
            if day is None:
                table, n = store.load_all(prefix, last=days)
                span = [d for d, _ in store.days(prefix)][-n:]
                name, note = f"{prefix}hist", f"{n} day(s): {span[0]} to {span[-1]}"
            else:
                d = normalise_day(day, app.market.zone)
                table = store.load_day(prefix, d)
                name, note = f"{prefix}{d.replace('-', '')}", d
        except (FileNotFoundError, ValueError) as e:                              # no such file, or days with different columns: say so, go on
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


def _users(users) -> tuple:
    items = [users] if isinstance(users, (int, str)) else list(users)
    out = []
    for u in items:
        if isinstance(u, bool) or not str(u).strip().isdigit() or int(u) <= 0:
            raise ValueError(f"users must be a user id like 3, or a list like [3, 4], got {users!r}")
        if int(u) not in out:
            out.append(int(u))
    if not out:
        raise ValueError("users must name at least one user")
    return tuple(out)


def _nats_wanted(previous, ip, port, users, send, dry_run, config):
    """The settings asked for: the running service's (or the defaults) with what was named changed. Returns (config, what was named)."""
    base = previous.cfg if previous is not None else NatsConfig()
    given = {k: v for k, v in (("ip", ip), ("port", port), ("dry_run", dry_run)) if v is not None}
    given.update(config)
    if users is not None:
        given["users"] = _users(users)
    if "ip" in given:
        given["ip"] = str(given["ip"]).strip()
        if not given["ip"] or "://" in given["ip"] or ":" in given["ip"]:
            raise ValueError(f'ip is just the address, like "192.168.1.130" (the port is a separate argument), got {ip!r}')
    if "port" in given:
        try:
            given["port"] = int(given["port"])
        except (TypeError, ValueError):
            raise ValueError(f"port must be a number like 4222, got {port!r}") from None
        if not 0 < given["port"] < 65536:
            raise ValueError(f"port must be between 1 and 65535, got {port!r}")
    return replace(base, send_on=send, **given), given


def _nats_line(cfg) -> str:
    return f"NATS {cfg.ip}:{cfg.port}, users {', '.join(map(str, cfg.users))}"


def nats_start(ip: str | None = None, port: int | None = None, users=None, *, send: bool = True,
               dry_run: bool | None = None, **config) -> None:
    """Start the ONE NATS service that every algo and the console send portfolio commands through. Start it once, for everyone:

        nats_start("192.168.1.130", 4222, users=[3, 4])  the server's address and port, and ALL the users that may send

    Safe to call again: if the service is already running with these settings (or none are given) nothing happens, so every
    algo script may call it; it does NOT restart the service. Different settings raise an error that points to nats_restart(),
    because restarting cuts off every algo. An algo should call nats_connect() instead.
    Every command says which user it is for (pf_pause(1913, user=4)); with a single user it may be left out; a command for a user
    not listed is refused (user_not_allowed). nats_start(dry_run=True) opens no connection and only logs.
    The master switch starts ON, but each strategy starts OFF and must switch itself on (nats_send(True, client="name")).
    Other settings: timeout_s, max_in_flight, lane_depth, sender (see NatsConfig)."""
    previous = _app().get("nats")
    cfg, given = _nats_wanted(previous, ip, port, users, send, dry_run, config)
    if previous is not None:
        running = previous.cfg
        extra = set(cfg.users) - set(running.users)
        same = replace(cfg, send_on=running.send_on, users=running.users) == running
        if not given or (same and not extra):
            print(f"{_nats_line(running)} is already running; nothing changed. An algo only needs nats_connect().")
            return
        if same and extra:
            raise RuntimeError(f"the NATS service is running for users {list(running.users)}, not {sorted(extra)}. Add them without a "
                               f"restart: nats_add_user({sorted(extra)[0]}).")
        raise RuntimeError(f"the NATS service is already running ({_nats_line(running)}) and these settings differ. Starting it again "
                           f"would cut off every algo; to change it on purpose call nats_restart(...).")
    print(_nats_line(cfg))
    _run(NatsService(cfg, lift_blocks=given.get("users", ()) if users is not None else ()))


def nats_restart(ip: str | None = None, port: int | None = None, users=None, *, send: bool = True,
                 dry_run: bool | None = None, **config) -> None:
    """Stop the NATS service and start it again, on purpose: nats_restart(port=4223). The ip, port, users and other settings are
    those of the running service unless you name them. It cuts off every algo for a moment, and every strategy starts OFF
    again and must switch itself on. Naming users here UNBLOCKS them and every portfolio under them (you are allowing them
    again); a restart with no users keeps every block."""
    previous = _app().get("nats")
    cfg, given = _nats_wanted(previous, ip, port, users, send, dry_run, config)
    print(_nats_line(cfg) + (" (restarting)" if previous is not None else ""))
    _run(NatsService(cfg, lift_blocks=given.get("users", ()) if users is not None else ()))


def nats_stop() -> None:
    """Stop the NATS service (every algo is cut off; the ip, port and users are forgotten)."""
    _app().stop("nats")


def _nats() -> "NatsService | None":
    return _app().get("nats")


def _service_down(what: str) -> None:
    print(f"{what}: NOT sent - the NATS service is not running. Start it once with nats_start(ip, port, users=[...]).")


def nats_connect(user: int | None = None, client: str | None = None) -> bool:
    """What an algo calls instead of nats_start: it only CHECKS that the shared service is running (and that your user may
    send), changes nothing, and says so. Returns True when you can go on.  nats_connect(user=4, client="nifty_sma")"""
    svc = _nats()
    if svc is None:
        print("The NATS service is not running. Start it once with nats_start(ip, port, users=[...]); do not start it from an algo.")
        return False
    if user is not None:
        (uid,) = _users(user)
        if uid not in svc.cfg.users:
            print(f"user {uid} may not send on the running NATS service (users {list(svc.cfg.users)}). "
                  f"Add it without a restart: nats_add_user({uid}).")
            return False
    line = f"connected to the NATS service: {svc.cfg.ip}:{svc.cfg.port}, users {list(svc.cfg.users)}, state {svc.state}"
    if client is not None:
        line += f"; {client} sending {'ON' if svc.is_enabled(client) else 'OFF (switch it on: nats_send(True, client=...))'}"
    print(line)
    return True


def nats_add_user(user) -> None:
    """Let one more user (or a list) send on the running service, without restarting it."""
    svc = _nats()
    if svc is None:
        return _service_down("nats_add_user")
    svc.add_users(*_users(user))
    print(f"users now {list(svc.cfg.users)}")


def nats_send(on: bool = True, client: str | None = None) -> None:
    """The switches. Every automation (strategy) has its own and starts OFF; the service also has a master switch for everyone. (The console's buttons are not a
    client: they send as an operator and need only the master switch; its ON / OFF button flips the master in the all-strategies window and one strategy's switch in that
    strategy's window.)

        nats_send(True, client="nifty_sma")       this strategy may send (nats_send(False, client="nifty_sma") stops only it); the name is the one given to algo_start
        nats_send(False)                          the master switch: nothing is sent for anyone;  nats_send(True) lets them again

    A command goes out only when the master switch and its own strategy's switch are both on. A name that is not a running strategy is refused with the list of those that are."""
    svc = _nats()
    if svc is None:
        return _service_down("nats_send")
    if client is None:
        (svc.send_on if on else svc.send_off)()
        return
    plugin = _app().get(client)
    if plugin is None or not hasattr(plugin, "send_on"):           # a typo would otherwise create a switch nobody reads and print nothing
        running = sorted(n for n, p in _app().plugins.items() if hasattr(p, "send_on") and hasattr(p, "send_off"))
        print(f"nats_send: no automation called {client!r} is running, so nothing was changed. Start it with algo_start(user, {client!r}). "
              f"Running: {', '.join(running) or 'none'}")
        return
    (plugin.send_on if on else plugin.send_off)()                   # the automation's own switch (and it re-checks what it was waiting for)


def _answer(call, what: str, paused: "tuple[int, bool] | None" = None) -> None:
    """Run `call(service)` and print how it went. A service that is not running, or a broken connection, is said in words."""
    svc = _nats()
    if svc is None:
        return _service_down(what)
    try:
        r = call(svc).result(svc.cfg.timeout_s + 10)
    except Exception as e:
        print(f"{what}: FAILED {e!r}")
        return
    bridge = _app().get("bridge")
    if paused is not None and bridge is not None and r.status == "ok":      # keep the strategy's book right after a manual pause
        bridge.book.set_paused(*paused)
    note = {"off": " (sending is OFF: nats_send(True) turns it on)", "dry": " (dry run, nothing sent)",
            "disconnected": " (NATS is down; it reconnects by itself, send again when it is back)",
            "no_user": " (several users are allowed: add user=...)", "user_not_allowed": " (that user was not given to nats_start)",
            "blocked": " (blocked: nats_unblock(...) lifts it)",
            "timeout": " (no answer: it is not known whether the bridge acted)"}.get(r.status, "")
    print(f"{what}: {'ok' if r.ok else 'NOT ok'} - {r.status}{note}")


def pf_subscribe(pf: int, option: int, user: int | None = None, **params) -> None:
    """Subscribe portfolio `pf` to the option token: pf_subscribe(1913, 47162, user=4). Extra keywords override the defaults
    sent. `user` may be left out when the service has a single user."""
    _answer(lambda s: s.subscribe(pf, option, params or None, user=user), f"PF {pf} subscribe {option}")


def pf_pause(pf: int, user: int | None = None) -> None:
    """Pause one portfolio: pf_pause(1913, user=4)."""
    _answer(lambda s: s.pause(pf, user=user), f"PF {pf} pause", (pf, True))


def pf_unpause(pf: int, user: int | None = None) -> None:
    """Unpause one portfolio: pf_unpause(1913, user=4)."""
    _answer(lambda s: s.unpause(pf, user=user), f"PF {pf} unpause", (pf, False))


def pf_squareup(pf: int, user: int | None = None) -> None:
    """Square up one portfolio: pf_squareup(1913, user=4)."""
    _answer(lambda s: s.squareup(pf, user=user), f"PF {pf} squareup")


def pf_unsubscribe(pf: int, option: int, user: int | None = None, **params) -> None:
    """Unsubscribe a portfolio from its option: pf_unsubscribe(1913, 47162, user=4)."""
    _answer(lambda s: s.unsubscribe(pf, option, params or None, user=user), f"PF {pf} unsubscribe {option}")


def pf_apply(pf: int, user: int | None = None, **params) -> None:
    """Set the parameters of portfolio `pf`: pf_apply(1913, user=4, ival=1.1, jval=0.8, sval=3.3, maxqty=3, side="BUY", ...)."""
    _answer(lambda s: s.apply(pf, params, user=user), f"PF {pf} apply")


def nats_block(user=None, pf=None) -> None:
    """Stop sending for a user, or for some of one user's portfolios. A portfolio number belongs to a user (user + portfolio
    is the unique id), so a portfolio is always named WITH its user:

        nats_block(user=4)                    user 4 and every portfolio under user 4
        nats_block(user=4, pf=1913)           only portfolio 1913 of user 4   (pf=[1913, 1914] for several)
        nats_block()                          list what is blocked

    It also stops the rest of a sequence already under way, and a squareup. Blocks stay through a restart of the service,
    not of Deephaven."""
    svc = _nats()
    if svc is None:
        return _service_down("nats_block")
    if user is None and pf is None:
        print(f"blocked: {svc.blocks()}")
        return
    svc.block(user=user, pf=pf)
    print(f"blocked: {svc.blocks()}")


def nats_unblock(user=None, pf=None) -> None:
    """Lift a block: nats_unblock(user=4) lifts user 4 and every portfolio blocked under user 4;
    nats_unblock(user=4, pf=1913) lifts only that portfolio."""
    svc = _nats()
    if svc is None:
        return _service_down("nats_unblock")
    svc.unblock(user=user, pf=pf)
    print(f"blocked: {svc.blocks()}")


def registry_start(file: str | None = None) -> None:
    """Start the portfolio registry ONCE: the one place that hands out portfolio numbers, so a user never has two portfolios with the
    same number, whichever strategy or screen creates them. It keeps its numbers in a file, so a restart remembers them, and
    shows them as the table `portfolio_registry`. A Book(user=...) needs it running.
    Safe to call again: if it is already running (with this file, or none named) nothing happens. An algo should call
    registry_connect() instead."""
    plugin = _app().get("registry")
    if plugin is not None:
        if file is None or file == plugin.file:
            print(f"The portfolio registry is already running; nothing changed. {plugin.describe()}. An algo only needs registry_connect().")
            return
        raise RuntimeError("the portfolio registry is already running with another file; registry_stop() first if you mean to change it")
    _run(PortfolioRegistry(file=file))


def registry_connect() -> bool:
    """What an algo calls instead of registry_start: checks that the registry is running and changes nothing."""
    plugin = _app().get("registry")
    if plugin is None:
        print("The portfolio registry is not running. Start it once with registry_start(); do not start it from an algo.")
        return False
    print(f"connected to the portfolio registry: {plugin.describe()}")
    return True


def registry_stop() -> None:
    """Stop the portfolio registry (its numbers stay in the file)."""
    _app().stop("registry")


def _registry():
    plugin = _app().get("registry")
    if plugin is None:
        print("The portfolio registry is not running. Call registry_start() first.")
        return None
    return plugin.state


def pf_create(user: int, strategy: str, name: str = "", start: int = FIRST_PF) -> int | None:
    """Create a portfolio and get its number: pf_create(3, "rsi_stocks", "RELIANCE") -> 1001.
    The number is the next free one for that user, reserved at once. `strategy` is who asks (an algo, or "console"), `name` what it
    trades; `start` is where this strategy's numbers begin. Returns None if the registry is not running."""
    reg = _registry()
    if reg is None:
        return None
    pf = reg.create(user, strategy, name, start=start)
    print(f"user {user}: created portfolio {pf} for {strategy}" + (f", {name}" if name else ""))
    return pf


def pf_register(user: int, pf: int, owner: str = "console", note: str = "", algo: str = "", name: str = "") -> bool:
    """Claim a portfolio number you already use, or record what yours is for: pf_register(3, 1001, algo="rsi_stocks", name="RELIANCE").
    Says who holds it if it is not available."""
    reg = _registry()
    if reg is None:
        return False
    claim = reg.register(user, pf, owner, note, algo=algo, name=name)
    print(claim)
    return claim.ok


def pf_list(user: int | None = None) -> None:
    """List the registered portfolios, one user's (pf_list(3)) or everyone's."""
    reg = _registry()
    if reg is None:
        return
    rows = reg.portfolios(user)
    if not rows:
        print("no portfolios registered" + (f" for user {user}" if user is not None else ""))
    for u, p, owner, name, algo, created, note in rows:
        print(f"user {u}  portfolio {p}  {name or '-'}  algo {algo or '-'}  book {owner}  {created}" + (f"  {note}" if note else ""))


def console_open(strategy: str) -> bool:
    """Make the console window of one strategy: its own summary, net positions, messages and order book, as `console_<strategy>`.
    (algo_start does this by itself while the console is running; this is for a strategy that has portfolios but was not started
    with algo_start, such as "manual".)"""
    c = current().get("console")
    if c is None:
        print("The console is not running. Start it with  app.run(algo.Console())  (scripts/03_console.py).")
        return False
    done = c.open(strategy)
    print(f"console_{strategy}: " + ("published" if done else "nothing to do (already there, or not a plain name)"))
    return done


def algo_status() -> None:
    """What is running."""
    _app().status()


# The functions by service, for algo_help(). Every function in FUNCTIONS must be in exactly one group (a test checks it).
GROUPS = {
    "MASTER (contracts)": ("master_build", "contract", "contracts", "expiries", "strikes", "nearest_strikes"),
    "CANDLES": ("candles_start", "candles_stop", "last_price", "last_candles"),
    "STRADDLE and END OF DAY": ("straddle_start", "straddle_stop", "eod_save", "eod_load", "eod_auto"),
    "PORTFOLIOS (the book)": ("registry_start", "registry_connect", "registry_stop", "pf_create", "pf_register", "pf_list"),
    "NATS (commands)": ("nats_start", "nats_connect", "nats_add_user", "nats_restart", "nats_stop", "nats_send", "pf_subscribe", "pf_pause",
                        "pf_unpause", "pf_squareup", "pf_unsubscribe", "pf_apply", "nats_block", "nats_unblock"),
    "AUTOMATIONS (a script you write)": ("algo_start", "algo_stop", "pf_track", "pf_assign", "pf_activate", "pf_leg", "pf_mark", "pf_cooling", "pf_allowed",
                                         "algo_sending", "watch", "algo_portfolios", "algo_pause", "algo_unpause", "algo_exit", "algo_unsubscribe"),
    "ABSOLUTE / RELATIVE (now vs before, now vs a basket)": ("absolute", "relative", "basket", "absolute_warm", "absolute_settings"),
    "CONSOLE (one window per strategy)": ("console_open",),
    "WHAT IS RUNNING": ("algo_status", "algo_help"),
}


def algo_help(name: str | None = None) -> None:
    """Every function by service, one line each: algo_help().  algo_help("pf_create") shows the whole description of one."""
    by_name = {f.__name__: f for f in FUNCTIONS}
    if name is not None:
        fn = by_name.get(name)
        print(f"{name}: unknown function" if fn is None else f"{name}{inspect.signature(fn)}\n{inspect.getdoc(fn) or ''}")
        return
    for group, names in GROUPS.items():
        print(f"\n{group}")
        for n in names:
            doc = (inspect.getdoc(by_name[n]) or "").splitlines()
            print(f"  {n + '()':<20} {doc[0][:95] if doc else ''}")
    print("\nalgo_help('name') shows the full description of one function.")


FUNCTIONS = (master_build, contract, contracts, expiries, strikes, nearest_strikes, last_price, last_candles,
             candles_start, candles_stop,
             straddle_start, straddle_stop, eod_save, eod_load, eod_auto, nats_start, nats_restart, nats_connect, nats_add_user, nats_stop, nats_send,
             pf_subscribe, pf_pause, pf_unpause, pf_squareup, pf_unsubscribe, pf_apply, nats_block, nats_unblock,
             registry_start, registry_connect, registry_stop, pf_create, pf_register, pf_list, algo_start, algo_stop, pf_track,
             pf_assign, pf_activate, pf_leg, pf_mark, pf_cooling, pf_allowed, algo_sending, watch, algo_portfolios, algo_pause, algo_unpause, algo_exit,
             algo_unsubscribe, console_open, absolute, relative, basket, absolute_warm, absolute_settings, algo_status, algo_help)


def install(ns: dict, market: str = "NSE", paths: Paths | None = None, level: int = logging.INFO) -> App:
    """Connect to the session and put the functions above into `ns`."""
    app = connect(ns, market, paths, level)
    for fn in FUNCTIONS:
        ns[fn.__name__] = fn
    print("algo ready. algo_help() lists every function by service (master, candles, straddle, portfolios, NATS).")
    return app
