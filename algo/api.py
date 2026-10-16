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
from dataclasses import replace
from datetime import datetime, timedelta

from algo.comms.nats_service import NatsConfig, NatsService
from algo.core.app import App, MissingDependency, connect, current
from algo.core.config import Paths, market_for
from algo.data.candles import Candles
from algo.data.indicators import LIBRARY, Indicators
from algo.data.master import Master
from algo.data.store import DEFAULT_TABLES, PREFIX, Eod, make_store, normalise_day, save_tables
from algo.series.straddle import Straddle, StraddleConfig
from algo.strategy.registry import PortfolioRegistry

HINTS = {"master": "master_build()", "candles": "candles_start()", "indicators": "indicators(EMA(9), RSI(14))",
         "nats": "nats_start()", "registry": "registry_start()"}


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
    Calling it again restarts the builder (the candles table starts empty); indicators, the straddle and the strategy
    follow the new table by themselves. `build_from`: epoch second of the first bar; default = the next full bar."""
    _run(Candles(timeframe, multiplier=multiplier, symbols=symbols, kind=kind, expiries=expiries, build_from=build_from))


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
    candles = _app().get("candles")
    if candles is not None and getattr(candles, "expiries", None) is not None and candles.expiries < n + 1:
        print(f"warning: candles were started with expiries={candles.expiries} but straddle_start({n}) needs the {n + 1} nearest "
              f"expiries; the missing ones will be empty. Restart candles with expiries={n + 1} or more.")
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


def nats_start(ip: str | None = None, port: int | None = None, users=None, *, send: bool = False,
               dry_run: bool | None = None, **config) -> None:
    """Connect the one NATS service that everything sends portfolio commands through (strategies, the console, you).

        nats_start("192.168.1.130", 4222, users=[3, 4])  the server's address and port, and the users that may send
        nats_start()                                     again: reconnect with the SAME ip, port and settings as before
        nats_start(port=4223)                            again, changing only the port
        nats_start(dry_run=True)                         no connection, commands are only logged

    One service carries any number of users and portfolios. Every command says which user it is for (pf_pause(1913, user=4));
    with a single user it may be left out. A command for a user not listed here is refused (user_not_allowed).
    The ip, port, users and other settings are remembered while the service runs (nats_start() with nothing reuses them);
    nats_stop() forgets them. Naming users here UNBLOCKS them and every portfolio under them (you are allowing them again);
    nats_start() with no users keeps every block. Sending always starts OFF; nats_send(True) turns it on (or nats_start(send=True)).
    Other settings: timeout_s, max_in_flight, lane_depth, sender (see NatsConfig)."""
    previous = _app().get("nats")
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
    cfg = replace(base, send_on=send, **given)
    print(f"NATS {cfg.ip}:{cfg.port}, users {', '.join(map(str, cfg.users))}"
          + (" (same as the running connection)" if previous is not None and not given else ""))
    _run(NatsService(cfg, lift_blocks=given.get("users", ()) if users is not None else ()))


def nats_stop() -> None:
    _app().stop("nats")


def _nats() -> "NatsService | None":
    return _app().get("nats")


def _service_down(what: str) -> None:
    print(f"{what}: NOT sent - the NATS service is not running. Call nats_start() first.")


def nats_send(on: bool = True) -> None:
    """Turn sending on or off for everyone. Off: strategies still compute, nothing leaves the process."""
    svc = _nats()
    if svc is None:
        return _service_down("nats_send")
    bridge = _app().get("bridge")
    if bridge is not None:                                # the strategy bridge also re-checks tokens already in a signal zone
        (bridge.send_on if on else bridge.send_off)()
    else:
        (svc.send_on if on else svc.send_off)()


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
    _answer(lambda s: s.pause(pf, user=user), f"PF {pf} pause", (pf, True))


def pf_unpause(pf: int, user: int | None = None) -> None:
    _answer(lambda s: s.unpause(pf, user=user), f"PF {pf} unpause", (pf, False))


def pf_squareup(pf: int, user: int | None = None) -> None:
    _answer(lambda s: s.squareup(pf, user=user), f"PF {pf} squareup")


def pf_unsubscribe(pf: int, option: int, user: int | None = None, **params) -> None:
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
    """Start the portfolio registry: the one place that hands out portfolio numbers, so a user never has two portfolios with the
    same number, whichever strategy or screen creates them. It keeps its numbers in a file, so a restart remembers them, and
    shows them as the table `portfolio_registry`. A Book(user=...) needs it running."""
    _run(PortfolioRegistry(file=file))


def registry_stop() -> None:
    _app().stop("registry")


def _registry():
    plugin = _app().get("registry")
    if plugin is None:
        print("The portfolio registry is not running. Call registry_start() first.")
        return None
    return plugin.state


def pf_create(user: int, strategy: str, name: str = "", start: int = 1111) -> int | None:
    """Create a portfolio and get its number: pf_create(3, "rsi_stocks", "RELIANCE") -> 1111.
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


def algo_status() -> None:
    """What is running."""
    _app().status()


FUNCTIONS = (master_build, candles_start, candles_stop, indicators, indicators_remove,
             straddle_start, straddle_stop, eod_save, eod_load, eod_auto, nats_start, nats_stop, nats_send,
             pf_subscribe, pf_pause, pf_unpause, pf_squareup, pf_unsubscribe, pf_apply, nats_block, nats_unblock,
             registry_start, registry_stop, pf_create, pf_register, pf_list, algo_status)


def install(ns: dict, market: str = "NSE", paths: Paths | None = None, level: int = logging.INFO) -> App:
    """Connect to the session and put the functions above (and EMA, RSI, ...) into `ns`."""
    app = connect(ns, market, paths, level)
    for fn in FUNCTIONS:
        ns[fn.__name__] = fn
    ns.update(LIBRARY)
    print("algo ready - call:", ", ".join(f"{f.__name__}()" for f in FUNCTIONS))
    print("indicator functions:", ", ".join(LIBRARY))
    return app
