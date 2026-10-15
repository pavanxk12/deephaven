"""V / D straddle series per underlying, built from `candles` only: backfill the day, then one point per minute.

`StraddleEngine` holds the algorithm (pure Python, unit tested). `Straddle` is the plugin: it feeds the engine from
the `candles` table and writes `straddle_series`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from datetime import datetime

import numpy as np
import pandas as pd

from algo.core import log
from algo.core.nulls import NULL_LONG
from algo.core.plugin import Plugin

_log = log.get("straddle")


@dataclass(frozen=True)
class StraddleConfig:
    symbols: tuple[str, ...] | None = None      # None = every symbol with a future and options
    expiries: int = 1                           # how many of the nearest option expiries: 1 = nearest only, 2 = nearest + next, ...
    window: int = 5                             # +/- strikes around ATM that are searched
    shift_threshold: int = 3                    # re-centre the window when ATM drifts more than this
    min_search: int = 2                         # search +/- this many strikes around the previous minimum
    max_age_s: float | None = 180               # ignore a price older than this (None = no guard); at least 3 sample periods
    offset_s: int = 2                           # sample this long after each sample boundary


@dataclass(frozen=True)
class SeriesDef:
    fut: int
    strikes: np.ndarray
    ce: tuple[int, ...]
    pe: tuple[int, ...]


def reference_future(fut_expiries: list, option_expiry) -> int:
    """Index (into the future expiries, sorted) of the future that goes with an option expiry: the nearest future that
    expires on or after it, else the last one. Weekly options therefore use the monthly future, and next month's options
    use next month's future."""
    for i, e in enumerate(fut_expiries):
        if e >= option_expiry:
            return i
    return len(fut_expiries) - 1


def build_series(master, cfg: StraddleConfig, today: pd.Timestamp, log=None) -> dict:
    """{(symbol, 'YYYY-MM-DD'): SeriesDef} for the chosen symbols and the `cfg.expiries` nearest option expiries.

    Expiries are counted over everything listed (weeklies included). Each uses the future that matches it.
    """
    say = log.info if log else (lambda *a: None)
    futs = master.futs[master.futs["expiry"] >= today].sort_values("expiry")
    chain = master.chain[master.chain["expiry"] >= today]
    all_syms = sorted(set(futs["TckrSymb"]) & set(chain["TckrSymb"]))
    if cfg.symbols is None:
        use = all_syms
    else:
        use = [s.upper() for s in cfg.symbols if s.upper() in all_syms]
        for s in cfg.symbols:
            if s.upper() not in all_syms:
                say("%s: no future or no options in master, skipped", s)

    fut_by_sym = {s: g for s, g in futs.groupby("TckrSymb")}
    by_sym = {s: g for s, g in chain.groupby("TckrSymb")}
    defs, on_later_future = {}, 0
    for s in use:
        c = by_sym[s]
        f = fut_by_sym[s]
        fut_exps = [pd.Timestamp(e) for e in f["expiry"]]
        fut_tokens = [int(t) for t in f["fut_token"]]
        for exp in sorted(pd.Timestamp(e) for e in c["expiry"].unique())[:cfg.expiries]:
            cc = c[c["expiry"] == exp].sort_values("strike")
            if len(cc) < 2 * cfg.window + 1:
                continue
            k = reference_future(fut_exps, exp)
            if fut_exps[k] != exp:
                on_later_future += 1
            defs[(s, exp.strftime("%Y-%m-%d"))] = SeriesDef(
                fut=fut_tokens[k], strikes=cc["strike"].to_numpy(dtype=float),
                ce=tuple(int(x) for x in cc["ce_token"]), pe=tuple(int(x) for x in cc["pe_token"]))
    if on_later_future:
        say("%d of %d series reference a later (monthly) future than their own expiry", on_later_future, len(defs))
    return defs


class _State:
    __slots__ = ("day", "center", "min", "alt", "cum", "altcum")

    def __init__(self, day):
        self.day = day
        self.center = None       # window centre (strike index)
        self.min = None          # index of the current minimum straddle
        self.alt = None          # reference index of the alternative cost
        self.cum = 0.0
        self.altcum = 0.0


class StraddleEngine:
    """Feed prices with `set_price`, call `sample(when, now_ts)` once per minute; get one output row per series."""

    COLUMNS = ("Time", "Symbol", "Expiry", "Fut", "ATMStrike", "ATMStraddle", "MinStrike", "V",
               "Cost", "CumCost", "D", "AltCost", "AltCumCost", "AltD", "Synthetic")

    def __init__(self, defs: dict, cfg: StraddleConfig):
        self.defs, self.cfg = defs, cfg
        self.px: dict[int, tuple[float, float]] = {}        # token -> (price, timestamp)
        self._state: dict[tuple, _State] = {}
        self.needed = {t for d in defs.values() for t in (d.fut, *d.ce, *d.pe)}

    def set_price(self, token: int, price: float, ts: float) -> None:
        self.px[token] = (price, ts)

    def sample(self, when: datetime, now_ts: float) -> list[tuple]:
        """Run every series once for time `when`. Returns the output rows (series that could not be computed are skipped)."""
        day, stamp = when.date(), when.strftime("%H:%M:%S")
        rows = []
        for key, d in self.defs.items():
            st = self._state.get(key)
            if st is None or st.day != day:
                st = self._state[key] = _State(day)
            try:
                row = self._sample_one(key, d, st, stamp, now_ts)
            except Exception as e:                       # one broken series must not stop the others
                _log.warning("%s failed: %r", key, e)
                continue
            if row is not None:
                rows.append(row)
        return rows

    def _price(self, token: int, now_ts: float):
        v = self.px.get(token)
        if v is None:
            return None
        if self.cfg.max_age_s is not None and now_ts - v[1] > self.cfg.max_age_s:
            return None
        return v[0]

    def _sample_one(self, key, d: SeriesDef, st: _State, stamp: str, now_ts: float):
        cfg = self.cfg
        get = lambda tok: self._price(tok, now_ts)       # noqa: E731
        fut = get(d.fut)
        if not fut:
            return None
        strikes, n = d.strikes, len(d.strikes)

        def strad(i: int) -> float:
            c, p = get(d.ce[i]), get(d.pe[i])
            return (c + p) if (c and p) else 0.0

        atm = int(np.abs(strikes - fut).argmin())
        if st.center is None:
            st.center = atm
            st.min = atm
        drift = atm - st.center
        if abs(drift) > cfg.shift_threshold and atm - cfg.window >= 0 and atm + cfg.window < n:
            st.center = atm
        lo, hi = st.center - cfg.window, st.center + cfg.window
        if lo < 0 or hi >= n:
            return None

        prev = st.min
        cand = (range(lo, hi + 1) if prev is None
                else range(max(lo, prev - cfg.min_search), min(hi, prev + cfg.min_search) + 1))
        best_v, best_i = 0.0, None
        for i in cand:
            v = strad(i)
            if v <= 0:
                continue
            if best_v == 0 or v < best_v:
                best_v, best_i = v, i
        st.min = best_i
        if best_i is None:
            return None

        cost = 0.0
        if prev is not None:
            pv = strad(prev)
            if pv > 0:
                cost = abs(best_v - pv)
        st.cum += cost

        alt_cost = 0.0
        if st.alt is None:
            st.alt = best_i
        elif abs(best_i - st.alt) > 1:
            rv = strad(st.alt)
            if rv > 0:
                alt_cost = abs(best_v - rv)
            st.alt = best_i
        st.altcum += alt_cost

        synth = get(d.ce[best_i]) - get(d.pe[best_i]) + float(strikes[best_i])
        return (stamp, key[0], key[1], float(fut),
                float(strikes[atm]), float(strad(atm)), float(strikes[best_i]), float(best_v),
                float(cost), float(st.cum), float(best_v + st.cum),
                float(alt_cost), float(st.altcum), float(best_v + st.altcum), float(synth))


class Straddle(Plugin):
    name = "straddle"
    requires = ("master", "candles")

    def __init__(self, cfg: StraddleConfig = StraddleConfig()):
        super().__init__()
        self.cfg = cfg
        self.engine: StraddleEngine | None = None
        self.rows = 0

    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht

        app, mk, cfg = self.app, self.app.market, self.cfg
        today = pd.Timestamp(datetime.now(mk.zone).date())
        defs = build_series(app.master, cfg, today, self.log)
        self.engine = StraddleEngine(defs, cfg)
        self.log.info("%d series, %d tokens needed in candles", len(defs), len(self.engine.needed))

        self.bucket = app.shared.get("candle_seconds", 60)
        self.cadence = max(self.bucket, 60)                    # one point per minute, or per candle if candles are larger
        if cfg.max_age_s is not None:
            self.engine.cfg = replace(cfg, max_age_s=max(cfg.max_age_s, 3 * self.cadence))
        self.follow("candles", self._restart)
        s = dht.string
        d = dht.double
        self._writer = DynamicTableWriter(dict(zip(StraddleEngine.COLUMNS, (s, s, s, d, d, d, d, d, d, d, d, d, d, d, d))))
        app.publish("straddle_series", self._writer.table)

        self._scale = mk.price_div
        self.listen(app.table("candles"), self._on_candles, replay=False)        # live prices first, as before the backfill
        self._backfill(app.table("candles"))
        self.spawn(self._loop)
        self.log.info("backfilled %d rows, now live: `straddle_series` (one point per %ds)", self.rows, self.cadence)

    def _restart(self, _table) -> None:
        """candles was restarted: build the series again on the new table."""
        self.app.run(Straddle(self.cfg))

    def describe(self) -> str:
        return f"{len(self.engine.defs)} series, {self.rows} rows" if self.engine else ""

    # ---- inputs ------------------------------------------------------------------------------
    def _on_candles(self, update, is_replay) -> None:
        ch = update.added(["token", "Close"])
        if ch is None:
            return
        now_ts, needed = time.time(), self.engine.needed
        for tok, close in zip(ch["token"], ch["Close"]):
            tok = int(tok)
            if tok in needed and close != NULL_LONG and close > 0:
                self.engine.set_price(tok, float(close) / self._scale, now_ts)

    def _backfill(self, candles) -> None:
        from deephaven.pandas import to_pandas

        mk, eng = self.app.market, self.engine
        df = to_pandas(candles.view(["Ts = epochSeconds(Minute)", "token", "Close"]))
        df = df[df["token"].isin(eng.needed) & (df["Close"] != NULL_LONG) & (df["Close"] > 0)].sort_values("Ts")
        start, end = mk.session
        for ts, g in df.groupby("Ts"):
            close = float(ts) + self.bucket                                         # candle close time
            for t, c in zip(g["token"], g["Close"]):
                eng.set_price(int(t), float(c) / self._scale, close)
            if close % self.cadence:                                                 # not a sample boundary
                continue
            t_s = close + self.cfg.offset_s                                          # simulated "now"
            when = datetime.fromtimestamp(t_s, mk.zone)
            if start <= when.time() <= end:
                self._write(eng.sample(when, t_s))

    # ---- live ---------------------------------------------------------------------------------
    def _loop(self) -> None:
        mk, cad = self.app.market, self.cadence
        while not self.halted.is_set():
            now_ts = time.time()
            nxt = (now_ts // cad + 1) * cad + self.cfg.offset_s              # next boundary + offset
            if self.halted.wait(max(0.5, nxt - now_ts)):
                return
            now = datetime.now(mk.zone)
            start, end = mk.session
            if start <= now.time() <= end:
                self._write(self.engine.sample(now, time.time()))

    def _write(self, rows: list[tuple]) -> None:
        for r in rows:
            self._writer.write_row(*r)
        self.rows += len(rows)
