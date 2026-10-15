"""V / D straddle series per underlying, built from `candles` only: backfill the day, then one point per minute.

`StraddleEngine` holds the algorithm (pure Python, unit tested). `Straddle` is the plugin: it feeds the engine from
the `candles` table and writes `straddle_series`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime

import numpy as np
import pandas as pd

from algo.core import log
from algo.core.plugin import Plugin

_log = log.get("straddle")


@dataclass(frozen=True)
class StraddleConfig:
    symbols: tuple[str, ...] | None = None      # None = every symbol with a future and options
    expiry_index: int = 0                       # 0 = nearest option expiry, 1 = next, ...
    expiry_override: tuple[tuple[str, str], ...] = ()     # (("NIFTY", "2026-10-13"), ...)
    window: int = 5                             # +/- strikes around ATM that are searched
    shift_threshold: int = 3                    # re-centre the window when ATM drifts more than this
    min_search: int = 2                         # search +/- this many strikes around the previous minimum
    max_age_s: float | None = 180               # ignore a price older than this (None = no guard)
    offset_s: int = 2                           # sample this long after each minute boundary


@dataclass(frozen=True)
class SeriesDef:
    fut: int
    strikes: np.ndarray
    ce: tuple[int, ...]
    pe: tuple[int, ...]


def build_series(master, cfg: StraddleConfig, today: pd.Timestamp, log=None) -> dict:
    """{(symbol, 'YYYY-MM-DD'): SeriesDef} for the chosen symbols and expiry."""
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

    near_fut = futs.groupby("TckrSymb")["fut_token"].first()
    by_sym = {s: g for s, g in chain.groupby("TckrSymb")}
    override = dict(cfg.expiry_override)
    defs = {}
    for s in use:
        c = by_sym[s]
        exps = sorted(pd.Timestamp(e) for e in c["expiry"].unique())
        if s in override:
            exp = pd.Timestamp(override[s])
            if exp not in exps:
                say("%s: expiry %s not in file, skipped", s, exp.date())
                continue
        else:
            exp = exps[min(cfg.expiry_index, len(exps) - 1)]
        cc = c[c["expiry"] == exp].sort_values("strike")
        if len(cc) < 2 * cfg.window + 1:
            continue
        defs[(s, exp.strftime("%Y-%m-%d"))] = SeriesDef(
            fut=int(near_fut[s]), strikes=cc["strike"].to_numpy(dtype=float),
            ce=tuple(int(x) for x in cc["ce_token"]), pe=tuple(int(x) for x in cc["pe_token"]))
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

        s = dht.string
        d = dht.double
        self._writer = DynamicTableWriter(dict(zip(StraddleEngine.COLUMNS, (s, s, s, d, d, d, d, d, d, d, d, d, d, d, d))))
        app.publish("straddle_series", self._writer.table)

        self._scale = mk.price_div
        self._backfill(app.table("candles"))
        self.listen(app.table("candles"), self._on_candles, replay=False)
        self.spawn(self._loop)
        self.log.info("backfilled %d rows, now live: `straddle_series`", self.rows)

    def describe(self) -> str:
        return f"{len(self.engine.defs)} series, {self.rows} rows" if self.engine else ""

    # ---- inputs ------------------------------------------------------------------------------
    def _on_candles(self, update, is_replay) -> None:
        from deephaven.constants import NULL_LONG
        ch = update.added(["token", "Close"])
        if ch is None:
            return
        now_ts, needed = time.time(), self.engine.needed
        for tok, close in zip(ch["token"], ch["Close"]):
            tok = int(tok)
            if tok in needed and close != NULL_LONG and close > 0:
                self.engine.set_price(tok, float(close) / self._scale, now_ts)

    def _backfill(self, candles) -> None:
        from deephaven.constants import NULL_LONG
        from deephaven.pandas import to_pandas

        mk, eng = self.app.market, self.engine
        df = to_pandas(candles.view(["Ts = epochSeconds(Minute)", "token", "Close"]))
        df = df[df["token"].isin(eng.needed) & (df["Close"] != NULL_LONG) & (df["Close"] > 0)].sort_values("Ts")
        start, end = mk.session
        for ts, g in df.groupby("Ts"):
            for t, c in zip(g["token"], g["Close"]):
                eng.set_price(int(t), float(c) / self._scale, float(ts) + 60)     # candle close time
            t_s = float(ts) + 60 + self.cfg.offset_s                                 # simulated "now"
            when = datetime.fromtimestamp(t_s, mk.zone)
            if start <= when.time() <= end:
                self._write(eng.sample(when, t_s))

    # ---- live ---------------------------------------------------------------------------------
    def _loop(self) -> None:
        mk = self.app.market
        while not self.halted.is_set():
            now = datetime.now(mk.zone)
            nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=self.cfg.offset_s)
            if self.halted.wait(max(0.5, (nxt - now).total_seconds())):
                return
            now = datetime.now(mk.zone)
            start, end = mk.session
            if start <= now.time() <= end:
                self._write(self.engine.sample(now, time.time()))

    def _write(self, rows: list[tuple]) -> None:
        for r in rows:
            self._writer.write_row(*r)
        self.rows += len(rows)
