"""V / D straddle series per underlying, built from `candles` only: backfill the day, then one point per minute.

`StraddleEngine` holds the algorithm (pure Python, unit tested). `Straddle` is the plugin: it feeds the engine from
the `candles` table and writes `straddle_series`.
"""
from __future__ import annotations

import itertools
import math
import time
from dataclasses import dataclass, replace
from datetime import date, datetime
from datetime import time as dtime

import numpy as np
import pandas as pd

from algo.core import log
from algo.core.nulls import good_price
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
    risk_free_rate: float = 0.07                # R in the VIX-style IV below (annualised, continuously compounded)
    vix_n_forward: int = 4                      # average the synthetic forward over this many least-|C-P| strikes (3-5)
    vix_max_spread_pct: float = 0.30            # reject a quote whose (ask-bid)/mid exceeds this
    vix_min_knots: int = 3                      # need at least this many good quotes to spline-repair a rejected one


@dataclass(frozen=True)
class SeriesDef:
    ref: int            # token whose price decides the at-the-money strike: the future, or the spot when there is no future
    strikes: np.ndarray
    ce: tuple[int, ...]
    pe: tuple[int, ...]
    ref_kind: str = "FUT"


def implied_vol(v: float, synthetic: float, dte: int) -> float:
    """The IV column: v / (synthetic * sqrt(max(dte, 1))) * 100, with `dte` the calendar days left to the expiry (at least 1, so the expiry day
    itself counts as one day). NaN when the synthetic price is not positive."""
    if not synthetic > 0:
        return float("nan")
    return float(v / (synthetic * math.sqrt(max(dte, 1))) * 100.0)


# ---- CBOE-style VIX synthetic-forward IV, computed over the whole option chain --------------------------------
# F from matched call/put mid-prices nearest the money, K0 the strike at/below F, a strike-weighted variance over
# OTM puts/calls either side of K0, with the chain's quotes cleaned up before any of that runs:
#  - a quote whose (ask-bid)/mid spread is too wide, or missing, is replaced by a natural cubic spline through the
#    surviving quotes (needs >= `vix_min_knots`), or dropped if it falls outside their strike range.
# Pure numpy / stdlib math: no scipy dependency added for the spline.

def time_to_expiry(now: datetime, expiry: date, settle: dtime = dtime(15, 30)) -> float:
    """Years to expiry, minute-precision: (now -> `settle` on `expiry`) / 525,600. This is the same quantity as the
    spec's (M_today + M_between + M_expiry_day) decomposition -- one timestamp difference covers all three terms,
    same-day expiry included."""
    close = datetime.combine(expiry, settle, tzinfo=now.tzinfo)
    minutes = (close - now).total_seconds() / 60.0
    return max(minutes, 1.0) / 525_600.0


def _natural_cubic_spline(x: np.ndarray, y: np.ndarray):
    """Natural cubic spline through sorted, distinct knots (x, y). Returns evaluate(query) -> values."""
    n = len(x) - 1
    h = np.diff(x)
    alpha = np.zeros(n + 1)
    alpha[1:n] = 3 * ((y[2:n + 1] - y[1:n]) / h[1:n] - (y[1:n] - y[:n - 1]) / h[:n - 1])
    l = np.ones(n + 1)
    mu = np.zeros(n + 1)
    z = np.zeros(n + 1)
    for i in range(1, n):
        l[i] = 2 * (x[i + 1] - x[i - 1]) - h[i - 1] * mu[i - 1]
        mu[i] = h[i] / l[i]
        z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i]
    c = np.zeros(n + 1)
    b = np.zeros(n)
    d = np.zeros(n)
    for j in range(n - 1, -1, -1):
        c[j] = z[j] - mu[j] * c[j + 1]
        b[j] = (y[j + 1] - y[j]) / h[j] - h[j] * (c[j + 1] + 2 * c[j]) / 3
        d[j] = (c[j + 1] - c[j]) / (3 * h[j])
    a = y[:n]

    def evaluate(xq: np.ndarray) -> np.ndarray:
        xq = np.asarray(xq, dtype=np.float64)
        idx = np.clip(np.searchsorted(x, xq, side="right") - 1, 0, n - 1)
        dx = xq - x[idx]
        return a[idx] + b[idx] * dx + c[idx] * dx ** 2 + d[idx] * dx ** 3

    return evaluate


def repair_quotes(strikes: np.ndarray, bid: np.ndarray, ask: np.ndarray, mid: np.ndarray,
                   max_spread_pct: float, min_knots: int) -> tuple[np.ndarray, np.ndarray]:
    """Reject a quote that is missing or whose (ask-bid)/mid exceeds `max_spread_pct`; replace a rejected one by
    spline over the surviving ("good") knots when there are at least `min_knots`, else drop it (also dropped if
    its strike falls outside the good knots' range, where the spline doesn't apply). Returns (repaired mid, keep)."""
    good = (np.isfinite(mid) & (mid > 0) & np.isfinite(bid) & np.isfinite(ask)
            & ((ask - bid) / np.maximum(mid, 1e-9) <= max_spread_pct))
    out = mid.copy()
    if good.sum() < min_knots:
        return out, good
    order = np.argsort(strikes[good])
    spline = _natural_cubic_spline(strikes[good][order], mid[good][order])
    lo, hi = strikes[good].min(), strikes[good].max()
    repaired = ~good & (strikes >= lo) & (strikes <= hi)
    out[repaired] = spline(strikes[repaired])
    return out, good | repaired


def synthetic_forward(strikes: np.ndarray, call_mid: np.ndarray, put_mid: np.ndarray, r: float, T: float, n: int) -> float:
    """F, averaged over the `n` strikes with the smallest |C-P| gap: mean(Kj + e^(rT) * (C(Kj) - P(Kj)))."""
    d = call_mid - put_mid
    idx = np.argsort(np.abs(d))[:max(1, min(n, len(d)))]
    return float(np.mean(strikes[idx] + math.exp(r * T) * d[idx]))


def atm_strike(strikes: np.ndarray, forward: float) -> float:
    """K0: the largest strike at or below the synthetic forward."""
    below = strikes[strikes <= forward]
    return float(below.max()) if below.size else float(strikes.min())


def strike_weights(strikes: np.ndarray) -> np.ndarray:
    """DeltaKi: half the gap to each neighbour, the gap itself at the two ends."""
    n = len(strikes)
    w = np.empty(n)
    if n == 1:
        return np.zeros(1)
    w[0] = strikes[1] - strikes[0]
    w[-1] = strikes[-1] - strikes[-2]
    w[1:-1] = (strikes[2:] - strikes[:-2]) / 2.0
    return w


def vix_iv(strikes: np.ndarray, q: np.ndarray, weights: np.ndarray, r: float, T: float, forward: float, k0: float) -> float:
    """100 * sqrt(sigma^2), the CBOE VIX formula: sigma^2 = (2/T) sum(DeltaKi/Ki^2 * e^(rT) * Q(Ki)) - (1/T)(F/K0 - 1)^2."""
    term = np.sum((weights / strikes ** 2) * math.exp(r * T) * q)
    sigma2 = (2.0 / T) * term - (1.0 / T) * (forward / k0 - 1.0) ** 2
    return float(100.0 * math.sqrt(max(sigma2, 0.0)))


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

    Expiries are counted over everything listed (weeklies included). The at-the-money reference is the future that matches
    each expiry; **a symbol with no (unexpired) future falls back to its spot**; a symbol with neither is skipped.
    """
    say = log.info if log else (lambda *a: None)
    futs = master.futs[master.futs["expiry"] >= today].sort_values("expiry")
    chain = master.chain[master.chain["expiry"] >= today]
    spot = master.spot_of
    with_future = set(futs["TckrSymb"])
    optioned = set(chain["TckrSymb"])
    all_syms = sorted(optioned & (with_future | set(spot)))
    unusable = sorted(optioned - set(all_syms))
    if unusable:
        say("no future and no spot for %d symbol(s), skipped: %s", len(unusable), ", ".join(unusable[:12]))
    if cfg.symbols is None:
        use = all_syms
    else:
        use = [s.upper() for s in cfg.symbols if s.upper() in all_syms]
        for s in cfg.symbols:
            if s.upper() not in all_syms:
                say("%s: no options, or no future and no spot, in master: skipped", s)

    fut_by_sym = {s: g for s, g in futs.groupby("TckrSymb")}
    by_sym = {s: g for s, g in chain.groupby("TckrSymb")}
    defs, on_later_future, on_spot = {}, 0, 0
    for s in use:
        c = by_sym[s]
        f = fut_by_sym.get(s)
        fut_exps = [pd.Timestamp(e) for e in f["expiry"]] if f is not None else []
        fut_tokens = [int(t) for t in f["fut_token"]] if f is not None else []
        for exp in sorted(pd.Timestamp(e) for e in c["expiry"].unique())[:cfg.expiries]:
            cc = c[c["expiry"] == exp].sort_values("strike")
            if len(cc) < 2 * cfg.window + 1:
                continue
            if fut_exps:
                k = reference_future(fut_exps, exp)
                ref, kind = fut_tokens[k], "FUT"
                if fut_exps[k] != exp:
                    on_later_future += 1
            else:
                ref, kind = spot[s], "SPOT"
                on_spot += 1
            defs[(s, exp.strftime("%Y-%m-%d"))] = SeriesDef(
                ref=ref, ref_kind=kind, strikes=cc["strike"].to_numpy(dtype=float),
                ce=tuple(int(x) for x in cc["ce_token"]), pe=tuple(int(x) for x in cc["pe_token"]))
    if on_spot:
        say("%d of %d series use the spot as reference (no future)", on_spot, len(defs))
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
               "Cost", "CumCost", "D", "AltCost", "AltCumCost", "AltD", "Synthetic", "IV")

    def __init__(self, defs: dict, cfg: StraddleConfig):
        self.defs, self.cfg = defs, cfg
        self.px: dict[int, tuple[float, float]] = {}        # token -> (price, timestamp)
        self.bidask: dict[int, tuple[float, float, float]] = {}   # token -> (bid, ask, timestamp), for the VIX IV
        self._state: dict[tuple, _State] = {}
        self.needed = {t for d in defs.values() for t in (d.ref, *d.ce, *d.pe)}
        self._mask = np.zeros(max(self.needed, default=0) + 1, dtype=bool)         # dense lookup: is this token a leg of some series?
        self._mask[list(self.needed)] = True

    def set_price(self, token: int, price: float, ts: float) -> None:
        """Remember a price; a missing, NaN, infinite or non-positive price is ignored, so it can never be traded on."""
        if good_price(price):
            self.px[token] = (float(price), ts)

    def set_prices(self, tokens, prices, ts: float) -> None:
        """Many prices at once (a whole candle batch). Same rules as `set_price`, applied to the arrays in one pass:
        tokens that are not legs of any series, and null / NaN / infinite / non-positive prices, are dropped."""
        tokens = np.asarray(tokens, dtype=np.int64)
        prices = np.asarray(prices, dtype=np.float64)
        ok = np.isfinite(prices) & (prices > 0) & (tokens >= 0) & (tokens < len(self._mask))
        ok[ok] &= self._mask[tokens[ok]]
        self.px.update(zip(tokens[ok].tolist(), zip(prices[ok].tolist(), itertools.repeat(ts))))

    def set_bidasks(self, tokens, bids, asks, ts: float) -> None:
        """Best bid/ask for many tokens at once, same filtering rules as `set_prices` (plus ask >= bid)."""
        tokens = np.asarray(tokens, dtype=np.int64)
        bids = np.asarray(bids, dtype=np.float64)
        asks = np.asarray(asks, dtype=np.float64)
        ok = (np.isfinite(bids) & (bids > 0) & np.isfinite(asks) & (asks >= bids)
              & (tokens >= 0) & (tokens < len(self._mask)))
        ok[ok] &= self._mask[tokens[ok]]
        self.bidask.update(zip(tokens[ok].tolist(), zip(bids[ok].tolist(), asks[ok].tolist(), itertools.repeat(ts))))

    def sample(self, when: datetime, now_ts: float) -> list[tuple]:
        """Run every series once for time `when`. Returns the output rows (series that could not be computed are skipped)."""
        day, stamp = when.date(), when.strftime("%H:%M:%S")
        rows = []
        for key, d in self.defs.items():
            st = self._state.get(key)
            if st is None or st.day != day:
                st = self._state[key] = _State(day)
            try:
                row = self._sample_one(key, d, st, stamp, now_ts, when)
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

    def _ba(self, token: int, now_ts: float) -> tuple[float, float]:
        v = self.bidask.get(token)
        if v is None or (self.cfg.max_age_s is not None and now_ts - v[2] > self.cfg.max_age_s):
            return float("nan"), float("nan")
        return v[0], v[1]

    def _vix_iv(self, d: SeriesDef, expiry: date, when: datetime, now_ts: float) -> float:
        """The whole-chain, CBOE-style VIX IV for one series: see the functions above `reference_future`."""
        cfg = self.cfg
        strikes = d.strikes
        ba = [self._ba(t, now_ts) for t in d.ce]
        cbid, cask = (np.array(x) for x in zip(*ba)) if ba else (np.array([]), np.array([]))
        ba = [self._ba(t, now_ts) for t in d.pe]
        pbid, pask = (np.array(x) for x in zip(*ba)) if ba else (np.array([]), np.array([]))
        cmid, pmid = (cbid + cask) / 2.0, (pbid + pask) / 2.0

        fwd_ok = np.isfinite(cmid) & np.isfinite(pmid)
        if not fwd_ok.any():
            return float("nan")
        T = time_to_expiry(when, expiry)
        r = cfg.risk_free_rate
        F = synthetic_forward(strikes[fwd_ok], cmid[fwd_ok], pmid[fwd_ok], r, T, cfg.vix_n_forward)
        k0 = atm_strike(strikes, F)
        i0 = int(np.argmin(np.abs(strikes - k0)))

        q = np.where(strikes < k0, pmid, cmid)
        qbid = np.where(strikes < k0, pbid, cbid)
        qask = np.where(strikes < k0, pask, cask)
        q[i0], qbid[i0], qask[i0] = (cmid[i0] + pmid[i0]) / 2.0, (cbid[i0] + pbid[i0]) / 2.0, (cask[i0] + pask[i0]) / 2.0

        q, keep = repair_quotes(strikes, qbid, qask, q, cfg.vix_max_spread_pct, cfg.vix_min_knots)
        if keep.sum() < 2:
            return float("nan")
        order = np.argsort(strikes[keep])
        ks, qs = strikes[keep][order], q[keep][order]
        return vix_iv(ks, qs, strike_weights(ks), r, T, F, k0)

    def _sample_one(self, key, d: SeriesDef, st: _State, stamp: str, now_ts: float, when: datetime):
        cfg = self.cfg
        get = lambda tok: self._price(tok, now_ts)       # noqa: E731
        fut = get(d.ref)                                   # the future, or the spot when the symbol has no future
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
        iv = self._vix_iv(d, date.fromisoformat(key[1]), when, now_ts)
        return (stamp, key[0], key[1], float(fut),
                float(strikes[atm]), float(strad(atm)), float(strikes[best_i]), float(best_v),
                float(cost), float(st.cum), float(best_v + st.cum),
                float(alt_cost), float(st.altcum), float(best_v + st.altcum), float(synth), iv)


def replay(engine: "StraddleEngine", ts, token, close, bucket: int, cadence: int, offset_s: int, session, zone, write) -> int:
    """Rebuild today's series from the candles so far: for each candle time, in order, load that bar's closes, and at every
    sample boundary inside the session take a sample. Arrays in (bar start epoch seconds, token, close); returns rows written.

    The same logic as stepping through the candles one bar at a time, done on arrays: the candles are normally already in time
    order (checked, sorted only if not), and each bar is a slice, so there is no per-row Python except inside `dict.update`."""
    ts = np.asarray(ts, dtype=np.int64)
    token = np.asarray(token, dtype=np.int64)
    close = np.asarray(close, dtype=np.float64)
    if len(ts) == 0:
        return 0
    if not np.all(ts[1:] >= ts[:-1]):
        order = np.argsort(ts, kind="stable")
        ts, token, close = ts[order], token[order], close[order]
    cuts = np.flatnonzero(ts[1:] != ts[:-1]) + 1
    first = [0, *cuts.tolist()]
    last = [*cuts.tolist(), len(ts)]
    start, end = session
    rows = 0
    for a, b in zip(first, last):
        close_t = float(ts[a]) + bucket                                             # the candle's close time
        engine.set_prices(token[a:b], close[a:b], close_t)
        if close_t % cadence:                                                       # not a sample boundary
            continue
        t_s = close_t + offset_s                                                    # simulated "now"
        when = datetime.fromtimestamp(t_s, zone)
        if start <= when.time() <= end:
            out = engine.sample(when, t_s)
            write(out)
            rows += len(out)
    return rows


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
        self._writer = DynamicTableWriter(dict(zip(StraddleEngine.COLUMNS, (s, s, s, d, d, d, d, d, d, d, d, d, d, d, d, d))))
        app.publish("straddle_series", self._writer.table)

        opt_tokens = {t for d in defs.values() for t in (*d.ce, *d.pe)}
        divisor = getattr(app.master, "divisor_of", {})
        self._scale_of = {t: 1.0 / divisor.get(t, 1.0) for t in opt_tokens}    # feed price x scale -> rupees, for the VIX IV's bid/ask
        self.listen(app.table(mk.feed), self._on_feed, replay=False)

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
        self.engine.set_prices(ch["token"], ch["Close"], time.time())           # drops other tokens and unusable prices

    def _on_feed(self, update, is_replay) -> None:
        """Best bid/ask of the option legs, straight from the live feed (`candles` carries no depth): only for the
        VIX IV, scaled to rupees per contract the same way `candles.py` scales `ltp` (1/Divisor, per token)."""
        now = time.time()
        for ch in (update.added(["token", "b0p", "a0p"]), update.modified(["token", "b0p", "a0p"])):
            if ch is None:
                continue
            tok = np.asarray(ch["token"], dtype=np.int64)
            scale = np.fromiter((self._scale_of.get(t, 1.0) for t in tok.tolist()), dtype=np.float64, count=len(tok))
            bid = np.asarray(ch["b0p"], dtype=np.float64) * scale
            ask = np.asarray(ch["a0p"], dtype=np.float64) * scale
            self.engine.set_bidasks(tok, bid, ask, now)

    def _backfill(self, candles) -> None:
        """Replay the candles so far. The engine does the filtering (only the legs of our series), so only those rows are
        pulled into Python, as three arrays."""
        import pandas as pd
        from deephaven.pandas import to_pandas, to_table

        mk, eng = self.app.market, self.engine
        legs = to_table(pd.DataFrame({"token": pd.Series(sorted(eng.needed), dtype="int64")}))
        df = to_pandas(candles.where_in(legs, "token").view(["Ts = epochSeconds(Minute)", "token", "Close"]))
        ts, tok, close = (df["Ts"].to_numpy(np.int64), df["token"].to_numpy(np.int64),
                          df["Close"].to_numpy(np.float64, na_value=np.nan))
        del df
        replay(eng, ts, tok, close, self.bucket, self.cadence, self.cfg.offset_s, mk.session, mk.zone, self._write)   # _write counts the rows

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
                try:
                    self._write(self.engine.sample(now, time.time()))
                except Exception:                          # a bad sample must not end the loop
                    self.log.exception("sample failed")

    def _write(self, rows: list[tuple]) -> None:
        for r in rows:
            self._writer.write_row(*r)
        self.rows += len(rows)
