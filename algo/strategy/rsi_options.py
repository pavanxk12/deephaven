"""RSI signal on an underlying future -> BUY the nearest option, always the BUY side.

    RSI above rsi_high (overbought) -> BUY the PUT      RSI below rsi_low (oversold) -> BUY the CALL

The future is only the signal source (RSI, Pct, spot price); the portfolio trades the option. This plugin decides;
`Bridge` sends, `Portfolios` remembers. Stocks come from stocks.txt (ranked inside baskets by Pct from open), or a single
underlying such as NIFTY with the strike taken from the synthetic future.
"""
from __future__ import annotations

import time
from datetime import datetime

import pandas as pd

from algo.core.nulls import NULL_LONG
from algo.core.plugin import Plugin
from algo.strategy.config import RsiOptionsConfig
from algo.strategy.picker import OptionPicker
from algo.strategy.sizing import option_params
from algo.strategy.universe import Instrument, middle_set, resolve_single, resolve_stocks

ZONE_LEG = {"OVERBOUGHT": "PUT", "OVERSOLD": "CALL"}


class RsiOptions(Plugin):
    def __init__(self, cfg: RsiOptionsConfig | None = None):
        cfg = cfg or RsiOptionsConfig.stocks()
        super().__init__(cfg.name)
        self.cfg = cfg
        self.requires = ("master", "candles", "indicators", cfg.book, cfg.bridge)
        self.inst: dict[int, Instrument] = {}

    # ---- lifecycle ---------------------------------------------------------------------------
    def start(self) -> None:
        app, cfg = self.app, self.cfg
        for w in cfg.warnings():
            self.log.warning(w)
        missing = [c for c in (cfg.rsi_col, cfg.pct_col) if c not in app.table("indicators").column_names]
        if missing:
            raise RuntimeError(f"indicator_values has no {missing}: call indicators(RSI(...)) first "
                               f"(this strategy reads {cfg.rsi_col}; change rsi_col to use another)")
        self._setup(app.master, app.market, app[cfg.book].state, app[cfg.bridge], app.paths.market_data)
        self._seed()
        self._unregister = [self.bridge.on_enable(self._on_enable), self.bridge.on_halt(self._on_halt)]
        self._h_px = self.listen(app.table("candles"), self._on_price)
        self._h_sig = self.listen(app.table("indicators"), self._on_indicators)
        self.follow("candles", self._reattach_px)              # the tables are rebuilt when candles or indicators change
        self.follow("indicators", self._reattach_sig)

        self.log.info("armed (%s, sending %s), %d instruments, RSI >%s buy PUT / <%s buy CALL, mid-quartile filter %s",
                      "DRY RUN" if self.bridge.cfg.dry_run else "LIVE", "ON" if self.bridge.sending else "OFF",
                      len(self.tokens), cfg.rsi_high, cfg.rsi_low, "ON" if cfg.mid_filter else "OFF")
        if cfg.reset_on_start:
            self._reset_tokens(self.tokens)
        if cfg.act_on_start:
            for t in self.tokens:
                if self.zones[t] is not None:
                    self.log.info("%s already in %s at start, activating", self.inst[t].name, self.zones[t])
                    self.on_signal(t, self.zones[t])

    def _setup(self, master, market, book, bridge, data_dir: str) -> None:
        """All state, no Deephaven: what the signal logic needs."""
        cfg = self.cfg
        self.master, self.market, self.book, self.bridge = master, market, book, bridge
        self.today = pd.Timestamp(datetime.now(market.zone).date())

        self.inst = self._universe(data_dir)
        self.tokens = list(self.inst)
        self.basket_of = {t: i.basket for t, i in self.inst.items()}
        self.book.track(self.tokens)

        self.ltp: dict[int, float] = {}                       # token (futures AND options) -> last Close, raw
        self.zones = {t: None for t in self.tokens}           # RSI zone
        self.rsis = {t: None for t in self.tokens}            # last RSI x 100
        self.pcts = {t: None for t in self.tokens}            # last Pct-from-open x 100
        self.off_seen: dict[int, str] = {}                    # leg already logged as "would BUY" while sending is off
        self.picker = OptionPicker(self.master, cfg, self.market.price_div, self.opt_price)

    def _reattach_px(self, table) -> None:
        self.stop_listener(self._h_px)
        self._h_px = self.listen(table, self._on_price)
        self.log.info("following the restarted candles table")

    def _reattach_sig(self, table) -> None:
        self.stop_listener(self._h_sig)
        self._h_sig = self.listen(table, self._on_indicators)
        self.log.info("following the rebuilt indicator table")

    def on_stop(self) -> None:
        for undo in getattr(self, "_unregister", []):
            undo()
        self._unregister = []

    def describe(self) -> str:
        return (f"{len(self.inst)} instruments, RSI >{self.cfg.rsi_high} PUT / <{self.cfg.rsi_low} CALL, "
                f"{sum(1 for t in self.tokens if self.book.live.get(t))} live")

    def _universe(self, data_dir: str) -> dict[int, Instrument]:
        cfg = self.cfg
        if cfg.universe == "single":
            inst = resolve_single(cfg.underlying, self.master, self.today)
            self.log.info("signal source: %s (token %s)", next(iter(inst.values())).name, next(iter(inst)))
            return inst
        path = f"{data_dir}/{cfg.stocks_file}"
        with open(path) as f:
            inst = resolve_stocks(f.read(), self.master)
        sizes: dict[str, int] = {}
        for i in inst.values():
            sizes[i.basket] = sizes.get(i.basket, 0) + 1
        self.log.info("tracking %d stocks from %s, baskets %s", len(inst), path, sizes)
        if not inst:
            self.log.warning("no stock from %s could be resolved, nothing will be tracked", path)
        return inst

    # ---- state seeding ------------------------------------------------------------------------
    def _seed(self) -> None:
        """Prices, RSI zones and Pct from history; sends nothing."""
        if not self.tokens:
            return
        from deephaven.pandas import to_pandas
        flt = f"token in {', '.join(map(str, self.tokens))}"
        cfg = self.cfg
        try:
            snap = to_pandas(self.app.table("candles").where(flt).last_by("token"))
            for t, c in zip(snap["token"], snap["Close"]):
                self.ltp[int(t)] = float(c)
        except Exception as e:
            self.log.warning("could not seed prices: %s", e)
        try:
            h = to_pandas(self.app.table("indicators").where(flt).last_by("token").view(["token", cfg.rsi_col, cfg.pct_col]))
            for t, rsi, pct in zip(h["token"], h[cfg.rsi_col], h[cfg.pct_col]):
                if not pd.isna(pct) and int(pct) != NULL_LONG:
                    self.pcts[int(t)] = int(pct)
                if pd.isna(rsi) or int(rsi) == NULL_LONG:
                    continue
                self.zones[int(t)] = self.rsi_zone(int(rsi))
                self.rsis[int(t)] = int(rsi)
        except Exception as e:
            self.log.warning("could not replay history: %s", e)

    def opt_price(self, token: int) -> float | None:
        """Last Close (raw) of any token; falls back to a query on `candles` if no tick was seen yet."""
        if token in self.ltp:
            return self.ltp[token]
        try:
            from deephaven.pandas import to_pandas
            with self.app.in_ctx():
                s = to_pandas(self.app.table("candles").where(f"token == {token}").last_by("token"))
            if len(s):
                self.ltp[token] = float(s["Close"].iloc[0])
                return self.ltp[token]
        except Exception as e:
            self.log.warning("could not read price of token %s: %s", token, e)
        return None

    # ---- signal logic -------------------------------------------------------------------------
    def rsi_zone(self, rsi_x100: int) -> str | None:
        if rsi_x100 > self.cfg.rsi_high * 100:
            return "OVERBOUGHT"
        if rsi_x100 < self.cfg.rsi_low * 100:
            return "OVERSOLD"
        return None

    def in_window(self) -> bool:
        start, end = self.market.trade_window
        return start <= datetime.now(self.market.zone).time() <= end

    def middle(self) -> set[int]:
        return middle_set(self.pcts, self.basket_of)

    def eligible(self, token: int) -> bool:
        return (not self.cfg.mid_filter) or token in self.middle()

    def on_signal(self, token: int, zone: str, force: bool = False) -> None:
        """RSI of the underlying `token` entered a zone. Manual: sell(x) = buy PUT, buy(x) = buy CALL."""
        book, name, leg = self.book, self.inst[token].name, ZONE_LEG[zone]
        if not self.in_window():
            self.log.info("%s -> %s outside trade window, ignored", name, zone)
            return
        if token not in self.ltp:
            self.log.info("no candle close yet for %s, %s skipped", name, zone)
            return
        if not force and not book.allow.get(token, True):
            return                                              # unticked in the console: no params for this instrument
        if not force and not self.eligible(token):
            self.log.info("%s -> %s: not in middle Pct band (Q2+Q3), skipped", name, zone)
            return
        if not force and book.live.get(token) == leg:
            return                                              # already running this leg
        cooldown = self.cfg.fail_cooldown_s
        if not force and cooldown and time.time() - book.last_fail.get(token, 0) < cooldown:
            return                                              # recently failed, wait it out
        if not self.bridge.sending:
            if force or self.off_seen.get(token) != leg:
                self.off_seen[token] = leg                      # log each new would-be leg once, not on every candle
                opt, _ = self.picker.pick(self.inst[token].underlying, self.inst[token].expiry, self.ltp[token], leg, self.today)
                self.log.info("[OFF] %s PF %s would BUY %s (%s), NOT sent", name, book.pf_of(token) or "NEW",
                              self.master.symbol_of.get(opt, "?"), leg)
            return
        rsi = self.rsis[token]
        self.log.info("%s RSI %s -> %s", name, rsi / 100 if rsi is not None else "?", zone)
        self.activate(token, leg)

    def make_params(self, option: int) -> dict | None:
        px = self.opt_price(option)
        if px is None:
            return None
        tick = self.master.tick_of.get(option)
        if tick is None:
            self.log.warning("option %s has no tick in master, skipped", option)
            return None
        params, why = option_params(px / self.market.price_div, tick, self.cfg)
        if params is None:
            self.log.warning("option %s %s, NOT sent", option, why)
        return params

    def activate(self, token: int, leg: str) -> None:
        book, inst, bridge = self.book, self.inst[token], self.bridge
        opt, why = self.picker.pick(inst.underlying, inst.expiry, self.ltp[token], leg, self.today)
        if opt is None:
            self.log.info("%s no option found for %s (%s), not activated", inst.name, leg, why)
            return
        self.log.info("%s %s", inst.name, why)
        params = self.make_params(opt)
        if params is None:
            self.log.info("%s no option price or invalid params, not activated", inst.name)
            return
        prev_leg, prev_opt = book.live.get(token), book.opt_of.get(token)
        book.set_live(token, leg, opt)                          # mark now so an in-flight swap is not queued twice
        nm = self.master.symbol_of.get(opt, opt)
        cur_pf = book.pf_of(token)
        old_pf = cur_pf if cur_pf is not None and book.subscribed.get(cur_pf) not in (None, opt) else None   # moves to another option
        if bridge.cfg.dry_run:                                  # a dry run never creates a portfolio or writes the PF file
            self.log.info("[DRY] %s PF %s -> BUY %s: subscribe if needed, pause, apply %s, unpause", inst.name,
                          f"NEW (old {old_pf} retired)" if old_pf else cur_pf or "NEW", nm, params)
            return
        if old_pf is not None:
            pf = book.create(token)
            self.log.info("%s moves to %s, PF %s retired: created PF %s", inst.name, nm, old_pf, pf)
        elif cur_pf is None:
            pf = book.create(token)
            self.log.info("%s qualified and has no portfolio: created PF %s", inst.name, pf)
        else:
            pf = cur_pf
        self.log.info("%s PF %s -> BUY %s maxqty %s ival %s jval %s sval %s range %s-%s", inst.name, pf, nm,
                      book.caps.get(pf, 0) + bridge.cfg.add_qty, params["ival"], params["jval"], params["sval"],
                      params["lowerrange"], params["upperrange"])
        bridge.submit(bridge.swap(pf, token, opt, params, prev_leg, prev_opt, old_pf))

    # ---- listeners ------------------------------------------------------------------------------
    def _on_price(self, update, is_replay) -> None:
        ch = update.added(["token", "Close"])
        if ch is None:
            return
        for tok, close in zip(ch["token"], ch["Close"]):
            if close != NULL_LONG and close > 0:
                self.ltp[int(tok)] = float(close)              # futures AND options (options are needed for sizing)

    def _on_indicators(self, update, is_replay) -> None:
        cfg = self.cfg
        cols = ["token", cfg.rsi_col, cfg.pct_col]
        parts = [d for d in (update.added(cols), update.modified(cols)) if d is not None]
        if not parts:
            return
        for d in parts:
            for tok, rsi, pct in zip(d["token"], d[cfg.rsi_col], d[cfg.pct_col]):
                tok = int(tok)
                if tok not in self.inst:
                    continue
                if pct != NULL_LONG:
                    self.pcts[tok] = int(pct)
                if rsi == NULL_LONG:
                    continue
                rsi = int(rsi)
                zone = self.rsi_zone(rsi)
                self.rsis[tok], self.zones[tok] = rsi, zone
                if zone is None:
                    self.off_seen.pop(tok, None)               # left its zone: a later re-entry is logged again
                if cfg.log_rsi:
                    px = self.ltp.get(tok)
                    pc = self.pcts[tok]
                    self.log.info("rsi %-24s %.2f pct %s close %s zone %s", self.inst[tok].name, rsi / 100,
                                  pc / 100 if pc is not None else "?",
                                  px / self.market.price_div if px else "?", zone)
        # ranks shift on every update, so re-check every stock sitting in a zone
        if not self.in_window():
            return
        mid = self.middle() if cfg.mid_filter else None
        for t in self.tokens:
            z = self.zones[t]
            if z is not None and self.book.live.get(t) != ZONE_LEG[z] and (mid is None or t in mid):
                self.on_signal(t, z)

    def _on_enable(self) -> None:
        """send_on(): tokens already in a zone are acted on right away."""
        self.off_seen.clear()
        for t in self.tokens:
            if self.zones[t] is not None:
                self.on_signal(t, self.zones[t])

    def _on_halt(self) -> None:
        for h in (getattr(self, "_h_px", None), getattr(self, "_h_sig", None)):
            if h is not None:
                self.stop_listener(h)

    def _reset_tokens(self, tokens) -> None:
        stale = {self.book.pf_map[t]: self.book.caps.get(self.book.pf_map[t], 0)
                 for t in tokens if t in self.book.pf_map and self.book.caps.get(self.book.pf_map[t], 0)}
        if stale:
            self.log.warning("reset_on_start: caps cleared for PFs %s - their next apply restarts maxqty at add_qty", stale)
        self.book.reset(tokens)
        self.log.info("state reset for %d instrument(s)", len(tokens))

    # ---- manual calls (from the console panel or a notebook) -------------------------------------------
    def resolve(self, x) -> int:
        """Accept a master symbol (case-insensitive), an underlying token, or a PF number."""
        if isinstance(x, str):
            for tok, i in self.inst.items():
                if i.name.lower() == x.lower():
                    return tok
            raise KeyError(f"unknown symbol {x}")
        if x in self.inst:
            return x
        t = self.book.token_of_pf(x)
        if t in self.inst:
            return t
        raise KeyError(f"unknown instrument {x}")

    def sell(self, x) -> None:
        self.on_signal(self.resolve(x), "OVERBOUGHT", force=True)        # overbought -> BUY nearest PUT

    def buy(self, x) -> None:
        self.on_signal(self.resolve(x), "OVERSOLD", force=True)          # oversold -> BUY nearest CALL

    def reset(self, x=None) -> None:
        """Forget what the strategy believes it has done. x = symbol / token / PF, or None for all."""
        self._reset_tokens(self.tokens if x is None else [self.resolve(x)])

    def status(self) -> None:
        print(f"sending {'ON' if self.bridge.sending else 'OFF (compute only)'}")
        mid, book = self.middle(), self.book
        for t in self.tokens:
            r, p, pf, o = self.rsis[t], self.pcts[t], book.pf_of(t), book.opt_of.get(t)
            print(f"{self.inst[t].name:24} PF {pf if pf else '-'} basket {self.basket_of[t]} zone {self.zones[t]} "
                  f"rsi {r / 100 if r is not None else None} pct {p / 100 if p is not None else None} mid {t in mid} "
                  f"leg {book.live.get(t)} option {self.master.symbol_of.get(o, '-') if o else '-'} "
                  f"subscribed {self.master.symbol_of.get(book.subscribed.get(pf), '-') if pf else '-'} "
                  f"cap {book.caps.get(pf, 0)} fut_close {self.ltp.get(t)} "
                  f"allowed {book.allow.get(t, True)} paused {pf in book.paused}")
