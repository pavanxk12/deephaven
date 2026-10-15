"""RsiOptions signal logic against a real Portfolios and a recording bridge (no Deephaven, no NATS)."""
import asyncio
import time


from algo.core.config import NSE
from algo.strategy.book import Portfolios
from algo.strategy.config import BridgeConfig, RsiOptionsConfig
from algo.strategy.rsi_options import RsiOptions

RELIANCE_FUT = 3
CLOSE = 3000.0                         # rupees (the candles are in proper units)


class FakeBridge:
    """Records swaps the way Bridge would execute them, including the subscribe bookkeeping."""

    def __init__(self, book, sending=True, dry=False):
        self.book, self.sending, self.cfg = book, sending, BridgeConfig(dry_run=dry)
        self.swaps = []
        self.fail_next = False

    async def swap(self, pf, token, option, params, prev_leg, prev_option, old_pf=None):
        self.swaps.append(dict(pf=pf, token=token, option=option, params=params, old_pf=old_pf))
        if self.fail_next:
            self.fail_next = False
            self.book.set_live(token, prev_leg, prev_option)
            self.book.last_fail[token] = time.time()
            return
        if self.book.subscribed.get(pf) is None:
            self.book.subscribe(pf, option)

    def submit(self, coro):
        asyncio.run(coro)
        return True

    def on_enable(self, cb):
        return lambda: None

    def on_halt(self, cb):
        return lambda: None


def price_options(s, premium=100.0):
    for tok in s.master.opt["Token"]:
        s.ltp[int(tok)] = premium


def build(master, tmp_path, sending=True, priced=True, **cfg_kw):
    (tmp_path / "stocks.txt").write_text("BASKETS=1\nRELIANCE 30JAN30 FUT\n")
    cfg = RsiOptionsConfig.stocks(mid_filter=False, rsi_high=60, rsi_low=40, **cfg_kw)
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1111)
    bridge = FakeBridge(book, sending)
    s = RsiOptions(cfg)
    s._setup(master, NSE, book, bridge, str(tmp_path))
    s.in_window = lambda: True
    s.ltp[RELIANCE_FUT] = CLOSE
    if priced:
        price_options(s)
    return s, book, bridge


def sym(s, tok):
    return s.master.symbol_of[tok]


def test_universe_comes_from_the_stocks_file(master, tmp_path):
    s, book, _ = build(master, tmp_path)
    assert list(s.inst) == [RELIANCE_FUT] and book.tracked == {RELIANCE_FUT}


def test_overbought_buys_the_nearest_put_and_creates_a_portfolio(master, tmp_path):
    s, book, bridge = build(master, tmp_path)
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    (swap,) = bridge.swaps
    assert sym(s, swap["option"]) == "RELIANCE 30JAN30 3000 PE" and swap["pf"] == 1111 and swap["old_pf"] is None
    assert swap["params"]["side"] == "BUY" and swap["params"]["ival"] > 0
    assert book.live[RELIANCE_FUT] == "PUT" and book.opt_of[RELIANCE_FUT] == swap["option"]


def test_same_leg_is_not_sent_twice(master, tmp_path):
    s, _, bridge = build(master, tmp_path)
    s.ltp.update({s.master.token_of["RELIANCE 30JAN30 3000 PE"]: 120.0})
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert len(bridge.swaps) == 1


def test_flip_to_the_other_option_moves_to_a_new_portfolio(master, tmp_path):
    s, book, bridge = build(master, tmp_path)
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    s.on_signal(RELIANCE_FUT, "OVERSOLD")
    first, second = bridge.swaps
    assert sym(s, second["option"]) == "RELIANCE 30JAN30 3000 CE"
    assert second["old_pf"] == first["pf"] and second["pf"] == first["pf"] + 1
    assert book.pf_of(RELIANCE_FUT) == second["pf"]


def test_nothing_is_sent_while_sending_is_off_and_nothing_is_marked(master, tmp_path):
    s, book, bridge = build(master, tmp_path, )
    bridge.sending = False
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert bridge.swaps == [] and book.live[RELIANCE_FUT] is None and book.pf_of(RELIANCE_FUT) is None


def test_unticked_stock_is_blocked_but_manual_buy_still_works(master, tmp_path):
    s, book, bridge = build(master, tmp_path)
    book.set_allow(RELIANCE_FUT, False)
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert bridge.swaps == []
    s.sell("RELIANCE 30JAN30 FUT")
    assert len(bridge.swaps) == 1


def test_outside_trade_window_and_missing_price_are_skipped(master, tmp_path):
    s, _, bridge = build(master, tmp_path)
    s.in_window = lambda: False
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    s.in_window = lambda: True
    s.ltp.clear()
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert bridge.swaps == []


def test_cooldown_after_a_failed_swap(master, tmp_path):
    s, book, bridge = build(master, tmp_path, fail_cooldown_s=30)
    bridge.fail_next = True
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert book.live[RELIANCE_FUT] is None and RELIANCE_FUT in book.last_fail
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")                    # still cooling down
    assert len(bridge.swaps) == 1
    book.last_fail[RELIANCE_FUT] -= 31
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert len(bridge.swaps) == 2


def test_mid_filter_blocks_a_basket_that_is_too_small_to_rank(master, tmp_path):
    s, _, bridge = build(master, tmp_path)
    s.cfg = s.cfg.with_(mid_filter=True)
    s.pcts[RELIANCE_FUT] = 10
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")
    assert bridge.swaps == []


def test_rsi_zone_thresholds(master, tmp_path):
    s, _, _ = build(master, tmp_path)
    assert s.rsi_zone(60.01) == "OVERBOUGHT" and s.rsi_zone(60.0) is None
    assert s.rsi_zone(39.99) == "OVERSOLD" and s.rsi_zone(40.0) is None


class Update:
    def __init__(self, rows):
        self.rows = rows

    def added(self, cols):
        return {"token": [r[0] for r in self.rows], cols[1]: [r[1] for r in self.rows], cols[2]: [r[2] for r in self.rows]}

    def modified(self, cols):
        return None


def test_indicator_update_triggers_a_signal_once_per_zone_entry(master, tmp_path):
    s, book, bridge = build(master, tmp_path)
    col = s.cfg.rsi_col
    assert col == "RSI_9"
    s._on_indicators(Update([(RELIANCE_FUT, 70.0, 0.25)]), False)
    s._on_indicators(Update([(RELIANCE_FUT, 72.0, 0.3)]), False)        # still overbought: already running that leg
    assert len(bridge.swaps) == 1 and s.pcts[RELIANCE_FUT] == 0.3
    s._on_indicators(Update([(RELIANCE_FUT, 50.0, 0.3)]), False)        # back to neutral: no new order
    assert len(bridge.swaps) == 1 and s.zones[RELIANCE_FUT] is None


def test_null_rsi_is_ignored(master, tmp_path):
    from algo.core.nulls import NULL_DOUBLE
    s, _, bridge = build(master, tmp_path)
    s._on_indicators(Update([(RELIANCE_FUT, NULL_DOUBLE, float("nan"))]), False)
    assert bridge.swaps == [] and s.rsis[RELIANCE_FUT] is None


def test_unknown_token_in_the_indicator_stream_is_ignored(master, tmp_path):
    s, _, bridge = build(master, tmp_path)
    s._on_indicators(Update([(999, 90.0, 1.0)]), False)
    assert bridge.swaps == []


def test_reset_warns_about_caps_and_clears_them(master, tmp_path):
    s, book, _ = build(master, tmp_path)
    pf = book.create(RELIANCE_FUT)
    book.caps[pf] = 10
    s.reset()
    assert book.caps[pf] == 0


def test_make_params_needs_price_and_tick(master, tmp_path):
    s, _, _ = build(master, tmp_path, priced=False)
    opt = s.master.token_of["RELIANCE 30JAN30 3000 CE"]
    assert s.make_params(opt) is None                                  # no price for the option yet
    s.ltp[opt] = 100.0
    p = s.make_params(opt)
    assert p is not None and p["trp"] > 100.0 and p["lowerrange"] < 100.0 < p["upperrange"]


def test_nifty_single_underlying_uses_the_synthetic_future(master, tmp_path):
    cfg = RsiOptionsConfig.nifty(rsi_high=60, rsi_low=40)
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 1555)
    bridge = FakeBridge(book)
    s = RsiOptions(cfg)
    s._setup(master, NSE, book, bridge, str(tmp_path))
    s.in_window = lambda: True
    price_options(s)
    nifty_fut = 1
    s.ltp[nifty_fut] = 25000.0
    ce, pe = master.token_of["NIFTY 30JAN30 25000 CE"], master.token_of["NIFTY 30JAN30 25000 PE"]
    s.ltp[ce], s.ltp[pe] = 300.0, 100.0                            # synthetic 25200 -> strike 25100
    s.on_signal(nifty_fut, "OVERSOLD")
    (swap,) = bridge.swaps
    assert sym(s, swap["option"]) == "NIFTY 30JAN30 25100 CE" and swap["pf"] == 1555
