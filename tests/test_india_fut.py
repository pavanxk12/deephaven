"""examples/11_india_fut_rsi_crossorder.py and 12_india_fut_zscore_crossorder.py: 08 and 10 for the NSE. The universe is every STOCK future of the master (index futures and expired contracts left out),
dealt into 4 baskets; a basket entry is "SBIN FUT" (the nearest unexpired future, on each day), so a stock is compared with its own futures only.
Real registry, book, bridge and NATS service; fake wire and fake Deephaven listener; the history files are real Parquet with two contracts of the same stock in them."""
import re
import time

import numpy as np
import pandas as pd
import pytest

from algo.core.config import NSE
from algo.data.master import classify, normalise
from algo.series.absolute import Absolute, Relative
from tests.conftest import secs
from tests.test_examples import EXAMPLES, FUT, Update, console_with_services, wait_for
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

RSI = (EXAMPLES / "11_india_fut_rsi_crossorder.py").read_text(encoding="utf-8")
Z = (EXAMPLES / "12_india_fut_zscore_crossorder.py").read_text(encoding="utf-8")
ENGINE_NAME = re.search(r'^    ENGINE = [{]"strategyname": "([^"]+)"', RSI, re.M).group(1)                                       # whatever plugin name the owner has typed (CrossOrder or single)
USER_ID = int(re.search(r"^    USER = (\d+)", RSI, re.M).group(1))
ALWAYS_OPEN = "WINDOW = (dtime(0, 0), dtime(23, 59, 59))"
DAY0 = np.datetime64("2026-10-19T14:00")
STOCKS = [f"S{i:02d}" for i in range(20)]


def india_master(unders=STOCKS):
    """NIFTY (token 1, the one contract the other tests use) and BANKNIFTY as INDEX futures, a stock future per name of `unders` (two expiries each, tokens 100+), and one stock
    whose only future has already expired."""
    rows = [dict(Token=FUT, Symbol="NIFTY", ExpiryDate=secs("2030-01-30"), InstType="FUTIDX", OptionType="XX", StrikePrice=0, LotSize=50, TickSize=5),
            dict(Token=2, Symbol="BANKNIFTY", ExpiryDate=secs("2030-01-30"), InstType="FUTIDX", OptionType="XX", StrikePrice=0, LotSize=15, TickSize=5),
            dict(Token=3, Symbol="OLDSTOCK", ExpiryDate=secs("2020-01-30"), InstType="FUTSTK", OptionType="XX", StrikePrice=0, LotSize=100, TickSize=5)]
    for i, name in enumerate(unders):
        rows.append(dict(Token=100 + i, Symbol=name, ExpiryDate=secs("2030-01-30"), InstType="FUTSTK", OptionType="XX", StrikePrice=0, LotSize=100, TickSize=5))
        rows.append(dict(Token=500 + i, Symbol=name, ExpiryDate=secs("2030-02-27"), InstType="FUTSTK", OptionType="XX", StrikePrice=0, LotSize=100, TickSize=5))
    return classify(normalise(pd.DataFrame(rows), NSE))


def start(src_text, name, tmp_path, monkeypatch, unders=STOCKS, rel=None, extra_ns=None, pct=50.0, iv_cannot=False, **replace):
    ns, app, svc, wire = console_with_services(tmp_path, monkeypatch, "ok")
    if extra_ns:
        ns.update(extra_ns)                                                                          # names the script finds in the terminal, such as the live feed table
    app.plugins["master"].data = india_master(unders)
    saved, relative_calls, straddle_calls = [], [], []
    ns["_straddle_calls"] = straddle_calls

    def relative(series, level, symbol, basket=None, ref=None, **kw):
        relative_calls.append((symbol, tuple(basket), ref))
        return Relative(np.array([1.0, 2.0, 3.0]), -1 if (series == "iv" and iv_cannot) else 3, now=0.5, n_valid=7, pctile=pct, symbols=list(basket), series=series, level=level, symbol=symbol)

    def absolute(series, level, symbol, n=22, ref=None, **kw):
        return Absolute(np.arange(float(n)), -1 if (series == "iv" and iv_cannot) else 11, now=0.5, n_valid=n, pctile=pct, series=series, level=level, symbol=symbol)

    ns["absolute_warm"] = lambda *a, **k: None
    ns["straddle_start"] = lambda n=0, **k: straddle_calls.append(n)                                  # the script may start the straddle itself
    ns["relative"], ns["absolute"] = relative, absolute
    ns["basket"] = lambda n, members, **k: saved.append((n, list(members)))
    src_text = re.sub(r"^    ONLY = \[.*?\]", "    ONLY = []", src_text, count=1, flags=re.M | re.S)                    # the owner's own list of stocks: the tests use the fake futures
    src = src_text.replace("WINDOW = (dtime(9, 30), dtime(15, 0))", ALWAYS_OPEN).replace("EXIT_AT = dtime(15, 15)", "EXIT_AT = dtime(23, 59, 59)")
    if "ACT_ON_BAR_END = True" not in replace:                                                      # the older tests feed any number of candles: they look at every minute
        src = src.replace("ACT_ON_BAR_END = True", "ACT_ON_BAR_END = False")
    for old, new in replace.items():
        assert old in src, old
        src = src.replace(old, new)
    before = set(ns)
    exec(compile(src, name, "exec"), ns)
    return ns, app, svc, wire, saved, relative_calls, before


@pytest.mark.parametrize("text,name", [(RSI, "11_india_fut_rsi_crossorder.py"), (Z, "12_india_fut_zscore_crossorder.py")], ids=["11", "12"])
def test_four_baskets_of_stock_futures_only_no_index_no_expired_every_stock_once(text, name, tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, saved, rel, before = start(text, name, tmp_path, monkeypatch)
    assert [n for n, _ in saved] == ["fut_1", "fut_2", "fut_3", "fut_4"]
    members = [m for _, ms in saved for m in ms]
    assert sorted(members) == sorted(f"{s} FUT" for s in STOCKS)                                    # every stock future once; no NIFTY, BANKNIFTY, OLDSTOCK
    assert all(m.endswith(" FUT") and len(m.split()) == 2 for m in members)                         # "SBIN FUT" = the rolling nearest future, never a dated contract
    assert {len(ms) for _, ms in saved} == {5}                                                       # 20 stocks dealt into 4: five each
    svc.on_stop()


@pytest.mark.parametrize("text,name", [(RSI, "11_india_fut_rsi_crossorder.py"), (Z, "12_india_fut_zscore_crossorder.py")], ids=["11", "12"])
def test_only_and_exclude_and_too_few_futures(text, name, tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, saved, rel, before = start(text, name, tmp_path, monkeypatch, **{"ONLY = []": 'ONLY = ["S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09", "S10", "S11", "S12", "S13"]',
                                                                                          "EXCLUDE = []": 'EXCLUDE = ["s02"]'})
    members = sorted(m for _, ms in saved for m in ms)
    assert members == sorted(f"S{i:02d} FUT" for i in range(1, 14) if i != 2)                       # only those, minus the excluded (any case)
    svc.on_stop()
    with pytest.raises(ValueError, match="too few"):
        start(text, name, tmp_path, monkeypatch, unders=STOCKS[:5])


@pytest.mark.parametrize("text,name,tag", [(RSI, "11_india_fut_rsi_crossorder.py", "india_fut_rsi"), (Z, "12_india_fut_zscore_crossorder.py", "india_fut_zscore")], ids=["11", "12"])
def test_it_trades_the_nearest_future_with_the_india_plugin_fields_and_leaks_no_names(text, name, tag, tmp_path, fake_dh, monkeypatch):
    ns, app, svc, wire, saved, rel, before = start(text, name, tmp_path, monkeypatch)
    leaked = sorted(n for n in set(ns) - before if not n.startswith(tag) and n != f"run_{tag}" and not n.startswith("__"))
    assert leaked == []
    assert "Asia/Kolkata" in text and "America/New_York" not in text and "TRADE_AS" not in text
    assert f'"strategyname": "{ENGINE_NAME}"' in text and f"USER = {USER_ID}" in text and USER_ID in (3, 4)       # both scripts send as the same user, with the same plugin name
    svc.on_stop()


def write_two_contracts(folder, days, near_expiry, far_expiry, start_px=200.0, step=-0.25, minutes=60):
    """candles_<day>.parquet: the NEAR contract of S00 falls by `step` a minute; the FAR contract of S00 is 1000 higher and rising (it must never be used while the near one is alive)."""
    last = start_px
    for day in days:
        base = pd.Timestamp(f"{day} 14:30", tz="UTC")
        rows = []
        for i in range(minutes):
            rows.append((base + pd.Timedelta(minutes=i), f"S00 {near_expiry} FUT", last + step * i))
            rows.append((base + pd.Timedelta(minutes=i), f"S00 {far_expiry} FUT", 1000.0 + 5 * i))
        last = last + step * (minutes - 1) + step
        df = pd.DataFrame(rows, columns=["Minute", "Symbol", "Close"])
        pq.write_table(pa.Table.from_pandas(df.sort_values(["Symbol", "Minute"]), preserve_index=False), str(folder / f"candles_{day}.parquet"))
    return last


def test_the_rsi_history_is_seeded_from_each_days_nearest_future_only(tmp_path, fake_dh, monkeypatch, capsys):
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
                                                    **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True"})
    out = capsys.readouterr().out
    assert "history: 3 earlier day(s)" in out
    ns["india_fut_rsi_status"]()
    assert "RSI is ready for 1 of 3" in capsys.readouterr().out or "RSI is ready for 1 of" in out                     # only S00 has history
    token = 100                                                                                              # S00's nearest future
    fake_dh[0].fn(Update(added={"token": [token], "Close": [last - 0.25], "Minute": [DAY0]}), False)       # one more falling minute: oversold through the near contract's prices
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    assert wire.sent[0][2]["TOKEN"] == token and wire.sent[0][2]["strategyname"] == ENGINE_NAME and wire.sent[0][2]["SIDE"] == 1
    svc.on_stop()


def test_after_a_roll_the_next_contract_becomes_the_nearest_for_the_days_after_the_expiry(tmp_path, fake_dh, monkeypatch, capsys):
    """The near contract expired on 2020-01-30. The days 2020-01-31 and later seed from the FAR one, and the days before from the near one."""
    write_two_contracts(tmp_path, ["2020-01-29"], "30JAN20", "27FEB20", start_px=200.0, step=-0.25)           # falling near contract, rising far one
    last = write_two_contracts(tmp_path, ["2020-01-31"], "30JAN20", "27FEB20", start_px=500.0, step=-0.25)    # on this day the nearest is 27FEB20 (rising: 1000 + 5 i)
    ns, app, svc, wire, saved, rel, before = start(RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
                                                    **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "HIST_DAYS = 3": "HIST_DAYS = 2"})
    assert "history: 2 earlier day(s) (2020-01-29, 2020-01-31)" in capsys.readouterr().out
    fake_dh[0].fn(Update(added={"token": [100], "Close": [1300.0], "Minute": [DAY0]}), False)                  # the far contract keeps rising: overbought -> SELL
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][2]["SIDE"] == 2                              # had the near (falling) contract been used on the 31st, it would be a BUY
    svc.on_stop()


def test_the_z_status_lists_only_the_futures_near_the_band(tmp_path, fake_dh, monkeypatch, capsys):
    ns, app, svc, wire, saved, rel, before = start(Z, "12_india_fut_zscore_crossorder.py", tmp_path, monkeypatch)
    ns["india_fut_zscore_status"]()
    out = capsys.readouterr().out
    assert re.search(r"z-score ready for \d+ of 20 futures", out) and "baskets fut_1 (5), fut_2 (5), fut_3 (5), fut_4 (5)" in out
    svc.on_stop()


def test_at_the_time_exit_script_11_stops_like_algo_stop_and_the_others_use_the_same_code(tmp_path, fake_dh, monkeypatch):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    if now.hour == 23 and now.minute >= 58:
        pytest.skip("too close to midnight to place the exit a few seconds ahead")
    when = (now + timedelta(seconds=5)).time()
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "algo_start(USER, STRATEGY)": "algo_start(USER, STRATEGY, squareup_settle_s=0)"})
    # start() opened the exit far away; the real exit time is set by editing the input in a second run of the same text
    svc.on_stop()
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "algo_start(USER, STRATEGY)": "algo_start(USER, STRATEGY, squareup_settle_s=0)",
           "EXIT_AT = dtime(23, 59, 59)": f"EXIT_AT = dtime({when.hour}, {when.minute}, {when.second})"})
    feeder = fake_dh[-1].fn                                                                            # the listener of the second run
    feeder(Update(added={"token": [100], "Close": [last - 0.25], "Minute": [DAY0]}), False)            # the entry, before the exit time
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    end = time.time() + 8
    while datetime.now(ZoneInfo("Asia/Kolkata")).time() < when and time.time() < end:
        time.sleep(0.1)
    time.sleep(0.2)
    feeder(Update(added={"token": [100], "Close": [last - 0.26], "Minute": [DAY0 + np.timedelta64(1, "m")]}), False)       # the next candle after the exit time
    assert wait_for(lambda: len(wire.sent) == 4)
    assert [lab for _, lab, _ in wire.sent[1:]] == ["pause_unpause:Pause", "squareup", "unsubscribe"]
    assert wait_for(lambda: "india_fut_rsi" not in app.plugins)
    svc.on_stop()
    for text in (RSI, Z):                                                                              # 12 has the same code as 11
        assert "threading.Thread(target=time_stop" in text and 'leave(token, f"time exit' not in text


@pytest.mark.parametrize("text,name", [(RSI, "11_india_fut_rsi_crossorder.py"), (Z, "12_india_fut_zscore_crossorder.py")], ids=["11", "12"])
def test_a_day_file_with_a_null_symbol_no_longer_breaks_the_contract_picker(text, name, tmp_path, fake_dh, monkeypatch):
    import algo.series.absolute as absolute_module
    monkeypatch.setattr(absolute_module, "month_contract", absolute_module.month_contract)      # restored after the test, whatever the script puts there
    names = ["S00 30JAN20 FUT", None, "S00 27FEB20 FUT", float("nan")]
    ns, app, svc, wire, saved, rel, before = start(text, name, tmp_path, monkeypatch)
    assert absolute_module.month_contract(("fut", "S00"), names, "2020-01-13") == "S00 30JAN20 FUT"        # the nearest, the null and the NaN names skipped
    assert absolute_module.month_contract(("fut", "S00"), names, "2020-02-01") == "S00 27FEB20 FUT"
    start(text, name, tmp_path, monkeypatch)                                                       # pasted again: wrapped once, not twice
    assert getattr(absolute_module.month_contract, "skips_null_names", False)
    svc.on_stop()


def test_after_a_take_profit_the_portfolio_is_unsubscribed_so_the_engine_slot_is_freed(tmp_path, fake_dh, monkeypatch):
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "UNSUB_AFTER_S = 60": "UNSUB_AFTER_S = 0.3"})
    entry = last - 0.25
    fake_dh[-1].fn(Update(added={"token": [100], "Close": [entry], "Minute": [DAY0]}), False)
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    for i in range(1, 60):                                                                           # +3%: the target (1.5%). A candle before the reply opens the position is ignored, so repeat until the exit goes out
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [entry * 1.03], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        if any(lab == "squareup" for _, lab, _ in wire.sent):
            break
        time.sleep(0.1)
    assert wait_for(lambda: [lab for _, lab, _ in wire.sent] == ["subscribe", "squareup", "unsubscribe"])
    subject, _, payload = wire.sent[2]
    assert subject == wire.sent[0][0] and payload["T1"] == 100                                       # the same portfolio, and its contract
    svc.on_stop()


def test_no_entry_while_the_script_holds_as_many_portfolios_as_allowed(tmp_path, fake_dh, monkeypatch, capsys):
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", re.search(r"MAX_PORTFOLIOS = \d+", RSI).group(0): "MAX_PORTFOLIOS = 0"})
    fake_dh[-1].fn(Update(added={"token": [100], "Close": [last - 0.25], "Minute": [DAY0]}), False)       # oversold: it would buy
    time.sleep(0.3)
    assert wire.sent == []
    assert "MAX_PORTFOLIOS 0" in capsys.readouterr().out
    svc.on_stop()


@pytest.mark.parametrize("text", [RSI, Z, (EXAMPLES / "08_us_basket_rsi_crossorder.py").read_text(encoding="utf-8"), (EXAMPLES / "10_us_zscore_crossorder.py").read_text(encoding="utf-8")], ids=["11", "12", "08", "10"])
def test_all_four_scripts_release_a_closed_portfolio_and_have_the_limit(text):
    import re as _re
    assert _re.search(r"^    MAX_PORTFOLIOS = \d+", text, _re.M) and _re.search(r"^    UNSUB_AFTER_S = \d+", text, _re.M)             # whatever numbers the owner has typed
    for needle in ("def release(pf, token):", "threading.Timer(UNSUB_AFTER_S, release", "if len(live) >= MAX_PORTFOLIOS", "live.add(a[\"pf\"])"):
        assert needle in text, needle


def test_script_11_looks_for_an_entry_only_on_the_candle_that_ends_a_bar_when_asked(tmp_path, fake_dh, monkeypatch):
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "ACT_ON_BAR_END = True": "ACT_ON_BAR_END = True"})
    feed = fake_dh[-1].fn
    for i, px in enumerate([last - 0.25, last - 0.5, last - 0.75, last - 1.0]):                      # :00 to :03 of a 5-minute bar: oversold, but not the end of a bar
        feed(Update(added={"token": [100], "Close": [px], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
    time.sleep(0.3)
    assert wire.sent == []
    feed(Update(added={"token": [100], "Close": [last - 1.25], "Minute": [DAY0 + np.timedelta64(4, "m")]}), False)       # :04 ends the bar
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    svc.on_stop()


def test_the_tick_exit_edits_the_token_table_from_a_worker_thread_and_a_tick_closes_the_position(tmp_path, fake_dh, monkeypatch):
    import sys
    import threading
    import types
    edits = []

    class Keys:
        def add(self, t):
            edits.append(("add", threading.current_thread().name))

        def delete(self, t):
            edits.append(("delete", threading.current_thread().name))

    class Feed:                                                                                      # where_in / where / view all give back the same stub: only the listener on it matters
        def where_in(self, *a, **k):
            return self

        def where(self, *a, **k):
            return self

        def view(self, *a, **k):
            return self

    dh = sys.modules["deephaven"]
    monkeypatch.setattr(dh, "input_table", lambda **kw: Keys(), raising=False)
    monkeypatch.setattr(dh, "new_table", lambda cols: cols, raising=False)
    monkeypatch.setattr(dh.dtypes, "int32", "int32", raising=False)
    col = types.ModuleType("deephaven.column")
    col.int_col = lambda name, values: (name, values)
    monkeypatch.setitem(sys.modules, "deephaven.column", col)
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"], extra_ns={"fo_market_data": Feed()},
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "UNSUB_AFTER_S = 60": "UNSUB_AFTER_S = 30"})
    on_tick, on_candle = fake_dh[0].fn, fake_dh[1].fn                                                 # the tick listener is made first, then the candle one
    entry = last - 0.25
    on_candle(Update(added={"token": [100], "Close": [entry], "Minute": [DAY0]}), False)              # the entry, made inside the candle listener
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    assert wait_for(lambda: edits == [("add", "india_fut_rsi-tick-keys")])                            # the token was added by the worker thread, not by the listener
    div = app.master.divisor_of.get(100) or app.market.price_div
    for _ in range(60):                                                                              # +3% on the tick feed: the target (1.5%). Ticks before the reply opens the position are ignored.
        on_tick(Update(added={"token": [100], "ltp": [int(entry * div * 1.03)]}), False)
        if any(lab == "squareup" for _, lab, _ in wire.sent):
            break
        time.sleep(0.1)
    assert wait_for(lambda: [lab for _, lab, _ in wire.sent] == ["subscribe", "squareup"])
    assert wait_for(lambda: ("delete", "india_fut_rsi-tick-keys") in edits)                           # and the token left the table once the exit was confirmed
    svc.on_stop()


@pytest.mark.parametrize("text", [RSI, Z, (EXAMPLES / "13_india_fut_ema_cross_crossorder.py").read_text(encoding="utf-8")], ids=["11", "12", "13"])
def test_the_india_scripts_have_the_one_entry_per_stock_rule(text):
    for needle in ("ONE_ENTRY_PER_STOCK = True", "entered.add(token)", "entered.discard(token)", "ONE_ENTRY_PER_STOCK and token in entered"):
        assert needle in text, needle


def one_entry_run(tmp_path, fake_dh, monkeypatch, **replace):
    """Script 11: a long fall (oversold, BUY), a 3% jump (the target: squareup, then unsubscribe), a rise out of the zone, another long fall. Returns the subscribes sent."""
    last = write_two_contracts(tmp_path, ["2020-01-13", "2020-01-14", "2020-01-15"], "30JAN20", "27FEB20")
    ns, app, svc, wire, saved, rel, before = start(
        RSI, "11_india_fut_rsi_crossorder.py", tmp_path, monkeypatch, unders=["S00", "S01", "S02"],
        **{"N_BASKETS = 4": "N_BASKETS = 1", "RSI_LIVE = False": "RSI_LIVE = True", "ACT_ON_BAR_END = True": "ACT_ON_BAR_END = False", "UNSUB_AFTER_S = 60": "UNSUB_AFTER_S = 0.3", **replace})
    entry = last - 0.25
    fake_dh[-1].fn(Update(added={"token": [100], "Close": [entry], "Minute": [DAY0]}), False)
    assert wait_for(lambda: len(wire.sent) == 1) and wire.sent[0][1] == "subscribe"
    for i in range(1, 60):                                                                           # +3%: the target; repeat until the exit goes out
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [entry * 1.03], "Minute": [DAY0 + np.timedelta64(i, "m")]}), False)
        if any(lab == "squareup" for _, lab, _ in wire.sent):
            break
        time.sleep(0.1)
    assert wait_for(lambda: [lab for _, lab, _ in wire.sent][:3] == ["subscribe", "squareup", "unsubscribe"])
    top, minute = entry * 1.03, 100
    for k in range(1, 16):                                                                           # the RSI leaves the oversold zone
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [top + 0.5 * k], "Minute": [DAY0 + np.timedelta64(minute, "m")]}), False)
        minute += 1
    peak = top + 7.5
    for k in range(1, 70):                                                                           # and falls into it again
        fake_dh[-1].fn(Update(added={"token": [100], "Close": [peak - 0.25 * k], "Minute": [DAY0 + np.timedelta64(minute, "m")]}), False)
        minute += 1
    time.sleep(0.5)
    svc.on_stop()
    return [p["SIDE"] for _, lab, p in wire.sent if lab == "subscribe"]


def test_script_11_does_not_enter_a_stock_again_after_its_exit(tmp_path, fake_dh, monkeypatch):
    assert one_entry_run(tmp_path, fake_dh, monkeypatch) == [1]
    fake_dh.clear()
    (tmp_path / "b").mkdir()
    sides = one_entry_run(tmp_path / "b", fake_dh, monkeypatch, **{"ONE_ENTRY_PER_STOCK = True": "ONE_ENTRY_PER_STOCK = False"})
    assert sides[0] == 1 and len(sides) >= 2                                                         # with the rule off the same candles enter again
