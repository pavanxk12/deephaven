# The examples

Written for: you, using the services from the Deephaven terminal. Plain function calls and plain scripts: **no example asks you to define a class.**
Each file is meant to be pasted (or run) in the terminal after `import algo; algo.install(globals(), "NSE")`. `algo_help()` lists every function.

| file | what it is |
|---|---|
| `00_services.py` | starts the shared services **once** for everyone: master, candles, registry, NATS (users 3 and 4) |
| `01_lookups.py` | look up any contract, and read the latest price and candles |
| `02_manual_trade.py` | a trade by hand: find the option, take a portfolio number, send subscribe / pause / apply / unpause |
| `03_algo_template.py` | **an automation as a plain script**: your settings, a few functions, two listeners. Copy it for a new one |
| `04_nifty_multistrike.py` | your NIFTY automation with `N_STRIKES` strikes, as a plain script (functions and listeners, like your old one) |
| `06_nifty_rsi_churning.py` | the same on a **5-minute RSI** (made from the 1-minute candles), with your subscribe fields, the `blocked` slots, `buy()` / `sell()` / `exit_all()` and a check when it is switched on |
| `08_us_basket_rsi_crossorder.py` | the US version of 07: one basket of 19 stocks, New York hours, a **5-minute RSI** seeded from earlier saved days, the basket filter AND the stock's own comparison with its previous 22 trading days (`absolute()`, Q2 to Q3); target, stop-loss and a **time exit** (`EXIT_AT` 15:55 New York time, 5 minutes before the close: the automation is stopped like `algo_stop` (pause, square up, unsubscribe a minute later) and no entry is made after it) |
| `10_us_zscore_crossorder.py` | 08 without the RSI and without the own-history filter: the basket middle filter plus a **z-score of the % from the VWAP** (same minute, last 90 calendar days, on 5-minute bars): BUY at z <= -3, SELL at z >= +3, a BUY is closed when z > 0, a SELL when z < 0, and the 15:55 time exit stops the automation like `algo_stop` (pause, square up, unsubscribe a minute later); needs about 64 saved trading days |
| `11_india_fut_rsi_crossorder.py` | 08 for the NSE: every **stock future** (about 230, index futures left out) in **4 baskets** of about 58; an entry is `"SBIN FUT"` = the nearest unexpired future, on each day, so the basket ranking, the own-history comparison (`absolute()`, 22 days) and the RSI seed all use the stock's own futures; 5-minute RSI, target / stop-loss, India hours, time exit `EXIT_AT` 15:15 = the same stop as `algo_stop`. One new portfolio per trade, unsubscribed `UNSUB_AFTER_S` (60 s) after its exit; at most `MAX_PORTFOLIOS` (28) held at once (the engine has a limited number of plugin slots) |
| `12_india_fut_zscore_crossorder.py` | 10 for the NSE, same universe and baskets as 11: **z-score of the % from the VWAP** against the same minute of the nearest future on the last 90 calendar days, BUY at z <= -3, SELL at z >= +3, closed through 0; needs about 64 saved trading days with the futures in them; time exit 15:15 = `algo_stop` |
| `07_basket_rsi_crossorder.py` | a basket of symbols, a **1-minute RSI** entry (oversold buy / overbought sell) allowed only while the % from open and from VWAP sit in the middle of the basket (Q2 to Q3), your own target 1.5% and stop 0.5%, and the engine's single-order strategy (`single` / CrossOrder), started by one `subscribe` per trade on a new portfolio and closed with `squareup` |

An automation script uses only the five services, through functions: `contract` / `strikes` / `nearest_strikes` (master), `last_price` and the
table `candles` (candles), `algo_start` / `pf_assign` / `pf_activate` (the book and NATS) and `nats_send`.

Every file is tested: `tests/test_examples.py` runs all five for real in an installed console against the master, candles, registry and NATS
service (fake data and a fake connection). None of this has run on a live Deephaven yet.

## 0. The order: start once, then every script connects

```
00_services.py      once a day, by one person or at the end of init.py:  master, candles, registry_start(), nats_start(users=[3, 4])
01, 02              plain calls, any time
03, 04              nats_connect(user=..) registry_connect() algo_start(..) ... listeners ... nats_send(True, client="<its name>")
```
- **A script never starts a shared service.** `nats_connect` and `registry_connect` only check that they are up (and that the user may send);
  they start and change nothing. Any number of scripts can start, stop and be re-pasted without disturbing each other.
- **`nats_start` and `registry_start` are safe to call twice** (they do nothing the second time) and refuse to change a running service with other
  settings; that is `nats_restart`, on purpose. A new user is added with `nats_add_user(5)`, no restart.
- **Every automation starts OFF and switches itself on:** `nats_send(True, client="nifty_multi")`; `nats_send(False, client=...)` stops only that one,
  `nats_send(False)` is the master switch for everyone. A command goes out only when both are on. A typed command (example 02) needs only the master switch.
- **Data is started once too:** `candles_start` restarts the candle table every time it is called, so it is in `00_services.py`, not in a script.
  (`candles_start` and `master_build` are not yet "start-once" like NATS and the registry.)
- **Reading is cheap, but not free.** `last_price` and the others make a short-lived filtered table; do not call them on every update. Inside a
  listener use the update's own data (`update.added(...)`), as 03 and 04 do, and `show=False` on the lookups.

## 1. What the services do that your old scripts did themselves

| the old script did this itself | now done by |
|---|---|
| connect to NATS, one asyncio loop, per-PF locks, `MAX_PARALLEL`, `SEND_ON`, reply handling | the **NATS service** (started once; `nats_connect`, `nats_send(client=...)`); errors come back as a status, nothing raises |
| `last_pf()`, `new_pf()`, `PF_MAP_FILE`, `PF_SUB_FILE`, `PF_START`, never reuse a number | the **registry** (numbers, unique per user) and the **book** (stock -> portfolio, caps, paused, live): `pf_assign` |
| the order of commands: retire old, subscribe, pause, apply, unpause, roll back on failure | the **bridge**: `pf_activate` |
| `PRICE_DIV = 100`, RSI as int x100, Pct as int x100 | candles are already in rupees (doubles); the script computes its own RSI (0 to 100) |
| option sizing (`floor_tick`, `ceil_tick`, `_on_tick`, ival/jval/sval/trp/ranges) | `order_params(...)`, a plain function inside the script (sizing is strategy logic, not infra) |
| the master lookups (`OPT_IDX`, `EXPIRIES`, `FUT_INFO`, `TICK_OF`) | `contract`, `expiries`, `strikes`, `nearest_strikes` |

What stays in your script is the part that is yours: the settings, which option, how much, when.

## 2. Where each old constant goes

| old constant | now |
|---|---|
| `USER_ID` | `USER = 4` in the script, `algo_start(USER, ...)`, and the user in `nats_start(users=[3, 4])` |
| `NATS_URL` | `nats_start("192.168.1.130", 4222, ...)` in `00_services.py` (see gap 2) |
| `SENDER`, `TIMEOUT_S`, `MAX_PARALLEL` | `nats_start(sender=, timeout_s=, max_in_flight=)`; the defaults are AUTOMATION, 5 and 64 |
| `PF_START` | `algo_start(..., pf_start=1001)` |
| `PF_MAP_FILE`, `PF_SUB_FILE` | `algo_start(..., map_file=, sub_file=)` to keep the portfolios of the stocks script; the numbers are also in the registry |
| `SUB_PAYLOAD` | the service holds the defaults (`nats_start(sub_payload={...})`, for everyone); each strategy adds its own fields: `algo_start(..., subscribe={"strategyname": "Churning"})`. The service no longer sends a `strategyname` |
| `APPLY_PARAMS`, `OPT_IVAL_PCT`, `OPT_RANGE_*_PCT`, `TRP_PCT`, `JVAL_FRAC`, `SVAL_MULT` | the `SIZING` dict and `order_params(...)` inside the script |
| `ADD_QTY`, `UNSUBSCRIBE_ON_RETIRE`, `SQUAREUP_ON_REPOINT` | `algo_start(..., add_qty=, unsubscribe_on_retire=, squareup_on_repoint=)` |
| `RSI_HIGH`, `RSI_LOW`, `N_STRIKES`, `STRIKE_MODE`, `STRIKE_GAP`, `OPT_EXPIRY_IDX`, `FAIL_COOLDOWN_S` | constants at the top of your script (04 has them all) |
| `TRADE_START`, `TRADE_END` | a constant in your script (04 has `WINDOW`); the exchange's own window is NSE 9:20 to 15:00 |
| `SEND_ON` | `nats_send(True, client="<its name>")`, per automation; it starts OFF |
| `RESET_ON_START`, `ACT_ON_START` | what the script does at the end: 03 and 04 start acting on the next RSI update; `pf_leg` / `pf_mark` reset a leg |
| the operator functions (`pause_all`, `exit_all`, `unsubscribe_all` ...) | `algo_pause`, `algo_exit`, `algo_unsubscribe`, `algo_portfolios` |

## 3. What is not covered (be aware)

1. **The subscribe payload belongs to the NATS service, not to a script.** Stocks (`maxloss -1000`, `max_unhedge 1`) and NIFTY (`-400`, `3`) need
   different ones; with both running on one service, one of them subscribes with the wrong payload. It should be a setting of each automation.
2. **One NATS service means one NATS server.** Your stocks script used `192.168.1.130` and the NIFTY one `192.168.1.104`. If those are two servers,
   two services are needed (the class supports a name; `nats_start` does not yet). `00_services.py` uses `.130` for both.
3. **Old files.** The stocks files (`token -> pf`) are read as they are (`algo_start(..., map_file=, sub_file=)`). The NIFTY file has `token:slot`
   keys: the book refuses it with a clear message and leaves it untouched; use a new file (the registry has the numbers).
4. **Not in example 04, a few lines each:** the old `blocked` set (a slot you squared up by hand stays off until the RSI leaves its zone),
   `unsubscribe_pf` for one portfolio (use `pf_unsubscribe` / `pf_squareup`), and re-checking the RSI at the moment the automation is switched on.
5. **The stocks logic (baskets, the middle-half filter by percent-from-open, `stocks.txt`)** is not provided as a function: write it in the script,
   as in the old `automation_rsi.py`; `03_algo_template.py` is the shape. There is no packaged strategy any more.
6. **The console's per-instrument Subscribe / Unsubscribe** keys on the signal future. A slot (`future * 100 + k`) is not a future, so the console will not show
   the three strikes as separate stocks.

## 4. Still open (decisions for you)

1. Make the **subscribe payload** a setting of each automation (gap 1)?
2. Are `.130` and `.104` **two NATS servers**? If so, should one session talk to both?
3. Should the stocks logic (baskets and the mid filter) be a ready function, or stay in your script?

## 05_nifty_sma_cross.py
NIFTY near-expiry options, the 5 strikes nearest the money, churned when the fast SMA of the future crosses the slow one (above: CALLs, below: PUTs, the old
portfolios retire). It needs `candles_start("1min", symbols=["NIFTY"], expiries=1)` so the options have candles, and the registry and NATS service started.
