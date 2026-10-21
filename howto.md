# How to use the services: plain functions, no classes

Type **`algo_help()`** in the Deephaven terminal at any time: it lists every function by service, one line each.
`algo_help("pf_create")` shows the full description of one.

Written for: you, using the master, candles, portfolio and NATS services directly from the terminal.

## 0. Once a day (one person, or the end of init.py; on the India host init.py already does it at every container start, see run.md section 0)

```python
import algo; algo.install(globals(), "NSE")        # "US" for the US feed
master_build("NSE")                                 # the contract master
candles_start("1min", expiries=1)                   # candles (nearest option expiry only: about 0.7 GB instead of 1.8 GB)
registry_start()                                    # the portfolio numbers
nats_start("192.168.1.130", 4222, users=[3, 4])     # the commands, for every user that will send
algo_status()
```
By itself, on a timer: `day_auto()` (NSE: master 09:00:00, candles + registry + NATS + console 09:14:30, 15:30 save of the candles and straddle tables then stop of the straddle and candles; `day_auto("08:45", "09:10", "09:10:30", "15:35")`
for your own times, `day_auto(stop=True)` to switch it off; run.md section 0).
Calling any of these again does no harm to the NATS service and the registry (they say "already running"). `candles_start` and `master_build`
start over when called again, so run them once.

## 1. Master: look up any contract

```python
contract("NIFTY 27OCT26 25000 CE")        # one contract: token, symbol, kind, underlying, expiry, strike, type, tick, lot
contract(47162)                           # the same, from a token
contract("NIFTY")                         # a bare name means its nearest future (the cash instrument: contract(900) or contracts("RELIANCE", kind="EQ"))

expiries("NIFTY")                         # the dates that have not passed        ['2026-10-20', '2026-10-27', ...]
expiries("NIFTY", "FUT")                  # futures expiries
strikes("NIFTY")                          # strikes and tokens of the nearest expiry (CE)
strikes("NIFTY", expiry=1, type="PE")     # the next expiry, puts;  or expiry="2026-10-27"
nearest_strikes("NIFTY", 25010, "CE", n=3)    # the 3 strikes nearest a price (without a price: the future's last candle close)

contracts("NIFTY", type="CE", strike=25000)   # a table of contracts: every expiry's 25000 call
contracts("NIFTY", kind="FUT")                # filters: underlying, kind (FUT / OPT / EQ), expiry (0 = nearest, or a date), type, strike
contracts("RELIANCE", expiry=0, limit=None)   # limit=None shows them all
```
Each one prints what it found **and** returns it (a dict, a list or a table), so `opt = nearest_strikes("NIFTY")["token"].iloc[0]` works.
An unknown name prints a plain message and returns None.

## 2. Candles

```python
candles_start("1min")                     # 30s, 1min, 5min, 15min.  Options: symbols=["NIFTY"], kind="FUT", expiries=1
candles_stop()

last_price("NIFTY")                       # the latest candle close
last_candles("NIFTY", 10)                 # the last 10 candles (Minute, Open, High, Low, Close, Volume)
```
They take a token, a symbol or an underlying name, like `contract` does. The whole table is `candles` in the IDE. Indicators are not a service: compute them in your script from `last_candles` or the `watch("candles", fn)` updates (`examples/03_algo_template.py` does an RSI).

## 3. Portfolios: unique numbers per user

```python
pf = pf_create(4, "my_strategy", "NIFTY")     # create(user, strategy, name) -> the next free number for user 4, remembered
pf_create(4, "my_strategy", "NIFTY", start=2000)     # this strategy's numbers begin at 2000
pf_register(4, 1001, algo="my_strategy", name="TCS") # a number you already use (refused, with who holds it, if it is taken)
pf_list(4)                                    # user 4's portfolios: number, name, algo.  pf_list() = everyone's
```
A portfolio is **user + number**: 1001 under user 3 and 1001 under user 4 are two portfolios, but a user never gets the same number twice.
The registry remembers user, number, strategy and name in a file.

## 4. NATS: the commands

```python
pf_subscribe(pf, option_token, user=4)        # one portfolio, one command; user= may be left out when only one user is set up
pf_pause(pf, user=4)
pf_apply(pf, user=4, ival=1.0, jval=0.75, sval=3.0, trp=101.0, lowerrange=80.0, upperrange=120.0, maxqty=3, side="BUY",
         jqty1=1, jqty2=1, target=1000, stoploss=-400, maxloss=-400, max_unhedge=3, PRNT=1)
pf_unpause(pf, user=4)
pf_squareup(pf, user=4)
pf_unsubscribe(pf, option_token, user=4)
```
Each prints how it went: `PF 1913 pause: ok - ok`, or `NOT ok - timeout` (it is not known whether the bridge acted: it is never resent for you),
`disconnected` (NATS is down; it reconnects by itself), `blocked`, `off`, `user_not_allowed` and so on. **Nothing raises.**

**A whole trade by hand:**
```python
opt = int(nearest_strikes("NIFTY", type="CE")["token"].iloc[0])     # the call nearest the price
pf = pf_create(4, "manual", "NIFTY")
pf_subscribe(pf, opt, user=4); pf_pause(pf, user=4)
pf_apply(pf, user=4, ival=1.0, jval=0.75, sval=3.0, maxqty=3, side="BUY")
pf_unpause(pf, user=4)
```

**Switches and blocks**
```python
nats_send(False)                           # the master switch: nothing is sent for anyone;  nats_send(True) lets them again
nats_send(True, client="my_algo")          # one algo's own switch (every algo starts OFF);  nats_send(False, client="my_algo") stops just it
nats_block(user=4)                         # nothing is sent for user 4 (every portfolio under them);  nats_block(user=4, pf=1913) for one
nats_block()                               # list what is blocked;  nats_unblock(user=4) lifts it
nats_add_user(5)                           # let one more user send, no restart
nats_connect(user=4, client="my_algo")     # only CHECKS the service is up and the user may send (what an algo script calls)
```
Typed commands like `pf_pause(...)` need only the master switch. An algo's commands need the master switch **and** its own.

## 5. Your own automation: a plain script

A script reacts to the market with an ordinary function and a listener, like your old ones. The book and NATS do the rest, through functions:

```python
algo_start(3, "stocks", pf_start=1001)               # register it: its user, its name. It starts OFF.   pf_track("stocks", [token]) after a restart
# keep an older script's portfolios:  algo_start(3, "stocks", map_file="/data/stocks_pf.json", sub_file="/data/stocks_pf_sub.json")
a = pf_assign("stocks", token, option, name="RELIANCE")     # THE BOOK picks the portfolio (new / same / a replacement); None while it is off
params = {...}                                       # your own order parameters (03_algo_template.py shows an order_params function)
pf_activate(a, "CALL", params)                       # THE NATS SERVICE: [retire old] subscribe, pause, apply, unpause. Returns at once.
nats_send(True, client="stocks")                     # switch THIS automation on;  nats_send(False, client="stocks") stops only it
pf_leg("stocks", token)  pf_cooling("stocks", token, 30)  algo_sending("stocks")     # quick checks for use inside your listener
pf_allowed("stocks", token)                                       # False while the console has this slot UNTICKED (pf_assign then gives None)
# The console and your algo share one state: what the algo pauses / squares up shows on the console, and a Pause, Square up or untick on the
# console reaches the algo (a pause stands, a square up lets the slot restart, an untick blocks it). See nats.md "One state for the algos and the console".
algo_portfolios("stocks")                            # its portfolios as a table;  algo_exit("stocks"), algo_pause("stocks") ... for all of them
```
`examples/03_algo_template.py` is a complete one to copy; `examples/04_nifty_multistrike.py` is your multi-strike NIFTY script.
Inside a listener use these functions and `show=False` on the lookups (`nearest_strikes(..., show=False)`); they never wait for the network.

## 6. Where to read more

`nats.md` (the NATS service: switches, errors, reconnect, blocks), `registry.md` (portfolio numbers), `run.md` (every command),
`services.md` (what each service does and that they do not affect each other), `examples/` (your two scripts as short files).
