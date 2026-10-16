# LOOK THINGS UP. Plain function calls: nothing to define, nothing to start. Paste in the terminal after 00_services.py
# (which ran  import algo; algo.install(globals(), "NSE")  and started the master and candles).
#
# Every call prints what it found AND returns it, so you can use the result in your own code. An unknown name prints a message.

# ---- the contract master: any contract you want ---------------------------------------------------------------------------
contract("NIFTY")                           # a bare name = its nearest future: token, symbol, expiry, tick, lot
expiries("NIFTY")                           # the option expiry dates that have not passed
strikes("NIFTY", expiry=0, type="CE")       # the strikes and tokens of the nearest expiry (expiry=1 is the next, or "2026-10-27")
nearest_strikes("NIFTY", n=3)               # the 3 call strikes nearest the price (the price = the future's last candle close)
contracts("NIFTY", type="CE", strike=25000) # that strike in every expiry
contracts("NIFTY", kind="FUT")              # filters: underlying, kind (FUT / OPT / EQ), expiry, type, strike;  limit=None shows all

# ---- the candles that are running -----------------------------------------------------------------------------------------
last_price("NIFTY")                         # the latest candle close
last_candles("NIFTY", 5)                    # the last 5 candles (Minute, Open, High, Low, Close, Volume)
# Indicators are not a service: compute them from candles in your script (03_algo_template.py does RSI), e.g. from
#   closes = last_candles("NIFTY", 200, show=False)["Close"]

# ---- using a result in your own code --------------------------------------------------------------------------------------
near = nearest_strikes("NIFTY", type="CE")  # a table: strike, token, symbol
token = int(near["token"].iloc[0])
info = contract(token)                      # a dict
print("the nearest call is", info["symbol"], "tick", info["tick"], "lot", info["lot"], "last", last_price(token))

# algo_help() lists every function by service;  algo_help("contract") describes one.
