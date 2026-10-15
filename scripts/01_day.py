# Start of day, after 00_install.py. Each line is a function call; run the ones you want.
master_build("NSE")                    # contract master for today; the exchange ("NSE" or "US") sets timezone, prices, session
candles_start("1min")                 # "30s", "1min", "5min" or "15min"
indicators(EMA(9), EMA(18), RSI(9))   # call again later to ADD more, e.g. indicators(EMA(6), RSI(14, "5min"))
straddle_start(0)                     # nearest option expiry only; 1 = two nearest, 2 = three nearest ...

# algo_status()                       what is running
# indicators()                        what is being computed
# indicators_remove("EMA_6")          stop one
# candles_start("5min")               restarts candles on the new size; indicators / straddle / strategy follow by themselves
