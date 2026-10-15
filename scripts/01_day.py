# Start of day, after 00_install.py. Each line is a function call; run the ones you want.
master_build("NSE")                    # contract master for today; the exchange ("NSE" or "US") sets timezone, prices, session
candles_start("1min", 0.01)           # "30s", "1min", "5min" or "15min"; 0.01 = the feed is in paise x100 -> proper rates
indicators(EMA(9), EMA(18), RSI(9))   # call again later to ADD more, e.g. indicators(EMA(6), RSI(14, "5min"))
straddle_start(0)                     # nearest option expiry only; 1 = two nearest, 2 = three nearest ...

# eod_save()                          save the whole tables to Parquet right now;  eod_load("2026-10-15") loads a day back
# algo_status()                       what is running
# indicators()                        what is being computed
# indicators_remove("EMA_6")          stop one
# candles_start("5min")               restarts candles on the new size; indicators / straddle / strategy follow by themselves
eod_auto()                           # saves the whole candles + straddle tables to Parquet every day after the close
