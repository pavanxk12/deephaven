# Start of day, after 00_install.py. Each line is a function call; run the ones you want. (examples/00_services.py is the full version.)
master_build("NSE")                    # contract master for today; the exchange ("NSE" or "US") sets timezone, prices, session
candles_start("1min", 0.01)           # "30s", "1min", "5min" or "15min"; 0.01 = the feed is in paise x100 -> proper rates
straddle_start(0)                     # nearest option expiry only; 1 = two nearest, 2 = three nearest ...

# eod_save()                          save the whole tables to Parquet right now;  eod_load("2026-10-15") loads a day back
# algo_status()                       what is running
# candles_start("5min")               restarts candles on the new size; the straddle follows by itself
eod_auto()                           # saves the whole candles + straddle tables to Parquet every day after the close
# Indicators are not a service: a script computes the ones it needs from the candles (examples/03_algo_template.py).
