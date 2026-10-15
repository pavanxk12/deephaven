# Once per session (or put the two lines at the end of init.py): makes master_build(), candles_start(), indicators(),
# straddle_start(), EMA(), RSI() ... available as plain functions.
import algo
algo.install(globals(), "NSE")
