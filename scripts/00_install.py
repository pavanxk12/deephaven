# Once per session (or put the two lines at the end of init.py): makes master_build(), candles_start(),
# straddle_start(), contract(), pf_create() ... available as plain functions (algo_help() lists them).
import algo
algo.install(globals(), "NSE")
