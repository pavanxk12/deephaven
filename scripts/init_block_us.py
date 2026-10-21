

# =====================================================================================================================
# algo: the day on a timer (day_auto) for the US server, set up when the Deephaven container comes up. The same as the India server's block, with the US market.
# New York time, weekdays: 09:15:00 master (Postgres resultset_<today>, else the .db3)   09:29:30 candles, then registry, NATS (users 3 and 4), console
#                          16:00:00 the candles + straddle tables are saved to /data/fo_market_data, then the straddle and the candles are stopped
# A step whose time has passed when the container starts runs at once; one that fails is tried again every minute (10 times). day_auto(stop=True) switches it off.
# The image sets ALGO_MARKET=US and ALGO_NATS_IP=192.168.1.131. A failure here is printed and never stops the rest of init.py or the server.
# =====================================================================================================================
def _algo_day_start():
    import algo
    algo.install(globals(), "US")
    day_auto()


try:
    _algo_day_start()
except Exception as _e:
    print(f"[algo] start-up skipped: {_e}")
    print(traceback.format_exc())
