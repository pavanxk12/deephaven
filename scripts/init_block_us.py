

# =====================================================================================================================
# algo: the day's start-up for the US server, run when the Deephaven container comes up. Same as the India server's block, with the US market and this server's NATS address.
# Every step is guarded: a failure here is printed and never stops the rest of init.py or the server.
#   master   today's contracts (New York date): Postgres (resultset_<today>) first, the .db3 file when Postgres cannot answer
#   candles  1-minute candles     registry  portfolio numbers     nats  the one NATS service for users 3 and 4     console  operator_console + one window per strategy
# The image sets ALGO_MARKET=US and ALGO_NATS_IP=192.168.1.131; they are written out here as well so this block says what it does.
# =====================================================================================================================
def _algo_day_start():
    import algo
    algo_app = algo.install(globals(), "US")
    steps = (("master", lambda: master_build("US", source="auto")),
             ("candles", lambda: candles_start("1min")),
             ("registry", lambda: registry_start()),
             ("nats", lambda: nats_start("192.168.1.131", 4222, users=[3, 4])),
             ("console", lambda: algo_app.run(algo.Console())),
             ("status", lambda: algo_status()))
    for label, step in steps:
        try:
            step()
        except Exception as e:
            print(f"[algo] start-up step '{label}' failed: {e}")
            print(traceback.format_exc())


try:
    _algo_day_start()
except Exception as _e:
    print(f"[algo] start-up skipped: {_e}")
    print(traceback.format_exc())
