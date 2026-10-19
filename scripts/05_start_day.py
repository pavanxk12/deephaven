# The whole morning in one paste: data, the shared services and the console. Run it ONCE per Deephaven session; strategy scripts are pasted after it.
#   master     the contracts of today, read from Postgres (resultset_<today>); source="auto" would fall back to the .db3 file when Postgres cannot answer
#   candles    1-minute candles from the live feed
#   registry   portfolio numbers, unique per user
#   nats       the one NATS service, for users 3 and 4 (the master switch is ON; every strategy starts OFF and switches itself on)
#   console    operator_console and one console_<strategy> window per strategy (needs strategy_events, orders_trade, orders_ack and the feed from init.py)
# The Postgres login is the container's own AUTOMATION_DATABASE_URL and the driver is the volume's psycopg (run.md section 1). The host's init.py runs these same lines at every start.
import algo

app = algo.install(globals(), "NSE")
master_build("NSE", source="postgres")           # (a second positional argument would be the DAY, so the source is named)
candles_start("1min")
registry_start()
nats_start("192.168.1.130", 4222, users=[3, 4])
app.run(algo.Console())
algo_status()
