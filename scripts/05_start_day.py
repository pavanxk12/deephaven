# The whole day on a timer (India): paste ONCE per Deephaven session; strategy scripts are pasted after it.
#   09:00:00  master    the contracts of today, from Postgres (resultset_<today>), else the .db3 file
#   09:14:30  candles   1-minute candles from the live feed, and right after them: the registry, the NATS service (users 3 and 4; the master switch is ON,
#                       every strategy starts OFF and switches itself on) and the console (operator_console and one window per strategy)
#   15:30:00  close     the candles and straddle tables are SAVED (candles_<day>.parquet, straddle_<day>.parquet), then the straddle and the candles are stopped
# A step whose time has already passed when you paste this runs at once (a candle builder is not started after 15:30); a step that fails is tried again every minute.
# Saturdays and Sundays are skipped. Other times: day_auto("08:45", "09:10", "09:10:30", "15:35");  day_auto(stop=True) switches it off. See run.md section 0.
# The Postgres login is the container's own AUTOMATION_DATABASE_URL and the driver is the volume's psycopg (run.md section 1).
import algo

app = algo.install(globals(), "NSE")
day_auto()
