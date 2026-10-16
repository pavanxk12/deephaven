# START THE SHARED SERVICES ONCE, for every script and the UI. Run this first, one time a day (or put it at the end of init.py).
# Paste in the Deephaven console after:   import algo; algo.install(globals(), "NSE")
#
# The scripts (03_algo_template.py, 04_nifty_multistrike.py ...) do NOT start any of this. They only connect to it and switch their
# own sending on, so any number of scripts can start and stop without disturbing each other or restarting a service.

# ---- data: one set for all the scripts -----------------------------------------------------------------------------------
master_build("NSE")
candles_start("1min", expiries=1)        # futures + the nearest option expiry of every underlying: about 0.7 GB, not 1.8 GB a day
#   Candles are the only market data service. A script that needs more than 1-minute bars rolls the 1-minute candles up itself (5 minutes = five
#   candles); one that needs finer bars needs the service started finer ("30s"), which restarts it and doubles the rows.
#   Indicators (RSI, EMA ...) are not a service: each script computes the ones it needs from the candles it reads (see 03_algo_template.py).

# ---- the shared services -----------------------------------------------------------------------------------------------
registry_start()                         # portfolio numbers, unique per user. Calling it again does nothing.
nats_start("192.168.1.130", 4222, users=[3, 4])   # ALL the users that will send, from every script. Calling it again does nothing.
#   - the master switch is ON, but every script and the UI starts OFF and has to switch itself on
#   - one service means one NATS server and one subscribe payload (examples/README.md gaps 1 and 2): your NIFTY script used .104 and its own payload

straddle_start(0)                        # optional: the straddle series for the nearest option expiry (it is built from the candles)
algo_status()
# To change it on purpose (this cuts every script off for a moment and switches them all OFF again):  nats_restart(port=4223)
# To let one more user send without a restart:                                                         nats_add_user(5)
# Emergency stop for everyone:  nats_send(False)        and back:  nats_send(True)
