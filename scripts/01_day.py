# Start of day: run this first. Order matters only in that each line needs the ones above it.
# Paste into the Deephaven console (or open from the File Explorer and run).
from datetime import time as dtime

import algo

app = algo.connect(globals(), market="NSE")

app.run(algo.Master())                      # contract master -> `master`
app.run(algo.Candles())                     # 1-minute candles from fo_market_data -> `candles`
app.run(algo.Indicators())                  # RSI_9, EMA_9, EMA_18 + Pct on 1-minute bars -> `indicators`
app.run(algo.Straddle())                    # V / D straddle series -> `straddle_series`
app.run(algo.Eod(auto_at=dtime(15, 35)))    # saves candles + straddle_series at 15:35 (manual: app["eod"].save())
app.status()
