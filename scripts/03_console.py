# Operator console -> `operator_console` (needs strategy_events, orders_trade, orders_ack from init.py)
import algo

app = algo.connect(globals(), market="NSE")
app.run(algo.Console())
