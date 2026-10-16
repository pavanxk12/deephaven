# Operator console (needs strategy_events, orders_trade, orders_ack from init.py, and the registry running: registry_start()).
#   operator_console     all strategies, with a strategy picker and a time window
#   console_<strategy>   one strategy's OWN summary, net positions, message window and order book (made for every strategy that has
#                        portfolios, and by algo_start for a new one; console_open("name") makes one by hand)
# A portfolio belongs to the strategy the registry recorded for it (the name given to algo_start / pf_create).
import algo

app = algo.connect(globals(), market="NSE")
app.run(algo.Console())
