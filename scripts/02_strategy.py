# Automation: one shared book + bridge, one strategy per universe. Sending starts OFF (compute only);
# switch it on from the console panel or with app["bridge"].send_on().
import algo

app = algo.connect(globals(), market="NSE")

app.run(algo.Book(pf_start=1111))                                   # portfolio numbers + state files
app.run(algo.Bridge(algo.BridgeConfig(send_on=False)))              # NATS; dry_run=True = no connection at all
app.run(algo.RsiOptions(algo.RsiOptionsConfig.stocks()))            # stocks from stocks.txt
# app.run(algo.RsiOptions(algo.RsiOptionsConfig.nifty()))           # NIFTY, strike from the synthetic future

# app["rsi_stocks"].status()      app["rsi_stocks"].sell("RELIANCE 27OCT26 FUT")      app["bridge"].send_on()
app.status()
