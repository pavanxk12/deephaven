# A TRADE BY HAND with plain function calls: look the option up, take a portfolio number, send the commands.
# Paste in the terminal after 00_services.py has started the services. Nothing here defines a class or a function.
#
# Typed commands need only the master switch (nats_start turns it ON). Each one prints how it went; none of them raises.
USER = 4                                    # the user to trade as (must be one of the users given to nats_start)

# 0. CONNECT: these only CHECK that the NATS service is up (and this user may send) and that the registry is up; they start nothing
nats_connect(user=USER)
registry_connect()

# 1. WHICH option: the call nearest the price
opt = int(nearest_strikes("NIFTY", type="CE")["token"].iloc[0])
contract(opt)                               # check what it is before sending anything

# 2. A portfolio number: the next free one for this user, remembered with the strategy and the name
pf = pf_create(USER, "manual", "NIFTY")

# 3. The commands, in the order the bridge needs them. Each returns after the bridge answers and prints how it went.
pf_subscribe(pf, opt, user=USER)
pf_pause(pf, user=USER)
pf_apply(pf, user=USER, ival=1.0, jval=0.75, sval=3.0, trp=101.0, lowerrange=80.0, upperrange=120.0, maxqty=3, side="BUY",
         jqty1=1, jqty2=1, target=1000, stoploss=-400, maxloss=-400, max_unhedge=3, PRNT=1)
pf_unpause(pf, user=USER)

pf_list(USER)                               # what the registry remembers: number, name, algo

# To stop it again (also by hand):
#   pf_pause(pf, user=USER); pf_squareup(pf, user=USER); pf_unsubscribe(pf, opt, user=USER)
# If a line prints "NOT ok - timeout" it is not known whether the bridge acted: look before sending it again.
# To size the order from the option's price instead of typing numbers, write your own function (03_algo_template.py has order_params) and:
#   pf_apply(pf, user=USER, maxqty=3, **params)
