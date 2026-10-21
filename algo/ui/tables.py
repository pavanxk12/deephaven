"""Table queries behind the operator console. Pure Deephaven queries: no UI, no globals, constants inline."""
from __future__ import annotations


from algo.data.symbols import kind_table, symbol_table  # noqa: F401,E402  (token -> Symbol and token -> Kind lookup tables)


def strategy_map(registry, per_user: int):
    """portfolio_registry -> (PfKey, Strategy): the PF as it is written in the order / trade tables (user * per_user + number) and the
    strategy (the algo) recorded for it. A portfolio with no algo has no row, so its trades carry no strategy."""
    return registry.view([f"PfKey = (long) (User * {per_user} + PF)", "Strategy = Algo"]).where("Strategy != ``")


def tag(table, pf_col: str, smap):
    """Add a `Strategy` column to a table that has a PF column. Follows the registry live: a trade of a portfolio that is registered a
    moment later is tagged as soon as the registry row arrives. Rows of an unknown portfolio get a null Strategy."""
    return table.update_view(f"PfKey = (long) {pf_col}").natural_join(smap, on="PfKey", joins="Strategy").drop_columns("PfKey")


def messages(events, cols: dict, tz: str):
    """LOGMSG rows of strategy_events, classified. `Ts` is kept so the time filter can use it."""
    return (
        events
        .view([f"Seq = {cols['seq']}", f"Ts = {cols['ts']}", f"Pf = {cols['pf']}", f"Raw = {cols['text']}"])
        .where("Raw.contains(`MSGTYPE=LOGMSG`)")
        .update_view([
            f"Time = formatDateTime(Ts, timeZone(`{tz}`)).substring(11, 23)",
            "Type = (Raw.contains(`CAT=ERR`) || Raw.contains(`CAT=ALERT`) || Raw.contains(`REJECT`) || Raw.contains(`Reject`)"
            " || Raw.contains(`ERROR`) || Raw.contains(`Error`) || Raw.contains(`FAILED`) || Raw.contains(`Failed`)) ? `ERROR`"
            " : Raw.contains(`CAT=EVNT`) ? `EVENT`"
            " : Raw.contains(`FILLED`) ? `TRADE`"
            " : Raw.contains(`CAT=TRD`) ? `ORDER`"
            " : `INFO`",
            "Msg = Raw.contains(`MSG=`)"
            " ? Raw.substring(Raw.indexOf(`MSG=`) + 4, Raw.contains(`,COLOR=`) ? Raw.lastIndexOf(`,COLOR=`) : Raw.length())"
            " : Raw.substring(Raw.lastIndexOf(`LOGMSG,`) + 7)",
        ])
        .view(["Seq", "Ts", "Time", "Type", "Pf", "Msg"])
    )


def expense_formula(rates) -> str:
    """A Deephaven formula for the expense of a position row from its Kind, BuyVal and SellVal. `rates` is {"FUT": (buy, sell), ...} in rupees per crore (1.0E7) of the
    value bought / sold; a kind without a rate, or no rates at all, costs nothing."""
    if not rates:
        return "0.0"
    out = "0.0"
    for kind, (buy, sell) in reversed(list(rates.items())):
        out = f"Kind == `{kind}` ? BuyVal * {float(buy) / 1.0e7:.12f} + SellVal * {float(sell) / 1.0e7:.12f} : {out}"
    return out


def net_positions(trades, feed, symbols, price_div: float, expense=None, kinds=None):
    """One row per traded token with live LTP from the feed (one row per token, columns token and ltp).
    M2M is the gross result (realised + unrealised). `expense` ({"FUT": (buy, sell) ...} rupees per crore) adds Expense and NetM2M = M2M - Expense.
    `kinds` is the contract master's table (token, Kind = OPT / FUT / EQ): the kind of each row is taken from it. A token the master does not know (or no table at all) falls back to the symbol text."""
    from deephaven import agg
    t = (
        trades
        .where("fill_quantity > 0")
        .update_view([
            "BuyQty = buy_sell == 1 ? fill_quantity : 0L",
            "SellQty = buy_sell == 2 ? fill_quantity : 0L",
            f"BuyVal = buy_sell == 1 ? fill_quantity * fill_price / {price_div} : 0.0",
            f"SellVal = buy_sell == 2 ? fill_quantity * fill_price / {price_div} : 0.0",
        ])
        .agg_by([agg.sum_(["BuyQty", "SellQty", "BuyVal", "SellVal"]), agg.count_("Fills")], by=["token"])
        .update_view("TokenL = (long) token")                       # the tables disagree on int / long; a join needs the same type on both sides
        .natural_join(feed.view(["TokenL = (long) token", "ltp"]), on="TokenL", joins="LtpRaw = ltp")
        .natural_join(symbols, on="TokenL = token", joins="SymbolRaw = Symbol")
    )
    t = t.natural_join(kinds, on="TokenL = token", joins="MasterKind = Kind") if kinds is not None else t.update_view("MasterKind = (String) null")
    return (
        t
        .update([
            "Symbol = isNull(SymbolRaw) ? String.valueOf(token) : SymbolRaw",
            f"Ltp = LtpRaw / {price_div}",
            "BuyAvg = BuyQty > 0 ? BuyVal / BuyQty : 0.0",
            "SellAvg = SellQty > 0 ? SellVal / SellQty : 0.0",
            "NetQty = BuyQty - SellQty",
            "Realised = Math.min(BuyQty, SellQty) * (SellAvg - BuyAvg)",
            "M2M = isNull(LtpRaw) ? NULL_DOUBLE : SellVal - BuyVal + NetQty * Ltp",
            # the kind comes from the contract master; only a token the master does not know is read from the symbol text ("... CE" / "... PE" = option, "... FUT" = future,
            # else equity; the SPACE matters: RELIANCE ends in CE and is a stock)
            "Kind = !isNull(MasterKind) ? MasterKind : (Symbol.endsWith(` CE`) || Symbol.endsWith(` PE`)) ? `OPT` : Symbol.endsWith(` FUT`) ? `FUT` : `EQ`",
        ])
        .update_view(["Expense = " + expense_formula(expense), "NetM2M = isNull(M2M) ? NULL_DOUBLE : M2M - Expense"])
        .view(["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal",
               "NetQty", "Ltp", "Realised", "M2M", "Expense", "NetM2M", "Fills", "Kind"])
        .sort("Symbol")
    )


def strategy_totals(trades, feed, price_div: float):
    """One row per strategy: `Turnover` (rupees, buys + sells) and gross `M2M` of its whole day, live with the trades and the feed.
    The same sums as the net positions, taken per (Strategy, token) and then per Strategy."""
    from deephaven import agg
    per_token = (
        trades
        .where("fill_quantity > 0 && !isNull(Strategy)")
        .update_view([
            "BuyQty = buy_sell == 1 ? fill_quantity : 0L",
            "SellQty = buy_sell == 2 ? fill_quantity : 0L",
            f"BuyVal = buy_sell == 1 ? fill_quantity * fill_price / {price_div} : 0.0",
            f"SellVal = buy_sell == 2 ? fill_quantity * fill_price / {price_div} : 0.0",
        ])
        .agg_by([agg.sum_(["BuyQty", "SellQty", "BuyVal", "SellVal"])], by=["Strategy", "token"])
        .update_view("TokenL = (long) token")
        .natural_join(feed.view(["TokenL = (long) token", "ltp"]), on="TokenL", joins="LtpRaw = ltp")
        .update_view([
            "NetQty = BuyQty - SellQty",
            f"M2M = isNull(LtpRaw) ? NULL_DOUBLE : SellVal - BuyVal + NetQty * (LtpRaw / {price_div})",
        ])
    )
    return per_token.agg_by([agg.sum_(["BuyVal", "SellVal", "M2M"])], by="Strategy").view(["Strategy", "Turnover = BuyVal + SellVal", "M2M"])


def limits_view(strategies, limits_last, totals):
    """The table `strategy_limits`: one row per strategy with its two limits, where it stands and whether a limit is breached. Live."""
    return (
        strategies
        .natural_join(limits_last, on="Strategy", joins="TurnoverLimit, M2MLimit, TurnoverHit, M2MHit")
        .natural_join(totals, on="Strategy", joins="Turnover, M2M")
        .update_view([
            "TurnoverUsedPct = (isNull(TurnoverLimit) || TurnoverLimit == 0 || isNull(Turnover)) ? NULL_DOUBLE : 100.0 * Turnover / TurnoverLimit",
            "Status = ((TurnoverHit != null && TurnoverHit) || (M2MHit != null && M2MHit)) ? `BREACHED`"
            " : (isNull(TurnoverLimit) && isNull(M2MLimit)) ? `no limit` : `ok`",
        ])
        .view(["Strategy", "Status", "TurnoverLimit", "Turnover", "TurnoverUsedPct", "M2MLimit", "M2M", "TurnoverHit", "M2MHit"])
        .sort("Strategy")
    )


def pf_by_option(states_log, smap, per_user: int):
    """`pf_by_option`: for every option token, the portfolio that trades it (a subscribed one first, then the latest change) with its state, tagged with its strategy.
    `states_log` has one row per confirmed change (User, PF, Option, Subscribed, Paused, Squared, By, Cmd, AtN); it is the table version of algo.core.pfstate."""
    from deephaven import agg
    latest = (
        states_log.last_by(["User", "PF"])
        .where("!isNull(Option)")
        .update_view([f"PfKey = (long) (User * {per_user} + PF)", "Rank = (Subscribed ? 1000000000000000L : 0L) + AtN"])
        .natural_join(smap, on="PfKey", joins="Strategy")
    )
    best = latest.agg_by([agg.max_("MaxRank = Rank")], by="Option")
    return (latest.natural_join(best, on="Option", joins="MaxRank").where("Rank == MaxRank")
            .last_by("Option")                                         # one row per option even if two portfolios tie
            .view(["Strategy", "Option", "User", "PF", "Subscribed", "Paused", "Squared", "By"]))


def fills_per_order(trades):
    """Fills per order over the WHOLE day, so Remaining is right whatever the time window is."""
    from deephaven import agg
    return (trades.agg_by([agg.sum_("FilledQty = fill_quantity")], by="response_order_number")
            .view(["OrderId = (long) response_order_number", "FilledQty"]))


def order_book(acks, fills, symbols, price_div: float, order_time: str, tz: str, conds=None):
    """Latest ack per order, not cancelled (tcode 20075), still has quantity left after the fills."""
    t = (acks
         .view(["OrderId = (long) order_number", "PF", "Strategy", "tcode", "token_no", "TokenL = (long) token_no", "buy_sell", "volume", "price", order_time])
         .last_by("OrderId"))
    if conds:
        t = t.where(conds)
    return (
        t
        .natural_join(fills, on="OrderId", joins="FilledQty")
        .natural_join(symbols, on="TokenL = token", joins="SymbolRaw = Symbol")
        .update_view("Rem = volume - (isNull(FilledQty) ? 0L : FilledQty)")
        .where("tcode != 20075 && Rem > 0")
        .update([
            "Symbol = isNull(SymbolRaw) ? String.valueOf(token_no) : SymbolRaw",
            "Side = buy_sell == 1 ? `BUY` : `SELL`",
            f"Px = price / {price_div}",
            "Status = Rem < volume ? `PART` : `OPEN`",
            f"Time = formatDateTime(epochSecondsToInstant({order_time}), timeZone(`{tz}`)).substring(11, 19)",
        ])
        .view([order_time, "Time", "OrderId", "Strategy", "PF", "Side", "Symbol", "volume", "Rem", "Px", "Status"])
        .sort(order_time)
    )
