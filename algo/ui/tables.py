"""Table queries behind the operator console. Pure Deephaven queries: no UI, no globals, constants inline."""
from __future__ import annotations


from algo.data.symbols import symbol_table  # noqa: F401,E402  (token -> Symbol lookup table, shared with the candles)


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


def net_positions(trades, feed, symbols, price_div: float):
    """One row per traded token with live LTP from the feed (one row per token, columns token and ltp)."""
    from deephaven import agg
    return (
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
        .update([
            "Symbol = isNull(SymbolRaw) ? String.valueOf(token) : SymbolRaw",
            f"Ltp = LtpRaw / {price_div}",
            "BuyAvg = BuyQty > 0 ? BuyVal / BuyQty : 0.0",
            "SellAvg = SellQty > 0 ? SellVal / SellQty : 0.0",
            "NetQty = BuyQty - SellQty",
            "Realised = Math.min(BuyQty, SellQty) * (SellAvg - BuyAvg)",
            "M2M = isNull(LtpRaw) ? NULL_DOUBLE : SellVal - BuyVal + NetQty * Ltp",
            # instrument kind from the symbol text: ...CE / ...PE = option, contains FUT = future, else equity
            "Kind = (Symbol.endsWith(`CE`) || Symbol.endsWith(`PE`)) ? `OPT` : Symbol.contains(`FUT`) ? `FUT` : `EQ`",
        ])
        .view(["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal",
               "NetQty", "Ltp", "Realised", "M2M", "Fills", "Kind"])
        .sort("Symbol")
    )


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
