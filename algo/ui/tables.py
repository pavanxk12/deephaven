"""Table queries behind the operator console. Pure Deephaven queries: no UI, no globals, constants inline."""
from __future__ import annotations

import pandas as pd


def symbol_table(symbol_of: dict):
    """token -> Symbol lookup as a static table (joined in the queries instead of calling Python per row)."""
    from deephaven.pandas import to_table
    return to_table(pd.DataFrame({"token": pd.Series(list(symbol_of), dtype="int64"),
                                  "Symbol": pd.Series(list(symbol_of.values()), dtype="object")}))


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
        .natural_join(feed, on="token", joins="LtpRaw = ltp")
        .natural_join(symbols, on="token", joins="SymbolRaw = Symbol")
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
         .view(["OrderId = (long) order_number", "PF", "tcode", "token_no", "buy_sell", "volume", "price", order_time])
         .last_by("OrderId"))
    if conds:
        t = t.where(conds)
    return (
        t
        .natural_join(fills, on="OrderId", joins="FilledQty")
        .natural_join(symbols, on="token_no = token", joins="SymbolRaw = Symbol")
        .update_view("Rem = volume - (isNull(FilledQty) ? 0L : FilledQty)")
        .where("tcode != 20075 && Rem > 0")
        .update([
            "Symbol = isNull(SymbolRaw) ? String.valueOf(token_no) : SymbolRaw",
            "Side = buy_sell == 1 ? `BUY` : `SELL`",
            f"Px = price / {price_div}",
            "Status = Rem < volume ? `PART` : `OPEN`",
            f"Time = formatDateTime(epochSecondsToInstant({order_time}), timeZone(`{tz}`)).substring(11, 19)",
        ])
        .view([order_time, "Time", "OrderId", "PF", "Side", "Symbol", "volume", "Rem", "Px", "Status"])
        .sort(order_time)
    )
