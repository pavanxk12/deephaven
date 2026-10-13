import threading

from deephaven import ui, agg

# ===========================================================================
# OPERATOR CONSOLE: summary + message window + order book + net positions, ONE script, ONE dashboard.
#
# Needs in your session before running:
#   master          from build_master.py        (master["symbol_of"]: token -> symbol)
#   strategy_events KafkaPartition, KafkaOffset, KafkaTimestamp, PF, Payload
#   orders_trade    fills                       (token, buy_sell, fill_quantity, fill_price, response_order_number)
#   orders_ack      order confirmations         (order_number, token_no, tcode 20073/20074/20075, volume, price, buy_sell)
#   fo_market_data  live prices, one row per token (token, ltp)
#                   if it holds repeated ticks per token, change it to fo_market_data.last_by("token") below
#   automation.py   (same session) for the Controls panel and the per-position controls
# ===========================================================================

# ---------- settings ----------
SYMBOL_OF = master["symbol_of"]
PRICE_DIV = 100.0                      # NSE prices arrive in paise

C_SEQ, C_TS, C_PF, C_TEXT = "KafkaOffset", "KafkaTimestamp", "PF", "Payload"   # strategy_events columns

GREEN, RED, BLUE, ORANGE, AMBER, PURPLE, GREY, INK = (
    "#1a7f37", "#cf222e", "#1f5fbf", "#c2410c", "#b45309", "#8250df", "#6e7781", "#1f2328")


def symbol_of(token: int) -> str:
    return SYMBOL_OF.get(int(token), str(token))


# ===========================================================================
# TABLES (each is one lazy chain; per-row columns use update_view so nothing per-row is stored)
# ===========================================================================

# ---- messages: LOGMSG rows of strategy_events, classified ----
messages = (
    strategy_events
    .view([f"Seq = {C_SEQ}", f"Ts = {C_TS}", f"Pf = {C_PF}", f"Raw = {C_TEXT}"])
    .where("Raw.contains(`MSGTYPE=LOGMSG`)")
    .update_view([
        "Time = formatDateTime(Ts, timeZone(`Asia/Kolkata`)).substring(11, 23)",
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
    .view(["Seq", "Time", "Type", "Pf", "Msg"])
)

# ---- net positions: one row per traded token, live LTP joined straight from fo_market_data ----
net_positions_table = (
    orders_trade
    .where("fill_quantity > 0")
    .update_view([
        "BuyQty = buy_sell == 1 ? fill_quantity : 0L",
        "SellQty = buy_sell == 2 ? fill_quantity : 0L",
        "BuyVal = buy_sell == 1 ? fill_quantity * fill_price / PRICE_DIV : 0.0",
        "SellVal = buy_sell == 2 ? fill_quantity * fill_price / PRICE_DIV : 0.0",
    ])
    .agg_by([agg.sum_(["BuyQty", "SellQty", "BuyVal", "SellVal"]), agg.count_("Fills")], by=["token"])
    .natural_join(fo_market_data, on="token", joins="LtpRaw = ltp")
    .update([
        "Symbol = symbol_of(token)",
        "Ltp = LtpRaw / PRICE_DIV",
        "BuyAvg = BuyQty > 0 ? BuyVal / BuyQty : 0.0",
        "SellAvg = SellQty > 0 ? SellVal / SellQty : 0.0",
        "NetQty = BuyQty - SellQty",
        "Realised = Math.min(BuyQty, SellQty) * (SellAvg - BuyAvg)",
        "M2M = isNull(LtpRaw) ? NULL_DOUBLE : SellVal - BuyVal + NetQty * Ltp",
        # instrument kind from the symbol text: ...CE / ...PE = option, contains FUT = future, else equity
        "Kind = (Symbol.endsWith(`CE`) || Symbol.endsWith(`PE`)) ? `OPT` : Symbol.contains(`FUT`) ? `FUT` : `EQ`",
    ])
    .view(["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal", "NetQty", "Ltp", "Realised", "M2M", "Fills", "Kind"])
    .sort("Symbol")
)

# ---- order book: latest ack per order, not cancelled, still has quantity left after fills ----
order_book_table = (
    orders_ack
    .view(["OrderId = (long) order_number", "PF", "tcode", "token_no", "buy_sell", "volume", "price", "last_modified"])
    .last_by("OrderId")
    .natural_join(
        orders_trade.agg_by([agg.sum_("FilledQty = fill_quantity")], by="response_order_number")
        .view(["OrderId = (long) response_order_number", "FilledQty"]),
        on="OrderId", joins="FilledQty")
    .update_view("Rem = volume - (isNull(FilledQty) ? 0L : FilledQty)")
    .where("tcode != 20075 && Rem > 0")
    .update([
        "Symbol = symbol_of(token_no)",
        "Side = buy_sell == 1 ? `BUY` : `SELL`",
        "Px = price / PRICE_DIV",
        "Status = Rem < volume ? `PART` : `OPEN`",
        "Time = formatDateTime(epochSecondsToInstant(last_modified), timeZone(`Asia/Kolkata`)).substring(11, 19)",
    ])
    .view(["last_modified", "Time", "OrderId", "PF", "Side", "Symbol", "volume", "Rem", "Px", "Status"])
    .sort("last_modified")
)



# ===========================================================================
# FORMATS
# ===========================================================================
TYPE_COLOR = {"TRADE": GREEN, "ORDER": BLUE, "EVENT": PURPLE, "ERROR": RED, "INFO": GREY}
TYPE_TINT = {"TRADE": "#e6f4ea", "ORDER": "#eaf1fc", "EVENT": "#f1ebfb", "ERROR": "#fde8e8", "INFO": "#ffffff"}

MSG_FORMATS = (
    [ui.TableFormat(if_=f"Type == `{k}`", background_color=TYPE_TINT[k], color=INK) for k in TYPE_COLOR]
    + [ui.TableFormat(cols="Type", if_=f"Type == `{k}`", background_color=TYPE_COLOR[k], color="#ffffff") for k in TYPE_COLOR]
    + [
        ui.TableFormat(cols="Msg", if_="Msg.contains(`BUY`) || Msg.contains(`BOUGHT`)", color=BLUE),
        ui.TableFormat(cols="Msg", if_="Msg.contains(`SELL`) || Msg.contains(`SOLD`)", color=ORANGE),
        ui.TableFormat(cols="Msg", if_="Type == `ERROR`", color=RED),          # last, so it wins
    ]
)

ORD_FORMATS = [
    ui.TableFormat(cols=["volume", "Rem"], value="#,##0"),
    ui.TableFormat(cols="Px", value="#,##0.00"),
    ui.TableFormat(cols=["Side", "Symbol"], if_="Side == `BUY`", color=BLUE),
    ui.TableFormat(cols=["Side", "Symbol"], if_="Side == `SELL`", color=ORANGE),
    ui.TableFormat(cols="Status", if_="Status == `OPEN`", color=GREEN),
    ui.TableFormat(cols="Status", if_="Status == `PART`", color=AMBER),
]


# ===========================================================================
# WINDOWS
# ===========================================================================
def tile(label, text, color):
    # the grid cannot change font size, so the summary is drawn as tiles: small label, big number
    return ui.html.div(
        ui.html.div(label, style={"fontSize": "13px", "fontWeight": "600", "color": GREY, "letterSpacing": "0.5px"}),
        ui.html.div(text, style={"fontSize": "40px", "fontWeight": "700", "color": color, "lineHeight": "1.15"}),
        style={"flex": "1", "minWidth": "170px", "padding": "8px 16px", "borderRight": "1px solid #d0d7de"},
    )


@ui.component
def summary_window():
    # built here so nothing outside this panel needs to change
    totals = ui.use_memo(
        lambda: net_positions_table.agg_by(
            [agg.sum_(["BuyVal", "SellVal", "Realised", "M2M", "Fills"])]
        ).view([
            "Turnover = (BuyVal + SellVal) / 1.0E7",
            "Fills",
            "GrossM2M = M2M",
            "RealisedPnl = Realised",
            "Unrealised = M2M - Realised",
        ]),
        [],
    )
    open_cnt = ui.use_memo(lambda: order_book_table.count_by("OpenOrders"), [])

    row = ui.use_row_data(totals)
    orow = ui.use_row_data(open_cnt)
    v = row if isinstance(row, dict) else {}
    o = orow if isinstance(orow, dict) else {}

    def val(d, key):
        x = d.get(key)
        if x is None or (isinstance(x, float) and x != x):   # None or NaN = missing
            return None
        return x

    def num(d, key, fmt, default="-"):
        x = val(d, key)
        return default if x is None else format(x, fmt)

    def pnl_color(key):
        x = val(v, key)
        return GREEN if x is None or x >= 0 else RED

    return ui.panel(
        ui.flex(
            tile("TURNOVER (₹ CR)", num(v, "Turnover", ",.2f", "0.00"), ORANGE),
            tile("FILLS TODAY", num(v, "Fills", ",.0f", "0"), "#000000"),
            tile("OPEN ORDERS", num(o, "OpenOrders", ",.0f", "0"), "#000000"),
            tile("GROSS M2M (₹)", num(v, "GrossM2M", "+,.0f"), pnl_color("GrossM2M")),
            tile("REALISED (₹)", num(v, "RealisedPnl", "+,.0f", "0"), pnl_color("RealisedPnl")),
            tile("UNREALISED (₹)", num(v, "Unrealised", "+,.0f"), pnl_color("Unrealised")),
            wrap=True, align_items="center", height="100%",
        ),
        title="Summary",
    )

@ui.component
def message_window():
    kind, set_kind = ui.use_state("ALL")            # one of: ALL, TRADE, ORDER, EVENT, ERROR, INFO

    # "" = show all, otherwise the single selected type
    type_filter = "" if kind == "ALL" else kind

    def kind_button(label, value):
        return ui.toggle_button(label, is_selected=kind == value, on_change=lambda _: set_kind(value))

    chips = ui.flex(
        kind_button("All", "ALL"),
        kind_button("Trades", "TRADE"),
        kind_button("Orders", "ORDER"),
        kind_button("Events", "EVENT"),
        kind_button("Errors", "ERROR"),
        kind_button("Info", "INFO"),
        gap="size-100", wrap=True,
    )

    return ui.panel(
        ui.flex(
            chips,
            ui.table(
                messages,
                key=type_filter,                    # new key = the grid re-applies the filter right away
                reverse=True,                       # newest on top, done in the browser
                density="spacious",
                show_search=True,
                quick_filters={"Type": type_filter},
                format_=MSG_FORMATS,
                hidden_columns=["Seq"],
                always_fetch_columns=["Type"],
                column_display_names={"Time": "TIME IST", "Pf": "PF", "Msg": "MESSAGE"},
                frozen_columns=["Time", "Type"],
            ),
            direction="column", gap="size-100", height="100%",
        ),
        title="Message window",
    )

@ui.component
def order_book():
    return ui.panel(
        ui.table(
            order_book_table,
            reverse=True,
            density="spacious",
            show_search=True,
            format_=ORD_FORMATS,
            hidden_columns=["last_modified"],
            column_display_names={"Time": "TIME IST", "OrderId": "ORDER", "volume": "QTY", "Rem": "REMAINING", "Px": "PRICE"},
            frozen_columns=["Time"],
        ),
        title="Order book",
    )


@ui.component
def net_positions():
    hide_flat, set_hide_flat = ui.use_state(False)
    kind, set_kind = ui.use_state("ALL")            # one of: ALL, FUT, OPT, EQ

    def build():
        conds = []
        if hide_flat:
            conds.append("NetQty != 0")
        if kind != "ALL":
            conds.append(f"Kind == `{kind}`")
        return net_positions_table.where(conds) if conds else net_positions_table

    shown = ui.use_memo(build, [hide_flat, kind])

    def kind_button(label, value):
        return ui.toggle_button(label, is_selected=kind == value, on_change=lambda _: set_kind(value))

    return ui.panel(
        ui.flex(
            ui.flex(
                ui.toggle_button("Hide flat", is_selected=hide_flat, on_change=set_hide_flat),
                ui.flex(
                    kind_button("All", "ALL"),
                    kind_button("Futures", "FUT"),
                    kind_button("Options", "OPT"),
                    kind_button("Equity", "EQ"),
                    gap="size-100", wrap=True,
                ),
                gap="size-200", wrap=True, align_items="center",
                UNSAFE_style={"flex": "0 0 auto"},          # keep the button row its own height; the rows get the rest
            ),
            positions_grid(shown),
            direction="column", gap="size-100", height="100%",
        ),
        title="Net positions",
    )

def automation_call(name, *args):
    """Call a function of automation.py by name AT CLICK TIME. automation.py runs in this same Deephaven
    session, so its functions (send_on, send_off, pause_all, exit_all, set_allow, pause_pf, ...) are already global."""
    fn = globals().get(name)
    if fn is None:
        print(f"[console] {name}() not found - run automation.py in this session first")
        return
    fn(*args)


def watch_automation(redraw):
    """Effect for a component: pause / allow state lives in automation.py, so look at it every 2 s
    and call redraw() ONLY when it changed (nothing is drawn while it is idle)."""
    def sig():
        return (tuple(sorted(globals().get("paused_pf", ()))),
                tuple(sorted(globals().get("auto_allow", {}).items())))

    def start():
        stop = threading.Event()

        def loop():
            last = sig()
            while not stop.wait(2):
                now = sig()
                if now != last:
                    last = now
                    redraw()

        threading.Thread(target=loop, daemon=True).start()
        return stop.set

    return start


def small_btn(label, color, on_press, disabled=False):
    return ui.button(label, on_press=on_press, is_disabled=disabled,
                     UNSAFE_style={"background": color, "color": "#ffffff", "border": f"1px solid {color}",
                                   "fontWeight": "bold", "opacity": "0.35" if disabled else "1",
                                   "height": "22px", "minWidth": "0", "padding": "0 6px", "fontSize": "11px"})


POS_COLS = ["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal",
            "NetQty", "Ltp", "Realised", "M2M"]
# symbol (with checkbox) | BUY qty avg value | SELL qty avg value | NET qty ltp realised m2m | Pause | Square up
GRID_COLS = ("minmax(230px, 2.2fr) minmax(64px, 1fr) minmax(70px, 1fr) minmax(100px, 1.4fr) "
             "minmax(64px, 1fr) minmax(70px, 1fr) minmax(100px, 1.4fr) minmax(56px, 0.8fr) "
             "minmax(76px, 1fr) minmax(76px, 1fr) minmax(76px, 1fr) 70px 90px")
BUY_BG, SELL_BG = "#f7faff", "#fffaf6"


def fmt(x, spec):
    return "-" if x is None or x != x else format(x, spec)          # None / NaN -> "-"


def cell(text, color=INK, bg="transparent", bold=False, size="14px"):
    return ui.html.div(text, style={"textAlign": "right", "padding": "4px 8px", "color": color, "background": bg,
                                    "fontSize": size, "fontWeight": "700" if bold else "400"})


def sign_color(x):
    return INK if x is None or x != x or x == 0 else (GREEN if x > 0 else RED)


def grid_row(*cells, **kw):
    return ui.html.div(*cells, **kw, style={"display": "grid", "gridTemplateColumns": GRID_COLS, "alignItems": "center",
                                            "borderBottom": "1px solid #eef1f4"})


def group_cell(text, span, bg):
    return ui.html.div(text, style={"gridColumn": f"span {span}", "textAlign": "center", "background": bg,
                                    "fontSize": "12px", "fontWeight": "700", "padding": "3px"})


def head_cell(text, left=False):
    return ui.html.div(text, style={"textAlign": "left" if left else "right", "padding": "2px 8px",
                                    "fontSize": "12px", "fontWeight": "700", "color": GREY})


@ui.component
def positions_grid(table):
    """Net positions drawn as real rows, so each row can hold a checkbox (left of the symbol) and
    Pause / Square up buttons (right of M2M). Rows = positions only (tens), redrawn at most once per
    server cycle; nothing is stored."""
    src = ui.use_memo(lambda: table.view(POS_COLS), [table])
    data = ui.use_table_data(src)
    armed, set_armed = ui.use_state(None)           # PF whose Square up is waiting for its confirm click
    _, set_version = ui.use_state(0)                # only used to force a redraw

    def redraw():
        set_version(lambda n: n + 1)

    ui.use_effect(watch_automation(redraw), [])

    pf_of = globals().get("PF_OF", {})              # token -> PF, from automation.py
    allowed = globals().get("auto_allow", {})       # token -> True/False, from automation.py
    paused = globals().get("paused_pf", set())      # PFs the bridge confirmed as paused

    d = data if isinstance(data, dict) else {}
    rows = [{c: d[c][i] for c in POS_COLS} for i in range(len(d.get("token", [])))]

    def on_allow(token, net, on):
        if not on and net != 0:                     # rule: cannot block automation while a position is open
            print(f"[console] cannot block automation on token {token}: net position is {net}, square up first")
        else:
            automation_call("set_allow", token, on)
        redraw()

    def on_pause(pf, is_paused):
        set_armed(None)
        automation_call("unpause_pf" if is_paused else "pause_pf", pf)
        redraw()

    def on_square(pf):
        if pf not in globals().get("paused_pf", set()):     # rule: no square up until the PF is paused
            print(f"[console] square up PF {pf} refused: press Pause first")
            set_armed(None)
        elif armed != pf:
            set_armed(pf)                                   # first click arms it, second click sends it
        else:
            set_armed(None)
            automation_call("squareup_pf", pf)
        redraw()

    def line(r):
        token, net = int(r["token"]), int(r["NetQty"])
        pf = pf_of.get(token)                        # None = automation does not trade this token: controls disabled
        has_pf = pf is not None
        is_allowed = allowed.get(token, True)
        is_paused = has_pf and pf in paused
        return grid_row(
            ui.checkbox(str(r["Symbol"]), is_selected=is_allowed, is_disabled=(not has_pf) or (is_allowed and net != 0),
                        on_change=lambda on: on_allow(token, net, on),
                        UNSAFE_style={"fontSize": "14px", "height": "26px", "minHeight": "0", "paddingLeft": "6px",
                                      "whiteSpace": "nowrap"}),          # symbol stays on ONE line
            cell(fmt(r["BuyQty"], ",.0f"), bg=BUY_BG), cell(fmt(r["BuyAvg"], ",.2f"), bg=BUY_BG),
            cell(fmt(r["BuyVal"], ",.0f"), bg=BUY_BG),
            cell(fmt(r["SellQty"], ",.0f"), bg=SELL_BG), cell(fmt(r["SellAvg"], ",.2f"), bg=SELL_BG),
            cell(fmt(r["SellVal"], ",.0f"), bg=SELL_BG),
            cell(fmt(net, ",.0f"), color=BLUE if net > 0 else ORANGE if net < 0 else INK, bold=True),
            cell(fmt(r["Ltp"], ",.2f")),
            cell(fmt(r["Realised"], ",.0f"), color=sign_color(r["Realised"]), bold=True),
            cell(fmt(r["M2M"], ",.0f"), color=sign_color(r["M2M"]), bold=True),
            ui.flex(small_btn("Resume" if is_paused else "Pause", GREEN if is_paused else AMBER,
                              lambda: on_pause(pf, is_paused), disabled=not has_pf), justify_content="center"),
            ui.flex(small_btn("Confirm?" if armed == pf else "Square up", RED,
                              lambda: on_square(pf), disabled=not is_paused), justify_content="center"),
            key=str(token),
        )

    header = ui.html.div(
        grid_row(ui.html.div(), group_cell("BUY", 3, "#e8f0fe"), group_cell("SELL", 3, "#fdeee4"),
                 group_cell("NET", 4, "#eef1f4"), ui.html.div(), ui.html.div()),
        grid_row(head_cell("SYMBOL", left=True), head_cell("QTY"), head_cell("AVG"), head_cell("VALUE"),
                 head_cell("QTY"), head_cell("AVG"), head_cell("VALUE"),
                 head_cell("QTY"), head_cell("LTP"), head_cell("REALISED"), head_cell("M2M"),
                 ui.html.div(), ui.html.div()),
        style={"position": "sticky", "top": "0", "background": "#ffffff", "zIndex": "2"},
    )
    def total(key):                                  # sum of what is shown (follows the filters), skips missing values
        return sum(r[key] for r in rows if r[key] is not None and r[key] == r[key])

    big = "15px"
    footer = ui.html.div(                            # totals sit under their own columns
        grid_row(
            ui.html.div("TOTAL", style={"padding": "3px 8px", "fontSize": "12px", "fontWeight": "700", "color": GREY}),
            cell(fmt(total("BuyQty"), ",.0f"), bg=BUY_BG, bold=True, size=big), cell("", bg=BUY_BG),
            cell(fmt(total("BuyVal"), ",.0f"), bg=BUY_BG, bold=True, size=big),
            cell(fmt(total("SellQty"), ",.0f"), bg=SELL_BG, bold=True, size=big), cell("", bg=SELL_BG),
            cell(fmt(total("SellVal"), ",.0f"), bg=SELL_BG, bold=True, size=big),
            cell(fmt(total("NetQty"), "+,.0f"), bold=True, size=big), cell(""),
            cell(fmt(total("Realised"), "+,.0f"), color=sign_color(total("Realised")), bold=True, size=big),
            cell(fmt(total("M2M"), "+,.0f"), color=sign_color(total("M2M")), bold=True, size=big),
            ui.html.div(), ui.html.div(),
        ),
        style={"position": "sticky", "bottom": "0", "background": "#eef1f4", "borderTop": "2px solid #c9d1d9", "zIndex": "2"},
    )
    note = [] if pf_of else [ui.text("automation.py not loaded - checkboxes and buttons are disabled")]

    return ui.html.div(                              # outer box scrolls, inner box keeps the columns from squeezing
        *note,
        ui.html.div(header, *[line(r) for r in rows], footer, style={"minWidth": "1140px"}),
        style={"flex": "1", "minHeight": "0", "overflow": "auto"},
    )


@ui.component
def control_panel():
    loaded = "send_on" in globals()                                   # False until automation.py has been run
    sending, set_sending = ui.use_state(bool(globals().get("SEND_ON", False)))

    def handle_switch(on):
        automation_call("send_on" if on else "send_off")
        set_sending(bool(globals().get("SEND_ON", False)))             # show the real state (send_on can refuse)

    confirm, set_confirm = ui.use_state(False)          # square off needs a second click
    _, set_version = ui.use_state(0)                    # only used to force a redraw

    def redraw():
        set_version(lambda n: n + 1)

    ui.use_effect(watch_automation(redraw), [])

    def paused_count():
        paused = globals().get("paused_pf", set())
        pfs = globals().get("ALL_PF", [])
        return sum(1 for pf in pfs if pf in paused), len(pfs)

    n_paused, n_all = paused_count()
    all_paused = n_all > 0 and n_paused == n_all        # same rule as per position: Square off only when paused

    def handle_pause_all():
        set_confirm(False)
        automation_call("unpause_all" if all_paused else "pause_all")
        redraw()

    def handle_square_off():
        set_confirm(False)
        done, total = paused_count()                    # checked again at click time
        if total == 0 or done != total:
            print("[console] square off all refused: press Pause all first")
        else:
            automation_call("exit_all")
        redraw()

    def pbtn(label, color, on_press, outline=False, disabled=False):
        # plain button with explicit colours, same style as the buttons used earlier
        return ui.button(label, on_press=on_press, is_disabled=(not loaded) or disabled,
                         UNSAFE_style={"background": "#ffffff" if outline else color,
                                       "color": color if outline else "#ffffff",
                                       "border": f"1px solid {color}", "fontWeight": "bold",
                                       "opacity": "0.35" if disabled else "1"})

    square = (
        ui.flex(pbtn("Confirm square off", RED, handle_square_off),
                pbtn("Cancel", GREY, lambda: set_confirm(False), outline=True), gap="size-100", wrap=True)
        if confirm and all_paused else pbtn("Square off all", RED, lambda: set_confirm(True), disabled=not all_paused)
    )

    status_color = GREEN if sending else RED
    children = [
        # top row: AUTOMATION label + state on the left, ON / OFF buttons in the top-right corner
        ui.flex(
            ui.flex(
                ui.html.div("AUTOMATION", style={"color": GREY, "fontSize": "11px", "fontWeight": "bold"}),
                ui.html.div("ON" if sending else "OFF",
                            style={"color": status_color, "fontSize": "22px", "fontWeight": "700", "lineHeight": "1.1"}),
                direction="column",
            ),
            ui.flex(pbtn("ON", GREEN, lambda: handle_switch(True), outline=not sending),     # filled = current state
                    pbtn("OFF", RED, lambda: handle_switch(False), outline=sending), gap="size-100"),
            justify_content="space-between", align_items="start",
        ),
        ui.flex(pbtn("Resume all" if all_paused else "Pause all", GREEN if all_paused else AMBER, handle_pause_all),
                square, gap="size-100", wrap=True),
        ui.html.div(f"{n_paused}/{n_all} portfolios paused  (Square off needs all paused)",
                    style={"color": GREY, "fontSize": "11px"}),
    ]
    if not loaded:
        children.append(ui.text("automation.py not loaded"))

    return ui.panel(
        ui.flex(*children, direction="column", gap="size-150"),
        title="Controls",
    )


# ===========================================================================
# ONE DASHBOARD: summary + controls on top, messages left, order book + net positions right
# ===========================================================================
operator_console = ui.dashboard(
    ui.column(
        ui.row(
            ui.column(summary_window(), width=72),
            ui.column(control_panel(), width=28),
            height=20,
        ),
        ui.row(
            message_window(),
            ui.column(ui.row(order_book(), height=30), ui.row(net_positions(), height=70)),   # positions get most of the height
        ),
    )
)