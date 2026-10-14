import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from deephaven import ui, agg
from deephaven.time import to_j_instant, to_j_zdt

# ===========================================================================
# OPERATOR CONSOLE: summary + message window + order book + net positions, ONE script, ONE dashboard.
# ONE overall time filter (top-right of Summary) drives EVERY panel.
#
# Needs in your session before running:
#   master          from build_master.py        (master["symbol_of"]: token -> symbol)
#   strategy_events KafkaPartition, KafkaOffset, KafkaTimestamp, PF, Payload
#   orders_trade    fills                       (token, buy_sell, fill_quantity, fill_price, response_order_number,
#                                                activity_time = fill time, epoch seconds)
#   orders_ack      order confirmations         (order_number, token_no, tcode 20073/20074/20075, volume, price, buy_sell,
#                                                last_modified = epoch seconds)
#   fo_market_data  live prices, one row per token (token, ltp)
#                   if it holds repeated ticks per token, change it to fo_market_data.last_by("token") below
#   automation.py   (same session) for the Controls panel and the per-position controls
# ===========================================================================

# ---------- settings ----------
SYMBOL_OF = master["symbol_of"]
PRICE_DIV = 100.0                      # NSE prices arrive in paise
IST = ZoneInfo("Asia/Kolkata")
TRADE_TIME = "activity_time"           # orders_trade column with the fill time, epoch seconds
ORDER_TIME = "last_modified"           # order book column with the order's last update time, epoch seconds

LOGO_PATH = "/data/fo_market_data/logo.png"           # picture shown top-left of the console (png / jpg / svg); no file = nothing shown
LOGO_HEIGHT = "72px"                   # keep the file small (under ~100 KB): it is sent to the browser with the page


def load_logo():
    try:
        import base64, mimetypes
        mime = mimetypes.guess_type(LOGO_PATH)[0] or "image/png"
        with open(LOGO_PATH, "rb") as f:
            return f"data:{mime};base64," + base64.b64encode(f.read()).decode()
    except OSError:
        print(f"[console] no logo at {LOGO_PATH}, showing none")
        return None


LOGO_SRC = load_logo()

C_SEQ, C_TS, C_PF, C_TEXT = "KafkaOffset", "KafkaTimestamp", "PF", "Payload"   # strategy_events columns

GREEN, RED, BLUE, ORANGE, AMBER, PURPLE, GREY, INK = (
    "#1a7f37", "#cf222e", "#1f5fbf", "#c2410c", "#b45309", "#8250df", "#6e7781", "#1f2328")


def symbol_of(token: int) -> str:
    return SYMBOL_OF.get(int(token), str(token))


# ===========================================================================
# TABLES (each is one lazy chain; per-row columns use update_view so nothing per-row is stored)
# ===========================================================================

# ---- messages: LOGMSG rows of strategy_events, classified (Ts kept so the time filter can use it) ----
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
    .view(["Seq", "Ts", "Time", "Type", "Pf", "Msg"])
)


# ---- net positions: one row per traded token, live LTP joined straight from fo_market_data ----
def make_net(trades):
    return (
        trades
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
        .view(["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal",
               "NetQty", "Ltp", "Realised", "M2M", "Fills", "Kind"])
        .sort("Symbol")
    )


# whole day, never filtered: the per-position rules (no untick while open) always use this
net_positions_table = make_net(orders_trade)

# fills per order over the WHOLE day, so Remaining is always correct whatever the window is
fills_per_order = (
    orders_trade.agg_by([agg.sum_("FilledQty = fill_quantity")], by="response_order_number")
    .view(["OrderId = (long) response_order_number", "FilledQty"])
)


# ---- order book: latest ack per order, not cancelled, still has quantity left after fills ----
# conds = optional time conditions on the order's last update time (the overall time window)
def make_order_book(conds=None):
    t = (
        orders_ack
        .view(["OrderId = (long) order_number", "PF", "tcode", "token_no", "buy_sell", "volume", "price", ORDER_TIME])
        .last_by("OrderId")
    )
    if conds:
        t = t.where(conds)
    return (
        t
        .natural_join(fills_per_order, on="OrderId", joins="FilledQty")
        .update_view("Rem = volume - (isNull(FilledQty) ? 0L : FilledQty)")
        .where("tcode != 20075 && Rem > 0")
        .update([
            "Symbol = symbol_of(token_no)",
            "Side = buy_sell == 1 ? `BUY` : `SELL`",
            "Px = price / PRICE_DIV",
            "Status = Rem < volume ? `PART` : `OPEN`",
            f"Time = formatDateTime(epochSecondsToInstant({ORDER_TIME}), timeZone(`Asia/Kolkata`)).substring(11, 19)",
        ])
        .view([ORDER_TIME, "Time", "OrderId", "PF", "Side", "Symbol", "volume", "Rem", "Px", "Status"])
        .sort(ORDER_TIME)
    )


order_book_table = make_order_book()                 # whole day


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
def summary_window(net_tbl, book_tbl, filter_ctl=None):
    # net_tbl / book_tbl follow the overall time window; filter_ctl = the time filter button, top-right
    totals = ui.use_memo(
        lambda: net_tbl.agg_by(
            [agg.sum_(["BuyVal", "SellVal", "Realised", "M2M", "Fills"])]
        ).view([
            "Turnover = (BuyVal + SellVal) / 1.0E7",
            "Fills",
            "GrossM2M = M2M",
            "RealisedPnl = Realised",
            "Unrealised = M2M - Realised",
        ]),
        [net_tbl],
    )
    open_cnt = ui.use_memo(lambda: book_tbl.count_by("OpenOrders"), [book_tbl])

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

    tiles = ui.flex(
        tile("TURNOVER (₹ CR)", num(v, "Turnover", ",.2f", "0.00"), ORANGE),
        tile("FILLS", num(v, "Fills", ",.0f", "0"), "#000000"),
        tile("OPEN ORDERS", num(o, "OpenOrders", ",.0f", "0"), "#000000"),
        tile("GROSS M2M (₹)", num(v, "GrossM2M", "+,.0f"), pnl_color("GrossM2M")),
        tile("REALISED (₹)", num(v, "RealisedPnl", "+,.0f", "0"), pnl_color("RealisedPnl")),
        tile("UNREALISED (₹)", num(v, "Unrealised", "+,.0f"), pnl_color("Unrealised")),
        wrap=True, align_items="center",
        UNSAFE_style={"flex": "1", "minHeight": "0"},
    )

    body = [tiles]
    if filter_ctl is not None or LOGO_SRC:
        # thin row above the tiles: logo on the left, time filter button on the right
        logo = ui.html.img(src=LOGO_SRC, style={"height": LOGO_HEIGHT, "display": "block"}) if LOGO_SRC else ui.html.div()
        body.insert(0, ui.flex(logo, filter_ctl if filter_ctl is not None else ui.html.div(),
                               justify_content="space-between", align_items="center",
                               UNSAFE_style={"flex": "0 0 auto", "padding": "2px 8px"}))

    return ui.panel(
        ui.flex(*body, direction="column", height="100%"),
        title="Summary",
    )


@ui.component
def message_window(msgs):
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
                msgs,
                key=type_filter,                    # new key = the grid re-applies the filter right away
                reverse=True,                       # newest on top, done in the browser
                density="spacious",
                show_search=True,
                quick_filters={"Type": type_filter},
                format_=MSG_FORMATS,
                hidden_columns=["Seq", "Ts"],
                always_fetch_columns=["Type"],
                column_display_names={"Time": "TIME IST", "Pf": "PF", "Msg": "MESSAGE"},
                frozen_columns=["Time", "Type"],
            ),
            direction="column", gap="size-100", height="100%",
        ),
        title="Message window",
    )


@ui.component
def order_book(book_tbl):
    return ui.panel(
        ui.table(
            book_tbl,
            reverse=True,
            density="spacious",
            show_search=True,
            format_=ORD_FORMATS,
            hidden_columns=[ORDER_TIME],
            column_display_names={"Time": "TIME IST", "OrderId": "ORDER", "volume": "QTY", "Rem": "REMAINING", "Px": "PRICE"},
            frozen_columns=["Time"],
        ),
        title="Order book",
    )


@ui.component
def net_positions(net_tbl):
    hide_flat, set_hide_flat = ui.use_state(False)
    kind, set_kind = ui.use_state("ALL")            # one of: ALL, FUT, OPT, EQ

    def build():
        conds = []
        if hide_flat:
            conds.append("NetQty != 0")
        if kind != "ALL":
            conds.append(f"Kind == `{kind}`")
        return net_tbl.where(conds) if conds else net_tbl

    shown = ui.use_memo(build, [hide_flat, kind, net_tbl])

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


def owner_of(token):
    """Automation keys everything (PF_OF, auto_allow) by the UNDERLYING future, but a net-positions row is the OPTION
    that portfolio trades. Return the underlying token for a row, or None if no portfolio trades this instrument."""
    pf_of = globals().get("PF_OF", {})
    if token in pf_of:
        return token
    for pf, opt in globals().get("subscribed", {}).items():        # pf -> option token it is subscribed to
        if opt == token:
            for u, p in pf_of.items():
                if p == pf:
                    return u
    name = symbol_of(token).split()[0]               # old leg of a retired PF: same stock = same checkbox
    for u in pf_of:
        if symbol_of(u).split()[0] == name:
            return u
    return None


def pf_for_row(token):
    """PF of a net-positions row. One PF = one option, so first the PF subscribed to exactly this option (retired
    portfolios included, newest if several). Otherwise the current PF of the same stock."""
    exact = [pf for pf, opt in globals().get("subscribed", {}).items() if opt == token]
    if exact:
        return max(exact)
    owner = owner_of(token)
    return globals().get("PF_OF", {}).get(owner) if owner is not None else None


def watch_automation(redraw):
    """Effect for a component: pause / allow state lives in automation.py, so look at it every 2 s
    and call redraw() ONLY when it changed (nothing is drawn while it is idle)."""
    def sig():
        return (tuple(sorted(globals().get("paused_pf", ()))),
                tuple(sorted(globals().get("auto_allow", {}).items())),
                tuple(sorted(globals().get("PF_OF", {}).items())),
                tuple(sorted(globals().get("subscribed", {}).items())))

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
    full_src = ui.use_memo(lambda: net_positions_table.view(["token", "NetQty"]), [])    # whole-day net, for the rules
    full = ui.use_table_data(full_src)
    armed, set_armed = ui.use_state(None)           # PF whose Square up is waiting for its confirm click
    warn, set_warn = ui.use_state("")               # why the last tick / untick was refused
    _, set_version = ui.use_state(0)                # only used to force a redraw

    def redraw():
        set_version(lambda n: n + 1)

    ui.use_effect(watch_automation(redraw), [])

    pf_of = globals().get("PF_OF", {})              # token -> PF, from automation.py
    allowed = globals().get("auto_allow", {})       # token -> True/False, from automation.py
    paused = globals().get("paused_pf", set())      # PFs the bridge confirmed as paused

    d = data if isinstance(data, dict) else {}
    rows = [{c: d[c][i] for c in POS_COLS} for i in range(len(d.get("token", [])))]
    fd = full if isinstance(full, dict) else {}
    full_net = {int(t): int(n) for t, n in zip(fd.get("token", []), fd.get("NetQty", []))}

    def on_allow(token, net, on):
        if not on and net != 0:                     # rule: cannot block automation while a position is open
            set_warn(f"{symbol_of(token)}: cannot untick, net position is {net:+,}. Pause and Square up first.")
        elif owner_of(token) is None:
            set_warn(f"{symbol_of(token)}: no portfolio trades this instrument (yet), nothing to change.")
        else:
            set_warn("")
            automation_call("set_allow", owner_of(token), on)
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
        token = int(r["token"])
        net = full_net.get(token, int(r["NetQty"]))           # rules use the whole-day position, not the window
        shown_net = int(r["NetQty"])
        owner = owner_of(token)                      # the option row -> the underlying future automation tracks
        pf = pf_for_row(token)                       # owner's PF, else the PF of the same stock; None = stock not tracked
        has_pf = pf is not None
        is_allowed = allowed.get(owner, True) if owner is not None else True
        is_paused = has_pf and pf in paused
        return grid_row(
            ui.checkbox(str(r["Symbol"]), is_selected=is_allowed,
                        on_change=lambda on: on_allow(token, net, on),
                        UNSAFE_style={"fontSize": "14px", "height": "26px", "minHeight": "0", "paddingLeft": "6px",
                                      "whiteSpace": "nowrap"}),          # symbol stays on ONE line
            cell(fmt(r["BuyQty"], ",.0f"), bg=BUY_BG), cell(fmt(r["BuyAvg"], ",.2f"), bg=BUY_BG),
            cell(fmt(r["BuyVal"], ",.0f"), bg=BUY_BG),
            cell(fmt(r["SellQty"], ",.0f"), bg=SELL_BG), cell(fmt(r["SellAvg"], ",.2f"), bg=SELL_BG),
            cell(fmt(r["SellVal"], ",.0f"), bg=SELL_BG),
            cell(fmt(shown_net, ",.0f"), color=BLUE if shown_net > 0 else ORANGE if shown_net < 0 else INK, bold=True),
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
    note = ([] if pf_of else [ui.text("automation.py not loaded - checkboxes and buttons are disabled")]) + (
        [ui.html.div(warn, style={"color": RED, "fontSize": "13px", "fontWeight": "600", "padding": "2px 8px"})] if warn else [])

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
        pfs = list(globals().get("PF_OF", {}).values())     # current portfolios; retired ones stay paused
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
# OVERALL TIME WINDOW: From / To (IST). Filters EVERYTHING: Summary, Messages, Order book, Net positions.
# The button sits top-right of the Summary panel.
# ===========================================================================
import jpy
_ZoneId = jpy.get_type("java.time.ZoneId")
_IST_ZONE = _ZoneId.of("Asia/Kolkata")


def to_epoch(v, end=False):
    """value of a date picker (any date/time type) -> epoch seconds; None if nothing picked.
    `end` = include the whole minute."""
    if v is None:
        return None
    kind = str(type(v))
    if "LocalDateTime" in kind:                      # no zone given: treat as IST
        s = int(v.atZone(_IST_ZONE).toEpochSecond())
    elif hasattr(v, "atStartOfDay"):                 # LocalDate: start of that day, IST
        s = int(v.atStartOfDay(_IST_ZONE).toEpochSecond())
    elif hasattr(v, "getEpochSecond"):               # Instant
        s = int(v.getEpochSecond())
    elif hasattr(v, "toEpochSecond"):                # ZonedDateTime / OffsetDateTime (what your picker gives)
        s = int(v.toEpochSecond())
    else:
        s = int(to_j_instant(v).getEpochSecond())    # last resort
    return s + (59 if end else 0)
def ist_zdt(text):
    """'2026-10-14 09:15' (IST) -> Java ZonedDateTime, the type the date picker works with"""
    return to_j_zdt(text.replace(" ", "T") + ":00 Asia/Kolkata")


# the window lives here (plain Python): the picker sets it, the console tables follow it
if "TIME_WINDOW" not in globals():
    TIME_WINDOW = {"v": (None, None)}                       # (from, to) epoch seconds, None = no limit


@ui.component
def time_picker():
    draft_from, set_draft_from = ui.use_state(None)          # values picked in the calendar (None = nothing picked)
    draft_to, set_draft_to = ui.use_state(None)
    note, set_note = ui.use_state("")
    cur, set_cur = ui.use_state(TIME_WINDOW["v"])
    opened, set_opened = ui.use_state(False)

    def put(a, b):
        TIME_WINDOW["v"] = (a, b)
        set_cur((a, b))

    def today():
        d = datetime.now(IST).strftime("%Y-%m-%d")
        set_draft_from(ist_zdt(d + " 00:00"))
        set_draft_to(None)
        set_note("")
        put(to_epoch(ist_zdt(d + " 00:00")), None)

    def clear():
        set_draft_from(None)
        set_draft_to(None)
        set_note("")
        put(None, None)

    a, b = cur
    shown = "Whole day" if a is None and b is None else (
        f"{datetime.fromtimestamp(a, IST):%d-%b %H:%M}" if a is not None else "start") + " to " + (
        f"{datetime.fromtimestamp(b, IST):%d-%b %H:%M}" if b is not None else "now")

    def close():
        set_opened(False)

    def apply_now():
        a2, b2 = to_epoch(draft_from), to_epoch(draft_to, end=True)
        if a2 is not None and b2 is not None and a2 > b2:
            set_note("From is after To")
        else:
            set_note("")
            put(a2, b2)
            close()

    def today_btn():
        today()
        close()

    def clear_btn():
        clear()
        close()

    pick = dict(granularity="minute", hour_cycle=24, hide_time_zone=True)      # 24-hour clock, IST, no zone label
    dlg = ui.dialog(
        ui.heading("Time window (IST)"),
        ui.content(
            ui.flex(
                ui.date_picker(label="From (IST)", value=draft_from, on_change=set_draft_from,
                               placeholder_value=ist_zdt("2026-01-01 09:00"), **pick),
                ui.date_picker(label="To (IST), empty = now", value=draft_to, on_change=set_draft_to,
                               placeholder_value=ist_zdt("2026-01-01 15:30"), **pick),
                ui.text(note or "Filters everything: Summary, Messages, Order book and Net positions."),
                direction="column", gap="size-150",
            ),
        ),
        ui.button_group(
            ui.button("Clear", variant="secondary", on_press=clear_btn),
            ui.button("Today", variant="secondary", on_press=today_btn),
            ui.button("Apply", variant="accent", on_press=apply_now),
        ),
    )

    # trigger looks like a filter: plain when the whole day is shown, blue when a window is active
    active = a is not None or b is not None
    btn = ui.action_button(
        f"⏱ {shown}",
        UNSAFE_style={"fontWeight": "700", "color": BLUE, "borderColor": BLUE} if active else {},
    )
    # a popup opened by this button; open / close is our own state, children are plain elements
    return ui.dialog_trigger(btn, dlg, is_open=opened, on_open_change=set_opened)


# ===========================================================================
# ONE DASHBOARD: summary (with time filter) + controls, messages left, order book + net positions right
# ===========================================================================
@ui.component
def console():
    applied, set_applied = ui.use_state(TIME_WINDOW["v"])    # (from, to) epoch seconds, set by the time picker

    def follow_window():                                     # look at it every second, redraw only when it changed
        stop = threading.Event()

        def loop():
            last = TIME_WINDOW["v"]
            while not stop.wait(1):
                if TIME_WINDOW["v"] != last:
                    last = TIME_WINDOW["v"]
                    set_applied(last)

        threading.Thread(target=loop, daemon=True).start()
        return stop.set

    ui.use_effect(follow_window, [])
    a, b = applied

    def trade_tbl():
        conds = ([f"{TRADE_TIME} >= {a}"] if a is not None else []) + ([f"{TRADE_TIME} <= {b}"] if b is not None else [])
        return make_net(orders_trade.where(conds)) if conds else net_positions_table

    def msg_tbl():
        conds = ([f"Ts >= epochSecondsToInstant({a}L)"] if a is not None else []) + (
            [f"Ts <= epochSecondsToInstant({b}L)"] if b is not None else [])
        return messages.where(conds) if conds else messages

    def book_tbl_fn():
        conds = ([f"{ORDER_TIME} >= {a}"] if a is not None else []) + ([f"{ORDER_TIME} <= {b}"] if b is not None else [])
        return make_order_book(conds) if conds else order_book_table

    net_tbl = ui.use_memo(trade_tbl, [a, b])
    msgs = ui.use_memo(msg_tbl, [a, b])
    book_tbl = ui.use_memo(book_tbl_fn, [a, b])

    return ui.dashboard(
        ui.column(
            ui.row(
                ui.column(summary_window(net_tbl, book_tbl, time_picker()), width=72),
                ui.column(control_panel(), width=28),
                height=26,                                   # slightly taller: the filter row needs ~30px
            ),
            ui.row(
                message_window(msgs),
                ui.column(ui.row(order_book(book_tbl), height=30), ui.row(net_positions(net_tbl), height=70)),
            ),
        )
    )


operator_console = console()