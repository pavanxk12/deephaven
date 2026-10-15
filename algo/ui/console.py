"""Operator console: one dashboard with summary, message window, order book, net positions and the automation controls.

    app.run(algo.ui.Console())          # publishes `operator_console`

One time window and one user id (both top-right of the Summary panel) filter every panel. The controls talk to the
`Book` (state) and `Bridge` (commands) plugins directly; there are no lookups of names in the session globals.

Needs in the session: strategy_events, orders_trade, orders_ack and the live feed table (default `fo_market_data`).
"""
from __future__ import annotations

import base64
import mimetypes
from dataclasses import dataclass, field
from datetime import datetime

from deephaven import agg, ui
from deephaven.time import to_j_instant, to_j_zdt

from algo.core.plugin import Plugin
from algo.ui import tables as q
from algo.ui.filters import PF_PER_USER, Filters, time_conds, user_cond

GREEN, RED, BLUE, ORANGE, AMBER, PURPLE, GREY, INK = (
    "#1a7f37", "#cf222e", "#1f5fbf", "#c2410c", "#b45309", "#8250df", "#6e7781", "#1f2328")
TYPE_COLOR = {"TRADE": GREEN, "ORDER": BLUE, "EVENT": PURPLE, "ERROR": RED, "INFO": GREY}
TYPE_TINT = {"TRADE": "#e6f4ea", "ORDER": "#eaf1fc", "EVENT": "#f1ebfb", "ERROR": "#fde8e8", "INFO": "#ffffff"}

POS_COLS = ["token", "Symbol", "BuyQty", "BuyAvg", "BuyVal", "SellQty", "SellAvg", "SellVal",
            "NetQty", "Ltp", "Realised", "M2M"]
# symbol (with checkbox) | BUY qty avg value | SELL qty avg value | NET qty ltp realised m2m | Pause | Square up
GRID_COLS = ("minmax(230px, 2.2fr) minmax(64px, 1fr) minmax(70px, 1fr) minmax(100px, 1.4fr) "
             "minmax(64px, 1fr) minmax(70px, 1fr) minmax(100px, 1.4fr) minmax(56px, 0.8fr) "
             "minmax(76px, 1fr) minmax(76px, 1fr) minmax(76px, 1fr) 70px 90px")
BUY_BG, SELL_BG = "#f7faff", "#fffaf6"


@dataclass
class ConsoleConfig:
    events: str = "strategy_events"
    trades: str = "orders_trade"                 # token, buy_sell, fill_quantity, fill_price, response_order_number, PF, <trade_time>
    acks: str = "orders_ack"                     # order_number, token_no, tcode, volume, price, buy_sell, PF, <order_time>
    trade_time: str = "activity_time"            # epoch seconds
    order_time: str = "last_modified"            # epoch seconds
    event_cols: dict = field(default_factory=lambda: dict(seq="KafkaOffset", ts="KafkaTimestamp", pf="PF", text="Payload"))
    logo_file: str = "logo.png"                  # in paths.market_data; missing = nothing shown
    logo_height: str = "72px"                    # keep the file small (< ~100 KB): it is sent to the browser with the page
    book: str = "book"
    bridge: str = "bridge"
    pf_per_user: int = PF_PER_USER


@dataclass(eq=False)          # identity equality / hash: safe as a component prop
class Ctx:
    """Everything the components need, passed as one prop."""
    cfg: ConsoleConfig
    market: object
    book: object                 # Portfolios
    bridge: object               # Bridge
    symbol_of: object            # callable token -> symbol text
    filters: Filters
    net_all: object              # whole-day net positions, never filtered (the per-position rules use it)
    tz_label: str
    logo_src: str | None
    time: object                 # TimeTools
    # source tables and the whole-day tables derived from them
    trades: object
    acks: object
    feed: object
    symbols: object              # token -> Symbol lookup table
    fills: object                # fills per order
    messages: object
    book_all: object             # order book, whole day


class TimeTools:
    """Date picker values <-> epoch seconds in the exchange timezone."""

    def __init__(self, tz: str):
        import jpy
        self.tz = tz
        self._zone = jpy.get_type("java.time.ZoneId").of(tz)

    def to_epoch(self, v, end: bool = False):
        """Value of a date picker (any date/time type) -> epoch seconds; None if nothing is picked."""
        if v is None:
            return None
        kind = str(type(v))
        if "LocalDateTime" in kind:                      # no zone given: treat as exchange time
            s = int(v.atZone(self._zone).toEpochSecond())
        elif hasattr(v, "atStartOfDay"):                 # LocalDate
            s = int(v.atStartOfDay(self._zone).toEpochSecond())
        elif hasattr(v, "getEpochSecond"):               # Instant
            s = int(v.getEpochSecond())
        elif hasattr(v, "toEpochSecond"):                # ZonedDateTime / OffsetDateTime (what the picker gives)
            s = int(v.toEpochSecond())
        else:
            s = int(to_j_instant(v).getEpochSecond())
        return s + (59 if end else 0)

    def zdt(self, text: str):
        """'2026-10-14 09:15' (exchange time) -> Java ZonedDateTime, the type the date picker works with."""
        return to_j_zdt(text.replace(" ", "T") + ":00 " + self.tz)


# =================================================================================================
# small view helpers
# =================================================================================================
def fmt(x, spec):
    return "-" if x is None or x != x else format(x, spec)          # None / NaN -> "-"


def sign_color(x):
    return INK if x is None or x != x or x == 0 else (GREEN if x > 0 else RED)


def cell(text, color=INK, bg="transparent", bold=False, size="14px"):
    return ui.html.div(text, style={"textAlign": "right", "padding": "4px 8px", "color": color, "background": bg,
                                    "fontSize": size, "fontWeight": "700" if bold else "400"})


def grid_row(*cells, **kw):
    return ui.html.div(*cells, **kw, style={"display": "grid", "gridTemplateColumns": GRID_COLS, "alignItems": "center",
                                            "borderBottom": "1px solid #eef1f4"})


def group_cell(text, span, bg):
    return ui.html.div(text, style={"gridColumn": f"span {span}", "textAlign": "center", "background": bg,
                                    "fontSize": "12px", "fontWeight": "700", "padding": "3px"})


def head_cell(text, left=False):
    return ui.html.div(text, style={"textAlign": "left" if left else "right", "padding": "2px 8px",
                                    "fontSize": "12px", "fontWeight": "700", "color": GREY})


def small_btn(label, color, on_press, disabled=False):
    return ui.button(label, on_press=on_press, is_disabled=disabled,
                     UNSAFE_style={"background": color, "color": "#ffffff", "border": f"1px solid {color}",
                                   "fontWeight": "bold", "opacity": "0.35" if disabled else "1",
                                   "height": "22px", "minWidth": "0", "padding": "0 6px", "fontSize": "11px"})


def tile(label, text, color):
    # the grid cannot change font size, so the summary is drawn as tiles: small label, big number
    return ui.html.div(
        ui.html.div(label, style={"fontSize": "13px", "fontWeight": "600", "color": GREY, "letterSpacing": "0.5px"}),
        ui.html.div(text, style={"fontSize": "40px", "fontWeight": "700", "color": color, "lineHeight": "1.15"}),
        style={"flex": "1", "minWidth": "170px", "padding": "8px 16px", "borderRight": "1px solid #d0d7de"},
    )


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


def use_redraw(ctx: Ctx):
    """Redraw the calling component whenever the portfolio book changes (pause, allow, new PF, send switch)."""
    _, set_version = ui.use_state(0)

    def redraw():
        set_version(lambda n: n + 1)

    ui.use_effect(lambda: ctx.book.on_change(redraw), [])
    return redraw


# =================================================================================================
# panels
# =================================================================================================
@ui.component
def summary_window(net_tbl, book_tbl, logo_src, logo_height, filter_ctl=None):
    totals = ui.use_memo(
        lambda: net_tbl.agg_by([agg.sum_(["BuyVal", "SellVal", "Realised", "M2M", "Fills"])]).view([
            "Turnover = (BuyVal + SellVal) / 1.0E7",
            "Fills",
            "GrossM2M = M2M",
            "RealisedPnl = Realised",
            "Unrealised = M2M - Realised",
        ]),
        [net_tbl],
    )
    open_cnt = ui.use_memo(lambda: book_tbl.count_by("OpenOrders"), [book_tbl])
    row, orow = ui.use_row_data(totals), ui.use_row_data(open_cnt)
    v = row if isinstance(row, dict) else {}
    o = orow if isinstance(orow, dict) else {}

    def val(d, key):
        x = d.get(key)
        return None if x is None or (isinstance(x, float) and x != x) else x          # None or NaN = missing

    def num(d, key, spec, default="-"):
        x = val(d, key)
        return default if x is None else format(x, spec)

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
    if filter_ctl is not None or logo_src:
        logo = ui.html.img(src=logo_src, style={"height": logo_height, "display": "block"}) if logo_src else ui.html.div()
        body.insert(0, ui.flex(logo, filter_ctl if filter_ctl is not None else ui.html.div(),
                               justify_content="space-between", align_items="center",
                               UNSAFE_style={"flex": "0 0 auto", "padding": "2px 8px"}))
    return ui.panel(ui.flex(*body, direction="column", height="100%"), title="Summary")


@ui.component
def message_window(msgs):
    kind, set_kind = ui.use_state("ALL")            # ALL, TRADE, ORDER, EVENT, ERROR, INFO
    type_filter = "" if kind == "ALL" else kind

    def kind_button(label, value):
        return ui.toggle_button(label, is_selected=kind == value, on_change=lambda _: set_kind(value))

    chips = ui.flex(kind_button("All", "ALL"), kind_button("Trades", "TRADE"), kind_button("Orders", "ORDER"),
                    kind_button("Events", "EVENT"), kind_button("Errors", "ERROR"), kind_button("Info", "INFO"),
                    gap="size-100", wrap=True)
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
                column_display_names={"Time": "TIME", "Pf": "PF", "Msg": "MESSAGE"},
                frozen_columns=["Time", "Type"],
            ),
            direction="column", gap="size-100", height="100%",
        ),
        title="Message window",
    )


@ui.component
def order_book(book_tbl, order_time):
    return ui.panel(
        ui.table(
            book_tbl,
            reverse=True,
            density="spacious",
            show_search=True,
            format_=ORD_FORMATS,
            hidden_columns=[order_time],
            column_display_names={"Time": "TIME", "OrderId": "ORDER", "volume": "QTY", "Rem": "REMAINING", "Px": "PRICE"},
            frozen_columns=["Time"],
        ),
        title="Order book",
    )


@ui.component
def positions_grid(table, ctx: Ctx):
    """Net positions drawn as real rows, so each row can hold a checkbox (left of the symbol) and
    Pause / Square up buttons (right of M2M). Rows = positions only (tens)."""
    book, bridge, sym = ctx.book, ctx.bridge, ctx.symbol_of
    src = ui.use_memo(lambda: table.view(POS_COLS), [table])
    data = ui.use_table_data(src)
    full_src = ui.use_memo(lambda: ctx.net_all.view(["token", "NetQty"]), [])      # whole-day net, for the rules
    full = ui.use_table_data(full_src)
    armed, set_armed = ui.use_state(None)           # PF whose Square up is waiting for its confirm click
    warn, set_warn = ui.use_state("")               # why the last tick / untick was refused
    redraw = use_redraw(ctx)

    d = data if isinstance(data, dict) else {}
    rows = [{c: d[c][i] for c in POS_COLS} for i in range(len(d.get("token", [])))]
    fd = full if isinstance(full, dict) else {}
    full_net = {int(t): int(n) for t, n in zip(fd.get("token", []), fd.get("NetQty", []))}
    has_portfolios = bool(book.current_pf_map())

    def on_allow(token, net, on):
        owner = book.owner_of(token, sym)
        if not on and net != 0:                     # rule: cannot block automation while a position is open
            set_warn(f"{sym(token)}: cannot untick, net position is {net:+,}. Pause and Square up first.")
        elif owner is None:
            set_warn(f"{sym(token)}: no portfolio trades this instrument (yet), nothing to change.")
        else:
            set_warn("")
            book.set_allow(owner, on)
            ctx.bridge.log.info("%s automation %s", sym(owner), "ALLOWED" if on else "BLOCKED")
        redraw()

    def on_pause(pf, is_paused):
        set_armed(None)
        (bridge.unpause_pf if is_paused else bridge.pause_pf)(pf)
        redraw()

    def on_square(pf):
        if pf not in book.paused:                           # rule: no square up until the PF is paused
            bridge.log.warning("square up PF %s refused: press Pause first", pf)
            set_armed(None)
        elif armed != pf:
            set_armed(pf)                                   # first click arms it, second click sends it
        else:
            set_armed(None)
            bridge.squareup_pf(pf)
        redraw()

    def line(r):
        token = int(r["token"])
        net = full_net.get(token, int(r["NetQty"]))         # rules use the whole-day position, not the window
        shown_net = int(r["NetQty"])
        owner = book.owner_of(token, sym)                   # the option row -> the underlying future that is tracked
        pf = book.pf_for_row(token, sym)                    # None = stock not tracked
        has_pf = pf is not None
        is_allowed = book.allow.get(owner, True) if owner is not None else True
        is_paused = has_pf and pf in book.paused
        return grid_row(
            ui.checkbox(str(r["Symbol"]), is_selected=is_allowed,
                        on_change=lambda on: on_allow(token, net, on),
                        UNSAFE_style={"fontSize": "14px", "height": "26px", "minHeight": "0", "paddingLeft": "6px",
                                      "whiteSpace": "nowrap"}),
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
    footer = ui.html.div(
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
    note = ([] if has_portfolios else [ui.text("no strategy running yet - checkboxes and buttons are disabled")]) + (
        [ui.html.div(warn, style={"color": RED, "fontSize": "13px", "fontWeight": "600", "padding": "2px 8px"})] if warn else [])
    return ui.html.div(                              # outer box scrolls, inner box keeps the columns from squeezing
        *note,
        ui.html.div(header, *[line(r) for r in rows], footer, style={"minWidth": "1140px"}),
        style={"flex": "1", "minHeight": "0", "overflow": "auto"},
    )


@ui.component
def net_positions(net_tbl, ctx: Ctx):
    hide_flat, set_hide_flat = ui.use_state(False)
    kind, set_kind = ui.use_state("ALL")            # ALL, FUT, OPT, EQ

    def build():
        conds = (["NetQty != 0"] if hide_flat else []) + ([f"Kind == `{kind}`"] if kind != "ALL" else [])
        return net_tbl.where(conds) if conds else net_tbl

    shown = ui.use_memo(build, [hide_flat, kind, net_tbl])

    def kind_button(label, value):
        return ui.toggle_button(label, is_selected=kind == value, on_change=lambda _: set_kind(value))

    return ui.panel(
        ui.flex(
            ui.flex(
                ui.toggle_button("Hide flat", is_selected=hide_flat, on_change=set_hide_flat),
                ui.flex(kind_button("All", "ALL"), kind_button("Futures", "FUT"), kind_button("Options", "OPT"),
                        kind_button("Equity", "EQ"), gap="size-100", wrap=True),
                gap="size-200", wrap=True, align_items="center",
                UNSAFE_style={"flex": "0 0 auto"},          # keep the button row its own height; the rows get the rest
            ),
            positions_grid(shown, ctx),
            direction="column", gap="size-100", height="100%",
        ),
        title="Net positions",
    )


@ui.component
def control_panel(ctx: Ctx):
    book, bridge = ctx.book, ctx.bridge
    confirm, set_confirm = ui.use_state(False)          # square off needs a second click
    redraw = use_redraw(ctx)

    sending = bridge.sending
    pfs = book.current_pfs()                            # current portfolios; retired ones stay paused
    n_paused, n_all = sum(1 for pf in pfs if pf in book.paused), len(pfs)
    all_paused = n_all > 0 and n_paused == n_all        # same rule as per position: Square off only when paused

    def handle_switch(on):
        (bridge.send_on if on else bridge.send_off)()
        redraw()

    def handle_pause_all():
        set_confirm(False)
        (bridge.unpause_all if all_paused else bridge.pause_all)()
        redraw()

    def handle_square_off():
        set_confirm(False)
        cur = book.current_pfs()                        # checked again at click time
        if not cur or any(pf not in book.paused for pf in cur):
            bridge.log.warning("square off all refused: press Pause all first")
        else:
            bridge.exit_all()
        redraw()

    def pbtn(label, color, on_press, outline=False, disabled=False):
        return ui.button(label, on_press=on_press, is_disabled=disabled,
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
    return ui.panel(
        ui.flex(
            ui.flex(
                ui.flex(
                    ui.html.div("AUTOMATION", style={"color": GREY, "fontSize": "11px", "fontWeight": "bold"}),
                    ui.html.div("ON" if sending else "OFF",
                                style={"color": status_color, "fontSize": "22px", "fontWeight": "700", "lineHeight": "1.1"}),
                    direction="column",
                ),
                ui.flex(pbtn("ON", GREEN, lambda: handle_switch(True), outline=not sending),      # filled = current state
                        pbtn("OFF", RED, lambda: handle_switch(False), outline=sending), gap="size-100"),
                justify_content="space-between", align_items="start",
            ),
            ui.flex(pbtn("Resume all" if all_paused else "Pause all", GREEN if all_paused else AMBER, handle_pause_all),
                    square, gap="size-100", wrap=True),
            ui.html.div(f"{n_paused}/{n_all} portfolios paused  (Square off needs all paused)",
                        style={"color": GREY, "fontSize": "11px"}),
            direction="column", gap="size-150",
        ),
        title="Controls",
    )


@ui.component
def time_picker(ctx: Ctx):
    tt, filters = ctx.time, ctx.filters
    draft_from, set_draft_from = ui.use_state(None)          # values picked in the calendar (None = nothing picked)
    draft_to, set_draft_to = ui.use_state(None)
    note, set_note = ui.use_state("")
    cur, set_cur = ui.use_state(filters.window)
    opened, set_opened = ui.use_state(False)
    zone = ctx.market.zone

    def put(a, b):
        filters.set_window(a, b)
        set_cur((a, b))

    def today_btn():
        d = datetime.now(zone).strftime("%Y-%m-%d")
        set_draft_from(tt.zdt(d + " 00:00"))
        set_draft_to(None)
        set_note("")
        put(tt.to_epoch(tt.zdt(d + " 00:00")), None)
        set_opened(False)

    def clear_btn():
        set_draft_from(None)
        set_draft_to(None)
        set_note("")
        put(None, None)
        set_opened(False)

    def apply_now():
        a2, b2 = tt.to_epoch(draft_from), tt.to_epoch(draft_to, end=True)
        if a2 is not None and b2 is not None and a2 > b2:
            set_note("From is after To")
        else:
            set_note("")
            put(a2, b2)
            set_opened(False)

    a, b = cur
    shown = "Whole day" if a is None and b is None else (
        f"{datetime.fromtimestamp(a, zone):%d-%b %H:%M}" if a is not None else "start") + " to " + (
        f"{datetime.fromtimestamp(b, zone):%d-%b %H:%M}" if b is not None else "now")

    pick = dict(granularity="minute", hour_cycle=24, hide_time_zone=True)      # 24-hour clock, exchange time, no zone label
    dlg = ui.dialog(
        ui.heading(f"Time window ({ctx.tz_label})"),
        ui.content(
            ui.flex(
                ui.date_picker(label=f"From ({ctx.tz_label})", value=draft_from, on_change=set_draft_from,
                               placeholder_value=tt.zdt("2026-01-01 09:00"), **pick),
                ui.date_picker(label=f"To ({ctx.tz_label}), empty = now", value=draft_to, on_change=set_draft_to,
                               placeholder_value=tt.zdt("2026-01-01 15:30"), **pick),
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
    active = a is not None or b is not None
    btn = ui.action_button(f"⏱ {shown}",
                           UNSAFE_style={"fontWeight": "700", "color": BLUE, "borderColor": BLUE} if active else {})
    return ui.dialog_trigger(btn, dlg, is_open=opened, on_open_change=set_opened)


@ui.component
def user_picker(ctx: Ctx):
    filters = ctx.filters
    draft, set_draft = ui.use_state("")
    note, set_note = ui.use_state("")
    cur, set_cur = ui.use_state(filters.user)
    opened, set_opened = ui.use_state(False)

    def put(u):
        filters.set_user(u)
        set_cur(u)

    def apply_now():
        text = draft.strip()
        if text == "":
            put(None)
        elif text.lstrip("-").isdigit():
            put(int(text))
        else:
            set_note("User id must be a number")
            return
        set_note("")
        set_opened(False)

    def clear_btn():
        set_draft("")
        set_note("")
        put(None)
        set_opened(False)

    dlg = ui.dialog(
        ui.heading("User id"),
        ui.content(
            ui.flex(
                ui.text_field(label="User id, empty = all users", value=draft, on_change=set_draft, auto_focus=True),
                ui.text(note or "Filters everything: Summary, Messages, Order book and Net positions "
                                "(user id = first digits of the PF)."),
                direction="column", gap="size-150",
            ),
        ),
        ui.button_group(ui.button("Clear", variant="secondary", on_press=clear_btn),
                        ui.button("Apply", variant="accent", on_press=apply_now)),
    )
    # plain text: an emoji outside the BMP breaks the Java -> Python hand-over
    btn = ui.action_button(f"User {cur}" if cur is not None else "All users",
                           UNSAFE_style={"fontWeight": "700", "color": BLUE, "borderColor": BLUE} if cur is not None else {})
    return ui.dialog_trigger(btn, dlg, is_open=opened, on_open_change=set_opened)


# =================================================================================================
# dashboard
# =================================================================================================
def build_dashboard(ctx: Ctx):
    cfg, mk = ctx.cfg, ctx.market

    @ui.component
    def console():
        window, set_window = ui.use_state(ctx.filters.window)
        user, set_user = ui.use_state(ctx.filters.user)

        def follow():
            return ctx.filters.on_change(lambda: (set_window(ctx.filters.window), set_user(ctx.filters.user)))

        ui.use_effect(follow, [])
        a, b = window

        def net_fn():
            conds = time_conds(cfg.trade_time, window) + ([user_cond("PF", user, cfg.pf_per_user)] if user is not None else [])
            return q.net_positions(ctx.trades.where(conds), ctx.feed, ctx.symbols, mk.price_div) if conds else ctx.net_all

        def msg_fn():
            conds = ([f"Ts >= epochSecondsToInstant({a}L)"] if a is not None else []) + (
                [f"Ts <= epochSecondsToInstant({b}L)"] if b is not None else [])
            if user is not None:
                conds.append(user_cond("Pf", user, cfg.pf_per_user))
            return ctx.messages.where(conds) if conds else ctx.messages

        def book_fn():
            conds = time_conds(cfg.order_time, window) + ([user_cond("PF", user, cfg.pf_per_user)] if user is not None else [])
            return q.order_book(ctx.acks, ctx.fills, ctx.symbols, mk.price_div, cfg.order_time, mk.tz, conds) if conds else ctx.book_all

        net_tbl = ui.use_memo(net_fn, [a, b, user])
        msgs = ui.use_memo(msg_fn, [a, b, user])
        book_tbl = ui.use_memo(book_fn, [a, b, user])
        filters_bar = ui.flex(user_picker(ctx), time_picker(ctx), gap="size-100", align_items="center")
        return ui.dashboard(
            ui.column(
                ui.row(
                    ui.column(summary_window(net_tbl, book_tbl, ctx.logo_src, cfg.logo_height, filters_bar), width=72),
                    ui.column(control_panel(ctx), width=28),
                    height=26,
                ),
                ui.row(
                    message_window(msgs),
                    ui.column(ui.row(order_book(book_tbl, cfg.order_time), height=30),
                              ui.row(net_positions(net_tbl, ctx), height=70)),
                ),
            )
        )

    return console()


class Console(Plugin):
    name = "console"

    def __init__(self, cfg: ConsoleConfig | None = None):
        super().__init__()
        self.cfg = cfg or ConsoleConfig()
        self.requires = ("master", self.cfg.book, self.cfg.bridge)

    def start(self) -> None:
        app, cfg, mk = self.app, self.cfg, self.app.market
        events, trades, acks = app.table(cfg.events), app.table(cfg.trades), app.table(cfg.acks)
        feed = app.table(mk.feed)
        symbol_of = app.master.symbol_of
        symbols = q.symbol_table(symbol_of)
        fills = q.fills_per_order(trades)
        filters = app.shared.setdefault("console_filters", Filters())

        ctx = Ctx(cfg=cfg, market=mk, book=app[cfg.book].state, bridge=app[cfg.bridge],
                  symbol_of=lambda t: symbol_of.get(int(t), str(t)), filters=filters,
                  net_all=q.net_positions(trades, feed, symbols, mk.price_div),
                  tz_label="IST" if mk.code == "NSE" else mk.tz, logo_src=self._logo(), time=TimeTools(mk.tz),
                  trades=trades, acks=acks, feed=feed, symbols=symbols, fills=fills,
                  messages=q.messages(events, cfg.event_cols, mk.tz),
                  book_all=q.order_book(acks, fills, symbols, mk.price_div, cfg.order_time, mk.tz))
        app.publish("operator_console", build_dashboard(ctx))
        self.log.info("published `operator_console`")

    def _logo(self) -> str | None:
        path = f"{self.app.paths.market_data}/{self.cfg.logo_file}"
        try:
            mime = mimetypes.guess_type(path)[0] or "image/png"
            with open(path, "rb") as f:
                return f"data:{mime};base64," + base64.b64encode(f.read()).decode()
        except OSError:
            self.log.info("no logo at %s, showing none", path)
            return None
