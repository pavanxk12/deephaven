from datetime import datetime
from deephaven import ui

# ---------- light palette ----------
INK, MUTED, GREY, LINE = "#1f2328", "#57606a", "#6e7781", "#e3e7ec"
GREEN, RED, AMBER, BLUE, PURPLE = "#1a7f37", "#cf222e", "#b45309", "#1f5fbf", "#8250df"
WHITE, SOFT = "#ffffff", "#f6f8fa"
LOT = 50


def now():
    return datetime.now().strftime("%H:%M:%S")


def money(v):
    return f"+₹{v:,.0f}" if v > 0 else (f"-₹{abs(v):,.0f}" if v < 0 else "₹0")


# ---------- small building blocks (explicit colors, so theme can't override) ----------
def t(text, color=INK, bold=False, mono=False, size=None):
    style = {"color": color, "fontWeight": "bold" if bold else "normal"}
    if mono:
        style["fontFamily"] = "monospace"
    if size:
        style["fontSize"] = size
    return ui.html.span(str(text), style=style)


def h(text, size="14px"):
    return ui.html.div(text, style={"color": INK, "fontWeight": "bold", "fontSize": size})


def btn(label, color, on_press=None, outline=False):
    return ui.button(
        label, on_press=on_press,
        UNSAFE_style={"background": WHITE if outline else color,
                      "color": color if outline else WHITE,
                      "border": f"1px solid {color}", "fontWeight": "bold"})


def tbtn(label, color, bg, border, on_press):
    return ui.button(label, on_press=on_press, UNSAFE_style={
        "background": bg, "color": color, "border": f"1px solid {border}", "fontWeight": "bold"})


def cell(text, basis, color=INK, bold=False, align="left"):
    return ui.html.div(str(text), style={
        "flex": f"0 0 {basis}px", "width": f"{basis}px", "color": color, "textAlign": align,
        "fontFamily": "monospace", "fontSize": "12px", "fontWeight": "bold" if bold else "normal"})


def stat(label, value, color=INK):
    return ui.flex(t(label, MUTED), t(value, color, True, True),
                   justify_content="space-between", width="100%")


def box(*children, **kw):
    return ui.view(*children, UNSAFE_style={"background": WHITE, "padding": "12px",
                                            "height": "100%", "overflow": "auto", **kw})


def th(text, w, bg=SOFT, color=MUTED, align="right"):
    return ui.html.div(text, style={
        "flex": f"0 0 {w}px", "width": f"{w}px", "background": bg, "color": color,
        "fontWeight": "bold", "fontSize": "11px", "textAlign": align, "padding": "5px 8px",
        "boxSizing": "border-box", "borderRight": f"1px solid {LINE}", "borderBottom": f"1px solid {LINE}"})


def td(text, w, color=INK, bold=False, align="right", bg=WHITE):
    return ui.html.div(str(text), style={
        "flex": f"0 0 {w}px", "width": f"{w}px", "color": color, "background": bg,
        "textAlign": align, "fontFamily": "monospace", "fontSize": "12px",
        "fontWeight": "bold" if bold else "normal", "padding": "7px 8px", "boxSizing": "border-box",
        "borderRight": f"1px solid {LINE}", "borderBottom": f"1px solid {LINE}"})


TYPE_COLORS = {"TRADE": GREEN, "NEW": BLUE, "MODIFY": PURPLE, "CANCEL": GREY, "EVENT": PURPLE, "ALERT": RED}
CHIPS = [("Trades", "TRADE", GREEN), ("Orders", "NEW", BLUE), ("Cancels", "CANCEL", GREY),
         ("Events", "EVENT", PURPLE), ("Alerts", "ALERT", RED)]


@ui.component
def operator_console():
    ats, set_ats = ui.use_state(True)
    orders, set_orders = ui.use_state([
        {"id": 48675, "side": "BUY", "inst": "RELIANCE 27 OCT FUT", "qty": 500, "px": 1346.30, "st": "OPEN"},
        {"id": 48673, "side": "BUY", "inst": "NIFTY 27 OCT 25800 PE", "qty": 975, "px": 158.65, "st": "OPEN"},
        {"id": 48672, "side": "BUY", "inst": "NIFTY 27 OCT 26200 CE", "qty": 975, "px": 164.25, "st": "OPEN"},
        {"id": 48668, "side": "BUY", "inst": "ICICIBANK 27 OCT 1400 CE", "qty": 700, "px": 32.85, "st": "OPEN"},
        {"id": 48659, "side": "SELL", "inst": "RELIANCE", "qty": 250, "px": 1344.05, "st": "PART"},
    ])
    positions, set_positions = ui.use_state([
        {"inst": "NIFTY 27 OCT 26200 CE", "bq": 46800, "ba": 162.43, "bv": "76.02 L", "sq": 45825, "sa": 162.85, "sv": "74.62 L", "net": 975, "ltp": 163.30, "m2m": 19685},
        {"inst": "NIFTY 27 OCT 25800 PE", "bq": 44850, "ba": 159.57, "bv": "71.57 L", "sq": 45825, "sa": 160.03, "sv": "73.33 L", "net": -975, "ltp": 160.65, "m2m": 19822},
        {"inst": "HDFCBANK 27 OCT FUT", "bq": 68200, "ba": 1022.29, "bv": "6.97 Cr", "sq": 73150, "sa": 1022.52, "sv": "7.48 Cr", "net": -4950, "ltp": 1023.75, "m2m": 9708},
        {"inst": "HDFCBANK", "bq": 67100, "ba": 1019.13, "bv": "6.84 Cr", "sq": 61600, "sa": 1019.37, "sv": "6.28 Cr", "net": 5500, "ltp": 1020.20, "m2m": 20680},
        {"inst": "RELIANCE 27 OCT FUT", "bq": 27000, "ba": 1346.66, "bv": "3.64 Cr", "sq": 28500, "sa": 1346.86, "sv": "3.84 Cr", "net": -1500, "ltp": 1347.40, "m2m": 4520},
        {"inst": "RELIANCE", "bq": 29000, "ba": 1344.03, "bv": "3.90 Cr", "sq": 27500, "sa": 1344.17, "sv": "3.70 Cr", "net": 1500, "ltp": 1345.20, "m2m": 5388},
        {"inst": "ICICIBANK 27 OCT 1400 CE", "bq": 30800, "ba": 32.77, "bv": "10.09 L", "sq": 31500, "sa": 33.00, "sv": "10.40 L", "net": -700, "ltp": 34.65, "m2m": 6034},
        {"inst": "SBIN 27 OCT FUT", "bq": 35250, "ba": 862.20, "bv": "3.04 Cr", "sq": 34500, "sa": 862.22, "sv": "2.97 Cr", "net": 750, "ltp": 862.85, "m2m": 1268},
    ])
    log, set_log = ui.use_state([
        {"t": "17:42:26", "type": "TRADE", "msg": "SOLD 1,100 HDFCBANK @ 1,020.05"},
        {"t": "17:42:25", "type": "TRADE", "msg": "BOUGHT 750 SBIN 27 OCT FUT @ 862.55"},
        {"t": "17:42:23", "type": "MODIFY", "msg": "BUY NIFTY 27 OCT 25800 PE 158.85 → 158.65"},
        {"t": "17:42:22", "type": "NEW", "msg": "BUY 500 RELIANCE 27 OCT FUT limit 1,346.30"},
    ])
    kinds, set_kinds = ui.use_state(["TRADE", "NEW", "MODIFY", "CANCEL", "EVENT", "ALERT"])
    query, set_query = ui.use_state("")
    paused, set_paused = ui.use_state(False)
    frozen, set_frozen = ui.use_state([])
    hide_flat, set_hide_flat = ui.use_state(False)

    def add_log(kind, msg):
        set_log(lambda old: [{"t": now(), "type": kind, "msg": msg}] + old)

    # ---------- handlers ----------
    def handle_ats(on):
        set_ats(on)
        add_log("EVENT", "ATS switched ON" if on else "ATS switched OFF")

    def handle_cancel_bids():
        n = sum(1 for o in orders if o["side"] == "BUY")
        set_orders([o for o in orders if o["side"] != "BUY"])
        add_log("CANCEL", f"Cancelled {n} bid orders")

    def handle_square_off():
        n = sum(1 for p in positions if p["net"] != 0)
        set_positions([{**p, "net": 0} for p in positions])
        set_orders([])
        add_log("ALERT", f"Squared off {n} positions")

    def handle_cancel(order_id):
        set_orders([o for o in orders if o["id"] != order_id])
        add_log("CANCEL", f"Cancelled #{order_id}")

    def handle_trade(inst, side):
        def upd(p):
            if p["inst"] != inst:
                return p
            if side == "BUY":
                return {**p, "bq": p["bq"] + LOT, "net": p["net"] + LOT}
            return {**p, "sq": p["sq"] + LOT, "net": p["net"] - LOT}
        set_positions([upd(p) for p in positions])
        add_log("TRADE", f"{'BOUGHT' if side == 'BUY' else 'SOLD'} {LOT} {inst}")

    def handle_pause(is_paused):
        set_paused(is_paused)
        if is_paused:
            set_frozen(log)

    def handle_chip(kind):
        set_kinds(lambda ks: [k for k in ks if k != kind] if kind in ks else ks + [kind])

    # ---------- derived values ----------
    total_m2m = sum(p["m2m"] for p in positions)
    expenses = 83372
    net_m2m = total_m2m - expenses
    open_count = len(orders)
    bid_count = sum(1 for o in orders if o["side"] == "BUY")
    flat_count = sum(1 for p in positions if p["net"] == 0)
    shown_log = frozen if paused else log
    shown_log = [m for m in shown_log if m["type"] in kinds and query.lower() in m["msg"].lower()]
    shown_pos = [p for p in positions if not (hide_flat and p["net"] == 0)]

    # ---------- Controls ----------
    controls = ui.panel(box(
        h("ATS · Auto trading"),
        ui.flex(btn("ON", GREEN, lambda: handle_ats(True), outline=not ats),
                btn("OFF", RED, lambda: handle_ats(False), outline=ats), gap="size-100"),
        t("Running since 09:15:00 IST" if ats else "Stopped", GREEN if ats else RED, True),
        ui.flex(btn(f"Cancel all bids · {bid_count}", AMBER, handle_cancel_bids, outline=True),
                btn(f"Square off all · {len(positions) - flat_count}", RED, handle_square_off),
                gap="size-100", wrap=True),
        UNSAFE_style={"display": "flex", "flexDirection": "column", "gap": "10px"},
    ), title="Controls")

    # ---------- Summary ----------
    fills = 1606 + len([m for m in log if m["type"] == "TRADE"]) - 2
    summary = ui.panel(box(ui.flex(
        ui.flex(h("Trading activity"), stat("Turnover", "₹40.55 Cr"), stat("Fills today", f"{fills:,}"),
                stat("Open orders", str(open_count)), direction="column", flex_grow=1, gap="size-100"),
        ui.flex(h("Mark-to-market"), stat("Gross M2M", money(total_m2m), GREEN if total_m2m >= 0 else RED),
                stat("Realised", "+₹82,108", GREEN), stat("Unrealised", money(total_m2m - 82108), GREEN),
                direction="column", flex_grow=1, gap="size-100"),
        ui.flex(h("Net result"), stat("Net M2M", money(net_m2m), GREEN if net_m2m >= 0 else RED),
                stat("Expenses", f"₹{expenses:,}", RED), stat("Peak net today", "+₹50,855", GREEN),
                direction="column", flex_grow=1, gap="size-100"),
        gap="size-300")), title="Summary")

    # ---------- Message window ----------
    chips = ui.flex(*[btn(f"{label} {sum(1 for m in log if m['type'] == k)}", c, (lambda k=k: handle_chip(k)),
                          outline=(k not in kinds)) for label, k, c in CHIPS], gap="size-100", wrap=True)

    message_window = ui.panel(box(
        ui.flex(ui.text_field(label="Filter", value=query, on_change=set_query, width="size-2400"),
                ui.toggle_button("Pause", is_selected=paused, on_change=handle_pause),
                align_items="end", gap="size-100"),
        chips,
        *[ui.flex(cell(m["t"], 70, GREY), cell(m["type"], 60, TYPE_COLORS[m["type"]], True), cell(m["msg"], 360),
                  UNSAFE_style={"borderBottom": f"1px solid {LINE}", "padding": "4px 0"}) for m in shown_log[:60]],
        UNSAFE_style={"display": "flex", "flexDirection": "column", "gap": "8px"},
    ), title="Message window")

    # ---------- Order book ----------
    order_book = ui.panel(box(
        t(f"{open_count} open · {bid_count} buy · {open_count - bid_count} sell", MUTED),
        *[ui.flex(cell(o["side"], 50, GREEN if o["side"] == "BUY" else RED, True), cell(o["inst"], 210),
                  cell(f"{o['qty']:,}", 70, align="right"), cell(f"{o['px']:,.2f}", 90, bold=True, align="right"),
                  cell(o["st"], 60, AMBER if o["st"] == "PART" else GREEN, True),
                  btn("×", GREY, (lambda i=o["id"]: handle_cancel(i)), outline=True),
                  align_items="center", gap="size-100",
                  UNSAFE_style={"borderBottom": f"1px solid {LINE}", "padding": "4px 0"}) for o in orders],
        UNSAFE_style={"display": "flex", "flexDirection": "column", "gap": "6px"},
    ), title="Order book")

    # ---------- Net positions table ----------
    BUY_H, SELL_H, NET_H = "#e8f0fe", "#fdeee4", "#eef1f4"
    BUY_C, SELL_C, NET_C = "#1f5fbf", "#b45309", "#3b434d"
    BUY_B, SELL_B = "#f7faff", "#fffaf6"
    W = dict(n=30, inst=190, q=72, a=80, v=80, ltp=80, m2m=90, trade=120)
    TICK = {"HDFCBANK 27 OCT FUT": 1, "HDFCBANK": 1,                      # sample LTP flashes
            "RELIANCE 27 OCT FUT": -1, "RELIANCE": -1, "ICICIBANK 27 OCT 1400 CE": -1}

    group_hdr = ui.flex(
        th("", W["n"] + W["inst"]),
        th("BUY", W["q"] + W["a"] + W["v"], BUY_H, BUY_C, "center"),
        th("SELL", W["q"] + W["a"] + W["v"], SELL_H, SELL_C, "center"),
        th("NET", W["q"] + W["ltp"] + W["m2m"], NET_H, NET_C, "center"),
        th("", W["trade"]),
    )
    col_hdr = ui.flex(
        th("#", W["n"], align="center"), th("INSTRUMENT", W["inst"], align="left"),
        th("QTY", W["q"], BUY_H, BUY_C), th("AVG", W["a"], BUY_H, BUY_C), th("VALUE", W["v"], BUY_H, BUY_C),
        th("QTY", W["q"], SELL_H, SELL_C), th("AVG", W["a"], SELL_H, SELL_C), th("VALUE", W["v"], SELL_H, SELL_C),
        th("QTY", W["q"], NET_H, NET_C), th("LTP", W["ltp"], NET_H, NET_C), th("M2M", W["m2m"], NET_H, NET_C),
        th("TRADE", W["trade"], align="center"),
    )

    def pos_row(i, p):
        netc = BLUE if p["net"] > 0 else "#c2410c" if p["net"] < 0 else GREY
        tick = TICK.get(p["inst"], 0)
        ltp_bg = "#d5eadb" if tick > 0 else "#fbdcdc" if tick < 0 else WHITE
        m2m_c = GREEN if p["m2m"] >= 0 else RED
        return ui.flex(
            td(i + 1, W["n"], GREY, align="center"),
            td(p["inst"], W["inst"], bold=True, align="left"),
            td(f"{p['bq']:,}", W["q"], bg=BUY_B), td(f"{p['ba']:,.2f}", W["a"], bg=BUY_B),
            td(p["bv"], W["v"], MUTED, bg=BUY_B),
            td(f"{p['sq']:,}", W["q"], bg=SELL_B), td(f"{p['sa']:,.2f}", W["a"], bg=SELL_B),
            td(p["sv"], W["v"], MUTED, bg=SELL_B),
            td(f"{p['net']:+,}", W["q"], netc, True),
            td(f"{p['ltp']:,.2f}", W["ltp"], bg=ltp_bg),
            td(money(p["m2m"]), W["m2m"], m2m_c, True),
            ui.flex(tbtn("Buy", BLUE, "#e8f0fe", "#8fb3ee", (lambda n=p["inst"]: handle_trade(n, "BUY"))),
                    tbtn("Sell", "#c2410c", "#fdeee4", "#f0a87e", (lambda n=p["inst"]: handle_trade(n, "SELL"))),
                    gap="size-50", align_items="center", justify_content="center",
                    UNSAFE_style={"flex": f"0 0 {W['trade']}px", "width": f"{W['trade']}px", "background": WHITE,
                                  "borderRight": f"1px solid {LINE}", "borderBottom": f"1px solid {LINE}"}),
        )

    totals = ui.flex(
        td("Σ", W["n"], GREY, True, "center", SOFT),
        td(f"Total · {len(positions)} instruments", W["inst"], bold=True, align="left", bg=SOFT),
        td("", W["q"], bg=SOFT), td("", W["a"], bg=SOFT), td("25.96 Cr", W["v"], bold=True, bg=SOFT),
        td("", W["q"], bg=SOFT), td("", W["a"], bg=SOFT), td("25.85 Cr", W["v"], bold=True, bg=SOFT),
        td("", W["q"], bg=SOFT), td("", W["ltp"], bg=SOFT),
        td(money(total_m2m), W["m2m"], GREEN if total_m2m >= 0 else RED, True, bg=SOFT),
        td("", W["trade"], bg=SOFT),
    )

    positions_panel = ui.panel(
        ui.view(
            ui.flex(t(f"{len(positions) - flat_count} open · {flat_count} flat", MUTED),
                    ui.toggle_button("Hide flat", is_selected=hide_flat, on_change=set_hide_flat),
                    justify_content="space-between", align_items="center",
                    UNSAFE_style={"padding": "8px 12px"}),
            ui.flex(group_hdr, col_hdr, *[pos_row(i, p) for i, p in enumerate(shown_pos)], totals,
                    direction="column", UNSAFE_style={"minWidth": "max-content", "background": WHITE}),
            UNSAFE_style={"background": WHITE, "height": "100%", "overflow": "auto"}),
        title="Net positions")

    # Return a column (NOT a dashboard) so state can live in this component
    return ui.column(
        ui.row(controls, summary, height=28),
        ui.row(message_window, ui.column(order_book, positions_panel)),
    )


# Dashboard at the root of the script -> opens in its own new tab
operator_v2 = ui.dashboard(operator_console())