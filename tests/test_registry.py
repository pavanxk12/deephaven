"""The portfolio registry: a user never has two portfolios with the same number, whichever strategy or screen asks."""
import json
import threading
import types

import pytest

from algo.comms.nats_service import NatsConfig, NatsService
from algo.core.app import App
from algo.core.config import NSE, Paths
from algo.strategy.book import Book, Portfolios
from algo.strategy.bridge import Bridge
from algo.strategy.config import BridgeConfig
from algo.strategy.registry import PortfolioRegistry, Registry
from tests.test_plugins_wiring import fake_dh  # noqa: F401  (a pytest fixture)
from tests.test_nats_service import Wire


@pytest.fixture
def reg(tmp_path):
    return Registry(str(tmp_path / "reg.json"))


# ---- numbers ----------------------------------------------------------------------------------------------------
def test_numbers_for_one_user_never_repeat_whoever_asks(reg):
    got = [reg.allocate(3, "rsi_stocks", start=1111) for _ in range(3)] + [reg.allocate(3, "rsi_nifty", start=1111) for _ in range(3)]
    assert got[:3] == [1111, 1112, 1113] and len(set(got)) == 6          # the second strategy started at the same number: skipped past
    assert reg.allocate(3, "console", start=1111) not in got


def test_the_same_number_under_two_users_is_fine(reg):
    assert reg.allocate(3, "a", start=1001) == 1001 and reg.allocate(4, "b", start=1001) == 1001
    assert reg.owner_of(3, 1001) == "a" and reg.owner_of(4, 1001) == "b"


def test_each_strategy_keeps_its_own_range_and_skips_numbers_that_are_taken(reg):
    assert [reg.allocate(3, "stocks", start=1111) for _ in range(2)] == [1111, 1112]
    assert reg.allocate(3, "nifty", start=1555) == 1555                   # its own range, not 1113
    assert reg.allocate(3, "stocks", start=1111) == 1113
    for _ in range(441):                                                  # stocks runs up to the number just below the one nifty holds
        reg.allocate(3, "stocks", start=1111)
    assert reg.owner_of(3, 1555) == "nifty"
    assert reg.allocate(3, "stocks", start=1111) == 1556                  # ...and steps over it


def test_register_says_new_yours_or_taken_and_who_holds_it(reg):
    assert reg.register(3, 1001, "console").status == "registered"
    again = reg.register(3, 1001, "console")
    assert again.ok and again.status == "exists"
    clash = reg.register(3, 1001, "rsi_stocks")
    assert (clash.ok, clash.status, clash.held_by) == (False, "taken", "console") and "TAKEN by console" in str(clash)
    assert reg.owner_of(3, 1001) == "console"                              # the refusal changed nothing
    assert reg.register(4, 1001, "rsi_stocks").ok                          # another user's 1001 is another portfolio


def test_an_allocated_number_cannot_be_registered_by_someone_else(reg):
    n = reg.allocate(3, "rsi_stocks", start=1111)
    assert not reg.register(3, n, "console").ok and reg.register(3, n, "rsi_stocks").ok


def test_bad_users_numbers_and_owners_are_errors_not_registered(reg):
    for call in (lambda: reg.allocate(0, "a"), lambda: reg.allocate(-3, "a"), lambda: reg.allocate("x", "a"), lambda: reg.allocate(True, "a"),
                 lambda: reg.allocate(3, ""), lambda: reg.allocate(3, "  "), lambda: reg.allocate(3, "a", start=0),
                 lambda: reg.register(3, 0, "a"), lambda: reg.register(3, "abc", "a"), lambda: reg.register(3, 1.5, "a")):
        with pytest.raises(ValueError):
            call()
    assert reg.portfolios() == []


def test_many_threads_asking_at_once_never_get_the_same_number(reg):
    got, lock = [], threading.Lock()

    def worker(owner, user):
        mine = [reg.allocate(user, owner, start=1111) for _ in range(60)]
        with lock:
            got.extend((user, n) for n in mine)
    threads = [threading.Thread(target=worker, args=(f"algo{i}", 3 if i % 2 else 4)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(got) == 480 and len(set(got)) == 480                         # every (user, number) is different
    assert reg.count() == (2, 480)


# ---- the file ---------------------------------------------------------------------------------------------------
def test_a_new_registry_object_sees_every_number_ever_handed_out(tmp_path):
    path = str(tmp_path / "reg.json")
    first = Registry(path)
    a = first.allocate(3, "rsi_stocks", start=1111, note="stocks")
    first.register(4, 7, "console")
    second = Registry(path)                                                 # a restart, or another session
    assert second.owner_of(3, a) == "rsi_stocks" and second.owner_of(4, 7) == "console"
    assert second.allocate(3, "rsi_stocks", start=1111) == a + 1
    assert not second.register(3, a, "someone_else").ok
    assert [r[:3] for r in second.portfolios(3)] == [(3, 1111, "rsi_stocks"), (3, 1112, "rsi_stocks")]
    assert not (tmp_path / "reg.json.tmp").exists()                          # written through a temp file, which is gone


def test_a_damaged_file_stops_the_start_instead_of_handing_out_numbers_in_use(tmp_path):
    path = tmp_path / "reg.json"
    path.write_text('{"3": {"1111": {"owner": "rsi", "created": "x", "note": ""', encoding="utf-8")   # cut off
    with pytest.raises(RuntimeError, match="cannot be read"):
        Registry(str(path))
    assert path.read_text(encoding="utf-8").startswith('{"3"')              # and it was not overwritten
    path.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(RuntimeError, match="cannot be read"):
        Registry(str(path))


def test_a_missing_file_is_a_fresh_start(tmp_path):
    assert Registry(str(tmp_path / "sub" / "none.json")).portfolios() == []


def test_a_number_that_could_not_be_saved_is_not_handed_out(reg, monkeypatch):
    real = reg._save

    def broken(*args):
        raise OSError("disk full")
    monkeypatch.setattr(reg, "_save", broken)
    with pytest.raises(OSError):
        reg.allocate(3, "a", start=1111)
    assert reg.portfolios() == []
    monkeypatch.setattr(reg, "_save", real)
    assert reg.allocate(3, "a", start=1111) == 1111                         # the same number, nobody else saw it


def test_listeners_hear_about_new_portfolios_and_cannot_break_it(reg):
    heard = []
    reg.on_change(lambda u, p, rec: 1 / 0)
    off = reg.on_change(lambda u, p, rec: heard.append((u, p, rec["owner"])))
    reg.allocate(3, "a", start=5)
    reg.register(3, 5, "a")                                                 # already there: nothing new to hear
    reg.register(3, 6, "b")
    off()
    reg.allocate(3, "a", start=5)
    assert heard == [(3, 5, "a"), (3, 6, "b")]


# ---- the book ---------------------------------------------------------------------------------------------------
def _book(tmp_path, name, reg, user, start=1111, owner=None):
    return Portfolios(str(tmp_path / f"{name}_m.json"), str(tmp_path / f"{name}_s.json"), start, registry=reg, user=user,
                      owner=owner or name)


def test_two_books_of_one_user_never_get_the_same_portfolio_number(tmp_path, reg):
    a, b = _book(tmp_path, "stocks", reg, 3), _book(tmp_path, "nifty", reg, 3)        # the same pf_start on purpose
    a.track([10, 11])
    b.track([20, 21])
    got = [a.create(10), b.create(20), a.create(11), b.create(21)]
    assert len(set(got)) == 4
    assert reg.owner_of(3, got[0]) == "stocks" and reg.owner_of(3, got[1]) == "nifty"


def test_two_users_may_each_have_portfolio_1111(tmp_path, reg):
    a, b = _book(tmp_path, "u3", reg, 3), _book(tmp_path, "u4", reg, 4)
    a.track([10])
    b.track([10])
    assert a.create(10) == 1111 and b.create(10) == 1111


def test_a_book_without_a_user_numbers_itself_as_before(tmp_path):
    book = Portfolios(str(tmp_path / "m.json"), str(tmp_path / "s.json"), 500)
    book.track([1])
    assert book.create(1) == 500


def test_existing_numbers_in_a_books_files_are_claimed_and_a_clash_is_reported(tmp_path, reg):
    (tmp_path / "old_m.json").write_text(json.dumps({"10": 1111, "11": 1112}))
    (tmp_path / "old_s.json").write_text(json.dumps({"1112": 555}))
    reg.register(3, 1112, "someone_else")                                   # this number is already somebody else's
    old = _book(tmp_path, "old", reg, 3)
    clashes = old.claim_existing()
    assert [(c.pf, c.held_by) for c in clashes] == [(1112, "someone_else")]
    assert reg.owner_of(3, 1111) == "old"
    old.track([12])
    assert old.create(12) not in (1111, 1112)                                # new ones step around everything that is held


def test_the_book_plugin_needs_the_registry_when_it_has_a_user_and_takes_its_numbers_from_it(tmp_path):
    app = App({}, NSE, Paths(market_data=str(tmp_path), work=str(tmp_path)))
    assert Book(name="plain").requires == () and Book(user=3).requires == ("registry",)
    with pytest.raises(Exception, match="registry"):
        app.run(Book(user=3, name="book_a"))
    registry = Registry(str(tmp_path / "reg.json"))
    app.plugins["registry"] = types.SimpleNamespace(state=registry)
    a, b = Book(user=3, name="book_a"), Book(user=3, name="book_b")
    app.run(a)
    app.run(b)
    a.state.track([1])
    b.state.track([2])
    assert a.state.create(1) != b.state.create(2)
    assert {registry.owner_of(3, p) for p in (1111, 1112)} == {"book_a", "book_b"}


def test_a_bridge_sends_as_the_user_of_its_book(tmp_path):
    reg = Registry(str(tmp_path / "reg.json"))
    from tests.test_nats_service import make
    svc, wire = make(users=(3, 4))
    book = _book(tmp_path, "u4", reg, 4)
    book.track([10])
    pf = book.create(10)
    bridge = Bridge(BridgeConfig())                                          # no user of its own
    bridge.bind(svc, book)
    assert bridge.user == 4
    assert bridge.pause_pf(pf).result(5).ok and wire.sent[0][2]["USERIDENTIFIER"] == 4
    svc.on_stop()


# ---- the plugin and the console functions -------------------------------------------------------------------------
def test_the_plugin_publishes_the_table_with_every_portfolio_and_new_ones(tmp_path, fake_dh, monkeypatch):  # noqa: F811
    from tests.test_plugins_wiring import FakeTable
    monkeypatch.setattr(FakeTable, "last_by", lambda self, by: self, raising=False)       # the stand-in table has no last_by
    path = str(tmp_path / "reg.json")
    Registry(path).allocate(3, "rsi_stocks", start=1111)                     # from an earlier session
    app = App({}, NSE, Paths(market_data=str(tmp_path), work=str(tmp_path)))
    plugin = app.run(PortfolioRegistry(file=path))
    writer = app.shared[("registry_rows", path)]
    assert [r[:2] + r[4:5] for r in writer.rows] == [(3, 1111, "rsi_stocks")]          # User, PF, ..., Owner
    assert "portfolio_registry" in app.tables and "portfolio_registry_log" in app.tables
    plugin.state.allocate(4, "console", start=7, algo="screen", name="NIFTY")
    assert writer.rows[-1][:5] == (4, 7, "NIFTY", "screen", "console") and "2 portfolios across 2 users" in plugin.describe()
    app.run(PortfolioRegistry(file=path))                                    # restarted: the same table, no duplicate rows
    assert len(writer.rows) == 2
    plugin.state.allocate(3, "rsi_stocks", start=1111)
    assert len(writer.rows) == 3                                             # ...and only the new listener writes


def test_console_functions_hand_out_claim_and_list_and_never_raise_when_the_registry_is_down(tmp_path, capsys):
    import algo
    from algo import api
    from algo.core import app as app_module
    app_module._APP = None
    ns = {}
    app = algo.install(ns, "NSE")
    assert ns["pf_create"](3, "rsi_stocks", "RELIANCE") is None and ns["pf_register"](3, 1001) is False
    ns["pf_list"]()
    assert capsys.readouterr().out.count("registry is not running") == 3
    app.plugins["registry"] = types.SimpleNamespace(state=Registry(str(tmp_path / "reg.json")))
    n = ns["pf_create"](3, "rsi_stocks")
    assert n == 1111
    assert ns["pf_register"](3, n, "console") is False and ns["pf_register"](3, 1001, "console") is True
    ns["pf_list"](3)
    out = capsys.readouterr().out
    assert "created portfolio 1111 for rsi_stocks" in out and "TAKEN by rsi_stocks" in out
    assert "portfolio 1001  -  algo -  book console" in out and "portfolio 1111  -  algo rsi_stocks  book rsi_stocks" in out
    ns["pf_list"](9)
    assert "no portfolios registered for user 9" in capsys.readouterr().out
    assert callable(api.registry_start) and "registry_start" in ns


# ---- the NATS service's start() had no test; it runs here against the Deephaven stand-in ----------------------------------
def test_the_nats_service_start_creates_its_tables_and_keeps_blocks_across_a_restart(tmp_path, fake_dh):  # noqa: F811
    app = App({}, NSE, Paths(market_data=str(tmp_path), work=str(tmp_path)))
    wire = Wire()
    svc = app.run(NatsService(NatsConfig(users=(3, 4), send_on=True), transport_factory=lambda: wire))
    assert "cmd_queue_automated" in app.tables and "nats_events" in app.tables and svc.zone is not None
    svc.block(user=4)
    assert svc.pause(1, user=3).result(5).ok and app.shared["cmd_writer"].rows[0][:2] == (1, "pause_unpause")
    again = app.run(NatsService(NatsConfig(users=(3, 4), send_on=True), transport_factory=lambda: wire))
    assert again is not svc and again.blocks() == {"users": [4], "portfolios": {}}          # the block outlived the restart
    assert again.pause(1, user=4).result(5).status == "blocked"
    lifted = app.run(NatsService(NatsConfig(users=(3, 4)), transport_factory=lambda: wire, lift_blocks=(4,)))
    assert lifted.blocks() == {"users": [], "portfolios": {}}
    app.stop("nats")


# ---- what each portfolio is: name and algo --------------------------------------------------------------------------
def test_user_number_name_algo_and_book_are_all_remembered_and_survive_a_restart(tmp_path):
    path = str(tmp_path / "reg.json")
    first = Registry(path)
    n = first.allocate(3, "book_stocks", start=1111, algo="rsi_stocks", name="RELIANCE", note="first")
    assert first.get(3, n) == {"owner": "book_stocks", "name": "RELIANCE", "algo": "rsi_stocks", "created": first.get(3, n)["created"],
                               "note": "first"}
    again = Registry(path)                                                  # a restart
    (row,) = again.portfolios(3)
    assert row[:5] == (3, n, "book_stocks", "RELIANCE", "rsi_stocks") and row[6] == "first"
    assert again.get(3, 99999) is None and again.get(4, n) is None


def test_the_algo_and_name_can_be_updated_by_the_book_that_owns_the_number_a_blank_keeps_what_is_there(reg):
    n = reg.allocate(3, "book", start=1111)                                 # nothing known about it yet
    assert reg.get(3, n)["name"] == "" and reg.get(3, n)["algo"] == ""
    assert reg.register(3, n, "book", algo="rsi_stocks", name="TCS").status == "updated"
    assert reg.register(3, n, "book", algo="rsi_stocks", name="TCS").status == "exists"      # nothing new: not rewritten
    assert reg.register(3, n, "book").status == "exists"                                      # blank: nothing changes
    assert reg.get(3, n)["algo"] == "rsi_stocks" and reg.get(3, n)["name"] == "TCS"
    assert reg.register(3, n, "book", algo="rsi_nifty").status == "updated"                   # another algo took it over
    assert (reg.get(3, n)["algo"], reg.get(3, n)["name"]) == ("rsi_nifty", "TCS")
    refused = reg.register(3, n, "some_other_book", algo="hijack", name="X")
    assert refused.status == "taken" and reg.get(3, n)["algo"] == "rsi_nifty"                # the book owns the number


def test_a_failed_save_of_an_update_puts_the_old_record_back(reg, monkeypatch):
    n = reg.allocate(3, "book", start=1111, algo="old", name="OLD")
    real = reg._save
    monkeypatch.setattr(reg, "_save", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        reg.register(3, n, "book", algo="new", name="NEW")
    monkeypatch.setattr(reg, "_save", real)
    assert (reg.get(3, n)["algo"], reg.get(3, n)["name"]) == ("old", "OLD")


def test_a_file_from_before_name_and_algo_existed_still_loads(tmp_path):
    path = tmp_path / "reg.json"
    path.write_text(json.dumps({"3": {"1111": {"owner": "book", "created": "2026-10-15T09:00:00", "note": ""}}}), encoding="utf-8")
    reg = Registry(str(path))
    assert reg.get(3, 1111)["owner"] == "book" and reg.get(3, 1111)["name"] == "" and reg.get(3, 1111)["algo"] == ""
    assert reg.register(3, 1111, "book", algo="rsi", name="TCS").status == "updated"


def test_two_algos_sharing_one_book_are_told_apart(tmp_path, reg):
    book = _book(tmp_path, "shared", reg, 3)
    book.track([10, 20])
    a = book.create(10, name="RELIANCE", algo="rsi_stocks")
    b = book.create(20, name="NIFTY", algo="rsi_nifty")
    assert reg.get(3, a)["algo"] == "rsi_stocks" and reg.get(3, b)["algo"] == "rsi_nifty"
    assert reg.get(3, a)["owner"] == reg.get(3, b)["owner"] == "shared"
    book.label(a, algo="rsi_nifty")                                          # a book without a registry does nothing, with one it records
    assert reg.get(3, a)["algo"] == "rsi_nifty" and reg.get(3, a)["name"] == "RELIANCE"
    plain = Portfolios(str(tmp_path / "p_m.json"), str(tmp_path / "p_s.json"), 1)
    plain.label(1, algo="x", name="y")


def test_the_strategy_records_its_name_and_the_stock_when_it_creates_and_when_it_reuses_a_portfolio(master, tmp_path):
    from tests.test_strategy import RELIANCE_FUT, build
    registry = Registry(str(tmp_path / "reg.json"))
    s, book, bridge = build(master, tmp_path)
    book.registry, book.user, book.owner = registry, 3, "book_stocks"
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")                                  # creates the portfolio
    (row,) = registry.portfolios(3)
    assert row[:5] == (3, 1111, "book_stocks", "RELIANCE", "rsi_stocks")
    registry.register(3, 1111, "book_stocks", algo="someone_before")         # recorded as used by another algo
    book.set_live(RELIANCE_FUT, None)
    s.on_signal(RELIANCE_FUT, "OVERBOUGHT")                                  # the same option again: the portfolio is reused
    assert len(bridge.swaps) == 2 and bridge.swaps[1]["pf"] == 1111
    assert registry.get(3, 1111)["algo"] == "rsi_stocks"                     # and it is recorded as ours again
    s.on_signal(RELIANCE_FUT, "OVERSOLD")                                    # the other leg: a new portfolio, labelled too
    assert registry.get(3, 1112)["algo"] == "rsi_stocks" and registry.get(3, 1112)["name"] == "RELIANCE"


# ---- lean: the file is appended to, never rewritten --------------------------------------------------------------------
def _lines(path):
    with open(path, encoding="utf-8") as f:
        return [ln for ln in f.read().split("\n") if ln.strip()]


def test_every_change_is_one_appended_line_and_the_latest_line_wins_on_reload(tmp_path):
    path = str(tmp_path / "reg.json")
    reg = Registry(path)
    n = reg.allocate(3, "book", start=1111)
    reg.register(3, n, "book", algo="rsi_stocks", name="TCS")                # an update: a second line for the same portfolio
    reg.register(4, 5, "console")
    lines = _lines(path)
    assert len(lines) == 3 and all(ln.startswith('{"u":') for ln in lines)
    again = Registry(path)
    assert (again.get(3, n)["algo"], again.get(3, n)["name"]) == ("rsi_stocks", "TCS") and again.count() == (2, 2)


def test_allocating_never_rewrites_the_file_so_it_costs_the_same_at_any_size(tmp_path, monkeypatch):
    from algo.strategy import registry as registry_module
    rewrites = []
    monkeypatch.setattr(registry_module, "replace_with_retry", lambda *a: rewrites.append(a))
    reg = Registry(str(tmp_path / "reg.json"))
    for i in range(600):
        reg.allocate(3 + i % 2, "book", start=1111)
    assert rewrites == [] and len(_lines(str(tmp_path / "reg.json"))) == 600


def test_a_line_cut_off_by_a_crash_is_ignored_and_cleaned_away(tmp_path):
    path = tmp_path / "reg.json"
    reg = Registry(str(path))
    reg.allocate(3, "book", start=1111)
    reg.allocate(3, "book", start=1111)
    with open(path, "ab") as f:
        f.write(b'{"u":3,"p":1113,"owner":"bo')                              # the process died in the middle of an append
    again = Registry(str(path))
    assert again.count() == (1, 2) and again.get(3, 1113) is None             # that number was never answered, so it is free
    assert path.read_bytes().endswith(b"\n") and len(_lines(str(path))) == 2   # and the file was tidied
    assert again.allocate(3, "book", start=1111) == 1113 and len(_lines(str(path))) == 3


def test_garbage_in_the_middle_or_at_the_start_of_the_file_stops_the_start(tmp_path):
    path = tmp_path / "reg.json"
    good = b'{"u":3,"p":1111,"owner":"book","name":"","algo":"","created":"x","note":""}\n'
    path.write_bytes(good + b"this is not a record\n" + good.replace(b"1111", b"1112"))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="cannot be read"):
        Registry(str(path))
    assert path.read_bytes() == before                                         # not touched
    path.write_bytes(b"hello\n" + good)
    with pytest.raises(RuntimeError, match="cannot be read"):
        Registry(str(path))


def test_the_first_version_of_the_file_is_read_and_rewritten_as_lines(tmp_path):
    path = tmp_path / "reg.json"
    old = {"3": {"1111": {"owner": "book", "created": "2026-10-15T09:00:00", "note": "n"}, "1112": {"owner": "book", "created": "x",
                                                                                                    "note": ""}}}
    path.write_text(json.dumps(old, indent=1), encoding="utf-8")
    reg = Registry(str(path))
    assert reg.count() == (1, 2) and reg.get(3, 1111)["note"] == "n"
    assert len(_lines(str(path))) == 2 and _lines(str(path))[0].startswith('{"u":3')
    assert Registry(str(path)).count() == (1, 2)
    reg.allocate(3, "book", start=1111)
    assert Registry(str(path)).count() == (1, 3)


def test_many_updates_of_one_portfolio_are_squeezed_out_when_the_file_is_next_read(tmp_path):
    path = str(tmp_path / "reg.json")
    reg = Registry(path)
    n = reg.allocate(3, "book", start=1111)
    for i in range(400):
        reg.register(3, n, "book", algo=f"algo{i}")
    assert len(_lines(path)) == 401
    again = Registry(path)
    assert len(_lines(path)) == 1 and again.get(3, n)["algo"] == "algo399"


def test_a_write_that_fails_half_way_leaves_no_half_line_behind(tmp_path, monkeypatch):
    import builtins
    path = str(tmp_path / "reg.json")
    reg = Registry(path)
    reg.allocate(3, "book", start=1111)
    real_open = builtins.open

    class HalfWriter:
        def __init__(self, f):
            self.f = f

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.f.close()

        def tell(self):
            return self.f.tell()

        def truncate(self, pos):
            self.f.flush()
            return self.f.truncate(pos)

        def flush(self):
            self.f.flush()

        def write(self, data):
            self.f.write(data[:len(data) // 2])                                  # half the line reaches the disk...
            self.f.flush()
            raise OSError("disk full")                                           # ...and then it fails

    def fake_open(file, mode="r", *a, **k):
        f = real_open(file, mode, *a, **k)
        return HalfWriter(f) if str(file) == path and mode == "ab" else f
    monkeypatch.setattr(builtins, "open", fake_open)
    with pytest.raises(OSError):
        reg.allocate(3, "book", start=1111)
    monkeypatch.setattr(builtins, "open", real_open)
    assert len(_lines(path)) == 1 and open(path, "rb").read().endswith(b"\n")    # the file is as it was
    assert reg.portfolios(3)[0][1] == 1111 and len(reg.portfolios(3)) == 1
    assert reg.allocate(3, "book", start=1111) == 1112 and Registry(path).count() == (1, 2)


def test_owner_and_algo_names_are_shared_not_copied_for_every_portfolio(tmp_path):
    path = str(tmp_path / "reg.json")
    reg = Registry(path)
    for _ in range(50):
        reg.allocate(3, "".join(["book", "_s"]), start=1111, algo="".join(["rsi", "_stocks"]))
    again = Registry(path)
    owners = {id(again.get(3, 1111 + i)["owner"]) for i in range(50)}
    algos = {id(again.get(3, 1111 + i)["algo"]) for i in range(50)}
    assert len(owners) == 1 and len(algos) == 1


# ---- the book decides; the algo never picks a number ------------------------------------------------------------------------
def test_the_book_gives_an_algo_its_portfolio_and_says_when_one_retires(tmp_path, reg):
    book = _book(tmp_path, "stocks", reg, 3)
    book.track([10])
    pf, old, created = book.assign(10, option=55, name="RELIANCE", algo="rsi_stocks")
    assert (pf, old, created) == (1111, None, True)                           # none yet: a new one, numbered by the registry
    assert reg.get(3, 1111)["algo"] == "rsi_stocks" and reg.get(3, 1111)["name"] == "RELIANCE"
    book.subscribe(pf, 55)                                                      # the bridge subscribed it to option 55
    assert book.assign(10, 55, "RELIANCE", "rsi_stocks") == (1111, None, False)   # same option: the same portfolio
    pf2, old2, created2 = book.assign(10, 56, "RELIANCE", "rsi_stocks")          # another option: a NEW portfolio, the old one retires
    assert (pf2, old2, created2) == (1112, 1111, True) and book.pf_of(10) == 1112


def test_plan_shows_what_assign_would_do_and_changes_nothing(tmp_path, reg):
    book = _book(tmp_path, "stocks", reg, 3)
    book.track([10])
    assert book.plan(10, 55) == (None, None)
    pf = book.assign(10, 55)[0]
    book.subscribe(pf, 55)
    before = reg.count()
    assert book.plan(10, 55) == (pf, None) and book.plan(10, 56) == (pf, pf)
    assert reg.count() == before and book.pf_of(10) == pf                       # nothing was created or moved


def test_the_algo_asks_the_book_and_never_creates_or_numbers_a_portfolio_itself():
    import inspect
    from pathlib import Path
    assert "pf" not in inspect.signature(Portfolios.assign).parameters         # there is no number for the algo to pass
    source = (Path(__file__).resolve().parents[1] / "algo" / "strategy" / "rsi_options.py").read_text(encoding="utf-8")
    assert "book.assign(" in source and "book.create(" not in source


def test_two_users_books_assign_independently(tmp_path, reg):
    a, b = _book(tmp_path, "u3", reg, 3), _book(tmp_path, "u4", reg, 4)
    a.track([10])
    b.track([10])
    assert a.assign(10, 55)[0] == 1111 and b.assign(10, 55)[0] == 1111
    assert reg.owner_of(3, 1111) == "u3" and reg.owner_of(4, 1111) == "u4"


# ---- create(user, strategy, name) -> the number: that is all an algo says ---------------------------------------------------
def test_create_returns_the_number_and_remembers_who_asked_and_what_it_is_for(reg):
    pf = reg.create(3, "rsi_stocks", "RELIANCE")
    assert pf == 1111
    rec = reg.get(3, pf)
    assert (rec["owner"], rec["algo"], rec["name"]) == ("rsi_stocks", "rsi_stocks", "RELIANCE")
    assert reg.create(3, "rsi_stocks", "TCS") == 1112 and reg.create(3, "rsi_nifty", "NIFTY", start=1555) == 1555
    assert reg.create(4, "rsi_stocks", "RELIANCE") == 1111                                  # another user: its own numbers
    assert reg.create(3, "console", "X") == 1113                                            # a screen asking too: never a duplicate


def test_create_never_hands_out_the_same_number_to_two_strategies_asking_together(reg):
    got, lock = [], threading.Lock()

    def ask(strategy):
        mine = [reg.create(3, strategy, f"S{i}") for i in range(50)]
        with lock:
            got.extend(mine)
    threads = [threading.Thread(target=ask, args=(f"algo{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(got) == 300 and len(set(got)) == 300


def test_create_through_the_plugin_is_what_an_algo_calls(tmp_path, fake_dh, monkeypatch):  # noqa: F811
    from tests.test_plugins_wiring import FakeTable
    monkeypatch.setattr(FakeTable, "last_by", lambda self, by: self, raising=False)
    app = App({}, NSE, Paths(market_data=str(tmp_path), work=str(tmp_path)))
    plugin = app.run(PortfolioRegistry(file=str(tmp_path / "reg.json")))
    assert app["registry"].create(3, "rsi_stocks", "RELIANCE") == 1111
    assert app.shared[("registry_rows", str(tmp_path / "reg.json"))].rows[-1][:5] == (3, 1111, "RELIANCE", "rsi_stocks", "rsi_stocks")
    app.stop("registry")
    with pytest.raises(RuntimeError, match="not running"):
        PortfolioRegistry(file="x").create(3, "a", "b")
    assert plugin is not None


def test_create_rejects_a_bad_user_or_an_empty_strategy(reg):
    for call in (lambda: reg.create(0, "a", "b"), lambda: reg.create("x", "a", "b"), lambda: reg.create(3, "", "b"),
                 lambda: reg.create(3, "a", "b", start=0)):
        with pytest.raises(ValueError):
            call()
    assert reg.portfolios() == []
