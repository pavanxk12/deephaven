import json

from algo.strategy.book import Portfolios


def book(tmp_path, pf_start=1111):
    return Portfolios(str(tmp_path / "map.json"), str(tmp_path / "sub.json"), pf_start)


def sym(tok):
    return {10: "RELIANCE 30JAN30 FUT", 20: "INFY 30JAN30 FUT", 55: "RELIANCE 30JAN30 3000 CE",
            56: "RELIANCE 30JAN30 3100 CE"}.get(tok, str(tok))


def test_first_portfolio_uses_pf_start_then_increments(tmp_path):
    b = book(tmp_path, 1555)
    b.track([10, 20])
    assert b.create(10) == 1555 and b.create(20) == 1556
    assert b.pf_of(10) == 1555 and b.all_pfs() == [1555, 1556]


def test_numbers_are_never_reused_after_a_restart(tmp_path):
    b = book(tmp_path)
    b.track([10])
    pf = b.create(10)
    b.subscribe(pf, 55)
    again = book(tmp_path)                                  # new process reads the files
    again.track([10, 20])
    assert again.pf_of(10) == pf and again.subscribed == {pf: 55}
    assert again.create(20) == pf + 1


def test_numbering_sees_a_portfolio_written_by_another_session(tmp_path):
    b = book(tmp_path)
    b.track([10])
    (tmp_path / "sub.json").write_text(json.dumps({"5000": 1}))
    assert b.create(10) == 5001


def test_untracked_tokens_have_no_pf(tmp_path):
    b = book(tmp_path)
    b.pf_map[99] = 7                                        # left over from an earlier day
    b.track([10])
    assert b.pf_of(99) is None and b.current_pf_map() == {}


def test_reset_clears_caps_and_live(tmp_path):
    b = book(tmp_path)
    b.track([10])
    pf = b.create(10)
    b.caps[pf] = 15
    b.set_live(10, "CALL", 55)
    b.reset([10])
    assert b.caps[pf] == 0 and b.live[10] is None and 10 not in b.opt_of


def test_revert_goes_back_to_the_old_portfolio(tmp_path):
    b = book(tmp_path)
    b.track([10])
    old = b.create(10)
    new = b.create(10)
    b.revert(10, old, new)
    assert b.pf_of(10) == old and new not in b.session_pfs


def test_owner_and_pf_for_a_net_position_row(tmp_path):
    b = book(tmp_path)
    b.track([10, 20])
    old = b.create(10)
    b.subscribe(old, 55)
    new = b.create(10)
    b.subscribe(new, 56)
    assert b.owner_of(10, sym) == 10                        # the future itself
    assert b.owner_of(55, sym) == 10 and b.owner_of(56, sym) == 10     # either option leg -> the stock
    assert b.pf_for_row(55, sym) == old and b.pf_for_row(56, sym) == new
    assert b.owner_of(777, sym) is None


def test_change_listeners(tmp_path):
    b = book(tmp_path)
    seen = []
    off = b.on_change(lambda: seen.append(1))
    b.track([10])
    b.set_allow(10, False)
    b.set_paused(1, True)
    n = len(seen)
    off()
    b.set_allow(10, True)
    assert n >= 3 and len(seen) == n and b.paused == {1} and b.allow[10] is True
