import pytest

from algo.ui.filters import Filters, Strategies, number_of, plain, safe_strategy, strategy_cond, time_conds


def test_strategy_cond_selects_one_strategys_rows_or_everything():
    assert strategy_cond(None) == []
    assert strategy_cond("rsi_nifty") == ["Strategy == `rsi_nifty`"]
    with pytest.raises(ValueError):
        strategy_cond("bad`name")


@pytest.mark.parametrize("name,ok", [("stocks", True), ("rsi_nifty-2.0", True), ("", False), ("a b", False), ("a`b", False), ("x" * 61, False),
                                     (None, False), (5, False)])
def test_only_plain_names_go_into_queries(name, ok):
    assert safe_strategy(name) is ok


def test_time_conds():
    assert time_conds("t", (None, None)) == []
    assert time_conds("t", (10, None)) == ["t >= 10"]
    assert time_conds("t", (10, 20)) == ["t >= 10", "t <= 20"]


def test_filters_notify_and_unsubscribe():
    f, seen = Filters(), []
    off = f.on_change(lambda: seen.append((f.window, f.strategy)))
    f.set_window(1, 2)
    f.set_strategy("stocks")
    off()
    f.set_strategy(None)
    assert seen == [((1, 2), None), ((1, 2), "stocks")] and f.strategy is None


def test_strategies_lists_each_name_once_and_tells_listeners_only_about_new_ones():
    s, told = Strategies(), []
    off = s.on_change(lambda: told.append(s.names()))
    assert s.add("stocks") and s.add("nifty") and not s.add("stocks") and not s.add("")
    assert s.names() == ["nifty", "stocks"] and told == [["stocks"], ["nifty", "stocks"]]
    off()
    s.add("late")
    assert len(told) == 2


def test_a_limit_box_text_becomes_a_number_and_back():
    assert number_of("-5000") == -5000.0 and number_of("50,000") == 50000.0 and number_of(" ₹ 1,25,000.50 ") == 125000.5
    assert number_of("") is None and number_of(None) is None and number_of("   ") is None
    for bad in ("abc", "5k", "--5", "1 2 x"):
        with pytest.raises(ValueError):
            number_of(bad)
    assert plain(None) == "" and plain(0) == "" and plain(-5000.0) == "-5000" and plain(5000000.0) == "5000000" and plain(12.5) == "12.5"


def test_a_clicked_table_row_is_read_whether_it_holds_plain_values_or_cells():
    from algo.ui.filters import row_value
    assert row_value({"token": 5, "Symbol": "X"}, "token") == 5
    assert row_value({"token": {"value": 7, "text": "7"}}, "token") == 7                 # a cell: value and text
    assert row_value({"NetQty": float("nan")}, "NetQty", 0) == 0 and row_value({"a": None}, "a", 3) == 3
    assert row_value({}, "token") is None and row_value(None, "token", "x") == "x" and row_value("not a row", "token") is None


def test_a_clicked_cell_wrapped_twice_or_formatted_is_still_read_and_nothing_raises():
    from algo.ui.filters import as_int, row_value
    assert row_value({"NetQty": {"value": {"value": 5, "text": "5"}}}, "NetQty") == 5
    assert row_value({"NetQty": {"text": "+1,200"}}, "NetQty") == "+1,200" and as_int("+1,200") == 1200
    assert as_int({"value": 7}) == 7 and as_int(" -3 ") == -3 and as_int(4.9) == 4
    for junk in (None, "x", {}, [], {"value": {}}):
        assert as_int(junk, 9) == 9
    assert row_value({"a": {"value": {}}}, "a", "d") == "d"
