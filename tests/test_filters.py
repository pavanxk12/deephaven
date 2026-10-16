import pytest

from algo.ui.filters import Filters, Strategies, safe_strategy, strategy_cond, time_conds


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
