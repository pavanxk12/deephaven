from algo.ui.filters import Filters, time_conds, user_cond


def test_user_cond_selects_the_users_portfolio_range():
    assert user_cond("PF", 3) == "PF >= 30000 && PF < 40000"
    assert user_cond("Pf", 12, per_user=1000) == "Pf >= 12000 && Pf < 13000"


def test_time_conds():
    assert time_conds("t", (None, None)) == []
    assert time_conds("t", (10, None)) == ["t >= 10"]
    assert time_conds("t", (10, 20)) == ["t >= 10", "t <= 20"]


def test_filters_notify_and_unsubscribe():
    f, seen = Filters(), []
    off = f.on_change(lambda: seen.append((f.window, f.user)))
    f.set_window(1, 2)
    f.set_user(3)
    off()
    f.set_user(None)
    assert seen == [((1, 2), None), ((1, 2), 3)] and f.user is None
