from algo.strategy.config import RsiOptionsConfig
from algo.strategy.sizing import ceil_tick, floor_tick, on_tick, option_params


def paise(v):
    return int(v * 100)             # what the bridge does: truncate rupees x 100


def test_floor_and_ceil_land_on_tick_after_truncation():
    for x in (2.3, 0.7, 12.05, 99.95, 1.0):
        assert paise(floor_tick(x, 0.05)) % 5 == 0
        assert paise(ceil_tick(x, 0.05)) % 5 == 0
    assert paise(floor_tick(2.3, 0.1)) == 230               # 2.3 is stored as 2.2999999...


def test_floor_never_above_ceil_never_below():
    assert floor_tick(10.07, 0.05) <= 10.07 < ceil_tick(10.07, 0.05) + 1e-5
    assert ceil_tick(10.07, 0.05) >= 10.07


def test_on_tick():
    assert on_tick(floor_tick(1.23, 0.05), 0.05)
    assert not on_tick(1.23, 0.05)


def test_option_params_for_a_stock_option():
    cfg = RsiOptionsConfig.stocks()
    p, why = option_params(premium=100.0, tick=0.05, cfg=cfg)
    assert why == "" and p["side"] == "BUY"
    assert paise(p["ival"]) == 20                            # 0.2% of 100
    assert paise(p["jval"]) == 15                            # 0.75 x ival
    assert paise(p["sval"]) == 60                            # 3 x ival
    assert paise(p["lowerrange"]) == 9500 and paise(p["upperrange"]) == 10500
    assert paise(p["trp"]) == 10005                          # premium + 0.01%
    assert p["jqty1"] == 1                                   # apply_params carried over


def test_option_params_never_below_one_tick():
    p, _ = option_params(premium=0.1, tick=0.05, cfg=RsiOptionsConfig.stocks())
    assert paise(p["ival"]) >= 5 and paise(p["lowerrange"]) >= 5


def test_config_warns_when_thresholds_overlap():
    assert RsiOptionsConfig.stocks().warnings()              # 50 / 50 as in the current script
    assert not RsiOptionsConfig.nifty().warnings()
