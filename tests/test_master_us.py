"""US contracts: prices scaled by each row's own Divisor (options 10000, equities 100), no futures, equity underlyings."""
import pandas as pd
import pytest
from zoneinfo import ZoneInfo

from algo.core.config import NSE, US
from algo.data.candles import Candles, CandleAggregator, LttParser
from algo.data.master import classify, normalise
from tests.conftest import secs


def us_contracts() -> pd.DataFrame:
    """Shaped like the real US file: epoch-1970 expiries, strikes x10000, equity rows with expiry 0 and strike -1."""
    e = int((pd.Timestamp("2026-10-26") - pd.Timestamp("1970-01-01")).total_seconds())
    rows = [dict(Token=1, Symbol="AAPL", ExpiryDate="0", InstType="EQUITY", OptionType="XX", StrikePrice=-1,
                 LotSize=1, TickSize=1.0, Divisor=100)]
    tok = 3270
    for strike in (2600000, 2650000, 3075000):                 # 260.00, 265.00, 307.50
        for ot in ("CE", "PE"):
            rows.append(dict(Token=tok, Symbol="AAPL", ExpiryDate=str(e), InstType="OPTSTK", OptionType=ot,
                             StrikePrice=strike, LotSize=1, TickSize=1.0, Divisor=10000))
            tok += 1
    return pd.DataFrame(rows)


@pytest.fixture
def us():
    return classify(normalise(us_contracts(), US))


def test_strikes_are_divided_by_the_rows_own_divisor(us):
    exp = pd.Timestamp("2026-10-26")
    strikes, toks = us.strikes("AAPL", exp, "CE")
    assert list(strikes) == [260.0, 265.0, 307.5]
    assert us.symbol_of[int(toks[2])] == "AAPL 26OCT26 307.5 CE"
    assert len(us.chain) == 3


def test_equities_keep_their_own_tick_and_divisor(us):
    assert us.tick_of[1] == 0.01 and us.divisor_of[1] == 100.0
    assert us.tick_of[3270] == 0.0001 and us.divisor_of[3270] == 10000.0
    assert list(us.eqs["TckrSymb"]) == ["AAPL"] and us.symbol_of[1] == "AAPL"


def test_there_are_no_futures_and_the_equity_is_not_dropped_for_its_zero_expiry(us):
    assert len(us.futs) == 0 and len(us.eqs) == 1 and us.nearest_future("AAPL", pd.Timestamp("2026-10-14")) is None


def test_the_wrong_scale_would_have_been_visible():
    wrong = normalise(us_contracts().drop(columns=["Divisor"]), US)           # no Divisor: market constants
    right = normalise(us_contracts(), US)
    assert list(wrong["strike"][1:4:2]) == list(right["strike"][1:4:2])      # US constants now agree with the file
    assert normalise(us_contracts().drop(columns=["Divisor"]), NSE)["strike"][1] == 26000.0   # 2600000 / 100: why NSE constants fail


def test_a_missing_or_nonsense_divisor_falls_back_to_the_market_constant():
    raw = us_contracts()
    raw.loc[1, "Divisor"] = 0
    raw.loc[2, "Divisor"] = None
    df = normalise(raw, US)
    assert df.loc[1, "strike"] == 260.0 and df.loc[2, "strike"] == 260.0      # fell back to 10000


def test_nse_rows_without_a_divisor_column_still_use_the_markets_scale():
    from tests.conftest import raw_contracts
    m = classify(normalise(raw_contracts(), NSE))
    assert m.divisor_of[1] == 100.0 and m.tick_of[1] == 0.05


# ---- candles take each contract's own scale unless a multiplier is given --------------------------------------
def make_candles(master, multiplier=None):
    c = Candles(multiplier=multiplier, build_from=0)
    c.scale = multiplier if multiplier is not None else 1.0 / US.price_div
    c.scale_of = {} if multiplier is not None else {t: 1.0 / d for t, d in master.divisor_of.items()}
    c._parse = LttParser("%Y-%m-%dT%H:%M", ZoneInfo("Asia/Kolkata"))
    rows = []
    return c, CandleAggregator(lambda *r: rows.append(r), 0, 60), rows


def test_options_and_equities_get_different_scales(us):
    c, agg, rows = make_candles(us)
    c._tick(agg, 3270, 125000, 5, "2026-10-14T20:15:05.123")       # option: 12.5 dollars x 10000
    c._tick(agg, 1, 26150, 5, "2026-10-14T20:15:06.000")           # equity: 261.50 dollars x 100
    agg.flush(10 ** 12)
    by = {r[0]: r[2] for r in rows}
    assert by == {3270: 12.5, 1: 261.5}


def test_a_given_multiplier_overrides_the_files_divisors(us):
    c, agg, rows = make_candles(us, multiplier=0.01)
    c._tick(agg, 3270, 125000, 5, "2026-10-14T20:15:05")
    agg.flush(10 ** 12)
    assert rows[0][2] == 1250.0


def test_secs_helper_is_consistent():
    assert secs("1980-01-02") == 86400
