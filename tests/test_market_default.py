"""One wheel for both servers: the market and the NATS address a call uses when it names none come from the environment (ALGO_MARKET, ALGO_NATS_IP)."""
import pytest

import algo
from algo.comms.nats_service import NatsConfig
from algo.core import app as app_module
from algo.core.config import NSE, US, Paths, default_market, default_nats_ip, market_for


def test_without_the_environment_it_is_india(monkeypatch):
    monkeypatch.delenv("ALGO_MARKET", raising=False)
    monkeypatch.delenv("ALGO_NATS_IP", raising=False)
    assert default_market() == "NSE" and market_for() is NSE and market_for(None) is NSE
    assert default_nats_ip() == "192.168.1.130" and NatsConfig().ip == "192.168.1.130"


def test_the_us_image_sets_two_variables_and_every_default_follows(monkeypatch, tmp_path):
    monkeypatch.setenv("ALGO_MARKET", "us")
    monkeypatch.setenv("ALGO_NATS_IP", "192.168.1.131")
    assert default_market() == "us" and market_for() is US
    assert NatsConfig().ip == "192.168.1.131" and NatsConfig(ip="10.0.0.9").ip == "10.0.0.9"        # a call that names an address still wins
    monkeypatch.setattr(app_module, "_APP", None)
    ns = {}
    app = algo.install(ns, paths=Paths(market_data=str(tmp_path), work=str(tmp_path)))               # no market named
    assert app.market is US
    app = algo.install(ns, "NSE", Paths(market_data=str(tmp_path), work=str(tmp_path)))              # naming one still wins (and switches the session)
    assert app.market is NSE


def test_a_blank_variable_is_ignored_and_a_wrong_one_says_what_is_allowed(monkeypatch):
    monkeypatch.setenv("ALGO_MARKET", "  ")
    monkeypatch.setenv("ALGO_NATS_IP", "")
    assert default_market() == "NSE" and default_nats_ip() == "192.168.1.130"
    monkeypatch.setenv("ALGO_MARKET", "LSE")
    with pytest.raises(ValueError, match=r"unknown market 'LSE'.*NSE.*US"):
        market_for()
