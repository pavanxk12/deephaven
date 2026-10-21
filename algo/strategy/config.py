"""Settings of the bridge: how an automation sequences its commands for a portfolio."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BridgeConfig:
    """How the strategy sequences its commands. The connection itself (url, user, timeout, the send switch) is `NatsConfig`."""
    unsubscribe_on_retire: bool = True     # after the squareup, unsubscribe the old portfolio from its option
    squareup_on_repoint: bool = True       # square up the old portfolio when a stock moves to another option
    squareup_settle_s: float = 60.0        # in a stop / M2M-breach close (pause, squareup, unsubscribe): wait this long (a minute) after the squareup before unsubscribing. The unsubscribe destroys the
                                           # portfolio's strategy, so a plugin (CrossOrder) that closes by working an order needs the time to send and fill it first. 0 = no wait.
    add_qty: int = 5                       # added to the cumulative maxqty on every activation / flip
    subscribe: dict = field(default_factory=dict)  # fields added to this automation's subscribe / unsubscribe over the service's defaults, e.g. {"strategyname": "Churning"}
    user: int | None = None                # the user this automation sends as; needed when the NATS service has several users
