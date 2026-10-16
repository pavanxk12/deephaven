"""Settings of the bridge: how an automation sequences its commands for a portfolio."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BridgeConfig:
    """How the strategy sequences its commands. The connection itself (url, user, timeout, the send switch) is `NatsConfig`."""
    unsubscribe_on_retire: bool = True     # after the squareup, unsubscribe the old portfolio from its option
    squareup_on_repoint: bool = True       # square up the old portfolio when a stock moves to another option
    add_qty: int = 5                       # added to the cumulative maxqty on every activation / flip
    user: int | None = None                # the user this automation sends as; needed when the NATS service has several users
