"""The ON / OFF button of the console, without any Deephaven in it (so it can be tested).

    the all-strategies console   flips the service's MASTER switch: nothing is sent for anyone (nats_send(False) / nats_send(True))
    a strategy's own window      flips THAT strategy's switch only (nats_send(False, client="name")): the other strategies are not touched

A command goes out only when the master switch and its strategy's switch are both on, so a strategy window shows ON only when both are. Its own switch can be set
while the master is off (it then takes effect when the master comes on); the window says that the master is off.
"""
from __future__ import annotations


def own_switch(app, strategy: "str | None"):
    """The running automation of that strategy, if it can switch itself (algo_start made it), else None."""
    run = app.get(strategy) if strategy else None
    return run if run is not None and hasattr(run, "send_on") and hasattr(run, "send_off") else None


def is_sending(svc, strategy: "str | None") -> bool:
    """What the button shows. For a strategy: its own switch AND the master are on; for everyone: the master is on and the service is up."""
    if strategy:
        return bool(svc.allowed(strategy))
    return bool(svc.sending and svc.state != "service_down")


def master_is_off(svc) -> bool:
    return svc.state == "service_down" or not svc.sending


def set_sending(app, svc, strategy: "str | None", on: bool) -> str:
    """Do what the button says. Returns "" when it worked, else the reason it did not (to show under the button)."""
    if svc.state == "service_down":
        return "the NATS service is not running"
    if not strategy:
        (svc.send_on if on else svc.send_off)()
        return ""
    run = own_switch(app, strategy)
    if run is None:
        return f"{strategy!r} is not running (algo_start)"
    if on:
        if not run.send_on():                                       # refused: there is no NATS connection (a dry run, or it is down)
            return "cannot switch ON: no NATS connection (dry run, or the connection is down)"
    else:
        run.send_off()
    return ""
