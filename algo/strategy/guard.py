"""Enforces the turnover and M2M limits of a strategy (the numbers and the latches are in algo.core.limits).

    turnover limit breached   the strategy's sending is switched OFF (its own switch; other strategies and the master are untouched)
    M2M limit breached        every subscribed portfolio of the strategy is paused and squared up, then its sending is switched OFF

Both put a line in the nats_events table and in the log. The live values come from `Guard.on_values` (the console feeds it from the strategy's trades,
so it works with no browser open); `Guard.set_limit` changes a limit and acts at once if the strategy is already past the new one.
A strategy that is blocked cannot be switched back ON until its limit is raised past the value (algo.automation and the console both ask `Limits.blocked`).
"""
from __future__ import annotations

import logging

from algo.core.limits import Limits

log = logging.getLogger("algo.guard")


class Guard:
    def __init__(self, app, limits: Limits):
        self.app, self.limits = app, limits

    def on_values(self, strategy: str, turnover_cr, m2m) -> None:
        for kind in self.limits.update(strategy, turnover_cr, m2m):
            self._breach(strategy, kind)

    def set_limit(self, strategy: str, kind: str, value) -> str:
        """Change a limit. Returns '' or a sentence for the person ('still past the new limit ...')."""
        try:
            newly = self.limits.set_limit(strategy, kind, value)
        except ValueError as e:
            return str(e)
        for k in newly:
            self._breach(strategy, k)
        why = self.limits.blocked(strategy)
        return f"{strategy} is still blocked: {why}" if why and not newly else ""

    # ---- acting ----------------------------------------------------------------------------------
    def _breach(self, strategy: str, kind: str) -> None:
        turnover, m2m = self.limits.seen(strategy)
        limit = self.limits.get(strategy)[kind]
        if kind == "turnover":
            text = f"{strategy}: TURNOVER limit {limit:,.2f} Cr reached ({turnover:,.2f} Cr): sending switched OFF until the limit is raised"
        else:
            text = f"{strategy}: M2M limit -{limit:,.0f} reached ({m2m:+,.0f}): squaring up all portfolios, sending switched OFF until the limit is raised"
        log.warning(text)
        svc = self.app.get("nats")
        run = self.app.get(strategy)
        if run is not None and hasattr(run, "send_off"):
            run.send_off()                                           # first: the strategy sends nothing more of its own
        users = {}
        if kind == "m2m" and svc is not None and getattr(svc, "state", "service_down") != "service_down":
            users = self._square_all(strategy, svc)
        if svc is not None:
            user = next(iter(users), 0)
            svc.note(0, "limit", f"{kind}_breached", text, user)
        self.limits.notify(strategy)                                 # screens redraw now: the ON / OFF button reads the switch that was just turned off

    def _square_all(self, strategy: str, svc) -> dict:
        """Pause and square up the subscribed portfolios of the strategy, as an operator command (the strategy's own switch is already off)."""
        reg = self.app["registry"].state if self.app.get("registry") is not None else None
        sent: dict = {}
        for user, pf, st in svc.states.items():
            rec = reg.get(user, pf) if reg is not None else None
            if not st.subscribed or not rec or rec.get("algo") != strategy:
                continue

            async def job(call):
                await call("pause_unpause", {"Action": "Pause"})
                return await call("squareup", None)

            fut = svc.submit(pf, job, user=user, label="m2m_squareup")
            sent[user] = sent.get(user, 0) + 1

            def done(f, pf=pf):
                try:
                    r = f.result()
                    if not getattr(r, "ok", True):
                        log.error("%s: square up of PF %s was NOT done (%s)", strategy, pf, r.status)
                except Exception:
                    log.exception("%s: square up of PF %s failed", strategy, pf)
            fut.add_done_callback(done)
        if not sent:
            log.info("%s: no subscribed portfolio to square up", strategy)
        return sent


def guard_of(app, path: str | None = None) -> Guard:
    """The one Guard of an app (kept in app.shared)."""
    from algo.core.limits import limits_of
    g = app.shared.get("guard")
    if g is None:
        g = app.shared["guard"] = Guard(app, limits_of(app.shared, path))
    return g
