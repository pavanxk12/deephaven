"""The day's start-up on a timer: master build, candles and the other services each start at a time of the exchange's clock, every trading day.

`day_auto()` (algo/api.py) builds the steps and runs this plugin. A step that is already due when the plugin starts (the container came up late)
runs at once, in order; a step that fails (Postgres not ready yet, the feed not up) is tried again every `retry_s` seconds, `retries` times.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Callable

from algo.core.plugin import Plugin

MAX_WAIT_S = 30.0          # the loop looks at the clock at least this often, so a changed clock or a stop is noticed quickly


@dataclass(frozen=True)
class Step:
    label: str
    at: dtime
    run: Callable[[], None]
    until: dtime | None = None        # from this time on the step is not started any more (a candle builder started after the close would build nothing)


class DaySchedule(Plugin):
    """Runs each step once a day at its time (exchange timezone), weekdays only by default. Steps run in the order given."""
    name = "schedule"

    def __init__(self, steps, weekdays_only: bool = True, retries: int = 10, retry_s: float = 60.0, clock: Callable[[], datetime] | None = None,
                 poll_s: float = MAX_WAIT_S):
        super().__init__()
        self.steps = list(steps)
        if not self.steps:
            raise ValueError("DaySchedule needs at least one step")
        self.weekdays_only, self.retries, self.retry_s, self._clock, self.poll_s = weekdays_only, max(1, int(retries)), float(retry_s), clock, float(poll_s)
        self.done: dict[str, date] = {}               # step label -> the day it last finished (or was given up on)
        self.failed: dict[str, str] = {}              # step label -> why it was given up on, today
        self.tries: dict[str, int] = {}               # step label -> failed tries so far today
        self._retry_at: dict[str, float] = {}         # step label -> monotonic time of its next try: a failing step never holds the others up

    # ---- Plugin -------------------------------------------------------------------------------
    def start(self) -> None:
        self.spawn(self._loop)
        self.log.info("day start: %s (%s%s)", ", ".join(f"{s.label} {s.at:%H:%M:%S}" for s in self.steps), self.app.market.tz,
                      ", weekdays only" if self.weekdays_only else "")

    def describe(self) -> str:
        today = self._now().date()
        parts = []
        for s in self.steps:
            state = "failed" if s.label in self.failed else "done" if self.done.get(s.label) == today else "waiting"
            parts.append(f"{s.label} {s.at:%H:%M:%S} {state}")
        return ", ".join(parts)

    # ---- the loop -----------------------------------------------------------------------------
    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(self.app.market.zone)

    def _loop(self) -> None:
        while not self.halted.is_set():
            now = self._now()
            today = now.date()
            if self.weekdays_only and now.weekday() >= 5:
                self.halted.wait(self.poll_s)
                continue
            for step in self.steps:
                if self.halted.is_set():
                    return
                if self.done.get(step.label) != today and step.at <= now.time() and time.monotonic() >= self._retry_at.get(step.label, 0.0):
                    if step.until is not None and now.time() >= step.until:
                        self.done[step.label] = today
                        self.log.info("%s: not started, it is past %s", step.label, f"{step.until:%H:%M:%S}")
                        continue
                    self._run_step(step, today)
            waiting = [s.at for s in self.steps if self.done.get(s.label) != today and s.at > now.time()]
            if waiting:
                seconds = (datetime.combine(today, min(waiting)) - now.replace(tzinfo=None)).total_seconds()
            else:
                seconds = (datetime.combine(today + timedelta(days=1), dtime(0, 0, 1)) - now.replace(tzinfo=None)).total_seconds()
            self.halted.wait(min(max(seconds, 0.01), self.poll_s))

    def _run_step(self, step: Step, today: date) -> None:
        """One try. A failure is tried again after `retry_s` seconds (the steps after it do not wait), `retries` times, then it is given up for the day."""
        self.failed.pop(step.label, None)
        try:
            with self.app.in_ctx():
                step.run()
        except Exception as e:
            n = self.tries[step.label] = self.tries.get(step.label, 0) + 1
            self.log.warning("%s failed (try %d of %d): %s", step.label, n, self.retries, e)
            if n >= self.retries:
                self.failed[step.label] = str(e)
                self.done[step.label] = today
                self.tries.pop(step.label, None)
                self.log.error("%s: giving up for today", step.label)
            else:
                self._retry_at[step.label] = time.monotonic() + self.retry_s
        else:
            self.done[step.label] = today
            self.tries.pop(step.label, None)
            self._retry_at.pop(step.label, None)
            self.log.info("%s started", step.label)
