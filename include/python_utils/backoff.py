"""Resilience helpers for calling flaky external services.

Two independent concerns, kept separate on purpose:

  - RateLimiter enforces a minimum spacing between calls (client-side rate
    limiting), so we do not hammer an API into throttling us in the first place.
  - retry_with_backoff retries a call on transient errors with exponential
    backoff and jitter, so a throttle or a blip is absorbed automatically.

The sleep function is injectable so tests can run instantly without real waits.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

log = logging.getLogger(__name__)


class RateLimiter:
    """Enforce a minimum interval between successive calls."""

    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval = max(0.0, min_interval_seconds)
        self._last_call = 0.0

    def wait(self) -> None:
        if self.min_interval == 0.0:
            return
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.monotonic()


def retry_with_backoff(
    fn: Callable[[], T],
    *,
    retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.1,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call fn, retrying on retry_on exceptions with exponential backoff.

    Raises the last exception if all retries are exhausted. Every failure and
    every give-up is logged with structured context so the behaviour is
    observable in the task log.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except retry_on as exc:
            attempt += 1
            if attempt > retries:
                log.error(
                    "retry_with_backoff giving up",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                raise
            delay = min(max_delay, base_delay * (backoff_factor ** (attempt - 1)))
            delay += random.uniform(0.0, jitter)
            log.warning(
                "retry_with_backoff transient failure, will retry",
                extra={"attempt": attempt, "error": str(exc), "next_delay_s": round(delay, 3)},
            )
            sleep(delay)
