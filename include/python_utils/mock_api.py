"""MOCK flaky, rate-limited paginated API.

THIS IS A MOCK. It stands in for a real external HTTP API so the pattern runs
with no network and no credentials. It is labelled as a mock so no one mistakes
it for a real integration.

Behaviour: each page raises a fixed number of transient errors (alternating a
429 Too Many Requests and a 503 Service Unavailable) before it finally returns
its rows. This is deterministic, so the retry-and-backoff behaviour is
observable and the acceptance test is repeatable.
"""

from __future__ import annotations

from collections import defaultdict


class ApiError(Exception):
    """Base error carrying an HTTP-like status code."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


class ApiThrottledError(ApiError):
    """Simulates HTTP 429 Too Many Requests."""


class ApiServerError(ApiError):
    """Simulates HTTP 5xx Server Error."""


class MockThrottledApi:
    """A paginated API that throttles and errors before succeeding."""

    def __init__(
        self,
        *,
        pages: int = 5,
        rows_per_page: int = 20,
        failures_before_success: int = 2,
    ) -> None:
        self.pages = pages
        self.rows_per_page = rows_per_page
        self.failures_before_success = failures_before_success
        self._attempts: dict[int, int] = defaultdict(int)

    def get_page(self, page: int) -> list[dict]:
        """Return one page of rows, after raising the scheduled transient errors."""
        self._attempts[page] += 1
        attempt = self._attempts[page]
        if attempt <= self.failures_before_success:
            if attempt % 2 == 1:
                raise ApiThrottledError(429, f"rate limited on page {page} attempt {attempt}")
            raise ApiServerError(503, f"upstream error on page {page} attempt {attempt}")
        return [
            {
                "event_id": f"{page:03d}-{i:04d}",
                "page": page,
                "payload": f"evt-{page}-{i}",
            }
            for i in range(self.rows_per_page)
        ]
