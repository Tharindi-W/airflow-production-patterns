"""A genuinely custom operator: resilient ingestion from a flaky API.

ThrottledApiIngestOperator pulls every page from a (mock) paginated API using
client-side rate limiting and exponential-backoff retries, then upserts the
rows into the warehouse by primary key. It is not a thin wrapper: it owns the
pagination loop, the rate limiter, the retry policy, and the idempotent load.

It uses a labelled mock API (include/python_utils/mock_api.py) so it runs with
no network. Swapping in a real requests-based client would not change the shape
of the operator, only how a page is fetched.
"""

from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.backoff import RateLimiter, retry_with_backoff
from include.python_utils.mock_api import ApiError, MockThrottledApi


class ThrottledApiIngestOperator(BaseOperator):
    """Pull all pages from the mock API resiliently and upsert into the warehouse."""

    def __init__(
        self,
        *,
        conn_id: str = "warehouse",
        target_table: str = "core.api_events",
        pages: int = 5,
        rows_per_page: int = 20,
        failures_before_success: int = 2,
        rate_limit_interval: float = 0.1,
        retries: int = 5,
        base_delay: float = 0.2,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.target_table = target_table
        self.pages = pages
        self.rows_per_page = rows_per_page
        self.failures_before_success = failures_before_success
        self.rate_limit_interval = rate_limit_interval
        self.retries = retries
        self.base_delay = base_delay

    def execute(self, context: Any) -> int:
        load_date = context["ds"]
        api = MockThrottledApi(
            pages=self.pages,
            rows_per_page=self.rows_per_page,
            failures_before_success=self.failures_before_success,
        )
        limiter = RateLimiter(self.rate_limit_interval)
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        rows: list[tuple] = []
        for page in range(self.pages):
            # Client-side rate limiting: space out calls so we are a good citizen.
            limiter.wait()
            # Absorb injected 429s and 5xx with exponential backoff.
            page_rows = retry_with_backoff(
                lambda p=page: api.get_page(p),
                retries=self.retries,
                base_delay=self.base_delay,
                retry_on=(ApiError,),
            )
            self.log.info("fetched page %d with %d rows", page, len(page_rows))
            for r in page_rows:
                rows.append((r["event_id"], r["page"], r["payload"], load_date))

        # Idempotent load: upsert by primary key so a retried task never dupes.
        hook.insert_rows(
            table=self.target_table,
            rows=rows,
            target_fields=["event_id", "page", "payload", "load_date"],
            replace=True,
            replace_index="event_id",
            commit_every=500,
        )
        self.log.info("ingested %d rows across %d pages", len(rows), self.pages)
        return len(rows)
