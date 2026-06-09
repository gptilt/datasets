"""
Thin client for Leaguepedia's Cargo extension.

The Cargo API is a MediaWiki extension that exposes wiki data as queryable tables.
We hit it with `requests` directly rather than pulling in `mwclient` / `mwrogue` —
the surface we need is small and a thin wrapper keeps politeness behaviour visible.

Politeness:
- Identifies as `USER_AGENT` (with contact URL) so wiki maintainers can reach us.
- Sleeps `MIN_REQUEST_INTERVAL_SECONDS` between requests to honour the ≤2 req/s cap.

Pagination:
- Cargo returns up to `CARGO_PAGE_LIMIT` rows per request. `query` walks pages
  transparently and yields every row, so callers get a flat stream regardless of
  total size.
"""
import time
from typing import Iterator

import requests

from .constants import (
    CARGO_PAGE_LIMIT,
    LEAGUEPEDIA_API_URL,
    MIN_REQUEST_INTERVAL_SECONDS,
    USER_AGENT,
)


class CargoClient:
    """
    Minimal Cargo client that paginates politely.

    Single instance per asset run; `query` is the only public method. Each call
    paginates through one Cargo table; cross-call rate limiting is enforced via
    `_last_request_at`.
    """

    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        self._last_request_at: float = 0.0

    def query(
        self,
        tables: str,
        fields: str,
        where: str | None = None,
        order_by: str | None = None,
        join_on: str | None = None,
        group_by: str | None = None,
    ) -> Iterator[dict]:
        """
        Run a Cargo query against `tables` and yield each result row as a dict.

        Walks pages of `CARGO_PAGE_LIMIT` rows until Cargo returns fewer than a full
        page (the canonical end-of-results signal). The Cargo API never sets a
        `continue` token — pagination is offset-based.
        """
        offset = 0
        while True:
            params = {
                "action": "cargoquery",
                "format": "json",
                "tables": tables,
                "fields": fields,
                "limit": CARGO_PAGE_LIMIT,
                "offset": offset,
            }
            if where:
                params["where"] = where
            if order_by:
                params["order_by"] = order_by
            if join_on:
                params["join_on"] = join_on
            if group_by:
                params["group_by"] = group_by

            page = self._get(params)
            if not page:
                return

            # Cargo wraps each row under a "title" key; strip the wrapper here so
            # callers see flat dicts.
            for row in page:
                yield row["title"]

            if len(page) < CARGO_PAGE_LIMIT:
                return
            offset += CARGO_PAGE_LIMIT

    def _get(self, params: dict) -> list[dict]:
        # Honour the ≤2 req/s contract by waiting if the previous request fired
        # less than `MIN_REQUEST_INTERVAL_SECONDS` ago.
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)

        response = self._session.get(LEAGUEPEDIA_API_URL, params=params, timeout=60)
        self._last_request_at = time.monotonic()
        response.raise_for_status()

        body = response.json()
        if "error" in body:
            raise RuntimeError(f"Cargo API error: {body['error']}")
        return body.get("cargoquery", [])
