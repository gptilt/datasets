"""Leaguepedia Cargo access via mwcleric, as a Dagster resource.

mwcleric's FandomClient owns login. We deliberately do NOT use its auto-continue
pagination: it bursts pages with no client-side delay, and Leaguepedia's MediaWiki
`ratelimited` budget is *cumulative across pages over a rolling window*, so a big
table (or two tables back-to-back) trips it mid-walk. mwclient does NOT retry
`ratelimited` either — `handle_api_result` only retries DB/nonce errors — so we
paginate by hand: pace every page, and back off + retry an individual page on
`ratelimited` without restarting the whole query.

Credentials are injected via `dg.EnvVar` in `definitions.py`, consistent with
how the storage resources receive their secrets.
"""
import logging
import time

import dagster as dg
from mwcleric import AuthCredentials, FandomClient
from mwclient.errors import APIError
from pydantic import PrivateAttr

log = logging.getLogger(__name__)

# Cargo's hard server-side cap on rows per `cargoquery`. We request exactly this
# per page, so a short page unambiguously means "last page" regardless of the
# account's apihighlimits status.
CARGO_MAX_PAGE = 500


class CargoClient(dg.ConfigurableResource):
    """
    mwcleric FandomClient for lol.fandom.com (Leaguepedia).
    The logged-in session is built once on first use and reused across a run's tables.
    """

    username: str
    password: str

    # Pacing knobs (overridable where the resource is constructed).
    # Leaguepedia advertises ~2 req/s but its edge may throttle harder;
    # the known-good rate is ~1 req / 2s (0.5 req/s).
    page_delay_s: float = 1.0
    ratelimit_backoff_s: float = 60.0
    max_ratelimit_retries: int = 5

    _site: FandomClient | None = PrivateAttr(default=None)

    def _get_site(self) -> FandomClient:
        if self._site is None:
            self._site = FandomClient("lol", credentials=AuthCredentials(
                username=self.username,
                password=self.password
            ))
        return self._site

    def query(self, context, tables: str, fields: str, **kwargs) -> list[dict]:
        """
        Run a Cargo query and return every row as a flat dict, paginating
        manually so we control the request rate.
        Passing an explicit `limit` to mwcleric disables its auto-continue
        (one page per call), so we walk offsets ourselves.
        Extra kwargs (where, order_by, join_on, group_by, ...) pass straight through.
        """
        cargo = self._get_site().cargo_client
        rows: list[dict] = []
        offset = 0
        while True:
            page = self._fetch_page(cargo, context, tables, fields, offset, **kwargs)
            rows.extend(page)
            if len(page) < CARGO_MAX_PAGE:
                break  # short page => last page
            offset += len(page)
            time.sleep(self.page_delay_s)
        return rows

    def _fetch_page(self, cargo, context, tables: str, fields: str, offset: int, **kwargs) -> list[dict]:
        """
        Fetch one page,
        retrying *this page* on `ratelimited` with escalating backoff.
        Restarting the whole query would just re-spend the budget and loop,
        so we resume from the same offset instead.
        """
        for attempt in range(1, self.max_ratelimit_retries + 1):
            try:
                return cargo.query(
                    tables=tables,
                    fields=fields,
                    limit=CARGO_MAX_PAGE,
                    offset=offset,
                    **kwargs,
                )
            except APIError as e:
                if e.code != "ratelimited":
                    raise
                wait = self.ratelimit_backoff_s * attempt
                context.log.warning(
                    f"Cargo ratelimited at offset {offset} "
                    f"(attempt {attempt}/{self.max_ratelimit_retries}); "
                    f"backing off {wait:.0f}s",
                )
                time.sleep(wait)
        raise RuntimeError(
            f"Cargo still ratelimited after {self.max_ratelimit_retries} retries at offset {offset}"
        )
