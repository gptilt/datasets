"""Leaguepedia Cargo access via mwcleric, as a Dagster resource.

mwcleric's FandomClient owns login, offset pagination, and maxlag handling.

Credentials are injected via `dg.EnvVar` in `definitions.py` (FANDOM_USERNAME /
FANDOM_PASSWORD — bot password: username is "Account@BotName"), consistent with
how the storage resources receive their secrets. Left unset, queries run
anonymously (read-only, edge-throttled).

Note: mwcleric paginates with no client-side delay, so politeness now rests on
running one shared session sequentially (see raw.py's single multi_asset). If
full-table pagination re-triggers Fandom's edge throttle, add per-page pacing.
"""
import dagster as dg
from mwcleric import AuthCredentials, FandomClient
from pydantic import PrivateAttr


class CargoClient(dg.ConfigurableResource):
    """mwcleric FandomClient for lol.fandom.com (Leaguepedia). The logged-in
    session is built once on first use and reused across a run's tables."""

    username: str
    password: str

    _site: FandomClient | None = PrivateAttr(default=None)

    def _get_site(self) -> FandomClient:
        if self._site is None:
            self._site = FandomClient("lol", credentials=AuthCredentials(
                username=self.username,
                password=self.password
            ))
        return self._site

    def query(self, tables: str, fields: str, **kwargs) -> list[dict]:
        """Run a Cargo query and return every row as a flat dict. With no `limit`,
        mwcleric auto-paginates the whole table; extra kwargs (where, order_by,
        join_on, group_by, ...) pass straight through."""
        return self._get_site().cargo_client.query(tables=tables, fields=fields, **kwargs)
