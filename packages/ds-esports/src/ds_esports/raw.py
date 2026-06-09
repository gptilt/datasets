"""
Raw-layer assets: full Cargo-table snapshots written as JSON to S3.

Each asset paginates through one Cargo table at the Leaguepedia politeness limit
and writes the full result set as a single partition keyed by scrape date. The
partition cadence (biweekly Mon for persons/staff/teams) is set on the schedule
side, not the asset.

The fields lists below are the columns we actually consume in the clean layer —
deliberately narrow so that future Cargo schema changes only break us when they
touch something we use.
"""
import dagster as dg
from datetime import datetime, timezone
from ds_storage import StorageS3

from .cargo import CargoClient


# Columns from the Cargo `Players` table. `_pageName` is the canonical Leaguepedia
# entry name and serves as our person identifier.
_PLAYERS_FIELDS = ", ".join([
    "_pageName=PageName",
    "ID",                # the player's IGN
    "Name",              # real name (latin)
    "NameAlphabet",      # native-script real name (e.g. Hangul)
    "NativeName",
    "OtherNames",        # pipe-separated list of additional names/nicknames
    "Country",
    "Nationality",
    "NationalityPrimary",
    "Role",              # primary role(s)
    "Team",              # current team (may be null)
    "Birthdate",
    "IsRetired",
    "IsLowercase",
])

# `Staff` has casters/coaches/analysts/owners — the "non-player public figures".
_STAFF_FIELDS = ", ".join([
    "_pageName=PageName",
    "ID",
    "Name",
    "NativeName",
    "Country",
    "Nationality",
    "Occupation",        # caster | coach | analyst | owner | …
    "OtherNames",
    "Birthdate",
    "IsRetired",
])

# `Teams` is the org-level table.
_TEAMS_FIELDS = ", ".join([
    "_pageName=PageName",
    "Name",              # canonical short name (e.g. "T1")
    "Long",              # long-form name
    "Short",
    "Location",
    "Region",
    "OrganizationalAffiliation",
    "IsDisbanded",
    "RenamedTo",
])


def _scrape_partition_kwargs() -> dict:
    """
    Partition keys for a snapshot taken at the moment the asset runs.

    Same shape as the youtube buckets (year / month / week_of) so callers reading
    cross-dataset partitions don't have to special-case esports.
    """
    now = datetime.now(timezone.utc)
    iso_year, iso_week, _ = now.isocalendar()
    # Anchor each scrape to the Monday of its ISO week so re-runs within the same
    # week land in the same partition (idempotent overwrite).
    monday = datetime.fromisocalendar(iso_year, iso_week, 1)
    return {
        "year": now.year,
        "month": now.month,
        "week_of": monday.strftime("%Y%m%d"),
    }


def _scrape_table(
    context: dg.AssetExecutionContext,
    bucket: StorageS3,
    cargo_table: str,
    fields: str,
    raw_table_name: str,
) -> dg.MaterializeResult:
    client = CargoClient()
    rows = list(client.query(cargo_table, fields))
    context.log.info(f"Fetched {len(rows)} rows from Cargo table '{cargo_table}'")

    partition_kwargs = _scrape_partition_kwargs()
    bucket.upload(
        rows,
        raw_table_name,
        object_name=partition_kwargs["week_of"],
        **partition_kwargs,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "cargo_table": cargo_table,
            "scrape_partition": dg.MetadataValue.json(partition_kwargs),
        }
    )


@dg.asset(name="raw_leaguepedia_players", group_name="esports")
def asset_raw_leaguepedia_players(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
):
    """Full snapshot of Leaguepedia's `Players` Cargo table."""
    return _scrape_table(
        context, esports_bucket, "Players", _PLAYERS_FIELDS, "leaguepedia_players"
    )


@dg.asset(name="raw_leaguepedia_staff", group_name="esports")
def asset_raw_leaguepedia_staff(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
):
    """Full snapshot of Leaguepedia's `Staff` Cargo table (casters/coaches/analysts/owners)."""
    return _scrape_table(
        context, esports_bucket, "Staff", _STAFF_FIELDS, "leaguepedia_staff"
    )


@dg.asset(name="raw_leaguepedia_teams", group_name="esports")
def asset_raw_leaguepedia_teams(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
):
    """Full snapshot of Leaguepedia's `Teams` Cargo table."""
    return _scrape_table(
        context, esports_bucket, "Teams", _TEAMS_FIELDS, "leaguepedia_teams"
    )
