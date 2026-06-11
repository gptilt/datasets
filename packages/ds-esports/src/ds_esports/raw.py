"""
Raw-layer assets: full Cargo-table snapshots written as JSON to S3.

Leaguepedia's Players, PlayerRedirects, and Teams Cargo tables are materialized by
a single `@dg.multi_asset` op so the scrapes share one CargoClient and run
sequentially within a single execution step. Splitting them into independent assets
would blow through Leaguepedia's ≤2 req/s politeness budget.

`@no_backfills` is applied because a Cargo snapshot is inherently "right now":
re-running raw for an old week would just re-scrape current Leaguepedia state and
write it under last week's path, which would mislead the clean layer (and any
downstream auditor) about what the data represented at that point in time.

The fields lists below are the columns we actually consume in the clean layer —
deliberately narrow so that future Cargo schema changes only break us when they
touch something we use.
"""
import dagster as dg
from ds_common import no_backfills
from ds_storage import StorageS3

from .cargo import CargoClient
from .partitions import partition_kwargs, partition_per_week


# `Players` is Leaguepedia's unified persons table — players, ex-pros, coaches,
# casters, analysts (the old separate `Staff` table was removed). `OverviewPage`
# is Leaguepedia's canonical cross-table page key, so we use it as the person id
# (it's what `PlayerRedirects` joins on).
_PLAYERS_FIELDS = ", ".join([
    "OverviewPage",
    "ID",                # the player's IGN
    "Name",              # real name (latin)
    "NameAlphabet",      # native-script real name (e.g. Hangul)
    "NativeName",
    "Country",
    "NationalityPrimary",
    "Role",              # primary role(s) / occupation
    "IsRetired",
])

# Player alt-names / nicknames.
# `AllName` is the alias, `OverviewPage` the person it points at.
_PLAYER_REDIRECTS_FIELDS = ", ".join([
    "AllName",
    "OverviewPage",
])

# `Teams` is the org-level table.
# `Name` is the full team name, `Short` the abbreviation.
_TEAMS_FIELDS = ", ".join([
    "_pageName=PageName",
    "Name",
    "Short",
    "Location",
    "Region",
    "IsDisbanded",
])

# (output_name, cargo_table, fields, raw_table_name)
_SCRAPES = [
    ("raw_leaguepedia_players",          "Players",         _PLAYERS_FIELDS,          "leaguepedia_players"),
    ("raw_leaguepedia_player_redirects", "PlayerRedirects", _PLAYER_REDIRECTS_FIELDS, "leaguepedia_player_redirects"),
    ("raw_leaguepedia_teams",            "Teams",           _TEAMS_FIELDS,            "leaguepedia_teams"),
]


@dg.multi_asset(
    group_name="esports",
    partitions_def=partition_per_week,
    specs=[
        dg.AssetSpec(key=name, group_name="esports") for name, *_ in _SCRAPES
    ],
)
@no_backfills
async def asset_raw_leaguepedia(
    context: dg.AssetExecutionContext,
    esports_bucket: StorageS3,
    esports_cargo: CargoClient,
):
    """
    Full snapshot of Leaguepedia's Players, PlayerRedirects, and Teams Cargo tables.
    """
    kwargs = partition_kwargs(context.partition_key)

    for output_name, cargo_table, fields, raw_table_name in _SCRAPES:
        rows = list(esports_cargo.query(cargo_table, fields))
        context.log.info(f"Fetched {len(rows)} rows from Cargo table '{cargo_table}'")

        esports_bucket.upload(
            rows,
            raw_table_name,
            object_name=kwargs["week_of"],
            **kwargs,
        )
        yield dg.MaterializeResult(
            asset_key=output_name,
            metadata={
                "row_count": len(rows),
                "cargo_table": cargo_table,
                "partition": dg.MetadataValue.json(kwargs),
            },
        )
