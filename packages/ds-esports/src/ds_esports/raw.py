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

# `ScoreboardGames` is the per-game competitive record (one row per game played):
# the grounding payload — Patch, the two teams, champion picks/bans, the date, and the
# GameId / MatchId / RiotPlatformGameId keys. A narrow subset of the table's 69 columns;
# the per-team objective/stat columns (Team1Dragons/Barons/Towers/Kills/Gold, …) are
# available for a future game-level corroboration table, but aren't needed for grounding.
# Picks/bans are Cargo List fields (comma-delimited), returned as comma-separated strings.
_SCOREBOARD_GAMES_FIELDS = ", ".join([
    "GameId",
    "MatchId",
    "OverviewPage",
    "Tournament",
    "Team1",
    "Team2",
    "WinTeam",
    "DateTime_UTC",
    "Patch",
    "Team1Picks",
    "Team2Picks",
    "Team1Bans",
    "Team2Bans",
    "Gamelength",
    "N_GameInMatch",
    "RiotPlatformGameId",
    "VOD",
])

# `Tournaments` — one row per tournament; metadata for resolving a title's event
# (name, league, region, split, year, tier, start/end dates).
_TOURNAMENTS_FIELDS = ", ".join([
    "OverviewPage",
    "Name",
    "League",
    "Region",
    "Split",
    "Year",
    "TournamentLevel",
    "DateStart",
    "Date",
])

# `MatchSchedule` — one row per match (series): the two teams, series score, best-of,
# and scheduled date. Individual games are in ScoreboardGames.
_MATCH_SCHEDULE_FIELDS = ", ".join([
    "MatchId",
    "OverviewPage",
    "Tab",
    "Team1",
    "Team2",
    "Team1Score",
    "Team2Score",
    "Winner",
    "BestOf",
    "DateTime_UTC",
])

# (output_name, cargo_table, fields, raw_table_name)
# Bundled into one op so all Cargo scrapes share a CargoClient and run sequentially —
# splitting them would let pages interleave and blow the ≤2 req/s politeness budget.
_SCRAPES = [
    ("raw_esports_leaguepedia_players",          "Players",         _PLAYERS_FIELDS,          "players"),
    ("raw_esports_leaguepedia_player_redirects", "PlayerRedirects", _PLAYER_REDIRECTS_FIELDS, "player_redirects"),
    ("raw_esports_leaguepedia_teams",            "Teams",           _TEAMS_FIELDS,            "teams"),
    ("raw_esports_leaguepedia_scoreboard_games", "ScoreboardGames", _SCOREBOARD_GAMES_FIELDS, "scoreboard_games"),
    ("raw_esports_leaguepedia_tournaments",      "Tournaments",     _TOURNAMENTS_FIELDS,      "tournaments"),
    ("raw_esports_leaguepedia_match_schedule",   "MatchSchedule",   _MATCH_SCHEDULE_FIELDS,   "match_schedule"),
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
    leaguepedia_bucket: StorageS3,
    leaguepedia_cargo: CargoClient,
):
    """
    Full snapshot of Leaguepedia's Players, PlayerRedirects, and Teams Cargo tables.
    """
    kwargs = partition_kwargs(context.partition_key)

    # Rate limiting (per-page pacing + ratelimited backoff) lives in CargoClient,
    # so the scrape loop stays simple.
    for output_name, cargo_table, fields, raw_table_name in _SCRAPES:
        context.log.info(f"Querying Cargo table '{cargo_table}'...")
        rows = list(leaguepedia_cargo.query(context, cargo_table, fields))
        context.log.info(f"Fetched {len(rows)} rows from Cargo table '{cargo_table}'")

        leaguepedia_bucket.upload(
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
