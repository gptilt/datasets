"""
The clean layer is overwrite-only — these schemas describe the shape of the *latest
snapshot*, not historical state. SCD-Type-2 history (with `valid_from` / `valid_to`)
will live in the future `curated` layer.

Identifier fields are still declared so downstream readers can recognise primary keys,
but they are not used by the writer (overwrite ignores merge keys).
"""
from ds_storage import IcebergTableSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    ListType,
    NestedField,
    StringType,
)


SCHEMATA = {
    'public_figures': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'person_id', StringType(), required=True),
            NestedField(2, 'canonical_name', StringType(), required=True),
            NestedField(3, 'display_name', StringType(), required=True),
            # Roles and regions are list-typed in the plan, but Leaguepedia exposes
            # them as scalar strings (a player has one primary role, one nationality
            # primary). We keep them as strings here and fan out in `curated` when we
            # need lifetime role history.
            NestedField(4, 'role', StringType(), required=False),
            NestedField(5, 'region', StringType(), required=False),
            NestedField(6, 'active_from', DateType(), required=False),
            NestedField(7, 'active_to', DateType(), required=False),
            NestedField(8, 'is_retired', StringType(), required=False),
            NestedField(9, 'source', StringType(), required=True),
            NestedField(10, 'source_url', StringType(), required=True),
            identifier_field_ids=[1],
        ),
        # No partition spec: the table is small (tens of thousands of rows) and is
        # overwritten in full each refresh — partitions would just add overhead.
    ),
    'entity_aliases': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'alias', StringType(), required=True),
            NestedField(2, 'entity_id', StringType(), required=True),
            # 'person' | 'team' — the discriminator that lets one polymorphic lookup
            # cover both public figures and teams. A name resolver / privacy matcher
            # consumes the whole table and keeps entity_type alongside each hit.
            NestedField(3, 'entity_type', StringType(), required=True),
            # ign | real_name | romanization | other (person); short (team).
            NestedField(4, 'alias_type', StringType(), required=True),
            # Composite PK: an entity has many aliases, but each
            # (alias, entity_id, entity_type) triple is unique within a snapshot. The
            # same string can legitimately appear for a person *and* a team, so
            # entity_type is part of the key.
            identifier_field_ids=[1, 2, 3],
        ),
    ),
    'teams': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'team_id', StringType(), required=True),
            NestedField(2, 'canonical_name', StringType(), required=True),
            NestedField(3, 'long_name', StringType(), required=False),
            NestedField(4, 'region', StringType(), required=False),
            NestedField(5, 'location', StringType(), required=False),
            NestedField(6, 'active_from', DateType(), required=False),
            NestedField(7, 'active_to', DateType(), required=False),
            NestedField(8, 'is_disbanded', StringType(), required=False),
            NestedField(9, 'source', StringType(), required=True),
            NestedField(10, 'source_url', StringType(), required=True),
            identifier_field_ids=[1],
        ),
    ),
    # One row per competitive game (Leaguepedia `ScoreboardGames`). This is the
    # grounding payload: a video identified to a game inherits its patch + comps.
    # Scalar fields are strings (the "keep as string, fan out in curated" philosophy);
    # picks/bans are lists, matching Leaguepedia's own List typing.
    'games': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'game_id', StringType(), required=True),
            NestedField(2, 'match_id', StringType(), required=False),
            NestedField(3, 'tournament', StringType(), required=False),
            NestedField(4, 'game_number', StringType(), required=False),
            NestedField(5, 'datetime_utc', StringType(), required=False),
            NestedField(6, 'patch', StringType(), required=False),
            # Team1 = blue side, Team2 = red side (Leaguepedia convention).
            NestedField(7, 'team_blue', StringType(), required=False),
            NestedField(8, 'team_red', StringType(), required=False),
            NestedField(9, 'winner', StringType(), required=False),
            # Cargo List fields — champion names, role-ordered (top→jungle→mid→bot→
            # support) for picks. Stored as lists, the source's own typing.
            NestedField(10, 'picks_blue', ListType(element_id=101, element_type=StringType(), element_required=False), required=False),
            NestedField(11, 'picks_red', ListType(element_id=102, element_type=StringType(), element_required=False), required=False),
            NestedField(12, 'bans_blue', ListType(element_id=103, element_type=StringType(), element_required=False), required=False),
            NestedField(13, 'bans_red', ListType(element_id=104, element_type=StringType(), element_required=False), required=False),
            NestedField(14, 'gamelength', StringType(), required=False),
            # Riot's platform game id — the join key to the lolesports/Cito feed.
            NestedField(15, 'riot_game_id', StringType(), required=False),
            NestedField(16, 'vod', StringType(), required=False),
            NestedField(17, 'source_url', StringType(), required=False),
            identifier_field_ids=[1],
        ),
    ),
    # One row per tournament (Leaguepedia `Tournaments`), keyed by OverviewPage.
    'tournaments': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'tournament_id', StringType(), required=True),
            NestedField(2, 'name', StringType(), required=False),
            NestedField(3, 'league', StringType(), required=False),
            NestedField(4, 'region', StringType(), required=False),
            NestedField(5, 'split', StringType(), required=False),
            NestedField(6, 'year', StringType(), required=False),
            NestedField(7, 'tier', StringType(), required=False),
            NestedField(8, 'date_start', StringType(), required=False),
            NestedField(9, 'date_end', StringType(), required=False),
            NestedField(10, 'source_url', StringType(), required=False),
            identifier_field_ids=[1],
        ),
    ),
    # One row per match/series (Leaguepedia `MatchSchedule`), keyed by MatchId.
    # Individual games live in `games`; `winner` is 1 | 2 (which side won the series).
    'matches': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'match_id', StringType(), required=True),
            NestedField(2, 'tournament', StringType(), required=False),
            NestedField(3, 'tab', StringType(), required=False),
            NestedField(4, 'team1', StringType(), required=False),
            NestedField(5, 'team2', StringType(), required=False),
            NestedField(6, 'team1_score', StringType(), required=False),
            NestedField(7, 'team2_score', StringType(), required=False),
            NestedField(8, 'winner', StringType(), required=False),
            NestedField(9, 'best_of', StringType(), required=False),
            NestedField(10, 'datetime_utc', StringType(), required=False),
            NestedField(11, 'source_url', StringType(), required=False),
            identifier_field_ids=[1],
        ),
    ),
}
