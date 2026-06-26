"""
Grounding glue:
shape the Leaguepedia catalog into the alias-denormalized candidate frame
the `ds_esports` resolver expects.

The resolver does no alias resolution by design — it exact-matches extracted team
mentions against candidate rows.
So the catalog's `matches.team1/team2` (canonical team names)
must be expanded to *every* surface form via `entity_aliases`,
yielding one candidate row per (match, team1 alias, team2 alias).
"""
from __future__ import annotations

import polars as pl


def denormalize_team_candidates(
    matches: pl.DataFrame,
    entity_aliases: pl.DataFrame,
    *,
    team_1_column: str = "team1",
    team_2_column: str = "team2",
) -> pl.DataFrame:
    """
    Expand each match into one row per (team1 alias × team2 alias).

    `matches` carries canonical team names in `team_1_column`/`team_2_column`;
    `entity_aliases` is the Leaguepedia alias index.
    Matches whose team names are absent from the index
    (no alias, including the canonical self-alias) drop out.
    This is a precision-first approach; an un-resolvable team can't be matched anyway.
    All other match columns (match_id, datetime_utc, tournament, patch, …)
    ride along untouched.
    """
    teams = (
        entity_aliases.filter(pl.col("entity_type") == "team")
        .select("alias", "entity_id")
        .unique()
    )

    # Resolve each team column's canonical name to its entity_id
    # (inner join drops matches with an unknown team),
    # then expand that entity to all its aliases.
    return (
        matches
        .join(teams.rename({"alias": team_1_column, "entity_id": "_e1"}), on=team_1_column, how="inner")
        .join(teams.rename({"alias": team_2_column, "entity_id": "_e2"}), on=team_2_column, how="inner")
        .drop(team_1_column, team_2_column)
        .join(teams.rename({"entity_id": "_e1", "alias": team_1_column}), on="_e1")
        .join(teams.rename({"entity_id": "_e2", "alias": team_2_column}), on="_e2")
        .drop("_e1", "_e2")
    )
