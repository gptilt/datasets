"""
Dataset descriptors for HuggingFace publishing.

Everything that varies per published dataset lives in a `Dataset` instance —
repo, license, card prose, tables, upstream deps, source catalog.
The publish machinery (publish.py, render.py) is generic over these,
so adding or reviving a dataset is just defining another `Dataset`.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    # `slug` names the Dagster asset/job/schedule and the `templates/<slug>/` subtree.
    slug: str
    repo_id: str
    # Declared on the card. Per-dataset: Leaguepedia is CC BY-SA, Riot is CC BY-NC.
    license: str
    pretty_name: str
    dataset_summary: str
    purpose: str
    # table name (== Iceberg table == HF config) -> one-line description for the card.
    tables: dict[str, str]
    # Iceberg source: the resource key to read from, and the clean assets it trails.
    catalog_resource_key: str
    deps: list[str]
    # When to (re)publish. Defaults to weekly Sunday 08:00 UTC.
    cron: str = "0 8 * * 0"

    @property
    def dataset_id(self) -> str:
        """Slug used in card URLs / citation (the part after `org/`)."""
        return self.repo_id.split("/", 1)[1]


ESPORTS = Dataset(
    slug="esports",
    repo_id="gptilt/lol-esports-entities",
    license="cc-by-sa-3.0",
    pretty_name="League of Legends Esports Directory",
    dataset_summary=(
        "A periodically-refreshed directory of League of Legends esports "
        "**public figures** and **teams**, plus a unified **entity alias index** "
        "for resolving the many names each entity is known by. "
        "Built from Leaguepedia (lol.fandom.com) via its structured Cargo query API."
    ),
    purpose=(
        "It provides a clean, canonical reference for linking esports people "
        "and organizations across heterogeneous data sources, "
        "and for normalizing the many names each entity is known by."
    ),
    tables={
        "public_figures": (
            "One row per League of Legends esports public figure — "
            "professional players, ex-players, coaches, analysts, casters, and owners. "
            "Identity is `person_id`, the canonical Leaguepedia page key."
        ),
        "teams": (
            "One row per esports organization/team, keyed by `team_id` "
            "(the canonical Leaguepedia page key), with region, location, disbanded flag."
        ),
        "entity_aliases": (
            "A unified alias index: every known surface name "
            "(in-game names, real names, romanizations, redirects, team abbreviations) "
            "mapped to its canonical entity via `entity_id`. "
            "`entity_type` ('person' | 'team') distinguishes the two; "
            "`alias_type` records where the name came from."
        ),
    },
    catalog_resource_key="leaguepedia_catalog_clean",
    deps=["clean_public_figures", "clean_teams", "clean_entity_aliases"],
)
