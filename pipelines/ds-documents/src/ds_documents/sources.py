"""
Producer asset:
ingest staged transcripts into `source_registry`,
grounding each to the pro match it discusses along the way.

Weekly-partitioned:
1) For a given week it reads every `document.stg.transcripts` object;
2) Builds the alias-denormalized match candidates once from the Leaguepedia clean catalog;
3) Grounds each transcript on its title;
4) **Upserts** a registry row on `source_id`.

Upsert is what lets other producers (articles, forums) contribute to the same table later.
"""
from datetime import datetime, timezone
import tempfile

import dagster as dg
import polars as pl

from ds_esports import resolve_match
from ds_models import LocalLLM
from ds_platform import partition_kwargs, partition_per_week
from ds_storage import StorageIceberg, StorageS3

from .grounding import denormalize_team_candidates
from .registry import build_source_registry_row
from .schemata import SCHEMATA


def _parse_published(value) -> datetime | None:
    """
    Parse the transcript's `published_at` (yt-dlp `YYYYMMDD`) to aware UTC.
    """
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _build_candidates(leaguepedia_catalog_clean: StorageIceberg) -> pl.DataFrame:
    """
    Alias-denormalized match candidates with a match-level `patch` column.

    `matches` has no patch (it's per-game on `games`),
    so we fold the per-series patch up from `games`
    and join it on before denormalizing — it then rides through
    to the matched row for the registry to read.
    """
    matches = leaguepedia_catalog_clean.load_table_to_polars("matches")
    games = leaguepedia_catalog_clean.load_table_to_polars("games")
    aliases = leaguepedia_catalog_clean.load_table_to_polars("entity_aliases")

    match_patch = (games
        .filter(pl.col("patch").is_not_null())
        .group_by("match_id")
        .agg(pl.col("patch").mode().first().alias("patch"))
    )
    matches = matches.join(match_patch, on="match_id", how="left")
    return denormalize_team_candidates(matches, aliases)


@dg.asset(
    name="clean_source_registry",
    group_name="document",
    partitions_def=partition_per_week,
)
def asset_clean_source_registry(
    context: dg.AssetExecutionContext,
    document_bucket: StorageS3,
    document_catalog_clean: StorageIceberg,
    leaguepedia_catalog_clean: StorageIceberg,
    local_llm: LocalLLM,
):
    """Ground this week's transcripts and upsert them into `source_registry`."""
    part = partition_kwargs(context.partition_key)  # {year, month, week_of}

    # 1. Read every transcript object for the week,
    # any platform — omitted partitions are auto-traversed.
    with tempfile.TemporaryDirectory() as tmp:
        files = document_bucket.download_all_files_in_partition(
            target_local_directory=tmp,
            table_name="transcripts",
            **part,
        )
        if not files:
            return dg.MaterializeResult(metadata={"processed": 0, "note": "no transcripts this week"})
        transcripts = pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")

    # 2. Build grounding candidates once for the whole partition.
    candidates = _build_candidates(leaguepedia_catalog_clean)

    # 3. Ground each transcript on its title plus an excerpt of the body.
    # Then assemble its registry row.
    rows = []
    for t in transcripts.iter_rows(named=True):
        title = t["title"] or ""
        timestamp = _parse_published(t["published_at"])
        segments = t["segments"] or []
        content = " ".join(s.get("text", "") for s in segments) if segments else title

        grounding = None
        grounding_text = f"{title}\n{content}".strip()
        if grounding_text and timestamp is not None:
            grounding = resolve_match(
                timestamp=timestamp,
                content=grounding_text,
                candidates=candidates,
                llm=local_llm,
                tournament_column="tournament",
            )

        rows.append(build_source_registry_row(
            source_id=t["source_id"],
            origin_uri=t["origin_uri"],
            source_type="transcript",
            content=content,
            platform=t["platform"],
            author_name=t["author_name"],
            title=title,
            published_at=t["published_at"],
            grounding=grounding,
        ))

    # 4. Upsert on source_id, so re-runs refresh and other producers can co-write.
    document_catalog_clean.create_table_if_not_exists("source_registry", SCHEMATA["source_registry"])
    document_catalog_clean.write_dataframe_to_table("source_registry", pl.DataFrame(rows), mode="upsert")

    grounded = sum(1 for r in rows if r["processing_status"] == "grounded")
    return dg.MaterializeResult(
        metadata={
            "processed": len(rows),
            "grounded": grounded,
            "ungrounded": len(rows) - grounded,
        }
    )
