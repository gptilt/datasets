"""
Partition definitions for ds-esports.

Both raw and clean layers share a single weekly partition keyed on the Monday of
each ISO week. Raw snapshots land at `year=YYYY/month=MM/week_of=YYYYMMDD/...`
on S3; clean assets read the same partition by re-deriving those kwargs from
`context.partition_key` (the Monday date string).

`end_offset=1` makes the *current* ongoing week selectable for materialization —
the schedule fires on Monday 06:00 UTC and we want today's run to be valid.
Raw assets are decorated with `@no_backfills` (see ds_common), so older
partitions can't be materialized by mistake even though they remain in the
partitions list.
"""
import dagster as dg
from datetime import date


# Anchored on a Sunday so all keys are ISO-week Sundays.
partition_per_week = dg.WeeklyPartitionsDefinition(
    start_date="2026-01-05",
    end_offset=1,
)


def partition_kwargs(partition_key: str) -> dict:
    """
    Translate a weekly partition key ("YYYY-MM-DD" Monday) into the storage kwargs
    used by `StorageS3.upload/get` — `year`, `month`, `week_of`.

    Both raw (writer) and clean (reader) call this with the same partition_key so
    the S3 paths always line up.
    """
    day = date.fromisoformat(partition_key)
    return {
        "year": day.year,
        "month": day.month,
        "week_of": day.strftime("%Y%m%d"),
    }
