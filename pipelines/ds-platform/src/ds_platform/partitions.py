"""
The one weekly partition every pipeline shares.

Keyed on the Monday of each ISO week. Raw snapshots land at
`year=YYYY/month=MM/week_of=YYYYMMDD/...` on S3; downstream assets read the same
partition by re-deriving those kwargs from `context.partition_key` via
`partition_kwargs`.

`end_offset=1` makes the *current* ongoing week selectable for materialization —
schedules fire early in the week and today's run must be valid. `start_date` is the
project's data-history epoch; older partitions stay un-materializable via the
`no_backfills` decorator (see `ds_platform.backfills`).
"""
import dagster as dg
from datetime import date


partition_per_week = dg.WeeklyPartitionsDefinition(
    start_date="2024-01-01",
    end_offset=1,
)


def partition_kwargs(partition_key: str) -> dict:
    """
    Translate a weekly partition key ("YYYY-MM-DD") into the storage kwargs used by
    `StorageS3.upload/get` — `year`, `month`, `week_of`.

    Writers and readers call this with the same partition_key so the S3 paths line up.
    """
    day = date.fromisoformat(partition_key)
    return {
        "year": day.year,
        "month": day.month,
        "week_of": day.strftime("%Y%m%d"),
    }
