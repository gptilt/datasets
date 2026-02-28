from datetime import date, datetime, timezone
from dagster import SkipReason


def skip_backfill(partition_date: date):
    # Get today's actual current date (in UTC)
    today_date = datetime.now(timezone.utc).date()
    
    # If someone tries to run a partition for yesterday, last week, etc.
    if partition_date != today_date:
        # Halt execution gracefully
        raise SkipReason(
            f"Backfills disabled. Tried to run partition for {partition_date}, "
            f"but today is {today_date}."
        )