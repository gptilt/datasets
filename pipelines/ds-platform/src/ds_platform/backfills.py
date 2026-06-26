import inspect
import functools
from datetime import datetime, timezone
import dagster as dg

def _validate_no_backfill(args, kwargs):
    """Helper function to extract context and validate the date."""
    context = kwargs.get("context")
    if not context:
        for arg in args:
            if isinstance(arg, dg.AssetExecutionContext):
                context = arg
                break

    if not (context and context.has_partition_key):
        return

    # Use the partition's time window so the check works for any time-based
    # partition (daily, weekly, monthly, …) and any time-based dimension of a
    # MultiPartitionsDefinition. Strict equality against `partition_key` only
    # works for daily — for weekly partitions today's date almost never matches
    # the partition's anchor day.
    try:
        window = context.partition_time_window
    except (ValueError, AttributeError):
        # Pure static partition with no time dimension — nothing to validate.
        return

    today_date = datetime.now(timezone.utc).date()
    start_date = window.start.date()
    end_date = window.end.date()

    if not (start_date <= today_date < end_date):
        raise ValueError(
            f"Cannot fetch historical data for '{context.asset_key.to_user_string()}'. "
            f"Today ({today_date}) is outside the partition window "
            f"[{start_date}, {end_date}). This asset only works with current data. "
            "If you are trying to backfill downstream tables, please deselect the "
            "raw assets and try again."
        )

def no_backfills(func):
    """
    Decorator to prevent backfilling historical partitions.
    Supports both standard async functions (return) and async generators (yield).
    """
    if inspect.isasyncgenfunction(func):
        # Wrapper for assets that use `yield`
        @functools.wraps(func)
        async def async_gen_wrapper(*args, **kwargs):
            _validate_no_backfill(args, kwargs)
            async for item in func(*args, **kwargs):
                yield item
        return async_gen_wrapper
    else:
        # Wrapper for assets that use `return`
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _validate_no_backfill(args, kwargs)
            return await func(*args, **kwargs)
        return async_wrapper