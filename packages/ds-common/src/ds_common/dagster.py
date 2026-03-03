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
                
    if context and context.has_partition_key:
        if isinstance(context.partition_key, dg.MultiPartitionKey):
            date_str = context.partition_key.keys_by_dimension.get("day")
        else:
            date_str = context.partition_key

        if date_str:
            p_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            today_date = datetime.now(timezone.utc).date()
            
            if p_date != today_date:
                raise ValueError(
                    f"Cannot fetch historical data for '{context.asset_key.to_user_string()}'. "
                    f"Target partition: {p_date}. Today: {today_date}. "
                    "This asset only works with current data. If you are trying to "
                    "backfill downstream tables, please deselect the raw assets and try again."
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