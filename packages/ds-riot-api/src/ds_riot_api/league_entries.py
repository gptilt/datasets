from ds_common import tqdm_range
from .get import *
from .constants import SERVERS, TIERS, DIVISIONS
import dagster as dg
import itertools
from ds_storage import StorageS3, StorageIceberg
import time


# Define partitions
partition_per_day = dg.DailyPartitionsDefinition(start_date="2025-09-13")
partition_per_server_x_tier_x_division = dg.StaticPartitionsDefinition([
    "_".join(product)
    for product in itertools.product(
    SERVERS,
    TIERS,
    DIVISIONS,
)])
partition_per_day_per_server_x_tier_x_division = dg.MultiPartitionsDefinition({
    "day": partition_per_day,
    "server_x_tier_x_division": partition_per_server_x_tier_x_division
})


@dg.asset(
    name="raw_riot_api_league_entries",
    group_name="riot_api",
    partitions_def=partition_per_day_per_server_x_tier_x_division,
)
async def asset_raw_riot_api_league_entries_per_day_per_server_x_tier_x_division(
    context: dg.AssetExecutionContext, riot_api_bucket: StorageS3
):
    """
    A partitioned asset that fetches ranked league entries from the Riot API.

    Each partition corresponds to a unique combination of day, server, tier, and division.
    """
    # Get the partition keys for the current run
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    date = partition_keys["day"]
    server, tier, division = partition_keys["server_x_tier_x_division"].split("_")
    context.log.info(f"Fetching league entries for {date} - {server} - {tier} - {division}")

    list_of_league_entries = []

    for page in tqdm_range(500, start=1):
        response = await fetch_with_rate_limit(
            'league_entries',
            platform=server,
            tier=tier,
            division=division,
            page=page
        )

        # Pages after the maximum return an empty list.
        if len(response) == 0:
            context.log.info("No more pages to fetch")
            break

        # Add a timestamp to each entry
        list_of_league_entries.extend([
            dict(entry, timestamp=time.time())
            for entry in response
        ])
    
    # Save raw file to S3 storage
    riot_api_bucket.upload_json(
        list_of_league_entries,
        f"league_entries/{server}/{tier}_{division}/{date}/",
    )
    
    yield dg.MaterializeResult()


@dg.asset(
    deps=[asset_raw_riot_api_league_entries_per_day_per_server_x_tier_x_division],
    name="clean_riot_api_league_entries",
    group_name="riot_api",
    partitions_def=partition_per_day_per_server_x_tier_x_division,
)
async def asset_clean_riot_api_league_entries_per_day_per_server_x_tier_x_division(
    context: dg.AssetExecutionContext,
    riot_api_bucket: StorageS3,
    catalog_clean: StorageIceberg
):
    """
    Takes the respective partition from raw and writes to a table.

    Each partition corresponds to a unique combination of day, server, tier, and division.
    """
    # Get the partition keys for the current run
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    date = partition_keys["day"]
    server, tier, division = partition_keys["server_x_tier_x_division"].split("_")
 
    # Read raw file from S3 storage
    league_entries = riot_api_bucket.read_files("league_entries", f"{tier}_{division}", date=date, server=server)
    
    catalog_clean.save_records_to_table()
    
    yield dg.MaterializeResult()


# Create a job scheduled to run daily
job_riot_api_league_entries = dg.define_asset_job(
    name="job_riot_api_league_entries",
    selection=[asset_clean_riot_api_league_entries_per_day_per_server_x_tier_x_division],
)
@dg.schedule(
    job=job_riot_api_league_entries,
    cron_schedule="0 0 * * *",  # Run at 0:00 AM every day
)
def schedule_riot_api_league_entries(context):
     # Create a run request for each partition
    today = context.scheduled_execution_time.date()
    return [
        dg.RunRequest(
            run_key=f"{today}|{server_x_tier_x_division}",
            partition_key=dg.MultiPartitionKey({
                "day": today,
                "server_x_tier_x_division": server_x_tier_x_division
            }),
        )
        for server_x_tier_x_division in partition_per_server_x_tier_x_division.get_partition_keys()
    ]