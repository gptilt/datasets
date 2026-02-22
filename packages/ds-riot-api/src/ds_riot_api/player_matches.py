import dagster as dg
from .get import *
from .constants import REGION_PER_SERVER, SERVERS


# Define partitions
partition_per_day = dg.DailyPartitionsDefinition(start_date="2025-01-01")
partition_per_server = dg.StaticPartitionsDefinition(SERVERS)
partition_per_day_per_server = dg.MultiPartitionsDefinition({
    "day": partition_per_day,
    "server": partition_per_server
})


@dg.multi_asset(
    specs=[
        dg.AssetSpec("riot_api_player_match_ids"),
        dg.AssetSpec("riot_api_match_info"),
        dg.AssetSpec("riot_api_match_timeline"),
    ],
    group_name="riot_api",
    partitions_def=partition_per_day_per_server,
)
async def asset_riot_api_player_matches_per_day_per_server(
    context: dg.AssetExecutionContext
):
    """
    A partitioned asset that selects player IDs and fetches corresponding match IDs.
    """
    # Get the partition keys for the current run
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = partition_keys["day"]
    server = partition_keys["server"]
    context.log.info(f"Fetching data for {date} on {server}")

    storage_raw = new_storage('raw')

    list_of_match_ids = await fetch_with_rate_limit(
        'player_match_ids',
        region=REGION_PER_SERVER.get(server),
        puuid="QjHAbswPOuBJcq1IlLni4wTR8wbctL3a7XPl7-cGWO-FJkp53zYvLZ21Y7qeV1ybCz2BbeIhVAtQ4A",
        queue=420,
        type='ranked',
        count=100
    )
    print(list_of_match_ids)

    yield dg.MaterializeResult()


# Create a job scheduled to run daily
job_riot_api_player_matches = dg.define_asset_job(
    name="job_riot_api_player_matches",
    selection=[asset_riot_api_player_matches_per_day_per_server],
)
@dg.schedule(
    job=job_riot_api_player_matches,
    cron_schedule="0 0 * * *",  # Run at 0:00 AM every day
)
def schedule_riot_api_player_matches(context):
     # Create a run request for each partition
    today = context.scheduled_execution_time.date()
    return [
        dg.RunRequest(
            run_key=f"{today}|{server}",
            partition_key=dg.MultiPartitionKey({
                "day": today,
                "server": server
            }),
        )
        for server in partition_per_server.get_partition_keys()
    ]