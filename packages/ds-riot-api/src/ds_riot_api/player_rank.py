from ds_common import no_backfills, tqdm_range
from .get import *
from .constants import DATASET_NAME, SERVERS, TIERS, DIVISIONS
from .schemata import SCHEMATA
import dagster as dg
from datetime import date, datetime, timezone
from ds_storage import StorageS3, StorageIceberg
import time


# Define partitions
partition_per_day = dg.DailyPartitionsDefinition(
    start_date="2026-01-01",
    # Include the current, ongoing day in the list of valid partitions:
    end_offset=1
)
partition_per_server_x_tier_x_division = dg.StaticPartitionsDefinition([
    f"{server}_{tier}_{division}"
    for tier in TIERS
    for division in DIVISIONS
    for server in SERVERS
])
partition_per_day_per_server_x_tier_x_division = dg.MultiPartitionsDefinition({
    "day": partition_per_day,
    "server_x_tier_x_division": partition_per_server_x_tier_x_division
})


@dg.asset(
    name="raw_riot_api_league_entries",
    group_name=DATASET_NAME,
    partitions_def=partition_per_day_per_server_x_tier_x_division,
)
@no_backfills
async def asset_raw_riot_api_league_entries(
    context: dg.AssetExecutionContext, riot_api_bucket: StorageS3
):
    """
    A partitioned asset that fetches ranked league entries from the Riot API.

    Each partition corresponds to a unique combination of day, server, tier, and division.
    """
    # Get the partition keys for the current run
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    date = date.fromisoformat(partition_keys["day"])
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
    
    # Duplication can occur for a number of reasons:
    # a) Ladder updates while fetching players.
    # b) Players changing ranks between requests.
    # c) New players entering the ladder.
    # d) Players being removed from the ladder.
    # etc.
    # For this reason, we deduplicate the records.
    latest = {}

    for e in list_of_league_entries:
        latest[e["puuid"]] = max(
            e,
            latest.get(e["puuid"], e),
            # Choose the latest entry based on the timestamp
            key=lambda x: x["timestamp"]
        )
    
    list_of_league_entries_deduped = list(latest.values())

    # Save raw file to S3 storage
    riot_api_bucket.upload_json(
        list_of_league_entries_deduped,
        table_name="league_entries",
        object_name=date,
        year=date.year,
        month=date.month,
        server=server,
        tier=tier,
        division=division,
    )
    
    yield dg.MaterializeResult(
        metadata={
            "player_count": len(list_of_league_entries_deduped),
            # Log the number of pages queried,
            # to ensure it is below the constant that was set.
            "api_pages_queried": page,
        }
    )


@dg.asset(
    deps=[asset_raw_riot_api_league_entries],
    name="clean_riot_api_player_rank",
    group_name=DATASET_NAME,
    partitions_def=partition_per_day_per_server_x_tier_x_division,
)
async def asset_clean_riot_api_player_rank(
    context: dg.AssetExecutionContext,
    riot_api_bucket: StorageS3,
    catalog_clean: StorageIceberg
):
    """
    Takes a batch of league entries and builds a clean snapshot of player ranks.

    Each partition corresponds to a unique combination of day, server, tier, and division.
    """
    # Get the partition keys for the current run
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    date = partition_keys["day"]
    server, tier, division = partition_keys["server_x_tier_x_division"].split("_")
 
    # Read raw file from S3 storage
    league_entries = riot_api_bucket.get_object(
        "league_entries",
        object_name=date,
        server=server,
        tier=tier,
        division=division
    )

    # Get matching player names and tags
    for entry in league_entries:
        entry['server'] = server
        entry['division'] = division

        # Compute basic statistics
        entry['games_played'] = entry.get('wins', 0) + entry.get('losses', 0)
        entry['win_rate'] = entry.get('wins', 0) / entry['games_played'] if entry['games_played'] > 0 else 0

        # Rename variables to snake_case
        entry['fresh_blood'] = entry.get('freshBlood', False)
        entry['hot_streak'] = entry.get('hotStreak', False)
        entry['league_id'] = entry.get('leagueId', None)
        entry['league_points'] = entry.get('leaguePoints', 0)

        # Convert unix timestamp to UTC
        entry['timestamp'] = datetime.fromtimestamp(entry.get('timestamp', 0), tz=timezone.utc)
        # Get date
        entry['date'] = entry['timestamp'].date()

        for key in (
            'freshBlood',
            'hotStreak',
            'leagueId',
            'leaguePoints',
            'queueType',
        ):
            entry.pop(key, None)

    table_name = 'fact_player_rank'
    catalog_clean.upsert_records(
        table_name,
        list_of_records=league_entries,
        schema=SCHEMATA[table_name]['schema'],
        partition_spec=SCHEMATA[table_name]['partition_spec'],
        sort_order=SCHEMATA[table_name]['sort_order'],
    )
    
    yield dg.MaterializeResult(
        metadata={
            "player_count": len(league_entries),
        }
    )


# Create a job scheduled to run daily
job_riot_api_player_rank = dg.define_asset_job(
    name="job_riot_api_player_rank",
    selection=[
        asset_raw_riot_api_league_entries,
        asset_clean_riot_api_player_rank
    ],
)
@dg.schedule(
    job=job_riot_api_player_rank,
    cron_schedule="0 0 * * *",  # Run at 0:00 AM every day
)
def schedule_riot_api_player_rank(context):
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