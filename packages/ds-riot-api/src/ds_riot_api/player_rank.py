from ds_common import no_backfills, tqdm_range
from .get import *
from .constants import DATASET_NAME, SERVERS, TIERS, DIVISIONS, REGION_PER_SERVER
from .schemata import SCHEMATA
import dagster as dg
from datetime import date
from ds_storage import StorageS3
import polars as pl
from pyiceberg.arrow import to_arrow_schema
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


def parse_partition(context):
    keys = context.partition_key.keys_by_dimension
    day = date.fromisoformat(keys["day"])
    server, tier, division = keys["server_x_tier_x_division"].split("_")
    return day, server, tier, division


def get_tags_for_partition(partition_key: str):
    # Split the multi-partition string "YYYY-MM-DD|server_tier_division"
    # to get the 'server_tier_division' part
    _day, server_x_tier_x_division = partition_key.split("|")
    server = server_x_tier_x_division.split('_')[0]
    return {
        # Tag per region
        "concurrency_group": f"fact_player_rank_region={REGION_PER_SERVER[server]}" 
    }


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
    start_time = time.time()
    # Get the partition keys for the current run
    day, server, tier, division = parse_partition(context)
    context.log.info(f"Fetching league entries for {day} - {server} - {tier} - {division}")

    list_of_batches = []

    for page in tqdm_range(500, start=1):
        response = await fetch_with_rate_limit(
            context,
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
        df_batch = pl.DataFrame(response).with_columns(
            pl.lit(int(time.time())).alias("timestamp")
        )
        
        # Append the DataFrame to our Python list (extremely fast)
        list_of_batches.append(df_batch)
    
    # Duplication can occur for a number of reasons:
    # a) Ladder updates while fetching players.
    # b) Players changing ranks between requests.
    # c) New players entering the ladder.
    # d) Players being removed from the ladder.
    # etc.
    # For this reason, we deduplicate the records.
    player_count = 0
    
    if list_of_batches:
        df = pl.concat(list_of_batches)

        # Sort by timestamp, then take the unique puuids, keeping the last (latest) one
        df = (
            df.sort("timestamp")
            .unique(subset=["puuid"], keep="last")
        )

        # Save raw file to S3 storage
        riot_api_bucket.upload(
            df,
            table_name="league_entries",
            object_name=day,
            year=day.year,
            month=day.month,
            server=server,
            tier=tier,
            division=division,
        )
        player_count = len(df)

    yield dg.MaterializeResult(
        metadata={
            "duration_seconds": dg.MetadataValue.float(time.time() - start_time),
            "player_count": dg.MetadataValue.int(player_count),
            # Log the number of pages queried,
            # to ensure it is below the constant that was set.
            "api_pages_queried": dg.MetadataValue.int(page),
        }
    )


@dg.op(
    ins={"raw_riot_api_league_entries": dg.In(dg.Nothing)},
    required_resource_keys={"riot_api_bucket"},
)
def op_extract_and_process_league_entries(
    context: dg.OpExecutionContext,
):
    # Get the partition keys for the current run
    day, server, tier, division = parse_partition(context)

    # Read file from S3 storage
    riot_api_bucket = context.resources.riot_api_bucket
    df_league_entries = riot_api_bucket.get_object_as_dataframe(
        "league_entries",
        object_name=day,
        year=day.year,
        month=day.month,
        server=server,
        tier=tier,
        division=division
    )

    # Apply vectorised transformations
    return (
        df_league_entries.with_columns([
            # Add static values
            pl.lit(server).alias("server"),
            pl.lit(division).alias("division"),
            
            # Compute games played using defaults if null
            (pl.col("wins").fill_null(0) + pl.col("losses").fill_null(0)).alias("games_played"),
        ])
        # Compute win rate safely to prevent division by zero
        .with_columns([pl
            .when(pl.col("games_played") > 0)
            .then(pl.col("wins").fill_null(0) / pl.col("games_played"))
            .otherwise(0.0)
            .alias("win_rate")
        ])
        # Convert timestamp (epoch seconds) to UTC datetime
        .with_columns([pl
            .from_epoch(pl.col("timestamp").fill_null(0), time_unit="s")
            .alias("timestamp"),
        ])
        .with_columns([
            # Extract date from the newly created timestamp
            pl.col("timestamp").dt.date().alias("date"),
            
            # snake_case renaming and applying defaults
            pl.col("freshBlood").fill_null(False).alias("fresh_blood"),
            pl.col("hotStreak").fill_null(False).alias("hot_streak"),
            pl.col("leagueId").alias("league_id"),  # None becomes null automatically
            pl.col("leaguePoints").fill_null(0).alias("league_points"),
        ])
        # Drop the old camelCase columns + queueType. 
        # strict=False ignores missing columns.
        .drop(["freshBlood", "hotStreak", "leagueId", "leaguePoints", "queueType"], strict=False)
    ).to_arrow().cast(to_arrow_schema(SCHEMATA['fact_player_rank']['schema']))


@dg.op(
    pool='catalog_player_rank',
    required_resource_keys={"catalog_clean"},
)
def op_upsert_fact_player_rank(context: dg.OpExecutionContext, table):
    catalog_clean = context.resources.catalog_clean
    table_name = 'fact_player_rank'

    catalog_clean.upsert_table(
        table_name,
        pyarrow_table=table,
        schema=SCHEMATA[table_name]['schema'],
        partition_spec=SCHEMATA[table_name]['partition_spec'],
        sort_order=SCHEMATA[table_name]['sort_order'],
    )

    return dg.Output(
        value=None, 
        metadata={
            "player_count": dg.MetadataValue.int(len(table)),
        }
    )


@dg.graph_asset(
    name="clean_riot_api_player_rank",
    group_name=DATASET_NAME,
    partitions_def=partition_per_day_per_server_x_tier_x_division,
    ins={"raw_riot_api_league_entries": dg.AssetIn(dagster_type=dg.Nothing)}
)
def asset_clean_riot_api_player_rank(raw_riot_api_league_entries):
    """
    Takes a batch of league entries and builds a clean snapshot of player ranks.

    Each partition corresponds to a unique combination of day, server, tier, and division.

    This asset is divided into two ops:
    - Extraction and processing of league entries;
    - Upsert of processed player rank data into the fact table.

    This separation is for performance reasons:
    - The upsert operation is bound by the catalog's write throughput limits,
      which, for a REST catalog, is just 1 write at a time.
    - The extraction and processing of league entries, on the other hand,
      is compute-intensive, and thus should be parallelized
      (within reason, because the worker could otherwise run out of memory).
    """

    table = op_extract_and_process_league_entries(raw_riot_api_league_entries)
    return op_upsert_fact_player_rank(table)


# Define a partitioned job config
riot_job_config = dg.PartitionedConfig(
    partitions_def=partition_per_day_per_server_x_tier_x_division,
    run_config_for_partition_fn=lambda _: {},  # Return empty dict if run_config isn't used
    tags_for_partition_key_fn=get_tags_for_partition
)

# Attach the config to the job
job_riot_api_player_rank = dg.define_asset_job(
    name="job_riot_api_player_rank",
    selection=[
        asset_raw_riot_api_league_entries,
        asset_clean_riot_api_player_rank
    ],
    config=riot_job_config, # <-- Now the job knows how to tag itself!
)
@dg.schedule(
    job=job_riot_api_player_rank,
    cron_schedule="0 0 * * *",  
)
def schedule_riot_api_player_rank(context):
    today = context.scheduled_execution_time.date()
    
    return[
        dg.RunRequest(
            run_key=f"{today}|{server_x_tier_x_division}",
            partition_key=dg.MultiPartitionKey({
                "day": str(today),
                "server_x_tier_x_division": server_x_tier_x_division
            })
        )
        for server_x_tier_x_division in partition_per_server_x_tier_x_division.get_partition_keys()
    ]