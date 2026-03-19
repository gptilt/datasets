from ds_common import no_backfills, tqdm_range
from .get import *
from .constants import DATASET_NAME, SERVERS, TIERS_AND_DIVISIONS, ELITE_TIERS, REGION_PER_SERVER
import dagster as dg
from datetime import date, datetime, timedelta, timezone
from ds_storage import StorageS3, StorageIceberg, RecordNotFoundError
import polars as pl
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
import random
import re
import time


# Define partitions
partition_per_day = dg.DailyPartitionsDefinition(
    start_date="2026-01-01",
    # Include the current, ongoing day in the list of valid partitions:
    end_offset=1
)
partition_per_server = dg.StaticPartitionsDefinition(SERVERS)
partition_per_day_per_server = dg.MultiPartitionsDefinition({
    "day": partition_per_day,
    "server": partition_per_server
})


RAW_TABLE_NAME = "league_entries"
CLEAN_TABLE_NAME = "fact_player_rank"


def parse_partition(context):
    keys = context.partition_key.keys_by_dimension
    day = date.fromisoformat(keys["day"])
    server = keys["server"]
    return day, server


def get_tags_for_partition(partition_key: str):
    # Split the multi-partition string "YYYY-MM-DD|server"
    _day, server = partition_key.split("|")
    return {
        # Tag per region
        "concurrency_group": f"fact_player_rank_region={REGION_PER_SERVER[server]}" 
    }


def log_progress(context, current_step: int, tier: str, division: str, msg: str):
    context.log.info(f"[{current_step}/{len(TIERS_AND_DIVISIONS)}][{tier} {division}] {msg}")


async def fetch_league_entries(
    context: dg.AssetExecutionContext,
    server: str,
    tier: str,
    division: str,
) -> list[pl.DataFrame]:
    """
    Fetch league entries from the Riot API.
    Loop through pages to fetch all entries.
    Then concatenate all entries into a single DataFrame.
    """
    list_of_batches = []

    for page in tqdm_range(500, start=1):
        if tier in ELITE_TIERS:
            response = await fetch_with_rate_limit(
                context,
                'league_entries_elite',
                platform=server,
                elite_tier=tier
            )
            entries = response.pop('entries', [])
            response = [{**response, **entry} for entry in entries]
        else:
            response = await fetch_with_rate_limit(
                context,
                'league_entries',
                platform=server,
                tier=tier,
                division=division,
                page=page
            )

            if not response:  # Empty list means no more pages
                context.log.info("No more pages to fetch")
                break

        # Convert to Polars and add timestamp
        df_batch = pl.DataFrame(response).with_columns(
            timestamp=pl.lit(int(time.time()))
        )
            
        list_of_batches.append(df_batch)

        # Elite tiers only have a single page
        if tier in ELITE_TIERS:
            break

    return list_of_batches


@dg.asset(
    name="raw_riot_api_league_entries",
    group_name=DATASET_NAME,
    partitions_def=partition_per_day_per_server,
)
@no_backfills
async def asset_raw_riot_api_league_entries(
    context: dg.AssetExecutionContext, riot_api_bucket: StorageS3
):
    """
    A partitioned asset that fetches ranked league entries from the Riot API.

    Each partition corresponds to a unique combination of day and server.
    
    Fetches missing rank data and stores it in S3 file by file.
    This creates natural checkpoints in case the worker is interrupted.
    """
    day, server = parse_partition(context)
    player_count = 0

    # Fetch list of objects already stored in S3
    objects_already_stored = riot_api_bucket.list_objects(
        table_name=RAW_TABLE_NAME,
        object_name=day,
        year=day.year,
        month=day.month,
        server=server
    )

    # Extract the existing (tier, division) combinations from the S3 keys
    existing_combinations = set()

    for key in objects_already_stored:
        tier_match = re.search(r'tier=([^/]+)', key)
        div_match = re.search(r'division=([^/]+)', key)
        if tier_match and div_match:
            existing_combinations.add((tier_match.group(1), div_match.group(1)))

    # Subtract the ones that already exist in S3
    missing_combinations = list(set(TIERS_AND_DIVISIONS) - existing_combinations)
    random.shuffle(missing_combinations)

    for i, (tier, division) in enumerate(missing_combinations):
        current_step = len(existing_combinations) + i + 1
        log_progress(context, current_step, tier, division, "Fetching...")

        list_of_batches = await fetch_league_entries(context, server, tier, division)
        
        # Duplication can occur for a number of reasons:
        # a) Ladder updates while fetching players.
        # b) Players changing ranks between requests.
        # c) New players entering the ladder.
        # d) Players being removed from the ladder.
        # etc.
        # For this reason, we deduplicate the records.        
        if list_of_batches:
            df = pl.concat(list_of_batches)

            # Keep latest record per player
            df = df.sort("timestamp").unique(subset=["puuid"], keep="last")
            player_count += len(df)

            # Upload to S3 as a checkpoint
            riot_api_bucket.upload(
                df,
                table_name=RAW_TABLE_NAME,
                object_name=day,
                year=day.year,
                month=day.month,
                server=server,
                tier=tier,
                division=division,
            )

            log_progress(context, current_step, tier, division, f"Completed. Player count: {player_count}")

    yield dg.MaterializeResult(
        metadata={
            "ranks_processed": dg.MetadataValue.json(missing_combinations),
            "player_count": dg.MetadataValue.int(player_count)
        }
    )


# @dg.op(
#     ins={"raw_riot_api_league_entries": dg.In(dg.Nothing)},
#     required_resource_keys={"riot_api_bucket"},
# )
# def op_extract_and_process_league_entries(
#     context: dg.OpExecutionContext,
# ) -> pl.DataFrame:
"""
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

# @dg.op(
#     required_resource_keys={"catalog_clean"},
#     tags={"concurrency_group": "catalog_clean"}
# )
# def op_upsert_fact_player_rank(context: dg.OpExecutionContext, df: pl.DataFrame):


@dg.asset(
    deps=['raw_riot_api_league_entries'],
    name="clean_riot_api_player_rank",
    group_name=DATASET_NAME,
    partitions_def=partition_per_day_per_server,
    tags={"concurrency_group": "catalog_clean"}
)
def asset_clean_riot_api_player_rank(
    context: dg.AssetExecutionContext,
    riot_api_bucket: StorageS3,
    catalog_clean: StorageIceberg
):
    """
    Takes a batch of league entries and builds a clean snapshot of player ranks.

    Each partition corresponds to a unique combination of day and server.

    Process:
    1. Check if the partition already exists in the catalog.
    2. Check if it exists in the raw layer.
    3. If both exist, read the data from the raw layer,
    apply transformations and write it to the catalog.
    """
    day, server = parse_partition(context)
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    
    # Get ranks already processed natively into a Set of Tuples using iter_rows
    df_existing_records = pl.from_arrow(
        catalog_clean.scan_table(
            table_name=CLEAN_TABLE_NAME,
            selected_fields=["tier", "division"],
            row_filter=And(
                GreaterThanOrEqual("timestamp", start),
                LessThan("timestamp", end)
            ),
            server=server,
        ).to_arrow()
    ).unique()
    
    existing_records = set(df_existing_records.select(["tier", "division"]).iter_rows())
    
    # Subtract the ones that already exist in S3
    missing_records = list(set(TIERS_AND_DIVISIONS) - existing_records)
    random.shuffle(missing_records)

    player_count = 0
    processed_records = set()

    # Extract and Transform
    for i, (tier, division) in enumerate(missing_records):
        current_step = len(existing_records) + i + 1
        log_progress(context, current_step, tier, division, "Processing...")

        try:
            object_key = riot_api_bucket.object_exists(
                table_name=RAW_TABLE_NAME,
                object_name=day,
                year=day.year,
                month=day.month,
                server=server,
                tier=tier,
                division=division
            )
        except RecordNotFoundError:
            log_progress(context, current_step, tier, division, f"Object does not exist: {object_key}")
            continue
        
        # Read file from S3 storage
        df_raw = riot_api_bucket.get_object_as_dataframe(
            table_name=RAW_TABLE_NAME,
            object_name=day,
            year=day.year,
            month=day.month,
            server=server,
            tier=tier,
            division=division
        )

        log_progress(context, current_step, tier, division, f"Fetched {len(df_raw)} rows from raw.")

        # Apply vectorised transformations
        df_clean = (
            df_raw.with_columns([
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
        )

        catalog_clean.write_dataframe_to_table(
            table_name=CLEAN_TABLE_NAME,
            df=df_clean,
            mode='upsert',
        )

        player_count += len(df_clean)
        processed_records.add((tier, division))

        log_progress(context, current_step, tier, division, f"Processed {len(df_clean)} rows.")

    # Final verification checks
    missing_unprocessed = set(missing_records).difference(processed_records)
    if missing_unprocessed:
        context.log.error(f"Missing combinations: {missing_unprocessed}")
        raise ValueError(f"Missing combinations: {missing_unprocessed}")

    return dg.MaterializeResult(
        metadata={
            "ranks_processed": dg.MetadataValue.json(list(processed_records)),
            "player_count": dg.MetadataValue.int(player_count)
        }
    )


# Define a partitioned job config
riot_job_config = dg.PartitionedConfig(
    partitions_def=partition_per_day_per_server,
    run_config_for_partition_fn=lambda _: {},  # Return empty dict if run_config isn't used
    tags_for_partition_key_fn=get_tags_for_partition
)

# Attach the config to the job
job_raw_riot_api_league_entries = dg.define_asset_job(
    name="job_raw_riot_api_league_entries",
    selection=[
        asset_raw_riot_api_league_entries
    ],
    config=riot_job_config, # <-- Now the job knows how to tag itself!
)
job_clean_riot_api_player_rank = dg.define_asset_job(
    name="job_clean_riot_api_player_rank",
    selection=[
        asset_clean_riot_api_player_rank
    ],
)

# The raw job runs automatically every day at midnight
@dg.schedule(
    job=job_raw_riot_api_league_entries,
    cron_schedule="0 0 * * *",
)
def schedule_riot_api_player_rank(context):
    today = context.scheduled_execution_time.date()
    
    return [
        dg.RunRequest(
            run_key=f"{today}|{server}",
            partition_key=dg.MultiPartitionKey({
                "day": str(today),
                "server": server
            })
        )
        for server in partition_per_server.get_partition_keys()
    ]

# The clean job is triggered when the raw job finishes
@dg.asset_sensor(
    asset_key=dg.AssetKey("asset_raw_riot_api_league_entries"),
    job=job_clean_riot_api_player_rank,
)
def sensor_riot_api_league_entries_to_player_rank(context, asset_event):
    # Pass the same partition key along to the second job
    partition_key = asset_event.partition_key

    return dg.RunRequest(
        partition_key=partition_key,
    )