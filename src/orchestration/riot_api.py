"""`riot_api` code location.

Self-contained Dagster code location for the Riot Games API source platform.
"""
import dagster as dg
import ds_riot_api, ds_storage

from .constants import *


modules = [ds_riot_api]
jobs = [
    ds_riot_api.job_raw_riot_api_league_entries,
    ds_riot_api.job_clean_riot_api_player_rank,
    ds_riot_api.job_riot_api_player_matches,
]
schedules = [
    ds_riot_api.schedule_riot_api_player_rank,
    ds_riot_api.schedule_riot_api_player_matches,
]
sensors = [
    ds_riot_api.sensor_riot_api_league_entries_to_player_rank,
]
resources = {
    "riot_api_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='riot_api',
        schema_name='raw',
        tables=['league_entries'],
        file_extension='parquet',
        bucket_endpoint=BUCKET_ENDPOINT,
        bucket_name=BUCKET_NAME,
        access_key_id=BUCKET_ACCESS_KEY_ID,
        secret_access_key=BUCKET_SECRET_ACCESS_KEY,
    ),
    "catalog_clean": ds_storage.StorageIceberg(
        root=DEPLOYMENT_NAME,
        dataset='riot_api',
        schema_name='clean',
        tables=list(ds_riot_api.SCHEMATA.keys()),
        warehouse_name=CATALOG_WAREHOUSE_NAME,
        catalog_uri=CATALOG_ENDPOINT,
        rest_signing_region=AWS_REGION,
    ),
}


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules),
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
    resources=resources,
)
