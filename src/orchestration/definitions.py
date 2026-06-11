import dagster as dg
from ds_runtime import DEPLOYMENT_NAME
import ds_esports, ds_riot_api, ds_storage


modules = [ds_riot_api, ds_esports]
jobs = [
    ds_riot_api.job_raw_riot_api_league_entries,
    ds_riot_api.job_clean_riot_api_player_rank,
    ds_riot_api.job_riot_api_player_matches,
    ds_esports.job_esports_phase_1,
]
schedules = [
    ds_riot_api.schedule_riot_api_player_rank,
    ds_riot_api.schedule_riot_api_player_matches,
    ds_esports.schedule_esports_phase_1,
]
sensors = [
    ds_riot_api.sensor_riot_api_league_entries_to_player_rank
]
resources = {
    "riot_api_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='riot_api',
        schema_name='raw',
        tables=['league_entries'],
        file_extension='parquet',
        bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
        bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
        access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
    ),
    "esports_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='esports',
        schema_name='raw',
        tables=['leaguepedia_players', 'leaguepedia_player_redirects', 'leaguepedia_teams'],
        file_extension='json',
        bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
        bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
        access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
    ),
    "esports_cargo": ds_esports.CargoClient(
        username=dg.EnvVar("FANDOM_USERNAME"),
        password=dg.EnvVar("FANDOM_PASSWORD"),
    ),
    "esports_catalog_clean": ds_storage.StorageIceberg(
        root=DEPLOYMENT_NAME,
        dataset='esports',
        schema_name='clean',
        tables=list(ds_esports.SCHEMATA.keys()),
        warehouse_name=dg.EnvVar("CATALOG_WAREHOUSE_NAME"),
        catalog_uri=dg.EnvVar("CATALOG_ENDPOINT"),
        token=dg.EnvVar("CATALOG_TOKEN"),
    ),
    "document_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='document',
        schema_name='stg',
        tables=['transcripts'],
        file_extension='parquet',
        bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
        bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
        access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
    ),
    "catalog_clean": ds_storage.StorageIceberg(
        root=DEPLOYMENT_NAME,
        dataset='riot_api',
        schema_name='clean',
        tables=list(ds_riot_api.SCHEMATA.keys()),
        warehouse_name=dg.EnvVar("CATALOG_WAREHOUSE_NAME"),
        catalog_uri=dg.EnvVar("CATALOG_ENDPOINT"),
        token=dg.EnvVar("CATALOG_TOKEN"),
    )
}


# Add definitions from private submodules
from .private import (
    modules as pv_modules,
    jobs as pv_jobs,
    schedules as pv_schedules,
    resources as pv_resources,
    sensors as pv_sensors
)


modules.extend(pv_modules)
jobs.extend(pv_jobs)
schedules.extend(pv_schedules)
sensors.extend(pv_sensors)
resources.update(pv_resources)


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules),
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
    resources=resources
)