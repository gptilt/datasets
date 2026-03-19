from .constants import DEPLOYMENT_NAME
import dagster as dg
import ds_riot_api, ds_storage


modules = [ds_riot_api]
jobs = [
    ds_riot_api.job_riot_api_player_rank,
    ds_riot_api.job_riot_api_player_matches,
]
schedules = [
    ds_riot_api.schedule_riot_api_player_rank,
    ds_riot_api.schedule_riot_api_player_matches,
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
    "catalog_clean": ds_storage.StorageIceberg(
        root=DEPLOYMENT_NAME,
        dataset='riot_api',
        schema_name='clean',
        tables=list(ds_riot_api.SCHEMATA.keys()),
        warehouse_name=dg.EnvVar("CATALOG_WAREHOUSE_NAME"),
        catalog_uri=dg.EnvVar("CATALOG_ENDPOINT"),
        token=dg.EnvVar("CATALOG_TOKEN"),
    ),
    "youtube_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='youtube',
        schema_name='raw',
        tables=['audio'],
        file_extension='m4a',
        bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
        bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
        access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
    )
}


# Add definitions from private submodules
from .private import modules as pv_modules, jobs as pv_jobs, schedules as pv_schedules, resources as pv_resources


modules.extend(pv_modules)
jobs.extend(pv_jobs)
schedules.extend(pv_schedules)
resources.update(pv_resources)


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules),
    jobs=jobs,
    schedules=schedules,
    resources=resources
)