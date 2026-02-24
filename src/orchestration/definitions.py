import dagster as dg
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud
import ds_riot_api, ds_youtube, ds_storage


defs = dg.Definitions(
    assets=link_code_references_to_git_if_cloud(assets_defs=dg.with_source_code_references(
        dg.load_assets_from_modules([ds_riot_api, ds_youtube])
    )),
    jobs=[
        ds_riot_api.job_riot_api_league_entries,
        ds_riot_api.job_riot_api_player_matches,
    ],
    schedules=[
        ds_riot_api.schedule_riot_api_league_entries,
        ds_riot_api.schedule_riot_api_player_matches
    ],
    resources={
        "riot_api_bucket": ds_storage.StorageS3(
            root=dg.EnvVar("ENV"),
            dataset='riot_api',
            schema_name='raw',
            tables=['league_entries'],
            bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
            bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
            access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
            secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
        ),
        "youtube_bucket": ds_storage.StorageS3(
            root=dg.EnvVar("ENV"),
            dataset='youtube',
            schema_name='raw',
            tables=['audio'],
            bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
            bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
            access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
            secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
        ),
        "catalog_clean": ds_storage.StorageIceberg(
            root=dg.EnvVar("ENV"),
            dataset='riot_api',
            schema_name='clean',
            tables=['league_entries'],
            warehouse_name=dg.EnvVar("CATALOG_WAREHOUSE_NAME"),
            catalog_uri=dg.EnvVar("CATALOG_ENDPOINT"),
            token=dg.EnvVar("CATALOG_TOKEN"),
        )
    },
)