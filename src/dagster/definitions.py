import dagster as dg
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud
from src import riot_api
from src import storage


defs = dg.Definitions(
    assets=link_code_references_to_git_if_cloud(assets_defs=dg.with_source_code_references(
        dg.load_assets_from_modules([riot_api])
    )),
    jobs=[
        riot_api.job_riot_api_league_entries,
        riot_api.job_riot_api_player_matches,
    ],
    schedules=[
        riot_api.schedule_riot_api_league_entries,
        riot_api.schedule_riot_api_player_matches
    ],
    resources={
        "youtube_bucket": storage.StorageS3(
            root='gptilt',
            schema_name='raw',
            dataset='riot_api',
            tables=['league_entries'],
            bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
            bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
            access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY"),
            secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_KEY"),
        ),
        "riot_api_bucket": storage.StorageS3(
            root='gptilt',
            schema_name='raw',
            dataset='youtube',
            tables=['audio'],
            bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
            bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
            access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY"),
            secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_KEY"),
        ),
        "catalog_clean": storage.StorageIceberg(
            root='gptilt',
            schema_name='raw',
            dataset='youtube',
            tables=['audio'],
            endpoint=dg.EnvVar("CATALOG_CLEAN_ENDPOINT"),
            access_key=dg.EnvVar("CATALOG_CLEAN_ACCESS_KEY"),
            secret_key=dg.EnvVar("CATALOG_CLEAN_SECRET_KEY"),
            catalog_name=dg.EnvVar("CATALOG_CLEAN_NAME")
        )
    },
)