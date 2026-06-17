"""`leaguepedia` code location.

Self-contained Dagster code location for the Leaguepedia (Fandom/Cargo) jobs
and the job that publishes leaguepedia tables to HuggingFace.
"""
import dagster as dg
from ds_runtime import DEPLOYMENT_NAME
import ds_esports, ds_hugging_face, ds_storage


modules = [ds_esports, ds_hugging_face]
jobs = [
    ds_esports.job_esports_phase_1,
    ds_hugging_face.job_publish_esports,
]
schedules = [
    ds_esports.schedule_esports_phase_1,
    ds_hugging_face.schedule_publish_esports,
]
resources = {
    "leaguepedia_bucket": ds_storage.StorageS3(
        root=DEPLOYMENT_NAME,
        dataset='leaguepedia',
        schema_name='raw',
        tables=['players', 'player_redirects', 'teams'],
        file_extension='json',
        bucket_endpoint=dg.EnvVar("S3_BUCKET_ENDPOINT"),
        bucket_name=dg.EnvVar("S3_BUCKET_NAME"),
        access_key_id=dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY"),
    ),
    "leaguepedia_cargo": ds_esports.CargoClient(
        username=dg.EnvVar("FANDOM_USERNAME"),
        password=dg.EnvVar("FANDOM_PASSWORD"),
    ),
    "leaguepedia_catalog_clean": ds_storage.StorageIceberg(
        root=DEPLOYMENT_NAME,
        dataset='leaguepedia',
        schema_name='clean',
        tables=list(ds_esports.SCHEMATA.keys()),
        warehouse_name=dg.EnvVar("CATALOG_WAREHOUSE_NAME"),
        catalog_uri=dg.EnvVar("CATALOG_ENDPOINT"),
        rest_signing_region=dg.EnvVar("AWS_REGION"),
    ),
    "hugging_face_hub": ds_hugging_face.HuggingFaceHub(
        token=dg.EnvVar("HUGGING_FACE_TOKEN"),
    ),
}


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules),
    jobs=jobs,
    schedules=schedules,
    resources=resources,
)
