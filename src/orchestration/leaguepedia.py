"""`leaguepedia` code location.

Self-contained Dagster code location for the Leaguepedia (Fandom/Cargo) jobs
and the job that publishes leaguepedia tables to HuggingFace.
"""
import dagster as dg
import ds_esports, ds_hugging_face, ds_storage
from ds_runtime import *


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
        root=ENVIRONMENT,
        dataset='leaguepedia',
        schema_name='raw',
        tables=['players', 'player_redirects', 'teams'],
        file_extension='json',
        bucket_endpoint=BUCKET_ENDPOINT,
        bucket_name=BUCKET_NAME,
        access_key_id=BUCKET_ACCESS_KEY_ID,
        secret_access_key=BUCKET_SECRET_ACCESS_KEY,
    ),
    "leaguepedia_cargo": ds_esports.CargoClient(
        username=FANDOM_USERNAME,
        password=FANDOM_PASSWORD,
    ),
    "leaguepedia_catalog_clean": ds_storage.StorageIceberg(
        root=ENVIRONMENT,
        dataset='leaguepedia',
        schema_name='clean',
        tables=list(ds_esports.SCHEMATA.keys()),
        warehouse_name=CATALOG_WAREHOUSE_NAME,
        catalog_uri=CATALOG_ENDPOINT,
        rest_signing_region=AWS_REGION,
    ),
    "hugging_face_hub": ds_hugging_face.HuggingFaceHub(
        token=HUGGING_FACE_TOKEN,
    ),
}


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules),
    jobs=jobs,
    schedules=schedules,
    resources=resources,
)
