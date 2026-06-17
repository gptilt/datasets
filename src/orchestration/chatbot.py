"""
`chatbot` code location.

`ds_chatbot` is a private git submodule, so its import is guarded:
in public-only checkouts (`make init`) the location still loads
with just `document_bucket` and no assets/jobs;
with the submodule present it loads the full chatbot definitions.
"""
import dagster as dg
from ds_runtime import DEPLOYMENT_NAME
import ds_storage


modules = []
jobs = []
schedules = []
sensors = []
resources = {
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
}

try:
    import ds_chatbot

    modules.append(ds_chatbot)
    jobs.extend(ds_chatbot.jobs)
    schedules.extend(ds_chatbot.schedules)
    sensors.extend(ds_chatbot.sensors)
    resources.update(ds_chatbot.resources)
except ImportError:
    pass


defs = dg.Definitions(
    assets=dg.load_assets_from_modules(modules) if modules else [],
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
    resources=resources,
)
