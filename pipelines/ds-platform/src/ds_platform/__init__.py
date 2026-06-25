"""
Shared deployment layer: environment config + orchestration scaffolding
(the weekly partition scheme and the backfill policy) that every pipeline builds on.
"""
import dagster as dg

from .backfills import no_backfills
from .partitions import partition_kwargs, partition_per_week

ENVIRONMENT = dg.EnvVar("ENVIRONMENT")

BUCKET_ENDPOINT = dg.EnvVar("S3_BUCKET_ENDPOINT")
BUCKET_NAME = dg.EnvVar("S3_BUCKET_NAME")
BUCKET_ACCESS_KEY_ID = dg.EnvVar("S3_BUCKET_ACCESS_KEY_ID")
BUCKET_SECRET_ACCESS_KEY = dg.EnvVar("S3_BUCKET_SECRET_ACCESS_KEY")

AWS_REGION = dg.EnvVar("AWS_REGION")
CATALOG_ENDPOINT = dg.EnvVar("CATALOG_ENDPOINT")
CATALOG_WAREHOUSE_NAME = dg.EnvVar("CATALOG_WAREHOUSE_NAME")

FANDOM_USERNAME = dg.EnvVar("FANDOM_USERNAME")
FANDOM_PASSWORD = dg.EnvVar("FANDOM_PASSWORD")

HUGGING_FACE_TOKEN = dg.EnvVar("HUGGING_FACE_TOKEN")
