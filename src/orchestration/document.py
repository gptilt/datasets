"""
`document` code location.

The enriched document store. Reads staged transcripts (produced by the `chatbot`
location) plus the Leaguepedia clean catalog, and builds `document.clean.source_registry`
— grounding each source to the pro match it discusses on the way in.
"""
import dagster as dg
import ds_storage
from ds_documents import sources
from ds_models import LocalLLM
from ds_platform import *


resources = {
    # Staged transcripts to ingest (written by the chatbot location).
    "document_bucket": ds_storage.StorageS3(
        root=ENVIRONMENT,
        dataset='document',
        schema_name='stg',
        tables=['transcripts'],
        file_extension='parquet',
        bucket_endpoint=BUCKET_ENDPOINT,
        bucket_name=BUCKET_NAME,
        access_key_id=BUCKET_ACCESS_KEY_ID,
        secret_access_key=BUCKET_SECRET_ACCESS_KEY,
    ),
    # The document clean layer — where source_registry is upserted.
    "document_catalog_clean": ds_storage.StorageIceberg(
        root=ENVIRONMENT,
        dataset='document',
        schema_name='clean',
        tables=['source_registry'],
        warehouse_name=CATALOG_WAREHOUSE_NAME,
        catalog_uri=CATALOG_ENDPOINT,
        rest_signing_region=AWS_REGION,
    ),
    # Leaguepedia clean tables read for grounding (matches + games + aliases).
    "leaguepedia_catalog_clean": ds_storage.StorageIceberg(
        root=ENVIRONMENT,
        dataset='leaguepedia',
        schema_name='clean',
        tables=['matches', 'games', 'entity_aliases'],
        warehouse_name=CATALOG_WAREHOUSE_NAME,
        catalog_uri=CATALOG_ENDPOINT,
        rest_signing_region=AWS_REGION,
    ),
    "local_llm": LocalLLM(max_new_tokens=120),
}


defs = dg.Definitions(
    assets=dg.load_assets_from_modules([sources]),
    resources=resources,
)
