"""
Iceberg schemas for the `document` dataset's clean layer.

`source_registry` is the spine: one row per ingested source document,
written by many producers, each upserting on `source_id`.
Grounding (see `ds_esports` resolver) stamps `match_id` + `patch`
when a source is about a specific pro game;
un-grounded sources still register.

Scalars are strings, including timestamps (ISO 8601),
to avoid Arrow timezone friction on upsert.
"""
from ds_storage import IcebergTableSpec
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType


SCHEMATA = {
    # One row per source document, keyed by `source_id` so any producer can upsert.
    'source_registry': IcebergTableSpec(
        schema=Schema(
            NestedField(1, 'source_id', StringType(), required=True),
            NestedField(2, 'origin_uri', StringType(), required=False),
            # transcript | article | forum | guide — drives reliability + privacy.
            NestedField(3, 'source_type', StringType(), required=False),
            # Source platform (youtube, reddit, …) — provenance, distinct from source_type.
            NestedField(4, 'platform', StringType(), required=False),
            # Pre-anonymization author; the privacy stage pseudonymizes it downstream.
            NestedField(5, 'author_name', StringType(), required=False),
            NestedField(6, 'title', StringType(), required=False),
            NestedField(7, 'published_at', StringType(), required=False),
            NestedField(8, 'ingested_at', StringType(), required=False),
            NestedField(9, 'raw_hash', StringType(), required=False),
            NestedField(10, 'reliability_tier', StringType(), required=False),
            NestedField(11, 'privacy_risk_level', StringType(), required=False),
            # ingested | grounded | ungrounded — coarse pipeline state.
            NestedField(12, 'processing_status', StringType(), required=False),
            # Grounding payload: the series this source discusses, and its patch-of-record.
            # Null when the source could not be grounded to a match.
            NestedField(13, 'match_id', StringType(), required=False),
            NestedField(14, 'patch', StringType(), required=False),
            # Weak in-text patch fallback for un-grounded sources (publish-date is NOT used).
            NestedField(15, 'patch_hint', StringType(), required=False),
            identifier_field_ids=[1],
        ),
    ),
}
