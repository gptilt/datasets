"""
The clean layer is overwrite-only — these schemas describe the shape of the *latest
snapshot*, not historical state. SCD-Type-2 history (with `valid_from` / `valid_to`)
will live in the future `curated` layer.

Identifier fields are still declared so downstream readers can recognise primary keys,
but they are not used by the writer (overwrite ignores merge keys).
"""
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    NestedField,
    StringType,
)


SCHEMATA = {
    'public_figures': {
        'schema': Schema(
            NestedField(1, 'person_id', StringType(), required=True),
            NestedField(2, 'canonical_name', StringType(), required=True),
            NestedField(3, 'display_name', StringType(), required=True),
            # Roles and regions are list-typed in the plan, but Leaguepedia exposes
            # them as scalar strings (a player has one primary role, one nationality
            # primary). We keep them as strings here and fan out in `curated` when we
            # need lifetime role history.
            NestedField(4, 'role', StringType(), required=False),
            NestedField(5, 'region', StringType(), required=False),
            NestedField(6, 'active_from', DateType(), required=False),
            NestedField(7, 'active_to', DateType(), required=False),
            NestedField(8, 'is_retired', StringType(), required=False),
            NestedField(9, 'source', StringType(), required=True),
            NestedField(10, 'source_url', StringType(), required=True),
            identifier_field_ids=[1],
        ),
        # No partition spec: the table is small (tens of thousands of rows) and is
        # overwritten in full each refresh — partitions would just add overhead.
        'partition_spec': None,
        'sort_order': None,
    },
    'person_aliases': {
        'schema': Schema(
            NestedField(1, 'alias', StringType(), required=True),
            NestedField(2, 'person_id', StringType(), required=True),
            NestedField(3, 'alias_type', StringType(), required=True),
            # Composite PK: a person has many aliases, but each (person, alias) pair
            # is unique within a snapshot.
            identifier_field_ids=[1, 2],
        ),
        'partition_spec': None,
        'sort_order': None,
    },
    'teams': {
        'schema': Schema(
            NestedField(1, 'team_id', StringType(), required=True),
            NestedField(2, 'canonical_name', StringType(), required=True),
            NestedField(3, 'long_name', StringType(), required=False),
            NestedField(4, 'region', StringType(), required=False),
            NestedField(5, 'location', StringType(), required=False),
            NestedField(6, 'active_from', DateType(), required=False),
            NestedField(7, 'active_to', DateType(), required=False),
            NestedField(8, 'is_disbanded', StringType(), required=False),
            NestedField(9, 'source', StringType(), required=True),
            NestedField(10, 'source_url', StringType(), required=True),
            identifier_field_ids=[1],
        ),
        'partition_spec': None,
        'sort_order': None,
    },
    'team_aliases': {
        'schema': Schema(
            NestedField(1, 'alias', StringType(), required=True),
            NestedField(2, 'team_id', StringType(), required=True),
            NestedField(3, 'alias_type', StringType(), required=True),
            identifier_field_ids=[1, 2],
        ),
        'partition_spec': None,
        'sort_order': None,
    },
}
