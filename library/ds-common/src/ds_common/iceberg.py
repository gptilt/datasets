import polars as pl
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
)


ICEBERG_TO_POLARS = {
    StringType: pl.String,
    IntegerType: pl.Int32,
    LongType: pl.Int64,
    FloatType: pl.Float32,
    DoubleType: pl.Float64,
    BooleanType: pl.Boolean,
    DateType: pl.Date,
    TimestampType: pl.Datetime("us"),
}


def iceberg_to_polars_schema(iceberg_schema):
    polars_schema = {}

    for field in iceberg_schema.fields:
        iceberg_type = type(field.field_type)

        if iceberg_type not in ICEBERG_TO_POLARS:
            raise ValueError(f"Unsupported Iceberg type: {field.field_type}")

        polars_schema[field.name] = ICEBERG_TO_POLARS[iceberg_type]

    return polars_schema


def convert_polars_df_to_pyarrow_table_using_iceberg_schema(
    df: pl.DataFrame,
    iceberg_schema: Schema
):
    """
    Convert a Polars DataFrame to a PyArrow Table using the provided Iceberg schema.
    This ensures 'nullable' fields are correctly represented.
    """
    # Shared categorical buffers may cause Arrow segfaults
    pl.disable_string_cache()

    polars_schema = iceberg_to_polars_schema(iceberg_schema)
    table = (df
        # Reorder Polars DataFrame
        .select(polars_schema.keys())
        .cast(polars_schema)
        # Convert to Arrow
        .to_arrow()
    )

    # Identify which fields are strictly required in the Iceberg schema
    required_fields = {f.name for f in iceberg_schema.fields if f.required}

    # Create a new PyArrow schema with the updated `nullable` flags 
    new_schema = pa.schema([
        field.with_nullable(field.name not in required_fields)
        for field in table.schema
    ])

    # Reconstruct the PyArrow Table using the existing column arrays but with the new schema.
    # Because we use `from_arrays`, PyArrow bypasses `.cast()` entirely, eliminating segfault risks!
    return pa.Table.from_arrays(table.columns, schema=new_schema)
