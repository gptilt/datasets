import polars as pl
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