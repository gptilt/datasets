import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
import fnmatch
import io
import json
import os
from pathlib import Path
import polars as pl
from pydantic import PrivateAttr
from .storage_base import NonEmptyStr, RecordNotFoundError, Storage


class StorageS3(Storage):
    bucket_endpoint: NonEmptyStr
    bucket_name: NonEmptyStr
    access_key_id: NonEmptyStr
    secret_access_key: NonEmptyStr
    file_extension: str = 'json'

    # Declare a private attribute that Pydantic/Dagster ignores during serialization
    _client: object = PrivateAttr(default=None)

    @property
    def client(self):
        """
        Lazy-loads the client.
        If it exists in memory, return it.
        If not, create it.
        """
        if self._client is None:
            self._client = boto3.client(
                's3',
                endpoint_url=self.bucket_endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                config=Config(signature_version='s3v4')
            )
        return self._client

    def object_path(
        self,
        table_name: str,
        object_name: str,
        file_extension: str = None,
        **partition_columns: dict[str, str] | None
    ) -> Path:
        """
        Returns the fully qualified file path for the given table and object name.
        """
        return Path(
            self.partition_path(table_name, **partition_columns),
            f"{object_name}.{file_extension or self.file_extension}"
        )
    
    def object_exists(
        self,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ) -> str:
        """
        Performs a HEAD request to check if the object exists in S3.
        Returns the object key if it exists, otherwise raises RecordNotFoundError.
        """
        key = str(self.object_path(table_name, object_name, **partition_columns))
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return key
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise RecordNotFoundError(f"Object {object_name} not found in S3.")
            raise e

    def list_objects(
        self,
        table_name: str,
        object_name: str = "*",
        **partition_columns: dict[str, str] | None
    ) -> list[str]:
        """
        Returns a flat list of S3 object keys matching the table, object_name (wildcard supported),
        and any provided partitions. Omitted nested partitions are automatically traversed.
        """
        # Get the base directory path based ONLY on the provided partitions
        # We don't use object_path here because omitted partitions would break the S3 prefix.
        prefix = str(self.partition_path(table_name, **partition_columns))
        
        # Ensure exact partition directory matching (e.g., 'server=1/' and not 'server=10/')
        if not prefix.endswith('/'):
            prefix += '/'

        # Query S3 for objects matching the prefix
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        # Create our filename wildcard pattern (e.g., "*.parquet" or "2026-03-08.parquet")
        file_pattern = f"{object_name}.{self.file_extension}"

        results = []

        # Iterate over S3 results and apply the wildcard filter to the filename
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                
                # Extract just the filename at the end of the S3 Key
                filename = key.split('/')[-1]
                
                # If it matches our wildcard/exact object name, add it to the flat list
                if fnmatch.fnmatch(filename, file_pattern):
                    results.append(key)

        return results


    def get_object_as_json(
        self,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ) -> dict | list:
        """Gets a JSON object from S3 and returns the parsed Python value."""
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, 'json', **partition_columns))
        )
        return json.loads(response['Body'].read())

    def get_object_as_dataframe(
        self,
        table_name: str,
        object_name: str,
        file_extension: str = None,
        **partition_columns: dict[str, str] | None
    ) -> pl.DataFrame | None:
        """
        Gets an object from S3 as a Polars DataFrame.
        """
        ext = file_extension or self.file_extension
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, file_extension, **partition_columns))
        )

        match ext:
            case 'json':
                return pl.read_json(response['Body'].read())
            case 'parquet':
                return pl.read_parquet(response["Body"].read())
            case _:
                raise ValueError(f"Unsupported file extension: {ext}")

    def download_file(
        self,
        target_file_path: str,
        table_name: str,
        object_name: str,
        file_extension: str = None,
        **partition_columns: dict[str, str] | None
    ):
        """Downloads a file from S3"""
        self.client.download_file(
            Filename=target_file_path,
            Bucket=self.bucket_name,
            Key=str(self.object_path(
                table_name,
                object_name,
                file_extension,
                **partition_columns
            ))
        )

    def download_all_files_in_partition(
        self,
        target_local_directory: str,
        table_name: str,
        object_name: str = "*",
        **partition_columns: dict[str, str]
    ) -> list[str]:
        """
        Downloads all files from a given partition into a local directory.
        
        Returns a list of local file paths.
        """
        os.makedirs(target_local_directory, exist_ok=True)

        # List all files in given partition
        keys = self.list_objects(
            table_name=table_name,
            object_name=object_name,
            **partition_columns
        )

        downloaded_files = []

        for key in keys:
            filename = Path(key).name
            local_path = os.path.join(target_local_directory, filename)

            self.client.download_file(
                Bucket=self.bucket_name,
                Key=key,
                Filename=local_path
            )

            downloaded_files.append(local_path)

        return downloaded_files

    def upload_file(
        self,
        source_file_path: str,
        table_name: str,
        object_name: str,
        file_extension: str = None,
        **partition_columns: dict[str, str] | None
    ):
        """Uploads a file to S3"""
        self.client.upload_file(
            Filename=source_file_path,
            Bucket=self.bucket_name,
            Key=str(self.object_path(
                table_name,
                object_name,
                file_extension,
                **partition_columns
            ))
        )

    def s3_uri(
        self,
        table_name: str | None = None,
        object_name: str | None = None,
        file_extension: str | None = None,
        **partition_columns: dict[str, str] | None
    ) -> str:
        """
        Build an `s3://bucket/...` URI for use in DuckDB / external queries.

        - With no args, returns the bucket root.
        - With `table_name`, returns the table or partition prefix.
        - With `object_name`, returns the fully qualified object URI.
        """
        if table_name is None:
            return f"s3://{self.bucket_name}"

        if object_name is None:
            base = self.partition_path(table_name, **partition_columns)
            return f"s3://{self.bucket_name}/{base}"

        return f"s3://{self.bucket_name}/{self.object_path(table_name, object_name, file_extension, **partition_columns)}"

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Returns a DuckDB connection pre-configured with httpfs and S3 credentials
        sourced from this resource. Use `self.s3_uri(...)` to build paths and
        query with `read_parquet` / `read_json` / `glob`.
        """
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = self.bucket_endpoint.removeprefix('https://').removeprefix('http://')
        con.execute(
            f"""
            CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{self.access_key_id}',
                SECRET '{self.secret_access_key}',
                ENDPOINT '{endpoint}',
                URL_STYLE 'path',
                REGION 'auto'
            )
            """
        )
        return con

    def upload(
        self,
        data: pl.DataFrame | list[dict],
        table_name: str,
        object_name: str,
        file_extension: str = None,
        **partition_columns: dict[str, str] | None
    ):
        file_extension = file_extension or self.file_extension
        
        object_key = str(self.object_path(
            table_name=table_name,
            object_name=object_name,
            file_extension=file_extension,
            **partition_columns
        ))

        match file_extension:
            case 'json':
                assert isinstance(data, list), "Data must be a list of records for JSON uploads"

                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=json.dumps(data).encode("utf-8"),
                    ContentType='application/json'
                )
            case 'parquet':
                assert isinstance(data, pl.DataFrame), "Data must be a DataFrame for parquet uploads"

                buffer = io.BytesIO()
                data.write_parquet(buffer)
                buffer.seek(0)

                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=buffer,
                    ContentType='application/octet-stream'
                )
            case _:
                raise ValueError(f"Uploads aren't supported for file extension: '{file_extension}'")