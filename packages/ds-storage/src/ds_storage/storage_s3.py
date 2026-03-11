import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import fnmatch
import io
import json
from pathlib import Path
import polars as pl
from pydantic import PrivateAttr
from .storage_base import NonEmptyStr, RecordNotFoundError
from .storage_file import StorageFile


class StorageS3(StorageFile):
    bucket_endpoint: NonEmptyStr
    bucket_name: NonEmptyStr
    access_key_id: NonEmptyStr
    secret_access_key: NonEmptyStr

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
        **partition_columns: dict[str, str] | None
    ) -> Path:
        """
        Returns the fully qualified file path for the given table and object name.
        """
        return Path(
            self.partition_path(table_name, **partition_columns),
            f"{object_name}.{self.file_extension}"
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

    def get_object_as_dataframe(
        self,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ) -> pl.DataFrame | None:
        """
        Gets an object from S3 as a Polars DataFrame.
        """
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, **partition_columns))
        )

        match self.file_extension:
            case 'json':
                return pl.read_json(response['Body'].read())
            case 'parquet':
                return pl.read_parquet(response["Body"].read())
            case _:
                raise ValueError(f"Unsupported file extension: {self.file_extension}")

    def upload_file(
        self,
        file_path: str,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ):
        """Uploads a file to S3"""
        self.client.upload_file(
            Filename=file_path,
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, **partition_columns))
        )
    
    def upload(
        self,
        data: pl.DataFrame | list[dict],
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ):
        assert isinstance(data, (pl.DataFrame, list)), "Data must be a DataFrame or a list of records"

        object_key = str(self.object_path(table_name, object_name, **partition_columns))

        match self.file_extension:
            case 'json':
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=json.dumps(data).encode("utf-8"),
                    ContentType='application/json'
                )
            case 'parquet':
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
                raise ValueError(f"Uploads aren't supported for file extension: '{self.file_extension}'")