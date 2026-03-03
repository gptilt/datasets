import boto3
from botocore.config import Config
import json
from pathlib import Path
from pydantic import PrivateAttr
from .storage_base import NonEmptyStr
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

    def get_object(
        self,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ) -> dict | None:
        """
        Gets an object from S3.
        """
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=str(self.object_path(table_name, object_name, **partition_columns))
            )
            return json.loads(response['Body'].read())
        except self.client.exceptions.NoSuchKey:
            return None

    def upload_json(
        self,
        data: dict,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ):
        """Uploads a JSON object to R2"""

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, **partition_columns)),
            Body=json.dumps(data).encode("utf-8"),
            ContentType='application/json'
        )

    def upload_file(
        self,
        file_path: str,
        table_name: str,
        object_name: str,
        **partition_columns: dict[str, str] | None
    ):
        """Uploads a file to R2"""
        self.client.upload_file(
            Filename=file_path,
            Bucket=self.bucket_name,
            Key=str(self.object_path(table_name, object_name, **partition_columns))
        )