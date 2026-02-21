import boto3
from botocore.config import Config
import json
from pydantic import PrivateAttr
from .storage import Storage, NonEmptyStr


class StorageS3(Storage):
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

    def upload_json(self, data: dict, object_name: str):
        """Uploads a JSON object to R2"""
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_name,
            Body=json.dumps(data).encode("utf-8"),
            ContentType='application/json'
        )

    def upload_file(self, file_path: str, object_name: str):
        """Uploads a file to R2"""
        self.client.upload_file(file_path, self.bucket_name, object_name)

        return f"r2://{self.bucket_name}/{object_name}"
