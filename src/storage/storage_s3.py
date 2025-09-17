import boto3
import json
from .storage import Storage


class StorageS3(Storage):
    def __init__(
        self,
        root,
        schema,
        dataset,
        tables,
        bucket_name: str,
        bucket_url: str,
        bucket_access_key: str,
        bucket_secret_key: str,
        file_extension = 'json',
    ):
        super().__init__(root, schema, dataset, tables, file_extension)
        
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=bucket_url,
            aws_access_key_id=bucket_access_key,
            aws_secret_access_key=bucket_secret_key,
        )
        

    def write(self, filename: str, contents: any):
        # Convert data -> JSON string -> bytes
        json_bytes = json.dumps(contents).encode("utf-8")

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=filename,  # path/key inside the bucket
            Body=json_bytes,
            ContentType="application/json"  # optional, helps when downloading
        )
