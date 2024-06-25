from collections.abc import Generator
from contextlib import closing
from flask import Flask
from botocore.client import Config
from botocore.exceptions import ClientError
from extensions.storage.base_storage import BaseStorage
import boto3


class OCIStorage(BaseStorage):
    def __init__(self, app: Flask):
        super().__init__(app)
        app_config = self.app.config
        self.bucket_name = app_config.get('OCI_BUCKET_NAME')
        if not self.bucket_name:
            raise ValueError("Bucket name is not configured")
        self.client = boto3.client(
                    's3',
                    aws_secret_access_key=app_config.get('OCI_SECRET_KEY'),
                    aws_access_key_id=app_config.get('OCI_ACCESS_KEY'),
                    endpoint_url=app_config.get('OCI_ENDPOINT'),
                    region_name=app_config.get('OCI_REGION')
                )

    def save(self, filename, data):
        self.client.put_object(Bucket=self.bucket_name, Key=filename, Body=data)

    def load_once(self, filename: str) -> bytes:
        try:
            data = self.client.get_object(Bucket=self.bucket_name, Key=filename)['Body'].read()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError("File not found")
            else:
                raise
        return data

    def load_stream(self, filename: str) -> Generator[bytes, None, None]:
        def generate(filename: str = filename) -> Generator[bytes, None, None]:
            try:
                response = self.client.get_object(Bucket=self.bucket_name, Key=filename)
                yield from response['Body'].iter_chunks()
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    raise FileNotFoundError("File not found")
                else:
                    raise
        return generate()

    def download(self, filename, target_filepath):
        self.client.download_file(self.bucket_name, filename, target_filepath)

    def exists(self, filename) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=filename)
            return True
        except ClientError as ex:
            if ex.response['Error']['Code'] == '404':
                return False
            else:
                raise

    def delete(self, filename):
        self.client.delete_object(Bucket=self.bucket_name, Key=filename)