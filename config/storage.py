import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv(
    os.path.join(PROJECT_ROOT, "infrastructure", "docker", ".env")
)


class StorageClient:

    def __init__(self):

        self.client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            region_name=os.getenv("S3_REGION", "us-east-1"),
            config=Config(signature_version="s3v4")
        )

        self.buckets = {
            "bronze": os.getenv("BRONZE_BUCKET"),
            "silver": os.getenv("SILVER_BUCKET"),
            "gold": os.getenv("GOLD_BUCKET")
        }

    def list_buckets(self):

        response = self.client.list_buckets()

        return [
            bucket["Name"]
            for bucket in response["Buckets"]
        ]

    def upload_file(self, file_path, bucket_name, object_key):

        self.client.upload_file(
            file_path,
            bucket_name,
            object_key
        )

        print(f"Uploaded {file_path} to s3://{bucket_name}/{object_key}")

    def download_file(self, bucket_name, object_key, destination):

        self.client.download_file(
            bucket_name,
            object_key,
            destination
        )

        print(f"Downloaded s3://{bucket_name}/{object_key}")

    def list_objects(self, bucket_name, prefix=""):

        response = self.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )

        return [
            obj["Key"]
            for obj in response.get("Contents", [])
        ]

    def delete_object(self, bucket_name, object_key):

        self.client.delete_object(
            Bucket=bucket_name,
            Key=object_key
        )

        print(f"Deleted s3://{bucket_name}/{object_key}")