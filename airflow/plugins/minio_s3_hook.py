from airflow.hooks.base import BaseHook
import boto3

class MinioS3Hook(BaseHook):
    def __init__(self, conn_id="minio_default"):
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        # In a real scenario, fetch conn from Airflow connections
        return boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin"
        )
