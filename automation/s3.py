import boto3
import logging


class S3:

    def __init__(self) -> None:
        self.client = boto3.resource('s3')

    def get_parquets_to_sync(self, bucket, archive_path, task_obj):
        s3_bucket = self.client.Bucket(bucket)
        parquets = []
        prefix = f"{archive_path}/{task_obj['ExportTaskIdentifier']}/"
        logging.info(f"Search prefix for parquets is {prefix}")
        for obj in s3_bucket.objects.filter(Prefix=prefix):
            if obj.key.endswith('.parquet'):
                parquet_path = f's3://{bucket}/{obj.key}'
                parquets.append(parquet_path)
        return parquets

    def get_jsons_to_delete(self, bucket, archive_path, task_obj):
        s3_bucket = self.client.Bucket(bucket)
        jsons = []
        prefix = f"{archive_path}/{task_obj['ExportTaskIdentifier']}/"
        logging.info(f"Search prefix for parquets is {prefix}")
        for obj in s3_bucket.objects.filter(Prefix=prefix):
            if obj.key.endswith('.json'):
                jsons.append(obj.key)
        return jsons

    def delete_object(self, bucket, key):
        logging.info(f"Cleaning object file from s3 {key}")
        self.client.Object(bucket, key).delete()
