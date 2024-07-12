import boto3
import botocore.exceptions
import botocore
import time 
import logging
from env import retention_in_weeks

class RDS:

    def __init__(self) -> None:
        self.client = boto3.client('rds')

    # Describe db instance 
    def get_db_instance(self, instance_name):
        try:
            response = self.client.describe_db_instances(
                DBInstanceIdentifier=instance_name
            )
            return response['DBInstances'][0]
        except Exception as e:
            logging.error(e)

    # Describe db cluster 
    def get_db_cluster(self, instance_name):
        try:
            response = self.client.describe_db_clusters(
                DBClusterIdentifier=instance_name
            )
            return response['DBClusters'][0]
        except Exception as e:
            logging.error(e)

    # Describe db instance snapshot
    def describe_db_snapshot(self, snapshot_id):
        try:
            response = self.client.describe_db_snapshots(
                DBSnapshotIdentifier=snapshot_id
            )
            return response['DBSnapshots'][0]
        except Exception as e:
            logging.error(e)

    # Describe db cluster snapshot
    def describe_db_cluster_snapshot(self, snapshot_id):
        try:
            response = self.client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=snapshot_id
            )
            return response['DBClusterSnapshots'][0]
        except Exception as e:
            logging.error(e)

    #Describe export task for the table
    def describe_export_task(self, task_identifier):
        try:
            response = self.client.describe_export_tasks(
                ExportTaskIdentifier=task_identifier
            )
            return response['ExportTasks'][0]
        except Exception as e:
            logging.error(e)
    
    # Create db snapshot for export
    def create_db_snapshot(self, instance_id, snapshot_id):
        logging.info(f"Starting to create a new snapshot {snapshot_id}")
        self.waiter_db_instance(instance_id)
        if self.describe_db_snapshot(snapshot_id):
            logging.info("Skipping this step the snapshot is already available for processsing")
            self.waiter_db_instance_snapshot(snapshot_id)
            return self.describe_db_snapshot(snapshot_id)
        response = self.client.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=instance_id,
            Tags=[{'Key':'user:retention-weeks',
                   'Value': str(retention_in_weeks)}]
        )
        self.waiter_db_instance_snapshot(snapshot_id)
        return self.describe_db_snapshot(snapshot_id)
 
    # Create db snapshot for export
    def create_db_cluster_snapshot(self, instance_id, snapshot_id):
        self.waiter_db_cluster(instance_id)
        if self.describe_db_cluster_snapshot(snapshot_id):
            logging.info("Skipping this step the snapshot is already available for processsing")
            self.waiter_db_cluster_snapshot(snapshot_id)
            return self.describe_db_cluster_snapshot(snapshot_id)
        response = self.client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snapshot_id,
            DBClusterIdentifier=instance_id,
            Tags=[{'Key':'user:retention-weeks',
                   'Value': str(retention_in_weeks)}]
        )
        self.waiter_db_cluster_snapshot(snapshot_id)
        return self.describe_db_cluster_snapshot(snapshot_id)

    # Waiter funciton to wait for db cluster
    def waiter_db_cluster(self, cluster_id):
        while self.get_db_cluster(cluster_id)['Status'] != 'available':
            logging.info(f"waiting for {cluster_id} to become available")
            time.sleep(5)

    # Waiter funciton to wait for db instance
    def waiter_db_instance(self, instance_id):
        while self.get_db_instance(instance_id)['DBInstanceStatus'] != 'available':
            logging.info(f"waiting for {instance_id} to become available")
            time.sleep(5)

    # Waiter funciton to wait for db cluster snapshot
    def waiter_db_cluster_snapshot(self, snapshot_id):
        while self.describe_db_cluster_snapshot(snapshot_id)['Status'] != 'available':
            logging.info(f"waiting for the snapshot {snapshot_id} to become available")
            time.sleep(5)
    
    # Waiter funciton to wait for db instanct snapshot
    def waiter_db_instance_snapshot(self, snapshot_id):
        while self.describe_db_snapshot(snapshot_id)['Status'] != 'available':
            logging.info(f"waiting for the snapshot {snapshot_id} to become available")
            time.sleep(5)

    # Waiter funciton for export task
    def waiter_export_task(self, task_name):
        while self.describe_export_task(task_name) and self.describe_export_task(task_name)['Status'] != 'COMPLETE':
            logging.info(f"waiting for the export task to complete for {task_name}")
            time.sleep(5)

    # Export table to S3 path for deep archive
    def export_snapshot(self, exchange_table_name, snapshot_arn, config, partition_name, role, kms):
        bucket_path = config['s3_archive_path']
        bucket_name = config['s3_bucket']
        table_name = config['table_name']
        env = config['env']
        country = config['country']
        brand = config['brand']

        partition_name = partition_name.replace("_", "").strip()
        table_name_postfix = table_name.replace("_", "-")
        partition_name = f"p{partition_name}" if partition_name[0].isdigit() else partition_name
        partition_name = f"{env}-{country}-{brand}-{partition_name}"

        task_id = f"{partition_name}-{table_name_postfix}"
        task_id = task_id[:60] if len(task_id) > 60 else task_id
        task_id = task_id[:-1] if task_id.endswith('-') else task_id
        
        logging.info(f"Exporting data to the s3 for the table {exchange_table_name} with task id {task_id}")
        res = self.describe_export_task(task_id)
        if res:
            self.waiter_export_task(task_id)
            logging.info("Export is already completed, skipping this step")
            return res
        
        return self.start_task(task_id, snapshot_arn, bucket_name, role, 
                               kms, bucket_path, exchange_table_name)

    # Start task classified in the export_snapshot function
    def start_task(self, task_id, snapshot_arn, bucket_name, role, kms, bucket_path, exchange_table_name):
        try:
            response = self.client.start_export_task(
                    ExportTaskIdentifier=task_id.strip(),
                    SourceArn=snapshot_arn,
                    S3BucketName=bucket_name,
                    IamRoleArn=role,
                    KmsKeyId=kms,
                    S3Prefix=bucket_path[1:],
                    ExportOnly=[exchange_table_name]
            )
            self.waiter_export_task(task_id)
            return self.describe_export_task(task_id)
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == 'ExportTaskLimitReachedFault':
                logging.info("The concurrency level has been achieved, will retry in 120 seconds again")
                time.sleep(120)
                return self.start_task(self, task_id, snapshot_arn, bucket_name, role, 
                               kms, bucket_path, exchange_table_name)
            else:
                raise err
