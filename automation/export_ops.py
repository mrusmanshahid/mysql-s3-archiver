from rds import RDS
from env import execution_role, kms_key
import logging

class ExportOps:

    def __init__(self) -> None:
        self.rds_operations = RDS()
        self.role = execution_role
        self.kms = kms_key

    # This function returns the name of the snapshot to created for the table
    def get_snapshot_name(self, config, month):    
        tbl = config['table_name'].replace("_", "-")
        db = config['database_name'].replace("_", "-")
        month = month.replace("_", "")
        return f"{config['identifier']}-{db}-{tbl}-{month}".strip()


    # Export process to export the exchanged partitioned table to s3 and append in path
    # This is for rds instance
    def export_partial_rds(self, config, exchanged_table, partition_name):
        logging.info(f"Starting export process for exchange_table {exchanged_table} in partition {partition_name}")
        exchange_table_name = f"{config['database_name']}.{exchanged_table}"
        snapshot_name = self.get_snapshot_name(config,partition_name)
        snapshot = self.rds_operations.create_db_snapshot(config['identifier'],snapshot_name)
        return self.rds_operations.export_snapshot(exchange_table_name, 
                                            snapshot['DBSnapshotArn'], 
                                            config,
                                            partition_name,
                                            self.role,
                                            self.kms)


    # Export process to export the exchanged partitioned table to s3 and append in path
    # This is for aurora cluster
    def export_partial_cluster(self, config, instance, exchanged_table, partition_name):
        logging.info(f"Starting export process for exchange_table {exchanged_table} in partition {partition_name}")
        exchanged_table_name = f"{config['database_name']}.{exchanged_table}"
        return self.rds_operations.export_snapshot(exchanged_table_name,
                                            instance['arn'], 
                                            config,
                                            partition_name,
                                            self.role,
                                            self.kms)
    

    # Export process to export the exchanged partitioned table to s3 and append in path
    def export_partial_to_s3(self, config, instance, exchange_table, partition_name):
        if config['is_cluster']:
            return self.export_partial_cluster(config, instance, exchange_table, partition_name)
        return self.export_partial_rds(config, exchange_table, partition_name)
