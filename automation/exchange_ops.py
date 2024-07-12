import logging
from sql import SQL
from credentials import Credentials
from database import Database
from s3 import S3

class ExchangeOps:

    def __init__(self) -> None:
        self.sql = SQL()
        self.db = Database()
        self.s3_ops = S3()
        self.credentials = Credentials

    def get_connection(self, instance):
        return self.db.establish_connection(instance['host'], self.credentials.mysql_user, self.credentials.mysql_password, instance['port'])

    def prepare_control_table(self, con, database):
        sql = self.sql.get_control_table_sql(database)
        logging.info(f"Creating control table at the database.")
        self.db.execute(con, sql)

    def get_partitioned_to_archive(self, con, table_name, table_schema, partition_descriptor, method):
        sql = self.sql.get_partition_for_month(table_name, table_schema, partition_descriptor, method)
        logging.info(f"Getting partition that needs to be archived for the table {table_name}.")
        return self.db.execute(con, sql)

    def prepare_exchange_table(self, con, config, exchange_table, table_name):
        sql = self.sql.get_exchange_table_sql(config['database_name'], exchange_table, table_name)
        logging.info(f"Creating exchange table with type {table_name}.")
        self.db.execute(con, sql)
        
    def remove_exchange_table_partitions(self, con, config, exchange_table):
        logging.info(f"Removing partitions of exchange table")
        sql = self.sql.get_remove_partitions_sql(config['database_name'], exchange_table)
        self.db.execute(con, sql)

    def swap_partition_table(self, con, config, partition, exchange_table):
        logging.info(f"Exchanging Partitions")
        sql = self.sql.get_exchange_partition_sql(config['database_name'], config['table_name'], partition, exchange_table)
        self.db.execute(con, sql)

    def check_exchange_table(self, con, exchange_table):
        sql = self.sql.get_check_exchange_table_sql(exchange_table)
        return self.db.execute(con, sql)

    def drop_exchanged_partition(self, con, config, partition):
        logging.info("Dropping exchanged partition")
        sql = self.sql.get_drop_partition_sql(config['database_name'],
                                              config['table_name'],
                                              partition
                                            ) 
        self.db.execute(con, sql)


    def drop_exchange_table(self, con, config, exchange_table):
        logging.info(f"Dropping exchange table {exchange_table}")
        sql = self.sql.get_drop_table_sql(config['database_name'],
                                          exchange_table
                                        ) 
        self.db.execute(con, sql)

    def record_in_control_table(self, con, config, partition, archive_month, exchange_table):
        logging.info("Recording log in control table")
        sql = self.sql.get_insert_control_table_sql(archive_month, 
                                                    config['database_name'],
                                                    config['table_name'],
                                                    partition,
                                                    exchange_table,
                                                    config['s3_archive_path']) 
        self.db.execute(con, sql)


    def exchange_partition(self, config, instance, archive_month, partition_month):
        con = self.get_connection(instance)
        self.prepare_control_table(con, config['database_name'])
        
        partition_result = self.get_partitioned_to_archive(con, config['table_name'], config['database_name'], partition_month, config['method'])
        
        if not partition_result:
            logging.info("No partition to archive")
            return None, None
        
        partition = partition_result[0]['PARTITION_NAME']
        exchange_table = f"{config['table_name']}_{partition}"
        exchange_res = self.check_exchange_table(con, exchange_table)
        if exchange_res:
            logging.info("Skipping exchange partitioning the table already exists")
            return exchange_table, partition
        
        self.prepare_exchange_table(con, config, exchange_table, config['table_name'])
        self.remove_exchange_table_partitions(con, config, exchange_table)
        self.swap_partition_table(con, config, partition, exchange_table)
        self.record_in_control_table(con, config, partition, archive_month, exchange_table)
    
        return exchange_table, partition

    def clean_json_s3_files(self, config, task_obj):
        logging.info("Cleaning extra json file from the bucket")
        json_files = self.s3_ops.get_jsons_to_delete(config['s3_bucket'], config['s3_archive_path'][1:], task_obj)
        for file in json_files:
            logging.info("Cleaning extra json file from the bucket")
            self.s3_ops.delete_object(config['s3_bucket'], file)

    def cleanup(self, config, instance, exchange_table, partition, task_obj):
        logging.info("Cleaning up leftovers")
        con = self.get_connection(instance)
        self.drop_exchanged_partition(con, config, partition)
        self.drop_exchange_table(con, config, exchange_table)
        self.clean_json_s3_files(config, task_obj)
