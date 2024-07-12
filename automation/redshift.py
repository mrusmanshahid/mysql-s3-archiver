from env import redshift_user, redshift_password
from s3 import S3
from sql import SQL
from database import Database
from credentials import Credentials
import psycopg2
import psycopg2.extras
import logging

class Redshift:

    def __init__(self) -> None:
        self.s3_ops = S3()
        self.sql = SQL()
        self.db = Database()
        self.credentials = Credentials

    def get_connection(self, config):
        logging.info("Connecting to redshift server for data-storage")
        connection = psycopg2.connect(host=config['redshift_endpoint'], 
                                     user=redshift_user,
                                     password=redshift_password,
                                     port=int(config['redshift_port']),
                                     dbname=config['redshift_database_name'],
                                     cursor_factory=psycopg2.extras.DictCursor)
        return connection

    def execute(self, con, sql):
        if not con:
            logging.error(f"No database connection, can't proceed")
            return False
        with con.cursor() as cur:
            cur.execute(sql)
            con.commit()
            return True

    def import_data(self, config, task_obj):
        try:
            con = self.get_connection(config)
            parquets = self.s3_ops.get_parquets_to_sync(config['s3_bucket'], config['s3_archive_path'][1:], task_obj)

            if len(parquets) == 0:
                logging.info("Nothing to archive the process will exit here reporting success.")
                return True

            for parquet_path in parquets:
                logging.info(f"Importing data from s3 to redshift for parition {task_obj['ExportTaskIdentifier']}")
                sql = self.sql.get_redshift_sync_statement_sql(config['redshift_schema_name'],
                                                        config['redshift_table_name'],
                                                        config['redshift_import_columns'],
                                                        config['redshift_iam_role'],
                                                        parquet_path)
                logging.debug(sql)
                self.execute(con, sql)
            return True
        
        except Exception as e:
            logging.error("Something went wrong, please fix the error and try again")
            logging.error(e)
            return False


    def redshift_import(self, instance, config, date_archive, partition_name, task_obj):
        status = self.get_record_in_control_table(instance, config, date_archive, partition_name)
        if status == 1:
            logging.info("The parition has already been synced to redshift, skipping this step.")
            return
        res = self.import_data(config, task_obj)
        if res:
            self.update_record_in_control_table(instance, config, date_archive, partition_name)


    def update_record_in_control_table(self,instance, config, archive_month, partition):
        logging.info("Updating reshift log in control table")
        con = self.db.establish_connection(instance['host'], self.credentials.mysql_user, 
                                           self.credentials.mysql_password, instance['port'])
        sql = self.sql.get_update_control_table_record_sql(config['database_name'],
                                                           archive_month,
                                                           config['table_name'],
                                                           partition) 
        self.db.execute(con, sql)


    def get_record_in_control_table(self,instance, config, archive_month, partition):
        logging.info("Get reshift sync status from control table")
        con = self.db.establish_connection(instance['host'], self.credentials.mysql_user, 
                                           self.credentials.mysql_password, instance['port'])
        sql = self.sql.get_control_table_record_sql(config['database_name'],
                                                           archive_month,
                                                           config['table_name'],
                                                           partition) 
        record  = self.db.execute(con, sql)
        return int(record[0]['redshift_archive'])
