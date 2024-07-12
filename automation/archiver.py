from export_ops import ExportOps
from exchange_ops import ExchangeOps
from redshift import Redshift
from credentials import Credentials
from rds import RDS
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import yaml
import logging

class Archiver:

    def __init__(self) -> None:
        self.export_ops = ExportOps()
        self.exchange_ops = ExchangeOps()
        self.redshift_ops = Redshift()
        self.rds = RDS()
        self.data = []
        self.credentials = Credentials
    
    def read_config(self, file_name):
        with open(file_name, "r") as stream:
            try:
                self.data = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                logging.error(f"Something went wrong {e}") 

    def get_db(self, config):
        if config['is_cluster']:
            db = self.rds.get_db_cluster(config['identifier'])
            return {'host': db['Endpoint'], 'port': db['Port'], 'arn': db['DBClusterArn']}
        db = self.rds.get_db_instance(config['identifier'])
        return {'host': db['Endpoint']['Address'], 'port': db['Endpoint']['Port']}

    def get_archive_month(self, config):
        delta = config['archive_period']
        dt_partition = (datetime.today() + relativedelta(months=-(delta-1))).strftime('%Y-%m-01')
        dt_hrf = (datetime.today() + relativedelta(months=-delta)).strftime('%Y-%m-01')
        return dt_hrf, dt_partition

    def run_archiver(self):
        for config in self.data:
            
            self.credentials.set_credentials(config['env'])
            
            instance = self.get_db(config)
            date_archive, date_partiton_funcion = self.get_archive_month(config)
            
            logging.info(f"starting for {config['table_name']} month for month {date_archive}")
            
            exchanged_table, partition_name = self.exchange_ops.exchange_partition(config, instance, date_archive, date_partiton_funcion)
            
            if not partition_name:
                logging.info("Nothing to export as partition")
                continue
            
            task_obj = self.export_ops.export_partial_to_s3(config, instance, exchanged_table, partition_name)

            if 'redshift_endpoint' in config:
                logging.info("The table requires redshift archiving")
                self.redshift_ops.redshift_import(instance, config, date_archive, partition_name, task_obj)

            self.exchange_ops.cleanup(config, instance, exchanged_table, partition_name, task_obj)
