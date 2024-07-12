

class SQL:

    def get_control_table_sql(self, database_name):
        return f"""
            CREATE TABLE IF NOT EXISTS `{database_name}`.`deep_archive_control_table`(
                `id` int AUTO_INCREMENT,`archive_month` datetime,
                `database_name` varchar(200),
                `table_name` varchar(200),
                `partition_archived` varchar(200),
                `exchange_partition_name` varchar(200),
                `redshift_archive` tinyint(1),
                `deep_archive_path` varchar(200), PRIMARY KEY (id));
        """

    def get_exchange_table_sql(self, database, exchange_table, partition_table):
        return f"""
            CREATE TABLE IF NOT EXISTS {database}.{exchange_table} LIKE {database}.{partition_table};
        """

    def get_check_exchange_table_sql(self, exchange_table):
        return f"""
            SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_NAME = '{exchange_table}'
        """

    def get_remove_partitions_sql(self, database, exchange_table):
        return f"""
            ALTER TABLE {database}.{exchange_table} REMOVE PARTITIONING;
        """
    
    def get_drop_partition_sql(self, database, table, partition):
        return f"""
            ALTER TABLE {database}.{table} DROP PARTITION {partition};
        """
    
    def get_drop_table_sql(self, database, exchange_table):
        return f"""
            DROP TABLE {database}.{exchange_table};
        """

    def get_exchange_partition_sql(self, database, table, partition, exchange_table):
        return f"""
            ALTER TABLE {database}.{table} EXCHANGE PARTITION {partition} WITH TABLE {database}.{exchange_table} WITHOUT VALIDATION;
        """

    def get_partition_for_month(self, table_name, schema, partition_descriptor, method):
        descriptor_method = "CAST(REPLACE(CONVERT(PARTITION_DESCRIPTION USING utf8), '''', '') AS DATE)" if method == 'date' else 'PARTITION_DESCRIPTION'
        return f"""
            SELECT PARTITION_NAME, TABLE_ROWS, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{schema}'
            AND {descriptor_method} <= (SELECT {method}('{partition_descriptor}'))
            AND PARTITION_NAME NOT LIKE '%default%'
            ORDER BY PARTITION_DESCRIPTION 
            LIMIT 1;
        """
    
    def get_insert_control_table_sql(self, archive_month, database_name, table_name, partition, exchange_table, path):
        return f"""
            INSERT INTO `{database_name}`.`deep_archive_control_table` (`archive_month`, `database_name`, `table_name`, `partition_archived`, `exchange_partition_name`, `redshift_archive`, `deep_archive_path`) 
            VALUES ('{archive_month}','{database_name}', '{table_name}', '{partition}', '{exchange_table}', 0, '{path}');
        """
    
    def get_update_control_table_record_sql(self, database_name, archive_month, table_name, partition):
        return f"""
            UPDATE `{database_name}`.`deep_archive_control_table`
            SET `redshift_archive` = 1
            WHERE archive_month = '{archive_month}'
            AND table_name = '{table_name}'
            AND partition_archived = '{partition}';
        """

    def get_control_table_record_sql(self, database_name, archive_month, table_name, partition):
        return f"""
            SELECT redshift_archive
            FROM {database_name}.deep_archive_control_table
            WHERE archive_month = '{archive_month}'
            AND table_name = '{table_name}'
            AND partition_archived = '{partition}';
        """
    
    def get_redshift_sync_statement_sql(self, database, table, columns, iam_role, parquet_path):
        return f"""
            COPY {database}.{table} ({columns})
            FROM '{parquet_path}'
            CREDENTIALS '{iam_role}'
            FORMAT AS PARQUET;
        """
