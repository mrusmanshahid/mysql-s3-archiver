# MySQL Athena, S3 & Redshift based Partition Archiver

The automation is designed to peform deep archival for any mysql partitioned table from RDS/aurora to S3 and athena for querying the data. The automation also supports importing data to Redshift directly as well

![workflow](img/workflow.png)

## How it works?

The aim of the datalife cycle project is to archive data out from RDS to S3 in dedicated bucket for each month.
Steps executed by this tool: 
- Create control table at the destination db
- Pick the partition to archive using method `to_days` and `unix_timestamp`
- Create exchange table on the target database
- Exchange the new exchange table with the target partition
- Record the change in control table
- Create a snapshot of the target database in case its RDS otherwise it will use aurora export directly
- Partially export the exchanged table to the S3 bucket and table path
- Import data to Redshift (if required)
- Cleanup all the resources related to archival, which includes cleaning exchange table and partition that was archived

## Environment Variables

To run this project, you will need to set the following environment variables while the Redshift variables are optional

`MYSQL_USER`
`MYSQL_PASSWORD`
`REDSHIFT_USER`
`REDSHIFT_PASSWORD`

## Parameters

You are required to modify `env.py` file 

- `execution_role` = the arn of the aws execution role that will be used to export partial snapshot to S3

- `kms_key` = the arn of the kms key that will be used to export the partital snapshot to S3
## Onboard New Table
In order to onboard, new table follow the following steps

##### 1. Setup Directory 
 - Add a new folder under the `configs` directory with `yaml`
 - for example: `configs/my_cool_app/fancy_env.yaml`

##### 2. Setup Table Configuration
 - Under the `yaml` add following properties for each table

| Parameter | Value |
|-----------|---------|
| <a name="identifier"></a> [identifier] | the instance identifier of history RDS/aurora |
| <a name="is_cluster"></a> [is\_cluster] | true if its aurora else false |
| <a name="database_name"></a> [database\_name] | the name of schema where table is present |
| <a name="table_name"></a> [table\_name] | the name of the table which needs scheduling |
| <a name="method"></a> [method] | the partition method, acceptable values are to_days and unix_timestamps |
| <a name="archive_period"></a> [archive\_period] | the period to archive in months, for example 12 for on year old partitions |
| <a name="s3_bucket"></a> [s3\_bucket] | the name of the S3 bucket where data will be archived |
| <a name="s3_archive_path"></a> [s3\_archive\_path] | the path of the S3 bucket where data will be archived |
| <a name="redshift_port"></a> [redshift\_port] | the port on which redshift is operating, the property is only required when rest of the redshift properties has been defined otherwise the property is optional |
| <a name="redshift_database_name"></a> [redshift\_database\_name] | the database name where the table is created for import. The property is only required when rest of the redshift properties has been defined |
| <a name="redshift_schema_name"></a> [redshift\_schema\_name] | the schema name where the table is located. The property is only required when rest of the redshift properties has been defined |
| <a name="redshift_table_name"></a> [redshift\_table\_name] | the instance identifier of history RDS/aurora |
| <a name="redshift_import_columns"></a> [redshift\_import\_columns] | the comma-separated list of columns that will be imported to Redshift, for example id,col1,col2,col3. The property is only required when rest of the redshift properties has been defined |

## How to Run
After setting the environment, run `main` to start the process
`python3 automation/main.py <file_path>`

The script is designed to restart from the point of failure in case of crash in between so it should not be a problem.
