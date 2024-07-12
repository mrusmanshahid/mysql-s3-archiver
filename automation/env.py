
import os

# You can override credentials for different environments in credentials.py file
mysql_user = os.environ.get('MYSQL_USER')
mysql_password = os.environ.get('MYSQL_PASSWORD')

# The following role and kms-key should have the required permissions
execution_role = '<ARN of the role that will be used to export snapshot to S3>'
kms_key = '<KMS key-id that will be used to encrypt the exported data in S3>'

redshift_user = os.environ.get('REDSHIFT_USER')
redshift_password = os.environ.get('REDSHIFT_PASSWORD')

# This script will add a retention tag (in-weeks) to the snapshots that it will create, you can put policy to delete 
# once the retention has passed
retention_in_weeks = os.environ.get('SNAPSHOT_RETENTION', "1") 
