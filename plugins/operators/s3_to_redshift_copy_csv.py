from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class StageCSVToRedshiftOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 aws_credential_id = "aws_credentials",
                 s3_conn_id = "s3",
                 schema = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 *args, **kwargs):

        super(StageCSVToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.s3_conn_id = s3_conn_id

        self.schema = schema 
        self.table = table 
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.delete_sql = (f"DELETE FROM {self.schema}.{self.table}")
        self.copy_sql = ("""
            COPY {}.{} FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            CSV
            IGNOREHEADER 1;
        """)
        
    def execute(self, context):

        self.log.info('Stage CSV Data From S3 to Redshift')
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3 = S3Hook(self.s3_conn_id)
        
        self.log.info(f'Clearing data from {self.schema}.{self.table}')
        redshift.run(self.delete_sql)

        s3_key = self.s3_key
        execute = s3.check_for_key(s3_key, self.s3_bucket)
        
        if execute:
            self.log.info(f'Copying data from S3 to {self.schema}.{self.table}')
            s3_path = f"s3://{self.s3_bucket}/{s3_key}"
            formatted_copy_sql = self.copy_sql.format(
                self.schema,
                self.table,
                s3_path, 
                credentials.access_key,
                credentials.secret_key
            )
            redshift.run(formatted_copy_sql)
        





