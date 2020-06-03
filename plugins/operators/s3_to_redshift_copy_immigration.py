from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageImmigrationDataToRedshiftOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 schema = "",
                 table = "",
                 s3_bucket = "",
                 s3_prefix = "", 
                 IAM_ROLE = "",   
                 append_only = False,
                 test = False,
                 *args, **kwargs):

        super(StageImmigrationDataToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema 
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.IAM_ROLE = IAM_ROLE
        self.append_only = append_only
        self.test = test

        self.delete_sql = (f"DELETE FROM {self.schema}.{self.table}")
        self.copy_sql = ("""
            COPY {}.{} from '{}'
            IAM_ROLE '{}'
            FORMAT AS PARQUET;
        """)
                
    def execute(self, context):

        self.log.info('Copy Parquet Data From S3 to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info(f'Clearing data from {self.schema}.{self.table}')
            redshift.run(self.delete_sql)
        
        if not self.test:
            # every time the operator only load partition data with respect to the execution date
            execution_date = context['execution_date']
            s3_prefix = f"{self.s3_prefix}/arrival_year={execution_date.year}/arrival_month={execution_date.month}/arrival_day={execution_date.day}/"
        else:
            # just for testing
            s3_prefix = self.s3_prefix
        
        self.log.info(f'Copying I94 IMMIGRATION DATA from S3 to {self.schema}.{self.table}')
        s3_path = f"s3://{self.s3_bucket}/{s3_prefix}"
        formatted_copy_sql = self.copy_sql.format(
            self.schema,
            self.table,
            s3_path, 
            self.IAM_ROLE
        )
        redshift.run(formatted_copy_sql)
        
        





