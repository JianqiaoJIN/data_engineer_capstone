from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 schema = "",
                 table = "",
                 insert_sql = "",
                 append_only = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.insert_sql = insert_sql
        self.delete_sql = (f"DELETE FROM {self.schema}.{self.table}")
        self.append_only = append_only

    def execute(self, context):
        self.log.info('Insert Data To Table {self.schema}.{self.table}')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f'Clearing All Records in Table')
            redshift.run(self.delete_sql)
        
        redshift.run(self.insert_sql)