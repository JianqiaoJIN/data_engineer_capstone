from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        #print(self.dq_checks[0]['expected_result'])

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        execution_date = context['ds']
        print(execution_date)
        for dq_check in self.dq_checks:
            self.validate_sql(redshift, dq_check, execution_date)

    def validate_sql(self, redshift_hook, dq_check, execution_date):
        """ check if the sql-result equal to the expected result 

        Paramters:
        ---------
        redshift_hook: connection to redshift cluster 
        dq_check: dic
            check_sql: key (str)
            dq_check: value (int)
        """
        
        if dq_check['execution_date'] == "TRUE":
            check_sql = dq_check['check_sql'].format(execution_date)
        else:
            check_sql = dq_check['check_sql']
        
        print(check_sql)
        records = redshift_hook.get_records(check_sql)

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. Table has NO records.')       

        num_records = records[0][0]

        expected_result = dq_check['expected_result']
        if expected_result == "NOT EMPTY":
            if num_records == 0:
                raise ValueError(f"Data quality check failed. Expected: {expected_result} | Got: {num_records}")
            else:
                self.log.info(f".Data quality on SQL {check_sql} check passed. INSERT DATA SUCCEEDS.")
        else:
            if num_records != expected_result:
                raise ValueError(f"Data quality check failed. Expected: {expected_result} | Got: {num_records}")
            else:
                self.log.info(f".Data quality on SQL {check_sql} check passed with {num_records} records equal to expeted value {expected_result}")