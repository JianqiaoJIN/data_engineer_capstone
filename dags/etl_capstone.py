import os, pathlib, configparser
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.s3_to_redshift_copy_immigration import StageImmigrationDataToRedshiftOperator
from operators.s3_to_redshift_copy_csv import StageCSVToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_etl_queries import SqlQueries

config = configparser.ConfigParser()
config.read('config.cfg')

# constant variable 
SCHEMA_NAME = "usa"
REDSHIFT_CONN_ID = "redshift"
S3_CONN_ID = "s3"
AWS_CREDENTIAL_ID = "aws_credentials"
S3_BUCKET = "jjq-capstone"
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# create dag 
default_args = {
    'owner': "jjq",
    'depends_on_past': False,
    'Catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2016, 4, 1),
    'end_date': datetime(2016, 4, 2)
}

dag = DAG(
    "capstone",
    default_args=default_args,
    description="Load and Transform I94 Immigration Data",
    schedule_interval='@daily'
)

# create operators 
start_operator = DummyOperator(task_id="start_exectution", dag=dag)

copy_immigration_data = StageImmigrationDataToRedshiftOperator(
    task_id="copy_i94_immigration_data",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="immigration_staging_day",
    s3_bucket= S3_BUCKET,
    s3_prefix="raw/i94_immigration_data", # raw/i94_immigration_data/arrival_year=2016/arrival_month=4/arrival_day=1/
    IAM_ROLE = IAM_ROLE,
    append_only=True,
    test = False
)

copy_usa_port = StageCSVToRedshiftOperator(
    task_id="copy_usa_port_description",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="usa_port",
    s3_bucket=S3_BUCKET,
    s3_key="raw/i94_immigration_labels_description/usa_port.csv"
)

copy_travel_way = StageCSVToRedshiftOperator(
    task_id="copy_travel_way_description",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="travel_way",
    s3_bucket=S3_BUCKET,
    s3_key="raw/i94_immigration_labels_description/travel_way.csv"
)

copy_visa_code = StageCSVToRedshiftOperator(
    task_id="copy_visa_code_description",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="visa_code",
    s3_bucket=S3_BUCKET,
    s3_key="raw/i94_immigration_labels_description/visa_code.csv"
)

copy_country_code = StageCSVToRedshiftOperator(
    task_id="copy_country_code_description",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="i94country_code",
    s3_bucket=S3_BUCKET,
    s3_key="raw/i94_immigration_labels_description/country_code.csv"
)

load_usa_travelers_info = LoadFactOperator(
    task_id="load_usa_travelers_info",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="city_state_travelers_entry",
    insert_sql=SqlQueries.city_state_travelers_entry_insert
)

load_arrival_date = LoadDimensionOperator(
    task_id="load_arrival_date",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    schema=SCHEMA_NAME,
    table="arrival_date",
    insert_sql=SqlQueries.arrival_date_insert
)

# Data Quality
# get the dq_checks_settings for data quality
# file: [airflow_file]/plugins/helpers/dq_check_settings.json
airflow_file = pathlib.Path(__file__).parent.parent.absolute()
dq_check_settings = os.path.join(airflow_file, "plugins", "helpers", "dq_check_settings.json")
with open (dq_check_settings) as json_file:
    dq_checks = json.load(json_file)
    dq_checks = dq_checks['dq_checks']

run_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id="end_execution", dag=dag)

# Task Dependencies 
start_operator >> [copy_immigration_data, copy_usa_port, copy_travel_way, copy_visa_code, copy_country_code] >> load_usa_travelers_info
load_usa_travelers_info >> load_arrival_date >> run_quality_checks >> end_operator