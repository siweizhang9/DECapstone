from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = 'decapstonesz'
flight_data_key ='T_100'
aircraft_code_key = 'Info/Aircraft Code.csv'
aircraft_group_key = 'Info/L_AIRCRAFT_GROUP.csv'
aircraft_config_key = 'Info/L_AIRCRAFT_CONFIG.csv'


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform flight data in Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs=1
        )


start_operator = PostgresOperator(
    task_id="Begin_execution",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_all_tables
)

stage_flights_to_redshift = StageToRedshiftOperator(
    task_id='Stage_flights',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_flights",
    s3_bucket=s3_bucket,
    s3_key=flight_data_key,
    s3_region = 'us-east-1'
)

stage_aircraft_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_aircraft_code',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_aircraft_code",
    s3_bucket=s3_bucket,
    s3_key=aircraft_code_key,
    s3_region = 'us-east-1'
)

stage_aircraft_group_to_redshift = StageToRedshiftOperator(
    task_id='Stage_aircraft_group',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_aircraft_group",
    s3_bucket=s3_bucket,
    s3_key=aircraft_group_key,
    s3_region = 'us-east-1'
)

stage_aircraft_config_to_redshift = StageToRedshiftOperator(
    task_id='Stage_aircraft_config',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_aircraft_configuration",
    s3_bucket=s3_bucket,
    s3_key=aircraft_config_key,
    s3_region = 'us-east-1'
)

load_flights_table = LoadFactOperator(
    task_id='Load_flights_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="public.fact_flights",
    fact_columns="DEPARTURES_SCHEDULED, DEPARTURES_PERFORMED, PAYLOAD, SEATS, \
    PASSENGERS, FREIGHT, MAIL, DISTANCE, RAMP_TO_RAMP, AIR_TIME, \
    UNIQUE_CARRIER_ENTITY, ORIGIN, DEST, AIRCRAFT_TYPE, AIRCRAFT_CONFIG, \
    YEAR, QUARTER, MONTH, DISTANCE_GROUP, CLASS, DATA_SOURCE",
    select_from_sql=SqlQueries.flights_fact_table_insert
)

load_dim_aircraft_code = LoadDimensionOperator(
    task_id='load_dim_aircraft_code',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="dim_aircraft_code",
    dim_columns="ac_typeid, ac_group, ssd_name, manufacturer, long_name, short_name",
    select_from_sql=SqlQueries.dim_aircraft_code_insert,
    truncate_boolean=True
)

load_dim_airport_group = LoadDimensionOperator(
    task_id='load_dim_airport_group',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="dim_aircraft_group",
    dim_columns="ac_group, ac_group_description",
    select_from_sql=SqlQueries.dim_airport_group_insert,
    truncate_boolean=True
)

load_dim_aircraft_config = LoadDimensionOperator(
    task_id='load_dim_aircraft_config',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="dim_aircraft_configuration",
    dim_columns="ac_config, ac_config_description",
    select_from_sql=SqlQueries.dim_aircraft_config_insert,
    truncate_boolean=True
)

load_origin_dim_airport_code = LoadDimensionOperator(
    task_id='load_origin_dim_airport_code',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="dim_airport_code",
    dim_columns="airport_id, airport_seq_id, city_market_id, airport_code, city_name, \
      state_abr, state_fips, state_nm, country, country_name, wac",
    select_from_sql=SqlQueries.dim_origin_airport_code_insert,
    truncate_boolean=True
)

load_destin_dim_airport_code = LoadDimensionOperator(
    task_id='load_destin_dim_airport_code',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="dim_airport_code",
    dim_columns="airport_id, airport_seq_id, city_market_id, airport_code, city_name, \
      state_abr, state_fips, state_nm, country, country_name, wac",
    select_from_sql=SqlQueries.dim_destination_airport_code_insert,
    truncate_boolean=False
)

# Provide a data qualicy check dictionary here with these elements:
# "test_sql": the sql commend to run and return a single value that can be used as indicator
#             for example: "SELECT COUNT(*) FROM..."
# "expected": the value that is to be valued againt the indicator
# "comparison": allowable comparison symbols are: >, >=, <, <=, ==, !=
# "error": if the indicator from test_sql compared with expected using the comparison symbol
#          returned False, what error message should be raised

dq_checks = [
    {'test_sql': "SELECT COUNT(*) FROM fact_flights WHERE year = 2020",
     'expected': 0,
     'comparison': ">",
     'error': "there are no data for 2020"
     },
    {'test_sql': "SELECT COUNT(*) FROM staging_aircraft_code",
     'expected': 0,
     'comparison': ">",
     'error': "there are no data on aircraft_code csv file"
     },
     {'test_sql': "SELECT COUNT(*) FROM staging_flights",
     'expected': 0,
     'comparison': ">",
     'error': "there are no data on staging flights table"
     }
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks = dq_checks,
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_flights_to_redshift
stage_flights_to_redshift >> stage_aircraft_group_to_redshift
stage_aircraft_group_to_redshift >> stage_aircraft_code_to_redshift
stage_aircraft_code_to_redshift >> stage_aircraft_config_to_redshift
stage_aircraft_config_to_redshift >> load_flights_table
load_flights_table >> load_dim_aircraft_code
load_flights_table >> load_dim_airport_group
load_flights_table >> load_dim_aircraft_config
load_flights_table >> load_origin_dim_airport_code
load_origin_dim_airport_code >> load_destin_dim_airport_code 
load_dim_aircraft_code >> run_quality_checks
load_dim_airport_group >> run_quality_checks
load_dim_aircraft_config >> run_quality_checks
load_destin_dim_airport_code >> run_quality_checks
run_quality_checks >> end_operator
