## Airflow ETL Project
This is an ETL (Extract, Transform, Load) project that loads and transforms data in Redshift with Airflow. This project has customized Airflow operators: stage_redshift.py, load_dimensions.py, load_fact.py, and data_quality.py. The project consists of the following files:

* dags/main.py - the main DAG (Directed Acyclic Graph) file that defines the tasks and dependencies for the ETL process.
* helpers/sql_queries.py - contains all the SQL statements used in the project.
* plugins/operators/stage_redshift.py - the Airflow custom operator for staging data in Redshift.
* plugins/operators/load_dimensions.py - the Airflow custom operator for loading dimensional data in Redshift.
* plugins/operators/load_fact.py - the Airflow custom operator for loading fact data in Redshift.
* plugins/operators/data_quality.py - the Airflow custom operator for running data quality checks in Redshift.
### Project Overview
This project loads and transforms data from two different sources, log data and song data. The log data contains user activity logs, while the song data contains metadata about the songs. Both datasets are stored in S3 buckets, and the ETL process extracts the data from S3, stages it in Redshift, transforms the data into a set of dimensional tables and a fact table, and performs data quality checks.

### DAG Overview
The main DAG file, main.py, is located in the project root directory. It consists of the following tasks:

* Begin_execution - creates all necessary tables in Redshift.
* Stage_events - stages the log data from S3 into Redshift.
* Stage_songs - stages the song data from S3 into Redshift.
* Load_songplays_fact_table - loads data into the fact table songplays using data from the staging_events and staging_songs tables.
* Load_user_dim_table - loads data into the dimension table users using data from the staging_events table.
* Load_song_dim_table - loads data into the dimension table songs using data from the staging_songs table.
* Load_artist_dim_table - loads data into the dimension table artists using data from the staging_songs table.
* Load_time_dim_table - loads data into the dimension table time using data from the staging_events table.
* Run_data_quality_checks - performs data quality checks on the fact and dimension tables.
* Stop_execution - the final task that signifies the end of the ETL process.
### Custom Airflow Operators
The project uses custom operators built with Airflow's BaseOperator as the parent class. The operators are located in the plugins/operators/ directory. The operators are defined as follows:

* StageToRedshiftOperator - a custom operator that stages data from S3 to Redshift.
* LoadDimensionOperator - a custom operator that loads data into a dimensional table in Redshift.
* LoadFactOperator - a custom operator that loads data into a fact table in Redshift.
* DataQualityOperator - a custom operator that performs data quality checks on Redshift tables.
### Configuring Airflow Connections
This project requires two connections to be configured in Airflow:

* aws_credentials - an Amazon Web Services connection that provides access to S3.
* redshift - a PostgreSQL connection that provides access to
