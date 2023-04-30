## Airflights Data ETL Project
This is an ETL (Extract, Transform, Load) project that loads and transforms data of flights involving at least one US airport from 2020 to 2022 from Bueraue of Transportation. This project has customized Airflow operators: stage_redshift.py, load_dimensions.py, load_fact.py, and data_quality.py. 
### Objective
The data comes from Buerau of Transportation and document all US (domestic and international) flight activities. It combines domestic and international T-100 segment data reported by U.S. and foreign air carriers, and contains non-stop segment data by aircraft type and service class for transported passengers, freight and mail, available capacity, scheduled departures, departures performed, aircraft hours, and load factor.

By creating the data model, we want to answer below questions:
* _"For Southwest Airline, how many landings does their 737 make each year?"_
* _"For Airbus A321, what are the usual route lengh chosen by US airlines?"_
* _"By comparing the Frieght flights activities, what are the most busiest US cities for cargo activities?"_
* _"..."_

### Choices of tools and technologies
I am using airflow to drive redshift to copy staging data from S3 to redshift then create fact tables and dimensions data from staging data. Here are the reasons:

1. Flexibility: Airflow allows you to define and execute arbitrary workflows of data processing tasks, making it easy to adapt to different use cases and requirements.
2. Scalability: Airflow can easily scale to handle large volumes of data processing tasks and complex workflows, and it can be integrated with other tools such as Kubernetes to run tasks in a distributed manner.
3. Extensibility: Airflow can be extended with custom operators and sensors, allowing you to integrate it with a wide range of tools and technologies.
4. Monitoring: Airflow provides a web interface for monitoring and managing workflows, making it easy to track the progress of data processing tasks and troubleshoot issues.
5. Integration with Redshift: Airflow comes with built-in operators for interacting with Redshift, making it easy to copy data from S3 to Redshift and create fact tables and dimensions data from staging data.

### Addressing Other Scenarios
* The data was increased by 100x.
  1. Increase the number of nodes in the cluster: Redshift allows you to add more nodes to the cluster, which can increase the computing power of the cluster and handle larger amounts of data.
  2. Use compression: Redshift supports columnar compression, which can significantly reduce the amount of storage required for data. By compressing data, Redshift can handle more data in the same amount of storage space.
  3. Partition the data: Partitioning the data can help improve query performance and reduce the amount of data that needs to be scanned. By partitioning the data based on a specific key, Redshift can perform queries more efficiently.
  4. Use Materialized Views: Materialized views can help improve query performance by pre-computing and storing the results of a query. By using materialized views, Redshift can avoid recomputing the same data multiple times.
  5. Tune the cluster: By tuning the cluster parameters such as sort and memory settings, it is possible to optimize the query performance and handle larger amounts of data.
  6.Consider using other tools or technologies: If the data continues to grow, it may be necessary to consider using other tools or technologies such as Spark or Hadoop, which are designed to handle large amounts of data.
* The pipelines would be run on a daily basis by 7 am every day.
  1. Switch the schedule in the dag setting from "@monthly" to "0 7 * * *"
  2. In the stage_redshift operator, the "s3_key" could be dynamically constructed so the date of the run could be factored into the operator and only the new data loaded after the previous run are loaded
* The database needed to be accessed by 100+ people.
  1. Use Redshift Spectrum: If you have a lot of data in S3, you can use Redshift Spectrum to query the data directly from Redshift without having to load it into Redshift first. This can help you to keep your Redshift cluster small and more cost-effective, while still allowing everyone to access the data they need.
  2. Use a BI tool: You can use a business intelligence (BI) tool like Looker, Tableau, or Power BI to connect to your Redshift cluster and create reports and visualizations that everyone can access. This can make it easier for people to find the data they need and understand it quickly.
  3. Create user accounts: You can create user accounts for everyone who needs access to the data and set up appropriate permissions so that each person can only access the data they need. This can help you to ensure that sensitive data is protected and that people are only seeing data that is relevant to their job.
  4. Use caching: You can use a caching layer like Amazon ElastiCache or Memcached to store frequently accessed data in memory, which can help to reduce the load on your Redshift cluster and make queries faster for everyone.
  5. Optimize your queries: You can optimize your queries to make sure that they are running as efficiently as possible. This can include things like using appropriate indexes, minimizing data movement, and avoiding expensive operations like full table scans.

### Files Overview
The project consists of the following files:

* dags/main.py - the main DAG (Directed Acyclic Graph) file that defines the tasks and dependencies for the ETL process.
* helpers/sql_queries.py - contains all the SQL statements used in the project.
* plugins/operators/stage_redshift.py - the Airflow custom operator for staging data in Redshift.
* plugins/operators/load_dimensions.py - the Airflow custom operator for loading dimensional data in Redshift.
* plugins/operators/load_fact.py - the Airflow custom operator for loading fact data in Redshift.
* plugins/operators/data_quality.py - the Airflow custom operator for running data quality checks in Redshift.
### Project Overview
This project loads and transforms data from two monthly summary log data and look up table data. Both datasets are stored in S3 buckets, and the ETL process extracts the data from S3, stages it in Redshift, transforms the data into a set of dimensional tables and a fact table, and performs data quality checks.

### DAG Overview
The main DAG file, main.py, is located in the project root directory. It consists of the following tasks:

* Begin_execution - creates all necessary tables in Redshift.
* stage_flights_to_redshift - load data from csv files in S3 to redshift
* stage_aircraft_group_to_redshift - load look up table from csv files in S3 to redshift
* stage_aircraft_code_to_redshift - load look up table from csv files in S3 to redshift
* stage_aircraft_config_to_redshift - load look up table from csv files in S3 to redshift
* load_flights_table - load fact table from stating tables
* load_origin_dim_airport_code  - load dimensional table from stating tables
* load_destin_dim_airport_code - load dimensional table from stating tables
* load_dim_aircraft_code - load dimensional table from stating tables
* load_dim_airport_group - load dimensional table from stating tables
* load_dim_aircraft_config - load dimensional table from stating tables
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
