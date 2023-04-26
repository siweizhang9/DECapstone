from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):
    load_fact_sql_template = """
        INSERT INTO {}
        ({})
        {}
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 fact_table="",
                 fact_columns="",
                 select_from_sql="",                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.fact_table = fact_table
        self.redshift_conn_id = redshift_conn_id
        self.select_from_sql = select_from_sql
        self.fact_columns = fact_columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.fact_table))

        self.log.info("loading data from staging tables to fact table")
        formatted_sql = LoadFactOperator.load_fact_sql_template.format(
            self.fact_table,
            self.fact_columns,
            self.select_from_sql
        )
        redshift.run(formatted_sql)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.fact_table}")
        num_records = records[0][0]
        self.log.info(f"Inserted {num_records} of rows to Redshift")






