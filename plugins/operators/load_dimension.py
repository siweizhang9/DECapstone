from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    load_dim_sql_template = """
        INSERT INTO {}
        ({})
        {}
    """
    table_distinct_template = """
        BEGIN;
        CREATE TABLE temp1 AS SELECT DISTINCT * FROM {};
        ALTER TABLE {} RENAME TO temp2;
        ALTER TABLE temp1 RENAME TO {};
        DROP TABLE temp2;
        COMMIT;
    
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dim_table="",
                 dim_columns="",
                 select_from_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dim_table = dim_table
        self.redshift_conn_id = redshift_conn_id
        self.select_from_sql = select_from_sql
        self.dim_columns = dim_columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.dim_table))

        self.log.info("loading data from staging tables to dimension table")
        formatted_sql = LoadDimensionOperator.load_dim_sql_template.format(
            self.dim_table,
            self.dim_columns,
            self.select_from_sql
        )
        redshift.run(formatted_sql)
        distinct_sql = LoadDimensionOperator.table_distinct_template.format(
            self.dim_table,
            self.dim_table,
            self.dim_table
        )
        redshift.run(distinct_sql)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.dim_table}")
        num_records = records[0][0]
        self.log.info(f"Inserted {num_records} of rows to Redshift")
