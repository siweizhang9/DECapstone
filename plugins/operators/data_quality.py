from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        # define compare function to pass the dq_checks paremeters
        def compare(a, b, comparison_symbol):
            if comparison_symbol == ">":
                return a > b
            elif comparison_symbol == ">=":
                return a >= b
            elif comparison_symbol == "<":
                return a < b
            elif comparison_symbol == "<=":
                return a <= b
            elif comparison_symbol == "==":
                return a == b
            elif comparison_symbol == "!=":
                return a != b
            else:
                raise ValueError(f"Invalid comparison symbol: {comparison_symbol}")
            
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, dq_check in enumerate(self.dq_checks):
            records = redshift_hook.get_records(dq_check['test_sql'])
            num_records = records[0][0]
            if not compare(num_records, dq_check['expected'], dq_check['comparison']):
                raise ValueError(f"Data quality check #{i} failed, {dq_check['error']}")
            else:
                 self.log.info(f"Data quality check #{i} succeeded")
