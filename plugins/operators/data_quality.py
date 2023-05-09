
"""
    
    This Class is responsible for running the checks in data in the Redshift tables.
    

"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = kwargs["params"]["conn_id"]
        self.table = kwargs["params"]["table"]
        self.col_name = kwargs["params"]["col_name"]

    def execute(self, context):
        self.log.info('table to check data quality -> {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.col_name} is NULL")
        if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")