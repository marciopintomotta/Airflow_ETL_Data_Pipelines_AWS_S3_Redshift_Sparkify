
"""
    
    This Class is responsible for loading dimensional tables.
    

"""


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,*args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = kwargs["params"]["sql"]
        self.table = kwargs["params"]["table"]
        self.redshift_conn_id = kwargs["params"]["redshift_conn_id"]
        

    def execute(self, context):
        self.log.info(f"Start loading {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(f"""INSERT INTO {self.table} ({self.sql});""")

        self.log.info(f"Finished loading {self.table}")
