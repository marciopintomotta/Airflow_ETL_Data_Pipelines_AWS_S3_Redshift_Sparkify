"""

This Class is responsible for loading fact table.

"""


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,*args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = kwargs["params"]["sql"]
        self.table = kwargs["params"]["table"]
        self.redshift_conn_id = kwargs["params"]["redshift_conn_id"] 

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('self.sql: {}'.format(self.sql))

        redshift.run(self.sql)

        self.log.info('self.sql: success')
