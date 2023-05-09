"""
    
    This Class is responsible from S3 to Stage area in Redshift.

"""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                JSON '{}'
                """

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = kwargs["params"]["table"]
        self.redshift_conn_id = kwargs["params"]["redshift_conn_id"]
        self.s3_bucket = kwargs["params"]["s3_bucket"]
        self.s3_key = kwargs["params"]["s3_key"]
        self.s3_json = kwargs["params"]["s3_json"]
        self.aws_credentials_id = kwargs["params"]["aws_credentials_id"]

    def execute(self, context):
        self.log.info('connect to aws')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('connect to redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))

        if self.table == 'staging_events':
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            
        if self.table == 'staging_songs':
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        self.log.info('s3_path: {}'.format(s3_path))

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_json
        )

        self.log.info('formatted_sql: {}'.format(formatted_sql))
        
        redshift.run(formatted_sql)





