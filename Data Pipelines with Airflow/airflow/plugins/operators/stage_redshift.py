from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="", 
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json="auto",
        ignore_headers=1,
        *args, 
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info(f'Start staging on table {self.table}')
        redshift.run(f'Delete from: {self.table}')

        self.log.info('Copying data into Redshift')
        key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key)
        
        redshift.run(
            StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path,
                )
        )
            
        self.log.info(f'Table {self.table} was copied from S3 to Redshift')
            
        