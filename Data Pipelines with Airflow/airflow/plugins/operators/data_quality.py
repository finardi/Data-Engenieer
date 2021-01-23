from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        tables='',
        *args, 
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f'Table: {table}: starting data quality validation')
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f'Table {table}: Failed on data quality')
                raise ValueError(f'Table {table}: Failed on data quality')
        
            self.log.info(f'Table {table}: data quality successed')