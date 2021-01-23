from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        sql_query='',
        table='',
        truncate_table='',
        *args, 
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncating table {self.table}')
            redshift.run(f'Truncating table {self.table}')
        
        self.log.info(f'Loading Dimension Table {self.table}')
        redshift.run(self.sql_query.format(self.table))
        self.log.info('Success')