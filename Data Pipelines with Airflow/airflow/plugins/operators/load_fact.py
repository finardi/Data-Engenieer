from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table='',
        sql_query='',
        truncate_table='',
        *args, 
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run(f'Truncating table {self.table}')
        
        self.log.info(f'Loading Dimension Table {self.table}')
        redshift.run(self.sql_query.format(self.table))
        self.log.info('Success')
