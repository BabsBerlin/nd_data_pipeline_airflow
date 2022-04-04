from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    DAG operator to populate dimension tables from staging tables
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 sql_statement='',
                 operation='',
                 redshift_conn_id='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_statement = sql_statement
        self.operation = operation
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.operation == 'truncate':
            self.log.info(f'LoadDimensionOperator - deleting from table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'LoadDimensionOperator - inserting into table {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql_statement}')

        
        
