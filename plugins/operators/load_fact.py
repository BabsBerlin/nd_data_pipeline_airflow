from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    DAG operator to populate fact tables from staging tables
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_statement="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_statement = sql_statement
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'LoadFactOperator - inserting into table {self.table}')
        redshift.run(f"INSERT INTO {self.table} {self.sql_statement}")
