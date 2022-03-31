from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check in self.checks:
            self.log.info(f"DataQualityOperator running check {check.get('name')}")
            result = redshift.get_records(check.get('sql_query'))
            if result == check.get('expected_result'):
                self.log.info(f"check {check.get('name')} passed")
            else:
                raise ValueError(f"DataQualityOperator failed: {check.get('name')}")
            
            
        