from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    DAG operator to populate staging tables from S3 json files
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 table="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"StageToRedshiftOperator - clearing data from redshift {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("StageToRedshiftOperator - copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        copy_sql = f"""
                    COPY {self.table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{credentials.access_key}'
                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                    format as json 'auto';
                    """
        redshift.run(copy_sql)
        self.log.info(f"StageToRedshiftOperator - copy to {self.table} complete")
