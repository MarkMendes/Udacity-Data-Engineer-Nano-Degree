from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Airflow Operator to Copy from AWS S3 Buckets to AWS Redshift, to stage data for futher processing.
    INPUT:
    redshift_conn_id:       Redshift connection id
    aws_credentials_id:     S3 credentials
    table:                  Redshift target table name
    s3_bucket:              S3 bucket name
    s3_key:                 S3 bucket key
    log_json_path:          Optional, path to JSON meta data structure

    OUTPUT:
    Source data from S3 Copied to Target Redshift Table
    """

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 log_json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_path = log_json_path
  
    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('StageToRedshiftOperator: Clearing Destination Table')
        redshift.run("TRUNCATE {}".format(self.table))

        self.log.info('StageToRedshiftOperator: Copying from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.log_json_path !="":
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                self.log_json_path
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                'auto'
            )

        redshift.run(formatted_sql)
        self.log.info("Complete: Copy data from s3 to table{}".format(self.table))















