from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Airflow Operator to Load Redshift Fact Table from Redshift Stage Table(s).

    INPUT:
    redshift_conn_id:       Redshift connection id
    aws_credentials_id:     S3 credentials
    table:                  Redshift target table name
    sql_query:              SQL query containing insert statement for target table

    OUTPUT:
    Stage data loaded to Fact Table.  Data is appended to Fact Table.
    """



    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('LoadFactOperator:  Loading from stage to fact')
        sql = "INSERT INTO {} {}".format(self.table, self.sql_query)
        redshift.run(sql)

