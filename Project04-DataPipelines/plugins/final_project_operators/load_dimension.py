from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Airflow Operator to Load Redshift Dimension Table(s).

    INPUT:
    redshift_conn_id:       Redshift connection id
    table:                  Redshift target table name
    sql_query:              SQL query containing insert statement for target table
    write_modde:            If argument is 'delete-load' target table will be truncated prior to load.  Else records will be appended to target table

    OUTPUT:
    Dimension Tables(s) loaded
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query = "",
                 table = "",
                 write_mode = "delete-load",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.write_mode = write_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('LoadDimensionOperator: Loading from stage to dimension table {}'.format(self.table))
        # Allow functionality to switch between apend and delete-load 
        if self.write_mode == "delete-load":
            self.log.info('LoadDimensionOperator: Truncating table {}'.format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info('LoadDimensionOperator: Inserting into table {}'.format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
