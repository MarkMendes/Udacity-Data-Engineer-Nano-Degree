from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    """
    Airflow Operator to perform data quality checks .

    INPUT:
    redshift_conn_id:       Redshift connection id
    tables:                 List of tables to perform data quality checks on
 
    OUTPUT:
    Reuslts of data quality checks written to log.  Error is raised if no records are loaded.
    Number of records loaded is written to log.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')

        for t in self.tables:
            self.log.info('DataQualityOperator: Running DQ on {}'.format(t))
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(t))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(t))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(t))
            self.log.info("Data quality on table {} check passed with {} records".format(t, records[0][0]))

