from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Summary line. 
DataQualityOperator creates a node in the dag 
to count number of rows in the Redshift tables
and raises ERROR when it encounters 0 rows. 
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)        
        errors = []
        self.log.info(self.tables)
        for table in self.tables:            
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                errors.append(f"Data quality check failed. {table} returned no results")
                                
            num_records = records[0][0]
            self.log.info(f"Number of rows in {table} = {num_records}")
            if num_records < 1:
                errors.append(f"Data quality check failed. {table} contained 0 rows")            
                
        if len(errors) > 0:
           for err in errors:
               self.log.info(err)                
           raise ValueError(f"Data quality check failed. Check log for more information")                
        else:
           self.log.info(f"Data quality on tables {self.tables} check passed.")
        
        