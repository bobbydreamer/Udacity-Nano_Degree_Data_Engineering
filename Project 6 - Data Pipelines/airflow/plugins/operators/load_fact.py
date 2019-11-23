from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Summary line. 
LoadFactOperator creates a node in the dag 
to load fact table via INSERT statement. 
Table name & SQL is passed as parameters. 
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql_source="",                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("LoadFactOperator : Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_source
        )
        self.log.info(f"LoadFactOperator : Executing {formatted_sql} ...")
        redshift.run(formatted_sql)