from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Summary line. 
LoadDimensionOperator creates a node in the dag 
to load dimension tables via INSERT statement. 
Table name & SQL is passed as parameters. 

Has options to append (or) truncate & insert 
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
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
                 append_rows=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source        
        self.append_rows = append_rows

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_rows == False:
            self.log.info("LoadDimensionOperator : Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_source
        )
        self.log.info(f"LoadDimensionOperator : Executing {formatted_sql} ...")
        redshift.run(formatted_sql)        
