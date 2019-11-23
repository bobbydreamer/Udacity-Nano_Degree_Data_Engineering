from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Summary line. 
StageToRedshiftOperator creates a node in the dag 
to COPY json data from S3 to Redshift staging tables.
"""
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 more_options="",
                 *args, **kwargs):    

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.more_options = more_options
        
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        
        #self.log.info("Key = ", credentials.access_key)
        #self.log.info("Secret = ", credentials.secret_key)
        
        self.log.info("Copying data from S3 to Redshift")
        #self.log.info("s3_key = ",self.s3_key)
        #self.log.info("context = ",context)
        
        #rendered_key = self.s3_key.format(**context)
        #self.log.info("rendered_key : s3_key : ",rendered_key)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.more_options
        )
        
        redshift.run(formatted_sql)
