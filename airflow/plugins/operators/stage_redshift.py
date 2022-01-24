from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class StageToRedshiftOperator(BaseOperator):
    
    template_fields = ("s3_key",)
    ui_color = '#358140'
  
   
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="udacity-dend",
                 s3_key="log_data",
                 schema="",
                 table="",
                 region="",
                 json_path="",
                 execution_date="2018-11-01",
                 use_partitioned_data=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.schema=schema
        self.table=table
        self.region=region
        self.json_path=json_path
        self.use_partitioned_data=use_partitioned_data
        self.execution_date=execution_date
              
    def execute(self, context):
        self.log.info('Implementing StageToRedshiftOperator')
        redshift=PostgresHook(
        postgres_conn_id=self.redshift_conn_id)
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        self.log.info(f"execution_date: {self.execution_date}")
        if self.json_path:
            format_path=f"FORMAT AS JSON '{self.json_path}'"
        else: 
            format_path=f"FORMAT AS JSON 'auto'"
        
       # rendered_date = self.execution_date.format(**context)
        date_obj = datetime.strptime(self.execution_date,"%Y-%m-%d")
        self.log.info("Execution_year: {}".format(date_obj.year))
        self.log.info("Execution_month: {}".format(date_obj.month))
        rendered_key_raw = self.s3_key.format(**context)
        self.log.info("Rendered_key_raw: {}".format(rendered_key_raw))
        
        if self.use_partitioned_data == "True":
            self.log.info("Rendered_key_raw: {}".format(rendered_key_raw))
            rendered_key = rendered_key_raw.format(\
                                        date_obj.year, \
                                        date_obj.month)
        else:
            rendered_key = rendered_key_raw
            self.log.info("Rendered_key: {}".format(rendered_key))
                
        s3_path ="s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("S3_path: ".format(s3_path))
              
        copy_query = f"""COPY {self.schema}.{self.table}
                   FROM '{s3_path}'
                   ACCESS_KEY_ID '{credentials.access_key}'
                   SECRET_ACCESS_KEY '{credentials.secret_key}'
                   COMPUPDATE OFF STATUPDATE OFF
                   REGION 'us-west-2'
                        {format_path}
                """
        self.log.info("Executing Redshift COPY operation...")
        redshift.run(copy_query)  
        self.log.info("Redshift COPY operation DONE.")
