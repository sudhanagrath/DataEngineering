from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 #table_names=[],
                 dq_checks=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        #self.table_names=table_names
        self.dq_checks=dq_checks
       
    def execute(self, context):
        self.log.info('Implementing DataQualityOperator')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        error_count=0
        test_failed=[]
        self.log.info("Executing Data Quality Query")
        #for table in self.table_names:
            #table = kwargs["params"]["table"]
            #records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            #if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                #self.logging.info(f"Data quality check failed. {table} returned no results")
                #raise ValueError(f"Data quality check failed. {table} returned no results")
            #self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        for check in self.dq_checks:
            test_statement=check.get('check_sql')
            expect_result=check.get('expected_result')
            records=redshift_hook.get_records(test_statement)[0]
            
            if expect_result != records[0]:
                error_count += 1
                test_failed.append(test_statement)
                
            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(test_failed)
                raise ValueError('Data Quality Check failed')
            self.log.info('All Data Quality Checks passed!')
                
        