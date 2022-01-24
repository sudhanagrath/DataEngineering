from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    TRUNCATE_SQL="""
                    TRUNCATE TABLE {};
                """
    INSERT_SQL="""
                  INSERT INTO {} {} {};
                """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='redshift',
                 table_name='',
                 columns_list='',
                 insert_string='',
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.columns_list=columns_list
        self.insert_string=insert_string
        self.append=append
        
    def execute(self, context):
        self.log.info('Implementing LoadDimensionOperator')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        if self.append:
            self.log.info(f'Appending data to {self.table_name}')
            redshift_hook.run(self.INSERT_SQL.format(self.table_name, self.columns_list, self.insert_string))
        else:
            self.log.info(f'deleting-loading into {self.table_name}')
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))
            redshift_hook.run(self.INSERT_SQL.format(
                self.table_name,
                self.columns_list,
                self.insert_string))
