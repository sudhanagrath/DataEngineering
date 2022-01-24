from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
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
                 truncate_table='False',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.columns_list=columns_list
        self.insert_string=insert_string 
        self.truncate_table=truncate_table
      
    def execute(self, context):
        self.log.info('Implementing LoadFactOperator')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Truncating table {self.table_name}')
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))
            self.log.info(f'Inserting into {self.table_name}')
            redshift_hook.run(self.INSERT_SQL.format(
                self.table_name,
                self.columns_list,
                self.insert_string))