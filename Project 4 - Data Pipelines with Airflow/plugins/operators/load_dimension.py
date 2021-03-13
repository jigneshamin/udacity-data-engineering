from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 insert_select,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id= redshift_conn_id
        self.table = table
        self.insert_select = insert_select

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run(f"TRUNCATE {self.table}")
        
        self.log.info('Loading data into dimensions table')
        
        redshift.run(f"INSERT INTO {self.table} {self.insert_select}")

