from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 table = "",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Delete load operation set to TRUE. Running delete statement on table {self.table}")
            redshift_hook.run(f"DELETE FROM {self.table}")
            
        self.log.info(f"Running query to load data into Dimension Table {self.table}")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimension Table {self.table} loaded.")
