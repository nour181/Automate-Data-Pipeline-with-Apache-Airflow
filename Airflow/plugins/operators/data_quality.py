import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for check in self.checks:
            records = redshift_hook.get_records(check["test_sql"])
            num_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. table '{}' returned no results".format(check["table"]))
                
            
            elif num_records < 1:
                raise ValueError("there is no rows inside table {} . {} found.".format(num_records, check["table"]))
            else:
                self.log.info("Data quality on table '{}' check passed with {} records".format(check["table"], num_records[0][0]))
       
