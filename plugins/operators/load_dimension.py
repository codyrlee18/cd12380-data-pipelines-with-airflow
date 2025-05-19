from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    template_fields = ("sql_query",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id= "",
                 table= "",
                 sql_query= "",
                 truncate_insert= True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table= table
        self.sql_query= sql_query
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info(f"Truncating table {self.table} before loading the new data")
            truncate_sql = f"Truncate Table {self.table}"
            redshift_hook.run(truncate_sql)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query}
        """
        self.log.info(f"Running the SQL query: \n{insert_sql}")

        redshift_hook.run(insert_sql)
        self.log.info(f"Finished loading the data into fact table {self.table}")

