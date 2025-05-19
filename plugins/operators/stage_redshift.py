from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)


    @apply_defaults
    def __init__(self,
                 redshift_conn_id= "",
                 aws_credentials_id= "",
                 table= "",
                 s3_bucket= "",
                 s3_key= "",
                 file_format= "JSON",
                 json_path= "auto",
                 region="us-east-1",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        # using the aws hook to fetch the credentials keys
        self.log.info("Getting AWS Creds")
        aws_hook= AwsBaseHook(self.aws_credentials_id, client_type= 'sts')
        credentials= aws_hook.get_credentials()
        
        # using postgreshook to get connect to redshift and be able to run sql with the aws creds
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Render S3 key")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info("Built the full S3 path")

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS {self.file_format}
            JSON '{self.json_path}'
            REGION '{self.region}';
        """
        
        self.log.info("Running the copy command")
        redshift_hook.run(copy_sql)
        self.log.info(f"Staging complete for table {self.table}")





