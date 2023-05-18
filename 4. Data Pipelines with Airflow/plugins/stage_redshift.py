import pendulum
from jinja2 import Template

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        s3_bucket="",
        s3_key="",
        jsonpath="auto",
        table_name="",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonpath = jsonpath
        self.table = table_name

    def execute(self, context, *args, **kwargs):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        execution_date = pendulum.parse('2018-11-01')
        year = execution_date.year
        month = execution_date.month
        ds = execution_date.strftime("%Y-%m-%d")

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Execution Date: %s", execution_date)
        self.log.info("Year: %s", year)
        self.log.info("Month: %s", month)
        self.log.info("DS: %s", ds)

        self.log.info("Copying data from S3 to Redshift")
        template = Template(self.s3_key)
        s3_key_rendered = template.render(execution_date=execution_date, year=year, month=month, ds=ds)
        s3_path = f"s3://{self.s3_bucket}/{s3_key_rendered}"
        
        if self.jsonpath != "auto":
            jsonpath = f"s3://{self.s3_bucket}/{self.jsonpath}"
        else:
            jsonpath = self.jsonpath
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            jsonpath,
            year=year,
            month=month
        )
        redshift.run(formatted_sql)