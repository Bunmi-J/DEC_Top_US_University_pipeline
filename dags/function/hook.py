import logging

from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logging.basicConfig(format="%(asctime)s %(message)s")

aws_access_key_id = Variable.get("access_key")
aws_secret_access_key = Variable.get("secret_key")
snowflake_table = "unirank"
s3_bucket = "top-university-bucket"
parquet_path = "transform/"  # Folder inside the S3 bucket


def create_snowflake_table():
    """Function to execute SQL from file in Snowflake"""
    try:
        hook = SnowflakeHook(snowflake_conn_id="snow_flake")
        sql_file_path = "/usr/local/airflow/include/snowflake/create_table.sql"
        with open(sql_file_path, "r") as f:
            create_table_sql = f.read()

        hook.run(create_table_sql)
    except Exception as e:
        logging.info(f"error {e}")


def create_snowflake_stage():
    """Create or replace a Snowflake stage pointing to S3."""
    hook = SnowflakeHook(snowflake_conn_id="snow_flake")
    try:

        sql_query = f"""
        CREATE OR REPLACE STAGE my_snowflake_stage
        URL = 's3://{s3_bucket}/'
        CREDENTIALS = (AWS_KEY_ID = '{aws_access_key_id}', 
                    AWS_SECRET_KEY = '{aws_secret_access_key}')
        FILE_FORMAT = (TYPE = PARQUET);
        """
        hook.run(sql_query)
    except Exception as e:
        logging.info(f"error {e}")


def load_parquet_to_snowflake():
    """Load Parquet file from the Snowflake stage into a Snowflake table."""
    try:
        hook = SnowflakeHook(snowflake_conn_id="snow_flake")

        sql_copy_into = f"""
        COPY INTO {snowflake_table}
        FROM @my_snowflake_stage/{parquet_path}
        FILE_FORMAT = (TYPE = PARQUET);
        """

        hook.run(sql_copy_into)
    except Exception as e:
        logging.info(f"error {e}")
