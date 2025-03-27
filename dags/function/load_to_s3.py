import logging

import awswrangler as wr
import boto3
from airflow.models import Variable

from function.extraction import match_universities

logging.basicConfig(format="%(asctime)s %(message)s")


def aws_session():
    """
    setting up aws boto3 session credentials
    """
    session = boto3.Session(
        aws_access_key_id=Variable.get("access_key"),
        aws_secret_access_key=Variable.get("secret_key"),
        region_name="eu-west-2",
    )
    return session


def transform_to_s3_parquet():
    """
    This function fetches country data, processes it,
    and writes it to S3 in Parquet format.
    """
    try:
        # Fetch the country data (ensure this is a pandas DataFrame)
        match_uni = match_universities()

        # Check if the data is not None before proceeding
        if match_uni is not None and not match_uni.empty:
            # Write the data to S3 in Parquet format
            wr.s3.to_parquet(
                df=match_uni,
                path="s3://top-university-bucket/transform/",
                boto3_session=aws_session(),
                mode="overwrite",
                dataset=True,
            )

            logging.info("Data successfully written to S3 in Parquet format.")
        else:
            logging.warning("No data available to write to S3.")

    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")
