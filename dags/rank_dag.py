from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from function.extraction import fetch_page, scrape_universities
from function.transform import transform_data
from function.load_to_s3 import raw_scorecard_to_s3, unirank_to_s3
from function.hook import (create_snowflake_table,
                           create_snowflake_stage, load_parquet_to_snowflake)

default_args = {
    'owner': 'airflow'
    }

with DAG(
    dag_id='rank_dag',
    default_args=default_args,
    description='A DAG to rank the data',
    # schedule_interval='@daily',
    start_date=datetime(2025, 3, 25),
    catchup=False
) as dag:

    top_university = PythonOperator(
        task_id='university_ranking',
        python_callable=scrape_universities,
    )

    scorecard_extract = PythonOperator(
        task_id='scorecard_api_extraction',
        python_callable=fetch_page,
    )

    load_raw_scorecard = PythonOperator(
        task_id='raw_scorecard_data',
        python_callable=raw_scorecard_to_s3,
    )

    load_unirank = PythonOperator(
        task_id='rank_uni',
        python_callable=unirank_to_s3,
    )

    transformation = PythonOperator(
        task_id='data_transformation',
        python_callable=transform_data,
    )

    snowflake_table = PythonOperator(
        task_id='snowflake_table_creation',
        python_callable=create_snowflake_table,
    )

    create_stage = PythonOperator(
        task_id='create_stage',
        python_callable=create_snowflake_stage,
    )

    data_to_snowflake = PythonOperator(
        task_id='load_data_to_snowflake',
        python_callable=load_parquet_to_snowflake,
    )

    top_university >> load_unirank
    scorecard_extract >> load_raw_scorecard
    [load_raw_scorecard, load_unirank] >> transformation
    transformation >> [snowflake_table, create_stage]
    [snowflake_table, create_stage] >> data_to_snowflake
