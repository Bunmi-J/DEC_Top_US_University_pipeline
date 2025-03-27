from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from function.extraction import (concurrent_fetch_pages, match_universities,
                                 scrape_universities)
from function.load_to_s3 import transform_to_s3_parquet
from function.hook import (create_snowflake_table,
                           create_snowflake_stage, load_parquet_to_snowflake)

default_args = {
    'owner': 'airflow'
    }

with DAG(
    dag_id='rank_dag',
    default_args=default_args,
    description='A DAG to rank the data',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 25),
    catchup=False
) as dag:

    rank_data = PythonOperator(
        task_id='uni_rank',
        python_callable=scrape_universities,
    )

    concurrent = PythonOperator(
        task_id='resposnse_fetch',
        python_callable=concurrent_fetch_pages,
    )

    similarity = PythonOperator(
        task_id='compare',
        python_callable=match_universities,
    )

    load_parquet = PythonOperator(
        task_id='transform_to_s3',
        python_callable=transform_to_s3_parquet,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_snowflake_table,
    )

    sta_ctage_task = PythonOperator(
        task_id="create_stage",
        python_callable=create_snowflake_stage,
    )

    load_parquet_to_snowflake_task = PythonOperator(
        task_id="load_parquet_to_snowflake",
        python_callable=load_parquet_to_snowflake,
    )

    rank_data >> concurrent >> similarity >> load_parquet >> create_table_task
    create_table_task >> sta_ctage_task >> load_parquet_to_snowflake_task
