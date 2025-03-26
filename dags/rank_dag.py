from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from function.ui import (concurrent_fetch_pages, match_universities,
                         scrape_universities)

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

    rank_data >> concurrent >> similarity
