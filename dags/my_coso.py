import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from dags.function.extraction import scrape_universities

dbt_project_path = Path("/usr/local/airflow/dags/dbt_job/uni_rank/")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snow_flake",
        profile_args={
            "database": "scorecard",
            "schema": "uni_ranking"
            },
        ))

default_args = {
    "owner": "adewunmi",
}


with DAG(
    dag_id="dbt_snowflake",
    start_date=datetime(2025, 3, 25),
    # schedule_interval="@daily",
    default_args=default_args,
) as dag:

    dbt_snowflake_dag = DbtTaskGroup(
        group_id="dbt_flake",
        project_config=ProjectConfig(dbt_project_path,),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",))

    rank_data = PythonOperator(
        task_id='uni_rank',
        python_callable=scrape_universities,
    )

    rank_data >> dbt_snowflake_dag
