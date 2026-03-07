"""
TPC-DS Lakehouse — Daily Transformation Pipeline
Runs dbt models: Bronze → Silver → Gold
Scheduled at 23:00 every day
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

DBT_VENV    = "/opt/dbt_venv/bin/dbt"
DBT_DIR     = "/opt/airflow/dbt"
PROFILES    = "/opt/airflow/dbt"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="tpcds_dbt_pipeline",
    description="Daily dbt transformations for TPC-DS lakehouse",
    schedule_interval="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["tpcds", "dbt", "lakehouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=(
            f"{DBT_VENV} run --select bronze "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES}"
        ),
    )

    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"{DBT_VENV} run --select silver "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES}"
        ),
    )

    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"{DBT_VENV} run --select gold "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_VENV} test "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {PROFILES}"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_bronze >> dbt_silver >> dbt_gold >> dbt_test >> end
