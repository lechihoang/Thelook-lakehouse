"""
TPC-DS Lakehouse — Daily Transformation Pipeline

Astronomer Cosmos renders each dbt model as an individual Airflow task.
Execution: each task spins up an ephemeral tpcds-dbt container via Docker.

DAG graph: start >> bronze >> silver >> gold
"""

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

# Mounted at /opt/airflow/dbt — used by Cosmos for model discovery (dbt ls)
DBT_PROJECT_PATH = Path("/opt/airflow/dbt")
DBT_IMAGE        = "tpcds-dbt:latest"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="tpcds_lakehouse",
)

profile_config = ProfileConfig(
    profile_name="tpcds_trino",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.DOCKER,
)

# Passed to every DockerOperator created by Cosmos
operator_args = {
    "image": DBT_IMAGE,
    "network_mode": "data_network",
    "auto_remove": "success",
    "environment": {
        "TRINO_HOST": "trino",
        "TRINO_PORT": "8080",
    },
}

default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="tpcds_dbt_pipeline",
    description="Daily dbt transformations — Bronze → Silver → Gold",
    schedule_interval="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["tpcds", "dbt", "lakehouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze = DbtTaskGroup(
        group_id="bronze",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args=operator_args,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            select=["path:models/bronze"],
        ),
    )

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args=operator_args,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            select=["path:models/silver"],
        ),
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args=operator_args,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            select=["path:models/gold"],
        ),
    )

    start >> bronze >> silver >> gold
