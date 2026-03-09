"""
TheLook Lakehouse — Daily Transformation Pipeline

Astronomer Cosmos renders each dbt model as an individual Airflow task.
Execution: LOCAL mode — dbt runs directly inside the Airflow container.

DAG graph: start >> bronze >> silver >> gold
"""

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="thelook_lakehouse",
)

profile_config = ProfileConfig(
    profile_name="thelook_trino",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

with DAG(
    dag_id="thelook_dbt_pipeline",
    description="Daily dbt transformations — Bronze → Silver → Gold",
    schedule_interval="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["thelook", "dbt", "lakehouse"],
) as dag:
    start = EmptyOperator(task_id="start")

    bronze = DbtTaskGroup(
        group_id="bronze",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/bronze"],
        ),
    )

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/silver"],
        ),
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/gold"],
        ),
    )

    start >> bronze >> silver >> gold
