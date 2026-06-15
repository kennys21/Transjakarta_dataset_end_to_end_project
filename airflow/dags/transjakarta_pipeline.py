from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig

from include.constants import DBT_PROJECT_PATH, DBT_EXECUTABLE_PATH
from include.profiles import my_airflow_db

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transjakarta'],
)
def transjakarta_pipeline() -> None:

    pre_process = EmptyOperator(task_id="pre_process")

    
    transjakarta = DbtTaskGroup(
    group_id="transjakarta_pipeline",
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
    profile_config=my_airflow_db,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
   )

    post_process = EmptyOperator(task_id="post_process")

    pre_process >> transjakarta >> post_process

transjakarta_pipeline()