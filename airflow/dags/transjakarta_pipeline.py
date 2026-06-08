from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig

from airflow.include.constants import DBT_PROJECT_PATH, venv_execution_config
from include.profile import my_airflow_db

@dag(
    schedule="@daily",
    start_date=datetime(2023,1,1),
    catchup=False,
    tags=['transjakarta'],
)
def transjakarta_pipeline() -> None:
    
    pre_process = EmptyOperator(task_id="pre_process")
    
    transjakarta = DbtTaskGroup(
        group_id = "transjakarta_pipeline",
        project_config = ProjectConfig(my_transjakarta_path),
        profil_config = my_airflow_db,
        execution_config=venv_execution_config,
    )
    
    post_process = EmptyOperator(task_id="post_processs")
    
    pre_process >> transjakarta >> post_process

transjakarta_pipeline()