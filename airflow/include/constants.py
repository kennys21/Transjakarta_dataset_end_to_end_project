from pathlib import Path

# Absolute path to dbt project mounted into the container
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt")

# Path to dbt executable inside the venv Cosmos will use
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")