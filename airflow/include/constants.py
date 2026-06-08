from pathlib import Path

# Absolute path to your dbt project inside include/
DBT_PROJECT_PATH = Path(__file__).parent / "dbt" 

# Path to dbt executable inside the venv Cosmos will use
DBT_EXECUTABLE_PATH = Path(__file__).parent / "dbt_venv" / "Scripts" / "dbt"