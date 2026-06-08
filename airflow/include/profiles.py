import os
from cosmos import ProfileConfig
from cosmos.profiles import DatabricksTokenProfileMapping

my_airflow_db = ProfileConfig(
    profile_name="transjakarta_dataset",          # must match profiles.yml profile name
    target_name="dev",                    # must match the target in profiles.yml
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",     # Airflow connection ID you'll set in the UI
        profile_args={
            "schema": os.getenv("DBT_SCHEMA", "dbt_kennys21"),
            "catalog": os.getenv("DBT_CATALOG", "transjakarta_dataset"),
        },
    ),
)