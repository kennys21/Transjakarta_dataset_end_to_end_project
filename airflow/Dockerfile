FROM astrocrpublic.azurecr.io/runtime:3.2-5

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-databricks==1.9.3 && deactivate

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt