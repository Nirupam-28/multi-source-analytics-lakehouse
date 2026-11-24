from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "retailpulse-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retailpulse_lakehouse_pipeline",
    description="Multi-source RetailPulse analytics lakehouse pipeline",
    default_args=default_args,
    start_date=datetime(2026, 11, 20),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["retailpulse", "lakehouse", "spark", "iceberg"],
) as dag:

    ingest_api = BashOperator(
        task_id="ingest_api_customers",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m ingestion.api.ingest_customers"
        ),
    )

    ingest_postgres = BashOperator(
        task_id="ingest_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m ingestion.postgres.ingest_postgres"
        ),
    )

    ingest_files = BashOperator(
        task_id="ingest_files",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m ingestion.files.ingest_files"
        ),
    )

    run_silver = BashOperator(
        task_id="run_silver_processing",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m processing.silver.run_silver"
        ),
    )

    run_gold = BashOperator(
        task_id="run_gold_analytics",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m processing.gold.run_gold"
        ),
    )

    [ingest_api, ingest_postgres, ingest_files] >> run_silver >> run_gold
