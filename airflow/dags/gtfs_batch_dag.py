from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# ---------------------------
# Load & validate environment variables
# ---------------------------
def get_env_or_fail(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value

POSTGRES_USER = get_env_or_fail("POSTGRES_USER")
POSTGRES_PASSWORD = get_env_or_fail("POSTGRES_PASSWORD")
POSTGRES_DB = get_env_or_fail("POSTGRES_DB")
POSTGRES_HOST = get_env_or_fail("POSTGRES_HOST")

with DAG(
        dag_id="gtfs_batch_pipeline",
        start_date=datetime(2025, 8, 14),
        schedule_interval=None,
        catchup=False,
        description="Run PySpark GTFS batch job using spark-submit"
) as dag:
    run_batch_job = BashOperator(
        task_id="run_gtfs_batch_job",
        bash_command="docker exec pyspark-batch spark-submit /app/gtfs_batch_job.py",
        dag=dag,
    )
    run_batch_job