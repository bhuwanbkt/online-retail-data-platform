from datetime import datetime
import subprocess
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path("/opt/airflow")


def run_script(script_name):
    script_path = BASE_DIR / "etl" / script_name
    result = subprocess.run(
        ["python", str(script_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


def run_seed_load():
    run_script("load_seed_data.py")


def run_fact_transform():
    run_script("transform_fact_orders.py")


def run_aggregate_transform():
    run_script("transform_aggregates.py")


with DAG(
    dag_id="full_pipeline_dag",
    start_date=datetime(2026, 4, 1),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["etl", "pipeline"],
) as dag:
    seed_task = PythonOperator(
        task_id="load_seed_data",
        python_callable=run_seed_load,
    )

    fact_task = PythonOperator(
        task_id="transform_fact_orders",
        python_callable=run_fact_transform,
    )

    aggregate_task = PythonOperator(
        task_id="transform_aggregates",
        python_callable=run_aggregate_transform,
    )

    seed_task >> fact_task >> aggregate_task