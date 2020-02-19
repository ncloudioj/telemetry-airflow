from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import gke_command

default_args = {
    "owner": "dthorn@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 24),
    "email": [
        "telemetry-alerts@mozilla.com",
        "dthorn@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("shredder", default_args=default_args, schedule_interval=timedelta(days=28))
docker_image = "mozilla/bigquery-etl:latest"
base_command = [
    "script/shredder_delete",
    "--state-table=moz-fx-data-shared-prod.shredder.state",
    "--start-date={{prev_ds}}",
    "--end-date={{ds}}",
]

on_demand = gke_command(
    task_id="on_demand",
    command=base_command + [
        "--billing-project=moz-fx-data-shared-prod",
        "--only=telemetry_stable.main_v4",
    ],
    docker_image=docker_image,
    dag=dag,
)

flat_rate = gke_command(
    task_id="flat_rate",
    command=base_command + [
        "--billing-project=moz-fx-data-bq-batch-prod",
        "--except=telemetry_stable.main_v4",
    ],
    docker_image=docker_image,
    dag=dag,
)
