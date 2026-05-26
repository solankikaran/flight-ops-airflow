import os
import sys

from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import dag, task

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay" : timedelta(minutes=5)
}

@dag(
    dag_id="flight_ops_medallion_pipeline",
    start_date=datetime(2026, 5, 25),
    default_args=default_args,
    schedule = "@daily",
    catchup=False
)
def flight_ops_medallion_pipeline():

    @task
    def bronze():
        run_bronze_ingestion()
    
    bronze()

flight_ops_medallion_pipeline()


