import os
import sys

from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import dag, task

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate

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

    @task
    def silver():
        run_silver_transform()
    
    @task
    def gold():
        run_gold_aggregate()
    
    
    bronze_task = bronze()
    silver_task = silver()
    gold_task = gold()

    bronze_task >> silver_task >> gold_task

flight_ops_medallion_pipeline()