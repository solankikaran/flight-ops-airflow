import sys
from pathlib import Path
from datetime import datetime

from airflow.sdk import dag, task

from scripts.bronze_ingest import run_bronze_ingestion

AIRFLOW_HOME = Path("/opt/airflow")

@dag(
    dag_id="flight_ops_medallion_pipeline",
    start_date=datetime(2026, 5, 25),
    schedule = "@daily"
)
def flight_ops_medallion_pipeline():

    @task
    def bronze():
        return run_bronze_ingestion()
    
    bronze()

flight_ops_medallion_pipeline()


