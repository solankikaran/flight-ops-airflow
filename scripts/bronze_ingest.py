import requests
import json
from datetime import datetime
from pathlib import Path

from airflow.sdk import get_current_context

URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion():

    context = get_current_context()

    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    bronze_file = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    with open(bronze_file, "w") as f:
        json.dump(data, f)

    context["ti"].xcom_push(
        key = "bronze_file",
        value = str(bronze_file)
    )