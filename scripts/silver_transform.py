import json
import pandas as pd
from datetime import datetime
from pathlib import Path

from airflow.sdk import get_current_context

def run_silver_transform(**context):
    
    context = get_current_context()

    bronze_file_path = context["ti"].xcom_pull(
        key = "bronze_file",
        task_ids = "bronze"
    )

    if bronze_file_path is None:
        raise ValueError("Could not find bronze file path in XCOM")
    
    with open(bronze_file_path) as f:
        raw = json.load(f)
    
    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
        "latitude", "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground"
        ]
    ]

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    silver_output_file = silver_path / f"flights_silver_{timestamp}.csv"

    df.to_csv(silver_output_file, index=False)

    context["ti"].xcom_push(
        key = "silver_file",
        value = str(silver_output_file)
    )