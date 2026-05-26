import pandas as pd
from pathlib import Path

from datetime import datetime
from airflow.sdk import get_current_context

def run_gold_aggregate(**context):
    
    context = get_current_context()

    silver_file_path = context["ti"].xcom_pull(
        key = "silver_file",
        task_ids = "silver"
    )

    if silver_file_path is None:
        raise ValueError("Could not find silver file path in XCOM")

    df = pd.read_csv(silver_file_path)

    df_agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),
            on_ground=("on_ground", "sum")
        )
        .reset_index()
    )

    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    gold_file = gold_path / f"flights_gold_{timestamp}.csv"

    df_agg.to_csv(gold_file, index=False)

    context["ti"].xcom_push(
        key = "gold_file",
        value = str(gold_file)
    )

