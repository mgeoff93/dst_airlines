from fastapi import APIRouter, Query, HTTPException
import pandas as pd
from api.postgres_client import PostgresClient
from api.services.flight_features import dataframe_to_json_safe, build_flight_datasets

router = APIRouter(prefix="/flights", tags=["Flights"])

@router.get("/dynamic/{callsign}")
def get_flight_dynamic(
    callsign: str,
    dataset: str = Query("normalized", enum=["normalized", "done", "current"])
):
    db = PostgresClient()
    df = pd.DataFrame(db.query("SELECT * FROM flight_dynamic WHERE callsign = %s ORDER BY last_update DESC", (callsign,)))

    datasets = build_flight_datasets(df)

    # 🔹 Utiliser la conversion safe
    return dataframe_to_json_safe(datasets[dataset])