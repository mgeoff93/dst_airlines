from fastapi import APIRouter, Query, HTTPException
import pandas as pd
import requests
from api.postgres_client import PostgresClient
from api.services.flight_features import dataframe_to_json_safe, build_flight_datasets
from ml.arrival_status_model import train_model, predict

router = APIRouter(prefix="/flights", tags=["Flights"])

# Endpoint existant
@router.get("/dynamic/{callsign}")
def get_flight_dynamic(
	callsign: str,
	dataset: str = Query("normalized", enum=["normalized", "done", "current"])
):
	db = PostgresClient()
	df = pd.DataFrame(db.query(
		"SELECT * FROM flight_dynamic WHERE callsign = %s ORDER BY last_update DESC",
		(callsign,)
	))
	datasets = build_flight_datasets(df)
	return dataframe_to_json_safe(datasets[dataset])

# Nouvel endpoint pour récupérer done + current
@router.get("/all_dynamic")
def get_all_dynamic():
	db = PostgresClient()
	df = pd.DataFrame(db.query("SELECT * FROM flight_dynamic ORDER BY last_update DESC"))
	datasets = build_flight_datasets(df)
	return {
		"done": dataframe_to_json_safe(datasets["done"]),
		"current": dataframe_to_json_safe(datasets["current"])
	}

@router.get("/predict_current")
def predict_current():
	db = PostgresClient()
	df = pd.DataFrame(db.query("SELECT * FROM flight_dynamic ORDER BY last_update DESC"))
	datasets = build_flight_datasets(df)

	done_df = datasets["done"]
	current_df = datasets["current"]

	# Entraîner le modèle sur done
	model, encoder, num_features = train_model(done_df)

	# Prédire arrival_status pour current
	predicted_df = predict(model, encoder, num_features, current_df)

	return dataframe_to_json_safe(predicted_df)