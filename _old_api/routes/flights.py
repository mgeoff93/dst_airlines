from fastapi import APIRouter, Query, HTTPException
import pandas as pd
import requests
from _old_api.postgres_client import PostgresClient
from _old_api.services.flight_features import dataframe_to_json_safe, build_flight_datasets, prepare_train_test
from ml.arrival_status_model import train_model, predict

router = APIRouter(prefix="/flights", tags=["Flights"])

# Endpoint existant
@router.get("/dynamic/{callsign}")
def get_flight_dynamic(
	callsign: str,
	dataset: str = Query("normalized", enum=["normalized", "done", "current"])
):
	db = PostgresClient()
	df = pd.DataFrame(db.query("SELECT * FROM flight_dynamic WHERE callsign = %s ORDER BY last_update DESC",(callsign,)))
	datasets = build_flight_datasets(df)
	return dataframe_to_json_safe(datasets[dataset])

@router.get("/predict_current")
def predict_current():
	db = PostgresClient()
	df = pd.DataFrame(db.query("SELECT * FROM flight_dynamic ORDER BY last_update DESC"))
	
	datasets = build_flight_datasets(df)
	done = datasets["done"]
	current = datasets["current"]

	X_train, y_train, X_test = prepare_train_test(done, current, db)

	# Entraîner le modèle sur done
	model, encoder, num_features = train_model(X_train.assign(arrival_status = y_train))

	# Prédire arrival_status pour current
	predicted_df = predict(model, encoder, num_features, X_test)

	return dataframe_to_json_safe(predicted_df)