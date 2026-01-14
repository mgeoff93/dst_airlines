from fastapi import APIRouter, HTTPException
import mlflow.pyfunc
import pandas as pd
import os
import numpy as np
import logging

from api.routers.live import get_live_current_all
from api.routers.dynamic import get_dynamic_flights, FlightStatus

router = APIRouter(prefix="/prediction", tags=["Prediction"])

MODEL_URI = f"models:/{os.getenv('MODEL_NAME', 'ArrivalDelayModel')}@production"
cached_model = None

def get_model():
	"""Charge le modèle une seule fois au premier appel."""
	global cached_model
	if cached_model is None:
		try:
			mlflow.set_tracking_uri(os.getenv("MLFLOW_API_URL"))
			cached_model = mlflow.pyfunc.load_model(MODEL_URI)
		except Exception:
			# Fallback sur latest si production n'existe pas
			latest_uri = MODEL_URI.replace("@production", "/latest")
			cached_model = mlflow.pyfunc.load_model(latest_uri)
	return cached_model

@router.get("/arrival_delay")
async def predict_all_delays():
	model = get_model()
	if not model:
		raise HTTPException(status_code=503, detail="Modèle indisponible.")

	try:
		# 1. Récupération des données (Arguments forcés à None pour éviter les objets Query)
		live_data = get_live_current_all(None, None).get("data", [])
		dyn_data = get_dynamic_flights(FlightStatus.live, None, None).get("data", [])

		if not live_data:
			return {"count": 0, "predictions": []}

		# 2. Préparation DataFrames
		df_live = pd.DataFrame(live_data)
		df_dyn = pd.DataFrame(dyn_data)

		# 3. Merge et Features
		# On utilise unique_key pour la jointure technique entre live et dynamic
		if not df_dyn.empty:
			df = df_live.merge(df_dyn[["unique_key", "departure_difference"]], on="unique_key", how="left")
		else:
			df = df_live.assign(departure_difference=np.nan)

		features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", "velocity", "global_condition", "departure_difference"]
		
		# S'assure que toutes les colonnes attendues par le modèle existent
		X = df.reindex(columns=features).fillna(0)

		# 4. Prédiction
		df["predicted_delay"] = model.predict(X).round(2)

		# 5. Réponse
		# On renvoie 'indice' au lieu de 'unique_key' dans le résultat final
		return {
			"status": "success",
			"count": len(df),
			"predictions": df[["indice", "predicted_delay"]].to_dict(orient="records")
		}

	except Exception as e:
		logging.error(f"Prediction Error: {e}")
		raise HTTPException(status_code = 400, detail = str(e))