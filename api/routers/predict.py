from fastapi import APIRouter, HTTPException
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
import pandas as pd
import os
import numpy as np
import logging
import time

from api.routers.live import get_live_current_all
from api.routers.dynamic import get_dynamic_flights, FlightStatus
from api.metrics import PREDICTION_LATENCY, PREDICTION_COUNT, PREDICTION_OUTPUTS, MODEL_LOAD_STATUS

router = APIRouter(prefix="/prediction", tags=["ML Prediction"])

# Configuration des cibles MLflow
MODEL_NAME = os.getenv('MODEL_NAME', 'ArrivalDelayModel')
MODELS_CONFIG = {
	"champion": f"models:/{MODEL_NAME}@production",
	"challenger": f"models:/{MODEL_NAME}/latest"
}

loaded_models = {}
model_versions = {}  # Dictionnaire pour stocker les versions réelles (ex: {"champion": "5"})

def load_models():
	"""Charge les modèles et récupère leurs numéros de version réels via MlflowClient."""
	mlflow.set_tracking_uri(os.getenv("MLFLOW_API_URL"))
	client = MlflowClient()
	
	for alias, uri in MODELS_CONFIG.items():
		try:
			# 1. Chargement du modèle
			loaded_models[alias] = mlflow.pyfunc.load_model(uri)
			
			# 2. Récupération de la version précise pour le monitoring
			if "@" in uri:
				# Récupère la version associée au tag (ex: @production)
				tag = uri.split("@")[1]
				v_info = client.get_model_version_by_alias(MODEL_NAME, tag)
				model_versions[alias] = v_info.version
			else:
				# Récupère la version la plus récente
				v_info = client.get_latest_versions(MODEL_NAME, stages=["None"])[0]
				model_versions[alias] = v_info.version
			
			MODEL_LOAD_STATUS.labels(model_alias=alias).set(1)
			logging.info(f"Modèle {alias} chargé (Version: {model_versions[alias]})")
			
		except Exception as e:
			logging.error(f"Erreur chargement {alias}: {e}")
			MODEL_LOAD_STATUS.labels(model_alias=alias).set(0)
			model_versions[alias] = "unknown"

@router.get("/arrival_delay")
async def predict_all_delays():
	# Chargement au premier appel
	if not loaded_models:
		load_models()
	
	if "champion" not in loaded_models:
		raise HTTPException(status_code=503, detail="Modèle de production indisponible.")

	try:
		# 1. Acquisition et Préparation des données
		live_data = get_live_current_all(None, None).get("data", [])
		dyn_data = get_dynamic_flights(FlightStatus.live, None, None).get("data", [])

		if not live_data:
			return {"status": "success", "count": 0, "predictions": []}

		df = pd.DataFrame(live_data)
		df_dyn = pd.DataFrame(dyn_data)

		if not df_dyn.empty:
			df = df.merge(df_dyn[["unique_key", "departure_difference"]], on="unique_key", how="left")
		else:
			df = df.assign(departure_difference=np.nan)

		features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", "velocity", "global_condition", "departure_difference"]
		X = df.reindex(columns=features).fillna(0)

		# 2. Inférence et Monitoring
		final_preds = None
		
		for alias, model in loaded_models.items():
			start_time = time.perf_counter()
			preds = model.predict(X)
			duration = time.perf_counter() - start_time

			# Métriques dynamiques avec la vraie version récupérée
			PREDICTION_LATENCY.labels(model_alias=alias).observe(duration)
			
			# Utilisation de la version stockée (ex: "5") au lieu de "v1"
			PREDICTION_COUNT.labels(
				model_alias=alias, 
				model_version=model_versions.get(alias, "unknown")
			).inc()
			
			for p in preds:
				PREDICTION_OUTPUTS.labels(model_alias=alias).observe(p)

			if alias == "champion":
				final_preds = preds.round(2)

		# 3. Réponse
		df["predicted_delay"] = final_preds
		return {
			"status": "success",
			"count": len(df),
			"predictions": df[["indice", "predicted_delay"]].to_dict(orient="records")
		}

	except Exception as e:
		logging.error(f"Prediction Error: {e}")
		raise HTTPException(status_code=400, detail=str(e))