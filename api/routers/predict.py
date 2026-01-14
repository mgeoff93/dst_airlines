from fastapi import APIRouter, HTTPException
import mlflow.pyfunc
import pandas as pd
import os
import numpy as np
import logging
import time

# Import des métriques définies dans ton plan
from api.metrics import PREDICTION_LATENCY, PREDICTION_COUNT, PREDICTION_OUTPUTS, MODEL_LOAD_STATUS

from api.routers.live import get_live_current_all
from api.routers.dynamic import get_dynamic_flights, FlightStatus

router = APIRouter(tags=["Prediction"])

MODEL_URI = f"models:/{os.getenv('MODEL_NAME', 'ArrivalDelayModel')}@production"

# Gestion du Cache avec TTL
cached_model = None
last_update_ts = 0
CACHE_TTL = 300  # 5 minutes : l'API vérifiera MLflow toutes les 5 min pour une nouvelle version
current_model_version = "unknown"

def get_model():
	global cached_model, last_update_ts, current_model_version
	now = time.time()

	if cached_model is None or (now - last_update_ts) > CACHE_TTL:
		try:
			mlflow.set_tracking_uri(os.getenv("MLFLOW_API_URL"))
			new_model = mlflow.pyfunc.load_model(MODEL_URI)
			cached_model = new_model
			last_update_ts = now
			
			# --- EXTRACTION DE LA VERSION ---
			# On récupère la version depuis les métadonnées de l'artefact
			current_model_version = new_model.metadata.model_uuid[:8] # Un ID court ou...
			# Si tu veux la version exacte du registre :
			client = mlflow.tracking.MlflowClient()
			model_name = os.getenv('MODEL_NAME', 'ArrivalDelayModel')
			latest_version = client.get_model_version_by_alias(model_name, "production").version
			current_model_version = f"v{latest_version}"
			
			MODEL_LOAD_STATUS.labels(model_alias="production").set(1)
			logging.info(f"Modèle Production {current_model_version} rafraîchi.")
		except Exception as e:
			logging.error(f"Erreur lors du rafraîchissement MLflow : {e}")
			# Si le chargement échoue mais qu'on a un vieux modèle, on le garde pour le service
			if cached_model is None:
				MODEL_LOAD_STATUS.labels(model_alias="production").set(0)
				# Fallback sur latest si jamais production est introuvable au démarrage
				try:
					latest_uri = MODEL_URI.replace("@production", "/latest")
					cached_model = mlflow.pyfunc.load_model(latest_uri)
				except Exception:
					return None
	return cached_model

@router.get("/prediction/arrival_delay")
async def predict_all_delays():
	model = get_model()
	if not model:
		raise HTTPException(status_code=503, detail="Modèle indisponible.")

	try:
		# 1. Récupération des données (fonctionnel comme avant)
		live_data = get_live_current_all(None, None).get("data", [])
		dyn_data = get_dynamic_flights(FlightStatus.live, None, None).get("data", [])

		if not live_data:
			return {"count": 0, "predictions": []}

		# 2. Préparation DataFrames
		df_live = pd.DataFrame(live_data)
		df_dyn = pd.DataFrame(dyn_data)

		# 3. Merge et Features (on garde ta logique unique_key)
		if not df_dyn.empty:
			df = df_live.merge(df_dyn[["unique_key", "departure_difference"]], on="unique_key", how="left")
		else:
			df = df_live.assign(departure_difference=np.nan)

		features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", "velocity", "global_condition", "departure_difference"]
		X = df.reindex(columns=features).fillna(0)

		# 4. Prédiction avec Monitoring (Piliers B et C du plan)
		start_time = time.time()
		
		preds = model.predict(X)
		
		# Enregistrement des métriques Prometheus
		duration = time.time() - start_time
		PREDICTION_LATENCY.labels(model_alias="production").observe(duration)
		PREDICTION_COUNT.labels(model_alias="production", model_version=current_model_version).inc()
		for p in preds:
			PREDICTION_OUTPUTS.labels(model_alias="production").observe(p)

		df["predicted_delay"] = preds.round(2)

		# 5. Réponse
		return {
			"status": "success",
			"count": len(df),
			"predictions": df[["indice", "predicted_delay"]].to_dict(orient="records")
		}

	except Exception as e:
		logging.error(f"Prediction Error: {e}")
		raise HTTPException(status_code=400, detail=str(e))