from fastapi import APIRouter, HTTPException
import mlflow.pyfunc
import pandas as pd
import os
import numpy as np
import logging

# Importation des fonctions logiques des autres routers
from api.routers.live import get_live_current_all
from api.routers.dynamic import get_dynamic_flights, FlightStatus

router = APIRouter(prefix="/prediction", tags=["ML Prediction"])

# Configuration
MLFLOW_URI = os.getenv("MLFLOW_API_URL")
MODEL_NAME = os.getenv("MODEL_NAME", "ArrivalDelayModel")

# Variable globale pour stocker le modèle en mémoire (Singleton)
cached_model = None

def load_prediction_model():
	"""Charge le modèle depuis MLflow avec mise en cache."""
	global cached_model
	if cached_model is not None:
		return cached_model

	if not MLFLOW_URI or not MODEL_NAME:
		logging.error("MLFLOW_API_URL ou MODEL_NAME manquants")
		return None

	try:
		mlflow.set_tracking_uri(MLFLOW_URI)
		model_uri = f"models:/{MODEL_NAME}@production"
		logging.info(f"Tentative de chargement du modèle : {model_uri}")
		cached_model = mlflow.pyfunc.load_model(model_uri)
		logging.info("Modèle chargé avec succès depuis l'alias @production")
		return cached_model
	except Exception as e:
		try:
			logging.warning(f"Alias @production non trouvé ({e}), essai sur 'latest'...")
			cached_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/latest")
			return cached_model
		except Exception as e2:
			logging.error(f"Erreur critique : Impossible de charger le modèle : {e2}")
			return None

@router.post("/arrival_delay_prediction")
async def predict_all_delays():
	"""
	Récupère les données en appelant les fonctions internes de live.py et dynamic.py,
	puis effectue la prédiction de retard à l'arrivée.
	"""
	model = load_prediction_model()
	
	if model is None:
		raise HTTPException(
			status_code=503, 
			detail="Le modèle n'est pas encore prêt ou est introuvable sur MLflow."
		)

	try:
		# CORRECTIF : On passe explicitement les arguments pour écraser les objets fastapi.params.Query
		# Si on ne passe rien, Python utilise l'objet Query() lui-même comme valeur, ce qui fait planter le SQL.
		res_live = get_live_current_all(callsign=None, limit=None)
		res_dyn = get_dynamic_flights(timeline=FlightStatus.live, callsign=None, limit=None)

		# Extraction des listes de données
		data_live = res_live.get("data", [])
		data_dyn = res_dyn.get("data", [])

		if not data_live:
			return {"count": 0, "predictions": [], "message": "Aucun vol en direct (live) trouvé."}

		# 2. Transformation en DataFrames
		df_live = pd.DataFrame(data_live)
		df_dyn = pd.DataFrame(data_dyn)

		# 3. Fusion (Merge)
		df_live["unique_key"] = df_live["unique_key"].astype(str)
		
		if not df_dyn.empty:
			df_dyn["unique_key"] = df_dyn["unique_key"].astype(str)
			df_final = df_live.merge(
				df_dyn[["unique_key", "departure_difference"]], 
				on="unique_key", 
				how="left"
			)
		else:
			df_final = df_live.copy()
			df_final["departure_difference"] = np.nan

		# 4. Préparation des Features
		features = [
			"callsign", "icao24", "longitude", "latitude", "geo_altitude", 
			"velocity", "global_condition", "departure_difference"
		]

		for col in features:
			if col not in df_final.columns:
				df_final[col] = np.nan

		X_input = df_final[features]

		# 5. Prédiction
		predictions = model.predict(X_input)

		# 6. Construction de la réponse
		results = []
		for i in range(len(df_final)):
			results.append({
				"unique_key": str(df_final.iloc[i].get("unique_key", "unknown")),
				"callsign": str(df_final.iloc[i].get("callsign", "unknown")),
				"predicted_delay": round(float(predictions[i]), 2)
			})

		return {
			"status": "success",
			"count": len(results),
			"predictions": results
		}

	except Exception as e:
		logging.error(f"Erreur lors de la prédiction : {e}")
		raise HTTPException(status_code=400, detail=f"Erreur de prédiction : {str(e)}")