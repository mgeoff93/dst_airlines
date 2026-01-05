import requests
import pandas as pd

API_URL = "http://api:8000"

def data_preprocessing():
	# Récupération des données (appels API)
	df_live_history = pd.DataFrame(requests.get(f"{API_URL}/live/history/all", timeout = 30).json()["data"])
	df_dynamic_history = pd.DataFrame(requests.get(f"{API_URL}/dynamic", params = {"status":"history"}, timeout = 30).json()["data"])
	# Concaténation des tables
	merge_cols = ["unique_key"]
	target_cols = ["departure_difference", "arrival_difference"]
	df_merged = df_live_history.merge(df_dynamic_history[merge_cols + target_cols],on = "unique_key", how = "left")
	# Suppression des lignes avec NaN dans les colonnes critiques
	df_strict = df_merged.dropna(subset = ["callsign", "icao24", "longitude", "latitude", "departure_difference", "arrival_difference"])
	# Suppression des lignes où toutes les données météo sont nulles
	weather_cols = ["temperature", "wind_speed", "gust_speed", "visibility", "cloud_coverage", "rain", "global_condition"]
	df_weather = df_strict[~df_strict[weather_cols].isna().all(axis = 1)].copy()
	# Sélection des features
	features = [
		'callsign', 'icao24', 'longitude', 'latitude', 'baro_altitude', 'geo_altitude', 'velocity', 'vertical_rate', 'temperature',
		'wind_speed', 'gust_speed', 'visibility', 'cloud_coverage', 'rain', 'global_condition', 'departure_difference', 'arrival_difference'
	]
	df_features = df_weather[features].copy()
	# Remplacement des NaN par 0 dans les données de position
	for col in ["baro_altitude", "geo_altitude", "velocity", "vertical_rate"]: df_features[col] = df_features[col].fillna(0)
	# Remplacement des NaN par 0 dans les données météo numériques
	for col in ["temperature", "wind_speed", "gust_speed", "visibility", "cloud_coverage", "rain"]: df_features[col] = df_features[col].fillna(0)
	# Remplacement des NaN par "Unknown" dans global_condition
	df_features["global_condition"] = df_features["global_condition"].fillna("Unknown")
	return df_features

### ---------------------------------------------------------------------------- #

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


# data = data_preprocessing()

# --- MLflow server ---
mlflow_uri = "http://mlflow:5000"   # URL de ton serveur MLflow dans Docker Compose
mlflow.set_tracking_uri(mlflow_uri)
mlflow.set_experiment("Model_Test")  # nom de l'expérience MLflow

# --- Définir hyperparamètres ---
n_estimators = 100
max_depth = 10
TARGET = "arrival_difference"

# --- Préparer X et y ---
X = data.drop(columns = [TARGET])
y = data[TARGET]

# Encodage des variables catégorielles
categorical_cols = ['callsign', 'icao24', 'global_condition']
encoders = {}
for col in categorical_cols:
	le = LabelEncoder()
	X[col] = le.fit_transform(X[col].astype(str))
	encoders[col] = le

# --- Split train/test ---
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

# --- MLflow run ---
with mlflow.start_run(run_name = "Run_Model_Test"):
	# Initialiser le modèle
	model = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, random_state = 42)
	# Log des paramètres
	mlflow.log_param("model_type", "RandomForestRegressor")
	mlflow.log_param("n_estimators", n_estimators)
	mlflow.log_param("max_depth", max_depth)
	mlflow.log_param("features", X_train.columns.to_list())
	# Entraînement
	model.fit(X_train, y_train)
	# Prédictions & métriques
	y_pred = model.predict(X_test)
	mae = mean_absolute_error(y_test, y_pred)
	mlflow.log_metric("MAE", mae)
	# Log du modèle avec versionning
	mlflow.sklearn.log_model(
		sk_model = model,
		artifact_path = "arrival_delay_model",      # dossier relatif sur MLflow server
		registered_model_name = "ArrivalDelayModel" # nom pour le versionning automatique
	)
	print(f"✅ MAE sur le test set : {mae:.2f}")
	print(f"🔗 Modèle loggé et versionné dans MLflow : http://mlflow:5000/#/experiments")