import logging
import requests
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import mlflow
import mlflow.sklearn

from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, max_error
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class MLClient:
	def __init__(self):
		self.api_url =  Variable.get("AIRFLOW_API_URL")
		self.mlflow_uri = Variable.get("MLFLOW_API_URL")
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		
		mlflow.set_tracking_uri(self.mlflow_uri)
		mlflow.set_experiment("model_arrival_difference")

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		
		# Métriques de Dataset
		self.metric_training_rows = Gauge(
			'ml_training_rows_count', 
			'Nombre de lignes utilisées pour l entraînement',
			registry=self.registry
		)
		# Métriques de Performance
		self.metric_r2_score = Gauge(
			'ml_model_r2_score', 
			'Score R2 du dernier modèle entraîné',
			registry=self.registry
		)
		self.metric_mae = Gauge(
			'ml_model_mae', 
			'Mean Absolute Error du dernier modèle',
			registry=self.registry
		)

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_ml_client', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed for MLClient: {e}")

	def optimize_memory(self, df: pd.DataFrame) -> pd.DataFrame:
		for col in df.columns:
			if df[col].dtype == "float64": df[col] = df[col].astype("float32")
			if df[col].dtype == "int64": df[col] = df[col].astype("int32")
		return df

	def data_preprocessing(self) -> str:
		try:
			res_live = requests.get(f"{self.api_url}/live/history/all", timeout=30)
			res_dynamic = requests.get(f"{self.api_url}/dynamic", params={"status": "history"}, timeout=30)
			
			data_live = res_live.json().get("data", [])
			data_dynamic = res_dynamic.json().get("data", [])

			if not data_live or not data_dynamic:
				raise AirflowSkipException("Pas assez de données en base pour l'entraînement.")

			df_live_history = pd.DataFrame(data_live)
			df_dynamic_history = pd.DataFrame(data_dynamic)

			merge_cols = ["unique_key"]
			target_cols = ["departure_difference", "arrival_difference"]
			df_merged = df_live_history.merge(df_dynamic_history[merge_cols + target_cols], on="unique_key", how="left")

			df_strict = df_merged.dropna(subset=["callsign", "icao24", "longitude", "latitude", "departure_difference", "arrival_difference"])

			features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", "velocity", 
						"global_condition", "departure_difference", "arrival_difference"]

			df_features = df_strict[features].copy()

			for col in ["geo_altitude", "velocity"]: df_features[col] = df_features[col].fillna(0)
			df_features["global_condition"] = df_features["global_condition"].fillna("Unknown")

			df_features = df_features[df_features["arrival_difference"].between(-60, 300)]
			df_features = self.optimize_memory(df_features)

			MAX_ROWS = 500000
			
			# Mise à jour de la métrique du nombre de lignes
			row_count = len(df_features)
			self.metric_training_rows.set(row_count)

			if row_count > MAX_ROWS:
				logging.warning(f"Dataset trop large ({row_count} lignes). Sampling à {MAX_ROWS}.")
				df_features = df_features.sample(n = MAX_ROWS, random_state = 42)
				self.metric_training_rows.set(MAX_ROWS)

			self._push_metrics()

			if len(df_features) < 10:
				raise AirflowSkipException(f"Volume insuffisant : {len(df_features)} lignes.")

			file_path = "/opt/airflow/data/processed_data.parquet"
			df_features.to_parquet(file_path, engine = "pyarrow")
			
			logging.info(f"Data saved to {file_path} (Rows: {len(df_features)})")
			return file_path
		
		except requests.RequestException as e:
			logging.error(f"Erreur API : {e}")
			raise

	def train_and_log_model(self, file_path: str):
		data = pd.read_parquet(file_path)
		TARGET = "arrival_difference"

		X = data.drop(columns = [TARGET])
		y = data[TARGET]

		categorical_cols = ["callsign", "icao24", "global_condition"]
		preprocessor = ColumnTransformer(
			transformers = [
				('cat', OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1), categorical_cols)
			], 
			remainder = "passthrough"
		)

		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

		with mlflow.start_run(run_name = "Optimized model"):
			pipeline = Pipeline(steps=[
				("preprocessor", preprocessor), 
				("regressor", RandomForestRegressor(random_state = 42, n_jobs = -1))
			])            

			param_grid = {
				"regressor__n_estimators": [100],
				"regressor__max_depth": [10, 15],
				"regressor__min_samples_leaf": [5]
			}
			
			grid_search = GridSearchCV(pipeline, param_grid, cv = 2, n_jobs = 1, scoring = "r2")
			grid_search.fit(X_train, y_train)
			
			best_model = grid_search.best_estimator_

			# Logging MLflow
			mlflow.log_params(grid_search.best_params_)
			mlflow.log_dict(data.describe().to_dict(), "dataset_summary.json")

			y_pred = best_model.predict(X_test)

			metrics = {
				"MAE": mean_absolute_error(y_test, y_pred),
				"RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
				"R2_Score": r2_score(y_test, y_pred),
				"Max_Error": max_error(y_test, y_pred)
			}
			mlflow.log_metrics(metrics)

			# --- Mise à jour Prometheus ---
			self.metric_r2_score.set(metrics["R2_Score"])
			self.metric_mae.set(metrics["MAE"])
			self._push_metrics()

			# (Le reste du code de logging d'artefacts et de modèle est identique...)
			# Feature Importance
			importances = best_model.named_steps['regressor'].feature_importances_
			indices = np.argsort(importances)
			plt.figure(figsize = (10, 8))
			plt.barh(range(len(indices)), importances[indices], align = "center", color = 'skyblue')
			plt.yticks(range(len(indices)), [X.columns[i] for i in indices])
			plt.title("Feature Importance (Optimized)")
			plt.savefig("feature_importance.png")
			mlflow.log_artifact("feature_importance.png")
			plt.close()

			# Analyse des Résidus
			plt.figure(figsize = (8, 6))
			plt.scatter(y_pred, y_test - y_pred, alpha = 0.5)
			plt.axhline(y = 0, color = 'r', linestyle = '--')
			plt.title("Analyse des Résidus")
			plt.savefig("residuals.png")
			mlflow.log_artifact("residuals.png")
			plt.close()

			try:
				mlflow.sklearn.log_model(
					sk_model = best_model, 
					name = "model",
					registered_model_name = "ArrivalDelayModel"
				)
			except Exception as e:
				logging.error(f"Erreur lors du log du modèle : {e}")

			return {**metrics, "rows": int(len(data))}