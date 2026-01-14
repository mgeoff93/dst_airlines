import logging
import time
import requests
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import mlflow
import sys

from mlflow.tracking import MlflowClient
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, max_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class MLClient:
	def __init__(self):
		# --- Configuration des Endpoints ---
		self.api_url = Variable.get("AIRFLOW_API_URL")
		self.mlflow_uri = Variable.get("MLFLOW_API_URL")
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		self.model_name = Variable.get("MLFLOW_MODEL_NAME", default_var="ArrivalDelayModel")

		mlflow.set_tracking_uri(self.mlflow_uri)
		mlflow.set_experiment(f"Experiment_{self.model_name}")

		# --- Stratégie de Monitoring (Prometheus) ---
		self.registry = CollectorRegistry()
		self.metric_r2_score = Gauge('ml_model_r2_score', 'Score R2', ['status'], registry=self.registry)
		self.metric_mae = Gauge('ml_model_mae', 'Mean Absolute Error', ['status'], registry=self.registry)
		self.metric_inference_speed = Gauge('ml_model_inference_latency_ms', 'Vitesse inference', ['status'], registry=self.registry)
		self.metric_training_rows = Gauge('ml_training_rows_count', 'Nombre de lignes utilisees', registry=self.registry)

	def _push_metrics(self):
		"""Envoi des métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_ml_client', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed: {e}")

	def optimize_memory(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Réduit l'empreinte RAM des données."""
		for col in df.columns:
			if df[col].dtype == "float64": df[col] = df[col].astype("float32")
			if df[col].dtype == "int64": df[col] = df[col].astype("int32")
		return df

	def data_preprocessing(self) -> str:
		"""Pipeline d'extraction et nettoyage des données Airlines."""
		try:
			res_live = requests.get(f"{self.api_url}/live/history/all", timeout=30)
			res_dynamic = requests.get(f"{self.api_url}/dynamic", params={"status": "history"}, timeout=30)
			
			data_live = res_live.json().get("data", [])
			data_dynamic = res_dynamic.json().get("data", [])

			if not data_live or not data_dynamic:
				raise AirflowSkipException("Données insuffisantes pour l'entraînement.")

			df_live = pd.DataFrame(data_live)
			df_dyn = pd.DataFrame(data_dynamic)

			# Fusion et nettoyage
			df_merged = df_live.merge(df_dyn[["unique_key", "departure_difference", "arrival_difference"]], on="unique_key", how="left")
			df_strict = df_merged.dropna(subset=["departure_difference", "arrival_difference"])

			features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", 
						"velocity", "global_condition", "departure_difference", "arrival_difference"]
			
			df_features = df_strict[features].copy()
			df_features = df_features[df_features["arrival_difference"].between(-60, 300)]
			df_features = self.optimize_memory(df_features)

			row_count = len(df_features)
			self.metric_training_rows.set(row_count)
			self._push_metrics()

			if row_count > 500000:
				df_features = df_features.sample(n=500000, random_state=42)
			
			file_path = "/opt/airflow/data/processed_data.parquet"
			df_features.to_parquet(file_path, engine="pyarrow")
			return file_path
		
		except Exception as e:
			logging.error(f"Erreur Preprocessing : {e}")
			raise

	def train_and_log_model(self, file_path: str):
		"""Entraînement, Audit Visuel et Gatekeeper de Promotion."""
		data = pd.read_parquet(file_path)
		TARGET = "arrival_difference"
		X = data.drop(columns=[TARGET])
		y = data[TARGET]

		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
		
		numeric_cols = ["longitude", "latitude", "geo_altitude", "velocity", "departure_difference"]
		categorical_cols = ["callsign", "icao24", "global_condition"]
		
		preprocessor = ColumnTransformer([
			('num', SimpleImputer(strategy='mean'), numeric_cols),
			('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), categorical_cols)
		])

		with mlflow.start_run(run_name="Champion_Challenger_Run"):
			# Entraînement
			pipeline = Pipeline([
				("prep", preprocessor), 
				("reg", RandomForestRegressor(n_estimators=100, max_depth=10, n_jobs=-1))
			])
			pipeline.fit(X_train, y_train)
			
			# --- 1. METRIQUES STATISTIQUES ---
			y_pred = pipeline.predict(X_test)
			metrics = {
				"R2_Score": r2_score(y_test, y_pred),
				"MAE": mean_absolute_error(y_test, y_pred),
				"RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
				"Max_Error": max_error(y_test, y_pred)
			}
			mlflow.log_metrics(metrics)

			# --- 2. AUDIT VISUEL (Artifacts) ---
			try:
				# Feature Importance
				importances = pipeline.named_steps['reg'].feature_importances_
				feat_names = numeric_cols + categorical_cols
				indices = np.argsort(importances)
				plt.figure(figsize=(10, 8))
				plt.barh(range(len(indices)), importances[indices], align="center")
				plt.yticks(range(len(indices)), [feat_names[i] for i in indices])
				plt.title("Audit : Importance des variables")
				plt.savefig("feature_importance.png")
				mlflow.log_artifact("feature_importance.png")
				plt.close()

				# Analyse des résidus
				plt.figure(figsize=(8, 6))
				plt.scatter(y_pred, y_test - y_pred, alpha=0.3)
				plt.axhline(y=0, color='r', linestyle='--')
				plt.title("Audit : Analyse des résidus")
				plt.savefig("residuals.png")
				mlflow.log_artifact("residuals.png")
				plt.close()
			except Exception as e:
				logging.warning(f"Audit graphique échoué : {e}")

			# --- 3. MESURE OPERATIONNELLE (Latence) ---
			start_inf = time.time()
			_ = pipeline.predict(X_test.iloc[:100])
			latence_ms = ((time.time() - start_inf) / 100) * 1000
			mlflow.log_metric("inference_latency_ms", latence_ms)

			# --- 4. LE GATEKEEPER (Logique de Promotion) ---
			model_size_mb = sys.getsizeof(pipeline) / (1024**2)
			top_3_features = [feat_names[i] for i in np.argsort(importances)[-3:]]
			
			client = MlflowClient()
			try:
				prod_ver = client.get_model_version_by_alias(self.model_name, "production")
				prod_run = client.get_run(prod_ver.run_id)
				champion_r2 = float(prod_run.data.metrics.get("R2_Score", -1))
			except:
				champion_r2 = -1

			checks = {
				"stat": metrics["R2_Score"] > champion_r2,
				"oper": latence_ms < 200,
				"logic": "departure_difference" in top_3_features,
				"tech": model_size_mb < 500
			}

			logging.info(f"Gatekeeper Checks: {checks}")

			# Décision finale
			if all(checks.values()):
				logging.info("PROMOTION VALIDEE")
				mlflow.sklearn.log_model(pipeline, "model", registered_model_name=self.model_name)
				latest_v = client.get_registered_model(self.model_name).latest_versions[0].version
				client.set_registered_model_alias(self.model_name, "production", latest_v)
				
				# Update Prometheus (Le Challenger devient le Champion)
				self.metric_r2_score.labels(status='production').set(metrics["R2_Score"])
				self.metric_mae.labels(status='production').set(metrics["MAE"])
				self.metric_inference_speed.labels(status='production').set(latence_ms)
				# Reset Challenger
				self.metric_r2_score.labels(status='challenger').set(0)
			else:
				logging.warning("PROMOTION REFUSEE")
				# Update Prometheus (On affiche le Challenger à côté du Champion actuel)
				self.metric_r2_score.labels(status='challenger').set(metrics["R2_Score"])
				self.metric_mae.labels(status='challenger').set(metrics["MAE"])
				self.metric_inference_speed.labels(status='challenger').set(latence_ms)
				if champion_r2 != -1:
					self.metric_r2_score.labels(status='production').set(champion_r2)

			self._push_metrics()
			return {**metrics, "promoted": all(checks.values())}