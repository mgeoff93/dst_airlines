import logging
import requests
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, max_error
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
# Correction : ordinal encoder gère mieux les catégories inconnues avec handle_unknown
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class MLClient:
	def __init__(self):
		# Configuration depuis Airflow Variables
		self.api_url = Variable.get("AIRFLOW_API_URL")
		self.mlflow_uri = Variable.get("MLFLOW_API_URL")
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		self.model_name = Variable.get("MLFLOW_MODEL_NAME", default_var="ArrivalDelayModel")

		mlflow.set_tracking_uri(self.mlflow_uri)
		mlflow.set_experiment(f"Experiment_{self.model_name}")

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		self.metric_training_rows = Gauge('ml_training_rows_count', 'Lignes utilisées', registry=self.registry)
		self.metric_r2_score = Gauge('ml_model_r2_score', 'Score R2', registry=self.registry)
		self.metric_mae = Gauge('ml_model_mae', 'MAE', registry=self.registry)

	def _push_metrics(self):
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_ml_client', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed: {e}")

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
				raise AirflowSkipException("Données insuffisantes.")

			df_live = pd.DataFrame(data_live)
			df_dyn = pd.DataFrame(data_dynamic)

			# Fusion stricte sur les cibles
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
		data = pd.read_parquet(file_path)
		TARGET = "arrival_difference"

		X = data.drop(columns=[TARGET])
		y = data[TARGET]

		# --- Pipeline de transformation (Embarquée pour la prod) ---
		categorical_cols = ["callsign", "icao24", "global_condition"]
		numeric_cols = [c for c in X.columns if c not in categorical_cols]

		numeric_transformer = Pipeline(steps=[
			('imputer', SimpleImputer(strategy='constant', fill_value=0))
		])

		categorical_transformer = Pipeline(steps=[
			('imputer', SimpleImputer(strategy='constant', fill_value='Unknown')),
			('encoder', OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1))
		])

		preprocessor = ColumnTransformer(transformers=[
			('num', numeric_transformer, numeric_cols),
			('cat', categorical_transformer, categorical_cols)
		])

		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		with mlflow.start_run(run_name="Champion-Challenger-Training"):
			pipeline = Pipeline(steps=[
				("preprocessor", preprocessor), 
				("regressor", RandomForestRegressor(random_state=42, n_jobs=-1))
			])            

			param_grid = {
				"regressor__n_estimators": [100],
				"regressor__max_depth": [10, 15],
			}
			
			grid_search = GridSearchCV(pipeline, param_grid, cv=2, n_jobs=1, scoring="r2")
			grid_search.fit(X_train, y_train)
			best_model = grid_search.best_estimator_

			# 1. Logging des paramètres et résumé
			mlflow.log_params(grid_search.best_params_)
			mlflow.log_dict(data.describe().to_dict(), "dataset_summary.json")

			# 2. Métriques
			y_pred = best_model.predict(X_test)
			metrics = {
				"MAE": mean_absolute_error(y_test, y_pred),
				"RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
				"R2_Score": r2_score(y_test, y_pred),
				"Max_Error": max_error(y_test, y_pred)
			}
			mlflow.log_metrics(metrics)
			
			# Prometheus
			self.metric_r2_score.set(metrics["R2_Score"])
			self.metric_mae.set(metrics["MAE"])
			self._push_metrics()

			# 3. Graphiques
			# Feature Importance
			try:
				importances = best_model.named_steps['regressor'].feature_importances_
				# On récupère les noms de colonnes après transformation
				feat_names = numeric_cols + categorical_cols
				indices = np.argsort(importances)
				plt.figure(figsize=(10, 8))
				plt.barh(range(len(indices)), importances[indices], align="center", color='skyblue')
				plt.yticks(range(len(indices)), [feat_names[i] for i in indices])
				plt.title("Feature Importance")
				plt.savefig("feature_importance.png")
				mlflow.log_artifact("feature_importance.png")
				plt.close()

				# Analyse des Résidus
				plt.figure(figsize=(8, 6))
				plt.scatter(y_pred, y_test - y_pred, alpha=0.3)
				plt.axhline(y=0, color='r', linestyle='--')
				plt.title("Analyse des Résidus")
				plt.xlabel("Prédit")
				plt.ylabel("Erreur (Réel - Prédit)")
				plt.savefig("residuals.png")
				mlflow.log_artifact("residuals.png")
				plt.close()
			except Exception as e:
				logging.warning(f"Erreur lors de la génération des graphiques : {e}")

			# 4. Sauvegarde et Enregistrement
			mlflow.sklearn.log_model(
				sk_model=best_model, 
				artifact_path="model",
				registered_model_name=self.model_name
			)

			# 5. Logique de Promotion (Champion vs Challenger)
			client = MlflowClient()
			new_r2 = metrics["R2_Score"]
			try:
				prod_ver = client.get_model_version_by_alias(self.model_name, "production")
				prod_run = client.get_run(prod_ver.run_id)
				prod_r2 = float(prod_run.data.metrics.get("R2_Score", -1))

				logging.info(f"Challenge: New R2 {new_r2:.4f} vs Prod R2 {prod_r2:.4f}")

				if new_r2 > prod_r2:
					logging.info("--- NOUVEAU CHAMPION PROMOTE EN PRODUCTION ---")
					latest_v = client.get_registered_model(self.model_name).latest_versions[0].version
					client.set_registered_model_alias(self.model_name, "production", latest_v)
				else:
					logging.info("Le Challenger a échoué. La production reste inchangée.")

			except Exception:
				logging.info("Aucun champion en titre. Promotion automatique de la Version 1.")
				latest_v = client.get_registered_model(self.model_name).latest_versions[0].version
				client.set_registered_model_alias(self.model_name, "production", latest_v)

			return {**metrics, "rows": int(len(data))}