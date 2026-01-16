import logging
import time
import requests
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import mlflow
import io
import base64
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
        self.api_url = Variable.get("AIRFLOW_API_URL")
        self.mlflow_uri = Variable.get("MLFLOW_API_URL")
        self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
        self.model_name = Variable.get("MLFLOW_MODEL_NAME", default_var="ArrivalDelayModel")

        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment(f"Experiment_{self.model_name}")

        self.registry = CollectorRegistry()
        # Métriques demandées
        self.metric_r2_score = Gauge('ml_model_r2_score', 'Score R2', ['status'], registry=self.registry)
        self.metric_mae = Gauge('ml_model_mae', 'Mean Absolute Error', ['status'], registry=self.registry)
        self.metric_mse = Gauge('ml_model_mse', 'Mean Squared Error', ['status'], registry=self.registry)
        self.metric_max_error = Gauge('ml_model_max_error', 'Max Error', ['status'], registry=self.registry)
        self.metric_inference_speed = Gauge('ml_model_inference_latency_ms', 'Vitesse inference', ['status'], registry=self.registry)
        self.metric_training_rows = Gauge('ml_training_rows_count', 'Lignes utilisees', registry=self.registry)

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
                raise AirflowSkipException("Donnees insuffisantes.")

            df_features = pd.DataFrame(data_live).merge(
                pd.DataFrame(data_dynamic)[["unique_key", "departure_difference", "arrival_difference"]], 
                on="unique_key", how="left"
            ).dropna(subset=["departure_difference", "arrival_difference"])

            features = ["callsign", "icao24", "longitude", "latitude", "geo_altitude", "velocity", "global_condition", "departure_difference", "arrival_difference"]
            df_features = df_features[features].copy()
            df_features = df_features[df_features["arrival_difference"].between(-60, 300)]
            df_features = self.optimize_memory(df_features)

            self.metric_training_rows.set(len(df_features))
            self._push_metrics()

            path = "/opt/airflow/data/processed_data.parquet"
            df_features.to_parquet(path, engine="pyarrow")
            return path
        except Exception as e:
            logging.error(f"Preprocessing Error: {e}")
            raise

    def train_and_log_model(self, file_path: str):
        data = pd.read_parquet(file_path)
        X = data.drop(columns=["arrival_difference"])
        y = data["arrival_difference"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        preprocessor = ColumnTransformer([
            ('num', SimpleImputer(strategy='mean'), ["longitude", "latitude", "geo_altitude", "velocity", "departure_difference"]),
            ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), ["callsign", "icao24", "global_condition"])
        ])

        with mlflow.start_run(run_name="Champion_Challenger_Run") as run:
            pipeline = Pipeline([("prep", preprocessor), ("reg", RandomForestRegressor(n_estimators=100, max_depth=10, n_jobs=-1))])
            pipeline.fit(X_train, y_train)

            # Calcul des métriques Challenger
            y_pred = pipeline.predict(X_test)
            metrics = {
                "R2_Score": r2_score(y_test, y_pred),
                "MAE": mean_absolute_error(y_test, y_pred),
                "MSE": mean_squared_error(y_test, y_pred),
                "Max_Error": max_error(y_test, y_pred)
            }
            
            # Calcul Latence Inference
            start_inf = time.time()
            _ = pipeline.predict(X_test.iloc[:100])
            latence_ms = ((time.time() - start_inf) / 100) * 1000
            
            mlflow.log_metrics({**metrics, "inference_latency_ms": latence_ms})

            # Récupération du Champion actuel
            client = MlflowClient()
            try:
                prod_ver = client.get_model_version_by_alias(self.model_name, "production")
                prod_run = client.get_run(prod_ver.run_id).data.metrics
                c_metrics = {
                    "R2": prod_run.get("R2_Score", -1),
                    "MAE": prod_run.get("MAE", 0),
                    "MSE": prod_run.get("MSE", 0),
                    "MAX": prod_run.get("Max_Error", 0),
                    "LAT": prod_run.get("inference_latency_ms", 0)
                }
            except:
                c_metrics = {"R2": -1, "MAE": 0, "MSE": 0, "MAX": 0, "LAT": 0}

            # Envoi vers Prometheus (Champion vs Challenger)
            for status, m in [('production', c_metrics), ('challenger', None)]:
                val_r2 = m["R2"] if m else metrics["R2_Score"]
                val_mae = m["MAE"] if m else metrics["MAE"]
                val_mse = m["MSE"] if m else metrics["MSE"]
                val_max = m["MAX"] if m else metrics["Max_Error"]
                val_lat = m["LAT"] if m else latence_ms

                self.metric_r2_score.labels(status=status).set(val_r2 if val_r2 != -1 else 0)
                self.metric_mae.labels(status=status).set(val_mae)
                self.metric_mse.labels(status=status).set(val_mse)
                self.metric_max_error.labels(status=status).set(val_max)
                self.metric_inference_speed.labels(status=status).set(val_lat)

            # Promotion Logic
            promoted = metrics["R2_Score"] > c_metrics["R2"] and latence_ms < 200
            
            if promoted:
                logging.info("PROMOTION VALIDEE")
                # Optionnel : Inscrire le modèle dans MLflow Registry ici
                self.metric_r2_score.labels(status='production').set(metrics["R2_Score"])

            self._push_metrics()
            return {**metrics, "promoted": promoted}