# import logging
# from datetime import datetime, timedelta
# from typing import List, Optional, Dict

# from airflow.decorators import dag, task
# import pandas as pd
# import mlflow
# import mlflow.sklearn
# from sklearn.ensemble import RandomForestClassifier

# # Import de ton client Postgres
# from airflow.plugins.clients.postgres_client import PostgresClient
# from api.services.flight_features import build_features  # ton module pour préparer les features

# # --- Logging ---
# logging.basicConfig(
# 	format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
# 	datefmt="%Y-%m-%dT%H:%M:%S",
# 	level=logging.INFO
# )
# logger = logging.getLogger(__name__)

# # --- Default DAG args ---
# default_args = {
# 	"owner": "DST Airlines",
# 	"start_date": datetime(2025, 12, 29),
# 	"retries": 2,
# 	"retry_delay": timedelta(seconds = 30),
# }

# # --- DAG ---
# @dag(
# 	dag_id = "train_arrival_status_model",
# 	default_args = default_args,
# 	schedule = "0 */12 * * *",
# 	catchup = False,
# 	tags = ["ML", "delay", "late"],
# )
# def arrival_status_training_pipeline():

# 	@task()
# 	def fetch_data() -> pd.DataFrame:
# 		"""Récupère les données depuis Postgres"""
# 		logger.info("Fetching flight_dynamic data from DB")
# 		pg = PostgresClient()
# 		sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
# 		rows = pg.query(sql)
# 		df = pd.DataFrame(rows)
# 		logger.info(f"Fetched {len(df)} rows")
# 		return df

# 	@task()
# 	def prepare_features(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
# 		"""Prépare les features et la target"""
# 		logger.info("Building features for arrival_status prediction")
# 		X, y = build_features(df, target="arrival_status")
# 		return {"X": X, "y": y}

# 	@task()
# 	def train_and_log_model(data: Dict[str, pd.DataFrame]):
# 		"""Entraîne le modèle et log dans MLflow"""
# 		X = data["X"]
# 		y = data["y"]

# 		logger.info("Starting MLflow run for arrival_status model")
# 		mlflow.set_experiment("arrival_status_prediction")

# 		with mlflow.start_run():
# 			model = RandomForestClassifier(n_estimators=200, max_depth=12, random_state=42)
# 			model.fit(X, y)

# 			# Log params, metrics et modèle
# 			mlflow.sklearn.log_model(model, "model")
# 			mlflow.log_params({"n_estimators": 200, "max_depth": 12})
# 			accuracy = model.score(X, y)
# 			mlflow.log_metric("accuracy", accuracy)

# 			logger.info(f"Model trained and logged with accuracy: {accuracy}")

# 	# --- DAG flow ---
# 	df = fetch_data()
# 	features = prepare_features(df)
# 	train_and_log_model(features)

# # Instanciation du DAG
# arrival_status_training_pipeline()