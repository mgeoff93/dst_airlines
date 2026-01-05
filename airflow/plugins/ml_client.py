import requests
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from airflow.models import Variable


class MLClient:
	def __init__(self):
		self.api_url = Variable.get("API_URL", default_var = "http://api:8000")
		self.mlflow_uri = Variable.get("MLFLOW_TRACKING_URI", default_var = "http://mlflow:5000")

		mlflow.set_tracking_uri(self.mlflow_uri)
		mlflow.set_experiment("Model_Test")

	def data_preprocessing(self) -> pd.DataFrame:

		df_live_history = pd.DataFrame(requests.get(f"{self.api_url}/live/history/all", timeout=30).json()["data"])
		df_dynamic_history = pd.DataFrame(requests.get(f"{self.api_url}/dynamic", params = {"status": "history"}, timeout = 30).json()["data"])

		merge_cols = ["unique_key"]
		target_cols = ["departure_difference", "arrival_difference"]
		df_merged = df_live_history.merge(df_dynamic_history[merge_cols + target_cols], on = "unique_key", how = "left")

		df_strict = df_merged.dropna(subset = ["callsign", "icao24", "longitude", "latitude", "departure_difference", "arrival_difference"])

		weather_cols = ["temperature", "wind_speed", "gust_speed", "visibility", "cloud_coverage", "rain", "global_condition"]
		df_weather = df_strict[~df_strict[weather_cols].isna().all(axis = 1)].copy()

		features = ["callsign", "icao24", "longitude", "latitude", "baro_altitude", "geo_altitude", "velocity", "vertical_rate", "temperature",
					"wind_speed", "gust_speed", "visibility", "cloud_coverage", "rain", "global_condition", "departure_difference", "arrival_difference"]
		df_features = df_weather[features].copy()

		for col in ["baro_altitude", "geo_altitude", "velocity", "vertical_rate"]:df_features[col] = df_features[col].fillna(0)

		for col in ["temperature","wind_speed","gust_speed","visibility","cloud_coverage","rain"]:df_features[col] = df_features[col].fillna(0)

		df_features["global_condition"] = df_features["global_condition"].fillna("Unknown")

		return df_features

	def train_and_log_model(self, data: pd.DataFrame):

		TARGET = "arrival_difference"
		n_estimators = 100
		max_depth = 10

		X = data.drop(columns = [TARGET])
		y = data[TARGET]

		categorical_cols = ["callsign", "icao24", "global_condition"]
		for col in categorical_cols:
			le = LabelEncoder()
			X[col] = le.fit_transform(X[col].astype(str))

		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

		with mlflow.start_run(run_name="Run_Model_Test"):

			model = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, random_state = 42)

			mlflow.log_param("model_type", "RandomForestRegressor")
			mlflow.log_param("n_estimators", n_estimators)
			mlflow.log_param("max_depth", max_depth)
			mlflow.log_param("features", X_train.columns.to_list())

			model.fit(X_train, y_train)

			y_pred = model.predict(X_test)
			mae = mean_absolute_error(y_test, y_pred)
			mlflow.log_metric("MAE", mae)

			mlflow.sklearn.log_model(sk_model = model, name = "arrival_delay_model", registered_model_name = "ArrivalDelayModel")

			return {
				"mae": float(mae),
				"rows": int(len(data)),
			}