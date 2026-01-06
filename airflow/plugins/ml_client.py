import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, mean_absolute_percentage_error, max_error
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder

class MLClient:
    def __init__(self):
        self.api_url = "http://api:8000"
        self.mlflow_uri = "http://mlflow:5000"
        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment("model_arrival_difference")

    def data_preprocessing(self) -> pd.DataFrame:
        df_live_history = pd.DataFrame(requests.get(f"{self.api_url}/live/history/all", timeout=30).json()["data"])
        df_dynamic_history = pd.DataFrame(requests.get(f"{self.api_url}/dynamic", params={"status": "history"}, timeout=30).json()["data"])

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
        
        return df_features

    def train_and_log_model(self, data: pd.DataFrame):
        TARGET = "arrival_difference"

        X = data.drop(columns=[TARGET])
        y = data[TARGET]

        categorical_cols = ["callsign", "icao24", "global_condition"]
        for col in categorical_cols:
            le = LabelEncoder()
            X[col] = le.fit_transform(X[col].astype(str))

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        with mlflow.start_run(run_name = "Run_Optimized_Model"):
            rf = RandomForestRegressor(random_state = 42)
            
            # --- OPTIMISATION : Paramètres plus robustes et calcul plus rapide ---
            param_grid = {
                'n_estimators': [100],
                'max_depth': [10, 15],
                'min_samples_leaf': [5]
            }
            
            grid_search = GridSearchCV(estimator = rf, param_grid = param_grid, cv = 2, n_jobs = 1, scoring = 'r2')
            grid_search.fit(X_train, y_train)
            
            model = grid_search.best_estimator_

            mlflow.log_params(grid_search.best_params_)
            mlflow.log_param("features_used", X.columns.to_list())

            y_pred = model.predict(X_test)

            metrics = {
                "MAE": mean_absolute_error(y_test, y_pred),
                "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
                "R2_Score": r2_score(y_test, y_pred),
                "Max_Error": max_error(y_test, y_pred)
            }
            mlflow.log_metrics(metrics)

            # Graphique Importance
            importances = model.feature_importances_
            indices = np.argsort(importances)
            plt.figure(figsize = (10, 8))
            plt.barh(range(len(indices)), importances[indices], align = "center", color = 'skyblue')
            plt.yticks(range(len(indices)), [X.columns[i] for i in indices])
            plt.title("Feature Importance (Optimized)")
            plt.savefig("feature_importance.png")
            mlflow.log_artifact("feature_importance.png")
            plt.close()

            # Graphique Résidus
            plt.figure(figsize = (8, 6))
            plt.scatter(y_pred, y_test - y_pred, alpha = 0.5)
            plt.axhline(y = 0, color = 'r', linestyle = '--')
            plt.title("Analyse des Résidus")
            plt.savefig("residuals.png")
            mlflow.log_artifact("residuals.png")
            plt.close()

            try:
                mlflow.sklearn.log_model(
                    sk_model = model, 
                    name = "arrival_delay_model",
                    registered_model_name = "ArrivalDelayModel"
                )
            except Exception as e:
                print(f"Erreur lors du log du modèle : {e}")

            return {**metrics, "rows": int(len(data))}