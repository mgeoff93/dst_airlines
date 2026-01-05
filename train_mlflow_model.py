from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

API_URL = "http://api:8000/dynamic"
MLFLOW_TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "flight_model_training"


def fetch_training_data(**context):
    response = requests.get(API_URL, params={"status": "history"}, timeout=30)
    response.raise_for_status()

    df = pd.DataFrame(response.json())

    if df.empty:
        raise ValueError("Aucune donnée retournée par /dynamic?status=history")

    context["ti"].xcom_push(key="raw_data", value=df.to_json())


def train_model(**context):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    df = pd.read_json(context["ti"].xcom_pull(key="raw_data"))

    # 🎯 À adapter selon ton schéma réel
    TARGET = "arrival_difference"
    FEATURES = [
        col for col in df.columns
        if col not in [TARGET, "flight_date", "unique_key"]
    ]

    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=100,
        random_state=42,
        n_jobs=-1
    )

    with mlflow.start_run():
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("n_estimators", 100)

        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)

        mlflow.log_metric("mae", mae)
        mlflow.sklearn.log_model(model, "model")

        mlflow.set_tag("data_source", "/dynamic?status=history")


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="train_flight_model_mlflow",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # déclenché manuellement ou par sensor plus tard
    catchup=False,
    default_args=default_args,
    tags=["mlflow", "training"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_training_data",
        python_callable=fetch_training_data,
    )

    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    fetch_data >> train