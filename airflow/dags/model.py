import logging
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from ml_client import MLClient

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logging.basicConfig(
    format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt = "%Y-%m-%dT%H:%M:%S",
    level = logging.INFO
)

default_args = {
    "owner": "DST Airlines",
    "start_date": datetime(2026, 1, 12),
    "retries": 2,
    "retry_delay": timedelta(seconds = 30),
}

@dag(
    dag_id = "model",
    default_args = default_args,
    schedule = None, #"0 * * * *",
    catchup = False,
    tags = ["airlines", "ml", "mlflow"],
)
def train_model_dag():

    def push_dag_metrics(registry):
        try:
            gateway_url = Variable.get("PUSHGATEWAY_URL")
            push_to_gateway(gateway_url, job = "airflow_dag_model", registry=registry)
        except Exception as e:
            logging.warning(f"Failed to push ML DAG metrics: {e}")

    @task
    def preprocessing():
        # Le MLClient gère déjà le comptage des lignes via Prometheus
        client = MLClient()
        return client.data_preprocessing()

    @task
    def training(data_path):
        registry = CollectorRegistry()
        metric_train_duration = Gauge(
            'ml_dag_training_duration_seconds', 
            'Temps passé sur la phase d entraînement pure', 
            registry=registry
        )
        
        start_time = time.time()
        
        client = MLClient()
        results = client.train_and_log_model(data_path)
        
        # Enregistrement du temps d'exécution
        duration = time.time() - start_time
        metric_train_duration.set(duration)
        
        push_dag_metrics(registry)
        
        logging.info(f"Training completed in {duration:.2f}s with R2: {results.get('R2_Score')}")
        return results

    training(preprocessing())

train_model_dag()