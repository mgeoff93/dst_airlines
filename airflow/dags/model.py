import logging
import time
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

logging.basicConfig(
	format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt = "%Y-%m-%dT%H:%M:%S",
	level = logging.INFO
)

default_args = {
	"owner": "DST Airlines",
	"start_date": datetime(2026, 1, 16),
	"retries": 1,
	"retry_delay": timedelta(seconds = 60),
}

@dag(
	dag_id = "model",
	default_args = default_args,
	schedule = "@daily",
	catchup = False,
	tags = ["airlines", "ml", "mlflow"],
	max_active_runs = 1
)
def train_model_dag():

	def push_dag_metrics(registry):
		try:
			from prometheus_client import push_to_gateway
			gateway_url = Variable.get("PUSHGATEWAY_URL")
			push_to_gateway(gateway_url, job = "airflow_dag_model", registry=registry)
		except Exception as e:
			logging.warning(f"Failed to push ML DAG metrics: {e}")

	@task
	def preprocessing():
		from ml_client import MLClient
		client = MLClient()
		return client.data_preprocessing()

	@task
	def training(data_path: str):
		from ml_client import MLClient
		from prometheus_client import CollectorRegistry, Gauge
		
		registry = CollectorRegistry()
		metric_train_duration = Gauge(
			'ml_dag_training_duration_seconds',
			'Temps passe sur la phase d entrainement pure',
			registry=registry
		)

		start_time = time.time()
		client = MLClient()
		
		results = client.train_and_log_model(data_path)

		duration = time.time() - start_time
		metric_train_duration.set(duration)
		push_dag_metrics(registry)

		logging.info(f"Training completed. Promoted: {results.get('promoted')}")
		return results

	@task
	def cleanup(data_path: str):
		"""Supprime le fichier temporaire pour liberer l'espace disque."""
		if os.path.exists(data_path):
			os.remove(data_path)
			logging.info(f"Temporary file {data_path} removed.")

	# Workflow
	path = preprocessing()
	train_results = training(path)
	cleanup(path) << train_results

train_model_dag()