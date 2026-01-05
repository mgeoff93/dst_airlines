from airflow.decorators import dag, task
from datetime import datetime
from ml_client import MLClient


@dag(
	dag_id = "arrival_delay_model",
	start_date = datetime(2025, 1, 1),
	schedule = None,
	catchup = False,
	tags = ["airlines", "ml", "mlflow"],
)
def train_model_dag():

	@task
	def preprocess():
		client = MLClient()
		return client.data_preprocessing()

	@task
	def train(data):
		client = MLClient()
		return client.train_and_log_model(data)

	train(preprocess())

train_model_dag()