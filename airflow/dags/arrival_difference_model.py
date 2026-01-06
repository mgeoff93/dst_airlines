import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from ml_client import MLClient

logging.basicConfig(
	format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt = "%Y-%m-%dT%H:%M:%S",
	level = logging.INFO
)

default_args = {
	"owner": "DST Airlines",
	"start_date": datetime(2025, 12, 29),
	"retries": 2,
	"retry_delay": timedelta(seconds = 30),
}

@dag(
	dag_id = "arrival_difference_model",
	default_args = default_args,
	schedule = "0 * * * *",
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