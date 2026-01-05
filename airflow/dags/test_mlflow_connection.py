from airflow.decorators import dag, task
from datetime import datetime
import mlflow
import logging

default_args = {
    "owner": "test",
    "start_date": datetime(2025, 1, 1),
}

@dag(
    dag_id="z_test_mlflow_connection",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["test", "mlflow"]
)
def test_mlflow_dag():

    @task
    def check_mlflow():
        mlflow_uri = "http://mlflow:5000"
        mlflow.set_tracking_uri(mlflow_uri)
        mlflow.set_experiment("Test_Airflow_Connection")
        
        with mlflow.start_run(run_name="Run_Test_Docker"):
            mlflow.log_param("status", "success")
            mlflow.log_metric("connection_check", 1.0)
            logging.info("Succès : Connexion MLflow établie !")

    check_mlflow()

test_mlflow_dag()