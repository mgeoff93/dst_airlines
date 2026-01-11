terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.2"
    }
  }
}

provider "docker" {
  host = "npipe:////./pipe/docker_engine"
}

# Création du réseau que tes services utilisent
resource "docker_network" "airflow_network" {
  name = "airflow_network_terraform"
  check_duplicate = true
}

# Création du volume pour la base de données
resource "docker_volume" "postgres_data" {
  name = "postgres-db-volume-terraform"
}

# Création du volume pour MLflow
resource "docker_volume" "mlflow_artifacts" {
  name = "mlflow-artifacts-terraform"
}