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

# 1. GÉNÉRATION DU FICHIER VARIABLES.JSON POUR AIRFLOW
resource "local_file" "airflow_variables" {
  filename = "${path.module}/../airflow/config/variables.json"
  content = jsonencode({
    "OPENSKY_USERNAME"     = var.OPENSKY_USERNAME
    "OPENSKY_PASSWORD"     = var.OPENSKY_PASSWORD
    "OPENSKY_TOKEN_URL"    = var.OPENSKY_TOKEN_URL
    "OPENSKY_TOKEN"        = var.OPENSKY_TOKEN
    "OPENSKY_API_URL"      = var.OPENSKY_API_URL
    "WEATHER_API_URL"      = var.WEATHER_API_URL
    "WEATHER_API_KEY"      = var.WEATHER_API_KEY
    "WEATHER_TIMEOUT"      = var.WEATHER_TIMEOUT
    "WEATHER_FIELDS"       = var.WEATHER_FIELDS
    "FLIGHTAWARE_BASE_URL" = var.FLIGHTAWARE_BASE_URL
    "SELENIUM_REMOTE_URL"  = var.SELENIUM_REMOTE_URL
    "SELENIUM_WAIT_TIME"   = var.SELENIUM_WAIT_TIME
    "CONNECTION_ID"        = var.CONNECTION_ID
  })
}

# 2. GÉNÉRATION DU FICHIER .ENV POUR DOCKER COMPOSE
resource "local_file" "docker_env" {
  filename = "${path.module}/../.env"
  content  = <<-EOT
    POSTGRES_USER=${var.POSTGRES_USER}
    POSTGRES_PASSWORD=${var.POSTGRES_PASSWORD}
    POSTGRES_DB=${var.POSTGRES_DB}
    AIRFLOW_PROJ_DIR=.
  EOT
}

# 3. RÉSEAUX ET VOLUMES (Partagés avec Docker Compose)
resource "docker_network" "airflow_network" {
  name            = "airflow_network_terraform"
  check_duplicate = true
}

resource "docker_volume" "postgres_data" {
  name = "postgres-db-volume-terraform"
}

resource "docker_volume" "mlflow_artifacts" {
  name = "mlflow-artifacts-terraform"
}