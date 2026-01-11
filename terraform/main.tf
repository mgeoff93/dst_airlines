terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = "dst-airlines-project" # Remplace par ton ID projet réel
  region  = "europe-west1"
}

# Bucket pour stocker les modèles MLflow (Artéfacts)
resource "google_storage_bucket" "mlflow_storage" {
  name          = "dst-airlines-mlflow-artifacts"
  location      = "EU"
  force_destroy = true
}

# Exemple de base de données Cloud SQL (PostgreSQL)
resource "google_sql_database_instance" "main_db" {
  name             = "dst-airlines-db"
  database_version = "POSTGRES_15"
  region           = "europe-west1"
  settings {
    tier = "db-f1-micro"
  }
}