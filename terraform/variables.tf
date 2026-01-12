# --- Base de Données PostgreSQL ---
variable "POSTGRES_HOST" {
  type    = string
  default = "postgres"
}

variable "POSTGRES_PORT" {
  type    = number
  default = 5432
}

variable "POSTGRES_DB" {
  type    = string
  default = "airlines"
}

variable "POSTGRES_USER" {
  type        = string
  description = "Sensible - Défini dans tfvars"
}

variable "POSTGRES_PASSWORD" {
  type        = string
  sensitive   = true
  description = "Sensible - Défini dans tfvars"
}

# --- OpenSky Network API ---
variable "OPENSKY_USERNAME" {
  type = string
}

variable "OPENSKY_PASSWORD" {
  type      = string
  sensitive = true
}

variable "OPENSKY_TOKEN_URL" {
  type    = string
  default = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
}

variable "OPENSKY_TOKEN" {
  type    = string
  default = ""
}

variable "OPENSKY_API_URL" {
  type    = string
  default = "https://opensky-network.org/api/states/all"
}

# --- Weather API ---
variable "WEATHER_API_URL" {
  type    = string
  default = "https://api.weatherapi.com/v1/current.json"
}

variable "WEATHER_API_KEY" {
  type      = string
  sensitive = true
}

variable "WEATHER_TIMEOUT" {
  type    = number
  default = 10
}

variable "WEATHER_FIELDS" {
  type    = list(string)
  default = ["temperature", "wind_speed", "gust_speed", "visibility", "cloud_coverage", "rain", "global_condition"]
}

# --- Selenium & Scrapers ---
variable "FLIGHTAWARE_BASE_URL" {
  type    = string
  default = "https://fr.flightaware.com/live/flight"
}

variable "SELENIUM_REMOTE_URL" {
  type    = string
  default = "http://selenium:4444/wd/hub"
}

variable "SELENIUM_WAIT_TIME" {
  type    = number
  default = 10
}

# --- Airflow & Services ---
variable "CONNECTION_ID" {
  type    = string
  default = "airlines"
}

variable "AIRFLOW_API_URL" {
  type    = string
  default = "http://api:8000"
}

variable "MLFLOW_API_URL" {
  type    = string
  default = "http://mlflow:5000"
}

# --- Monitoring ---
variable "PUSHGATEWAY_URL" {
  type    = string
  default = "pushgateway:9091"
}