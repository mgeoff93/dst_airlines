# --- Base de Données PostgreSQL ---
variable "POSTGRES_HOST" {
  description = "Hôte de la base de données (nom du service docker)"
  type        = string
  default     = "postgres"
}

variable "POSTGRES_PORT" {
  description = "Port PostgreSQL"
  type        = number
  default     = 5432
}

variable "POSTGRES_DB" {
  description = "Nom de la base de données"
  type        = string
}

variable "POSTGRES_USER" {
  description = "Utilisateur PostgreSQL"
  type        = string
}

variable "POSTGRES_PASSWORD" {
  description = "Mot de passe PostgreSQL"
  type        = string
  sensitive   = true # Masque la valeur dans les logs console
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
  type = string
}

variable "OPENSKY_TOKEN" {
  type    = string
  default = ""
}

variable "OPENSKY_API_URL" {
  type = string
}

# --- Weather API ---
variable "WEATHER_API_URL" {
  type = string
}

variable "WEATHER_API_KEY" {
  type      = string
  sensitive = true
}

variable "WEATHER_TIMEOUT" {
  type = number
}

variable "WEATHER_FIELDS" {
  type = list(string)
}

# --- Selenium & Scrapers ---
variable "FLIGHTAWARE_BASE_URL" {
  type = string
}

variable "SELENIUM_REMOTE_URL" {
  type = string
}

variable "SELENIUM_WAIT_TIME" {
  type = number
}

# --- Airflow ---
variable "CONNECTION_ID" {
  type = string
}