import os
from datetime import timedelta

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
AIRLINES_POSTGRES_DB = os.getenv("AIRLINES_POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- Autres constantes ---
STALE_THRESHOLD = timedelta(hours = 2)