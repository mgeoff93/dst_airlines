FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système minimales (souvent nécessaires pour sqlite/psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	&& rm -rf /var/lib/apt/lists/*

# Copier et installer les dépendances
COPY requirements.txt .
RUN pip install --upgrade pip \
	&& pip install --no-cache-dir -r requirements.txt

# Créer le dossier pour les artefacts
RUN mkdir -p /app/mlruns

EXPOSE 5000

# Commande optimisée
CMD ["mlflow", "server", \
	"--backend-store-uri", "sqlite:////app/mlruns/mlflow.db", \
	"--default-artifact-root", "file:///app/mlruns", \
	"--host", "0.0.0.0", \
	"--port", "5000", \
	"--workers", "1"]