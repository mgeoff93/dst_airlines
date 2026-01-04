FROM ghcr.io/mlflow/mlflow:latest

# Installation des dépendances
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Création du dossier pour les artefacts
RUN mkdir -p /mlflow/artifacts

EXPOSE 5000