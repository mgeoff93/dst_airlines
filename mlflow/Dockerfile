FROM python:3.11-slim

WORKDIR /app

# Copier les dépendances MLflow
COPY requirements.txt .

# Installer MLflow + dépendances éventuelles
RUN pip install --upgrade pip \
	&& pip install --no-cache-dir -r requirements.txt

# Dossier pour artefacts
RUN mkdir -p /app/mlruns

# Exposer le port
EXPOSE 5000

# Commande pour lancer MLflow
CMD ["mlflow", "server", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "/app/mlruns", "--host", "0.0.0.0", "--port", "5000"]