import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_healthcheck():
	"""Vérifie que l'API et ses routes de base répondent"""
	response = client.get("/healthcheck")
	assert response.status_code == 200

def test_prometheus_metrics():
	"""Vérifie que l'instrumentation Prometheus est active"""
	response = client.get("/metrics")
	assert response.status_code == 200
	assert "http_requests_total" in response.text

def test_api_docs_access():
	"""Vérifie que la documentation Swagger est générée"""
	response = client.get("/docs")
	assert response.status_code == 200