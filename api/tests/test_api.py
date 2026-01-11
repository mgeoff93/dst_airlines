import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_healthcheck():
	"""Vérifie que l'API est saine et la DB connectée"""
	response = client.get("/healthcheck")
	assert response.status_code == 200
	assert response.json() == {"status": "healthy", "database": "connected"}

def test_prometheus_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "http_request_duration_seconds" in response.text

def test_api_docs_access():
	response = client.get("/docs")
	assert response.status_code == 200