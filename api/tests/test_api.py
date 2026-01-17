import pytest
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from api.main import app
from api.services import flight_features

client = TestClient(app)

# --- FIXTURES ---

@pytest.fixture
def mock_mlflow_model():
	"""Crée un mock pour le modèle MLflow car le serveur n'existe pas en CI"""
	mock = MagicMock()
	# On simule une réponse de prédiction basée sur le nombre de lignes reçues
	def side_effect(X):
		return np.random.uniform(0, 30, size=len(X))
	mock.predict.side_effect = side_effect
	return mock

# --- 1. TESTS D'INFRASTRUCTURE ---

def test_healthcheck():
	"""Vérifie que l'API est saine et la DB connectée"""
	response = client.get("/healthcheck")
	assert response.status_code == 200
	assert response.json() == {"status": "healthy", "database": "connected"}

def test_prometheus_metrics():
	"""Vérifie l'exposition des métriques (Prometheus)"""
	client.get("/healthcheck")
	response = client.get("/metrics")
	assert response.status_code == 200
	assert "http_request_duration_seconds" in response.text

# --- 2. TESTS FONCTIONNELS (ROUTERS) ---

def test_get_static_flights_filtering():
	"""Teste les filtres et la limite sur les métadonnées statiques"""
	invalid_res = client.get("/static?limit=0")
	assert invalid_res.status_code == 422
	response = client.get("/static?limit=5")
	assert response.status_code == 200
	assert "flights" in response.json()

@pytest.mark.parametrize("timeline", ["all", "live", "historical"])
def test_dynamic_flights_enums(timeline):
	"""Vérifie que les états de l'Enum FlightStatus sont bien gérés"""
	response = client.get(f"/dynamic?timeline={timeline}")
	assert response.status_code == 200
	assert "data" in response.json()

def test_live_projections():
	"""Vérifie que les endpoints live renvoient les bonnes colonnes"""
	weather_res = client.get("/live/current/weather?limit=1")
	assert weather_res.status_code == 200
	if weather_res.json()["count"] > 0:
		fields = weather_res.json()["data"][0].keys()
		assert "wind_speed" in fields
		assert "longitude" in fields

# --- 3. TESTS DE PRÉDICTION (UTILISANT LE SEED SQL) ---

def test_predict_arrival_delay_with_seed_data(mock_mlflow_model):
    """
    Teste l'endpoint de prédiction en vérifiant le format réel de sortie.
    """
    with patch("api.routers.predict.get_model", return_value=mock_mlflow_model):
        response = client.get("/prediction/arrival_delay")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "success"
        assert data["count"] > 0
        
        # On vérifie les clés réellement présentes dans la réponse
        first_pred = data["predictions"][0]
        assert "indice" in first_pred
        assert "predicted_delay" in first_pred
        
        # Optionnel : On peut vérifier que le delay est bien un nombre
        assert isinstance(first_pred["predicted_delay"], (int, float))

def test_predict_503_when_no_model():
	"""Vérifie que l'API prévient si MLflow est inaccessible"""
	with patch("api.routers.predict.get_model", return_value=None):
		response = client.get("/prediction/arrival_delay")
		assert response.status_code == 503
		assert "indisponible" in response.json()["detail"]

# --- 4. TEST DE LA LOGIQUE MÉTIER (DATE WRAP / PASSAGE DE MINUIT) ---

def test_flight_features_date_wrap_logic():
	"""Vérifie que build_flight_datasets gère correctement le passage de minuit"""
	test_data = pd.DataFrame([{
		"unique_key": "TEST-WRAP",
		"callsign": "TEST01",
		"icao24": "ABC123",
		"flight_date": "2026-01-13",
		"departure_scheduled": "23:50:00",
		"departure_actual": "00:10:00",
		"arrival_scheduled": "01:00:00",
		"arrival_actual": "01:15:00",
		"status": "arrived",
		"last_update": "2026-01-14 01:20:00"
	}])

	datasets = flight_features.build_flight_datasets(test_data)
	flight = datasets["done"][0]
	assert flight["departure_difference"] == 20.0
	assert flight["arrival_difference"] == 15.0
	assert flight["departure_actual_ts"].day == 14

# --- 5. TEST DE FILTRAGE DES DONNÉES OBSOLÈTES (STALE DATA) ---

def test_stale_data_filtering():
	"""Vérifie que les vols obsolètes sont filtrés"""
	old_update = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=10)
	test_data = pd.DataFrame([{
		"unique_key": "STALE-FLIGHT",
		"callsign": "GHOST",
		"icao24": "GHOST1",
		"flight_date": "2026-01-13",
		"departure_scheduled": "10:00:00",
		"departure_actual": "10:05:00",
		"arrival_scheduled": "11:00:00",
		"arrival_actual": None,
		"status": "en route",
		"last_update": old_update
	}])

	datasets = flight_features.build_flight_datasets(test_data)
	current_calls = [f["callsign"] for f in datasets["current"]]
	assert "GHOST" not in current_calls