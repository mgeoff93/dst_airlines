import pytest
import pandas as pd
from fastapi.testclient import TestClient
from api.main import app
from api.services import flight_features

client = TestClient(app)

# --- 1. TESTS D'INFRASTRUCTURE ---

def test_healthcheck():
	"""Vérifie que l'API est saine et la DB connectée"""
	response = client.get("/healthcheck")
	assert response.status_code == 200
	assert response.json() == {"status": "healthy", "database": "connected"}

def test_prometheus_metrics():
	"""Vérifie l'exposition des métriques (Prometheus)"""
	# 1. On effectue d'abord un appel métier pour "réveiller" les compteurs
	client.get("/static?limit=1") 
	
	# 2. Maintenant on vérifie les métriques
	response = client.get("/metrics")
	assert response.status_code == 200
	assert "db_records_processed_total" in response.text

# --- 2. TESTS FONCTIONNELS (ROUTERS) ---

def test_get_static_flights_filtering():
	"""Teste les filtres et la limite sur les métadonnées statiques"""
	# Test de la validation géométrique de la limite (FastAPI Query)
	invalid_res = client.get("/static?limit=0")
	assert invalid_res.status_code == 422 
	
	# Test de récupération classique
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
	"""Vérifie que les endpoints live renvoient les bonnes colonnes (Weather/Position)"""
	weather_res = client.get("/live/current/weather?limit=1")
	assert weather_res.status_code == 200
	if weather_res.json()["count"] > 0:
		fields = weather_res.json()["data"][0].keys()
		assert "wind_speed" in fields
		assert "cloud_coverage" in fields
		assert "longitude" in fields

def test_merged_endpoint_404():
	"""Vérifie la gestion d'erreur sur un callsign inconnu"""
	response = client.get("/merged/UNKNOWN999")
	assert response.status_code == 404

# --- 3. TEST DE LA LOGIQUE MÉTIER (DATE WRAP / PASSAGE DE MINUIT) ---

def test_flight_features_date_wrap_logic():
	"""
	Vérifie que build_flight_datasets gère correctement le passage de minuit.
	Cas : Départ prévu à 23:50, départ réel à 00:10 (le lendemain).
	Le retard doit être de 20 minutes et non de -1420 minutes.
	"""
	# Simulation d'un vol franchissant minuit
	test_data = pd.DataFrame([{
		"unique_key": "TEST-WRAP",
		"callsign": "TEST01",
		"icao24": "ABC123",
		"flight_date": "2026-01-13",
		"departure_scheduled": "23:50:00",
		"departure_actual": "00:10:00",  # J+1
		"arrival_scheduled": "01:00:00",
		"arrival_actual": "01:15:00",
		"status": "arrived",
		"last_update": "2026-01-14 01:20:00"
	}])

	datasets = flight_features.build_flight_datasets(test_data)
	done_flights = datasets["done"]

	assert len(done_flights) > 0
	flight = done_flights[0]

	# La différence de départ doit être de 20.0 minutes
	assert flight["departure_difference"] == 20.0
	# La différence d'arrivée doit être de 15.0 minutes
	assert flight["arrival_difference"] == 15.0
	
	# Vérification que l'objet datetime a bien été incrémenté au jour suivant
	# Le départ réel doit être le 14 Janvier
	assert flight["departure_actual_ts"].day == 14

# --- 4. TEST DE FILTRAGE DES DONNÉES OBSOLÈTES (STALE DATA) ---

def test_stale_data_filtering():
	"""
	Vérifie que les vols qui n'ont pas été mis à jour depuis longtemps 
	et qui auraient dû arriver sont filtrés (STALE_THRESHOLD).
	"""
	# On simule un vol 'en route' dont la last_update est très vieille (10h avant)
	old_update = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=10)
	
	test_data = pd.DataFrame([{
		"unique_key": "STALE-FLIGHT",
		"callsign": "GHOST",
		"icao24": "GHOST1",
		"flight_date": "2026-01-13",
		"departure_scheduled": "10:00:00",
		"departure_actual": "10:05:00",
		"arrival_scheduled": "11:00:00", # Devrait déjà être arrivé
		"arrival_actual": None,
		"status": "en route",
		"last_update": old_update
	}])

	datasets = flight_features.build_flight_datasets(test_data)
	# Le dataset 'current' ne doit pas contenir ce vol car il est considéré comme 'stale'
	current_calls = [f["callsign"] for f in datasets["current"]]
	assert "GHOST" not in current_calls