import logging
from uuid import uuid4
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

# Configuration du logging
logging.basicConfig(
	format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt = "%Y-%m-%dT%H:%M:%S",
	level = logging.INFO
)

default_args = {
	"owner": "DST Airlines",
	"start_date": datetime(2026, 1, 11),
	"retries": 1,
	"retry_delay": timedelta(seconds = 30),
}

@dag(
	dag_id = "etl", 
	default_args = default_args,
	schedule = "*/2 * * * *",
	catchup = False,
	max_active_runs = 1,
	tags = ["airlines", "etl"]
)
def flight_data_pipeline():

	def push_dag_metrics(registry):
		"""Envoie les métriques au Pushgateway."""
		try:
			from prometheus_client import push_to_gateway
			gateway_url = Variable.get("PUSHGATEWAY_URL")
			push_to_gateway(gateway_url, job="airflow_dag_etl", registry=registry)
		except Exception as e:
			logging.warning(f"Failed to push DAG metrics: {e}")

	@task
	def requesting(airline_filter: str = "AFR") -> List[Dict]:
		from opensky_client import OpenskyClient
		from prometheus_client import CollectorRegistry, Gauge

		registry = CollectorRegistry()
		metric_extracted = Gauge('etl_extracted_flights_run', 'Vols extraits (run)', registry=registry)
		metric_errors = Gauge('etl_api_errors_run', 'Erreurs API (run)', ['api_name'], registry=registry)

		metric_extracted.set(0)
		metric_errors.labels(api_name='opensky').set(0)

		simulate_error = Variable.get("simulate_api_error", default_var="false").lower() == "true"
		openskycli = OpenskyClient()

		try:
			if simulate_error:
				logging.error("!!! SIMULATION D'ERREUR API ACTIVÉE !!!")
				raw = None
			else:
				raw = openskycli.get_rawdata()

			if raw is None:
				# 1. On marque l'erreur dans la métrique
				metric_errors.labels(api_name='opensky').set(1)
				# 2. On lève l'exception
				raise AirflowFailException("OpenSky API Error")

			flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
			metric_extracted.set(len(flights) if flights else 0)
			
			request_id = str(uuid4())
			for f in flights: f["request_id"] = request_id
			return flights

		finally:
			# 3. LE PLUS IMPORTANT : Le finally s'exécute QUOI QU'IL ARRIVE
			# (succès ou raise AirflowFailException)
			push_dag_metrics(registry)

	@task
	def triage(flights: List[Dict]) -> Dict[str, List[Dict]]:
		from postgres_client import PostgresClient
		from weather_client import WeatherClient
		from prometheus_client import CollectorRegistry, Gauge

		registry = CollectorRegistry()
		metric_triage = Gauge('etl_triage_run', 'Répartition triage (run)', ['type'], registry=registry)
		metric_triage.labels(type='scrape').set(0)
		metric_triage.labels(type='direct').set(0)

		postgrescli = PostgresClient()
		weathercli = WeatherClient()
		needs_scrape, direct_live = [], []

		try:
			for f in flights:
				current_static = postgrescli.get_static_flight(f["callsign"])
				latest_dynamic = postgrescli.get_latest_dynamic_flight(f["callsign"], f["icao24"])
				
				is_incomplete = not current_static or not all([current_static.get("origin_code"), current_static.get("destination_code")])

				if is_incomplete or postgrescli.needs_refresh(f["callsign"], f["icao24"], f["on_ground"]):
					needs_scrape.append(f)
				elif latest_dynamic:
					lat, lon = f.get("latitude"), f.get("longitude")
					if lat and lon: f.update(weathercli.get_weather(lat, lon))
					f.update({"flight_date": latest_dynamic["flight_date"], "unique_key": latest_dynamic["unique_key"]})
					direct_live.append(f)

			metric_triage.labels(type='scrape').set(len(needs_scrape))
			metric_triage.labels(type='direct').set(len(direct_live))
			push_dag_metrics(registry)
			return {"scrape": needs_scrape, "direct": direct_live}
		finally:
			postgrescli.close()

	@task
	def get_scrape_list(res): return res["scrape"]
	
	@task
	def get_direct_list(res): return res["direct"]

	@task(pool="selenium_pool", retries=2)
	def scraping(flight: Dict) -> Optional[Dict]:
		from selenium_client import SeleniumClient
		from postgres_client import PostgresClient
		from flightaware_client import FlightAwareClient
		from weather_client import WeatherClient
	
		seleniumcli = SeleniumClient()
		postgrescli = PostgresClient()
		weathercli = WeatherClient()
		flightawarecli = FlightAwareClient(seleniumcli, postgrescli)
	
		def time_to_str(t): return t.strftime("%H:%M:%S") if t else None
	
		try:
			callsign = flight.get("callsign")
			icao24 = flight.get("icao24")
			lat, lon = flight.get("latitude"), flight.get("longitude")
			
			# Enrichissement météo
			flight.update(weathercli.get_weather(lat, lon))
	
			# Scraping FlightAware
			static_row = flightawarecli.parse_static_flight(callsign)
			if not static_row: 
				return None
	
			dynamic_row = flightawarecli.parse_dynamic_flight(callsign, icao24)
			if dynamic_row:
				dynamic_row.update({
					"departure_scheduled": time_to_str(dynamic_row.get("departure_scheduled")),
					"departure_actual": time_to_str(dynamic_row.get("departure_actual")),
					"arrival_scheduled": time_to_str(dynamic_row.get("arrival_scheduled")),
					"arrival_actual": time_to_str(dynamic_row.get("arrival_actual"))
				})
	
			live_row = {
				**flight,
				"flight_date": dynamic_row["flight_date"] if dynamic_row else None,
				"unique_key": dynamic_row["unique_key"] if dynamic_row else None
			}
	
			return {
				"static_rows": [static_row] if static_row.get("commercial_flight") else [],
				"dynamic_rows": [dynamic_row] if dynamic_row else [],
				"live_rows": [live_row]
			}
		finally:
			seleniumcli.close()
			postgrescli.close()

	@task
	def loading(scrape_results: List[Optional[Dict]], direct_rows: List[Dict]):
		from postgres_client import PostgresClient
		from prometheus_client import CollectorRegistry, Gauge
	
		registry = CollectorRegistry()
		metric_loaded = Gauge('etl_loaded_rows_run', 'Lignes DB insérées (run)', ['table'], registry=registry)
		metric_loaded.labels(table='static').set(0)
		metric_loaded.labels(table='dynamic').set(0)
		metric_loaded.labels(table='live').set(0)
	
		postgrescli = PostgresClient()
		try:
			count_static, count_dynamic, count_live = 0, 0, 0
			
			for res in scrape_results:
				if res:
					s, d, l = res.get("static_rows", []), res.get("dynamic_rows", []), res.get("live_rows", [])
					if s: 
						postgrescli.insert_flight_static(s)
						count_static += len(s)
					if d: 
						postgrescli.insert_flight_dynamic(d)
						count_dynamic += len(d)
					if l: 
						postgrescli.insert_live_data(l)
						count_live += len(l)
	
			if direct_rows:
				postgrescli.insert_live_data(direct_rows)
				count_live += len(direct_rows)
	
			metric_loaded.labels(table='static').set(count_static)
			metric_loaded.labels(table='dynamic').set(count_dynamic)
			metric_loaded.labels(table='live').set(count_live)
			
			push_dag_metrics(registry)
			logging.info(f"Loading terminé: {count_live} lignes live insérées.")
		finally:
			postgrescli.close()

	# --- Orchestration ---
	raw_flights = requesting()
	triage_results = triage(raw_flights)
	
	# Extraction des listes pour le mapping dynamique
	list_to_scrape = get_scrape_list(triage_results)
	list_direct = get_direct_list(triage_results)
	
	# Exécution parallèle du scraping
	scraped_data = scraping.expand(flight=list_to_scrape)
	
	# Chargement final en base
	loading(scrape_results=scraped_data, direct_rows=list_direct)

flight_data_pipeline()