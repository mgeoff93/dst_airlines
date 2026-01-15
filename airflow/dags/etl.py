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
		try:
			from prometheus_client import push_to_gateway
			gateway_url = Variable.get("PUSHGATEWAY_URL")
			push_to_gateway(gateway_url, job = "airflow_dag_etl", registry=registry)
		except Exception as e:
			logging.warning(f"Failed to push DAG metrics: {e}")

	@task
	def requesting(airline_filter: str = "AFR") -> List[Dict]:
		from opensky_client import OpenskyClient
		from prometheus_client import CollectorRegistry, Counter
	
		registry = CollectorRegistry()
		metric_extracted = Counter('etl_extracted_flights_total', 'Vols extraits', registry=registry)
		metric_errors = Counter('etl_api_errors_total', 'Erreurs API', ['api_name'], registry=registry)

		# AJOUT : Initialiser les compteurs à 0 pour garantir leur existence
		metric_extracted.inc(0)
		metric_errors.labels(api_name='opensky').inc(0)

		openskycli = OpenskyClient()
		raw = openskycli.get_rawdata()

		if raw is None:
			metric_errors.labels(api_name='opensky').inc()
			push_dag_metrics(registry)
			raise AirflowFailException("OpenSky quota exceeded.")

		flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
		if flights: metric_extracted.inc(len(flights))

		push_dag_metrics(registry)
		request_id = str(uuid4())
		for f in flights: f["request_id"] = request_id
		return flights

	@task
	def triage(flights: List[Dict]) -> Dict[str, List[Dict]]:
		from postgres_client import PostgresClient
		from weather_client import WeatherClient
		from prometheus_client import CollectorRegistry, Counter

		registry = CollectorRegistry()
		metric_triage = Counter('etl_triage_total', 'Triage', ['type'], registry=registry)

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
					metric_triage.labels(type='scrape').inc()
				elif latest_dynamic:
					lat, lon = f.get("latitude"), f.get("longitude")
					if lat and lon: f.update(weathercli.get_weather(lat, lon))
					f.update({"flight_date": latest_dynamic["flight_date"], "unique_key": latest_dynamic["unique_key"]})
					direct_live.append(f)
					metric_triage.labels(type='direct').inc()

			push_dag_metrics(registry)
			return {"scrape": needs_scrape, "direct": direct_live}
		finally:
			postgrescli.close()

	@task
	def get_scrape_list(res): return res["scrape"] # Crucial pour le mapping

	@task
	def get_direct_list(res): return res["direct"]

	@task(pool = "selenium_pool", retries = 2)
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
			flight.update(weathercli.get_weather(lat, lon))
	
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
		from prometheus_client import CollectorRegistry, Counter
	
		registry = CollectorRegistry()
		metric_loaded = Counter('etl_loaded_rows_total', 'Lignes DB', ['table'], registry=registry)
	
		# AJOUT : Initialiser les compteurs à 0
		metric_loaded.labels(table='static').inc(0)
		metric_loaded.labels(table='dynamic').inc(0)
		metric_loaded.labels(table='live').inc(0)
	
		postgrescli = PostgresClient()
		try:
			# Initialiser les compteurs
			count_static = 0
			count_dynamic = 0
			count_live = 0
	
			# Traiter les résultats du scraping
			for res in scrape_results:
				if res:
					static_rows = res.get("static_rows", [])
					dynamic_rows = res.get("dynamic_rows", [])
					live_rows = res.get("live_rows", [])
	
					if static_rows:
						postgrescli.insert_flight_static(static_rows)
						count_static += len(static_rows)
	
					if dynamic_rows:
						postgrescli.insert_flight_dynamic(dynamic_rows)
						count_dynamic += len(dynamic_rows)
	
					if live_rows:
						postgrescli.insert_live_data(live_rows)
						count_live += len(live_rows)
	
			# Traiter les vols directs
			if direct_rows:
				postgrescli.insert_live_data(direct_rows)
				count_live += len(direct_rows)
	
			# Mettre à jour les métriques Prometheus
			metric_loaded.labels(table='static').inc(count_static)
			metric_loaded.labels(table='dynamic').inc(count_dynamic)
			metric_loaded.labels(table='live').inc(count_live)
	
			# Pousser les métriques
			push_dag_metrics(registry)
	
			logging.info(f"Loaded: {count_static} static, {count_dynamic} dynamic, {count_live} live")
	
		finally:
			postgrescli.close()

	# --- Orchestration Corrigée ---
	flights = requesting()
	triage_data = triage(flights)

	# On utilise les tâches intermédiaires pour extraire les listes
	# Cela permet à expand() de recevoir un XComArg direct sans clé dictionnaire
	list_to_scrape = get_scrape_list(triage_data)
	list_direct = get_direct_list(triage_data)

	scraped_data = scraping.expand(flight=list_to_scrape)

	loading(scrape_results=scraped_data, direct_rows=list_direct)

flight_data_pipeline()