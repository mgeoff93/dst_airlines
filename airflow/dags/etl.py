import logging
from uuid import uuid4
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from prometheus_client import CollectorRegistry, Counter, push_to_gateway

from opensky_client import OpenskyClient
from weather_client import WeatherClient
from flightaware_client import FlightAwareClient
from postgres_client import PostgresClient
from selenium_client import SeleniumClient

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

	# --- Métriques Globales du DAG ---
	def push_dag_metrics(registry):
		try:
			gateway_url = Variable.get("PUSHGATEWAY_URL")
			push_to_gateway(gateway_url, job = "airflow_dag_etl", registry=registry)
		except Exception as e:
			logging.warning(f"Failed to push DAG metrics: {e}")

	@task
	def requesting(airline_filter: str = "AFR") -> List[Dict]:
		registry = CollectorRegistry()
		metric_extracted = Counter('etl_extracted_flights_total', 'Vols extraits d OpenSky', registry=registry)
		
		# --- AJOUT : Métrique de sécurité pour le monitoring des erreurs ---
		metric_errors = Counter('etl_api_errors_total', 'Erreurs critiques lors des appels API', ['api_name'], registry=registry)
		
		logging.info("=== EXTRACT: OpenSky API ===")
		openskycli = OpenskyClient()
		raw = openskycli.get_rawdata()
	
		if raw is None: 
			# On incrémente l'erreur spécifique à OpenSky avant de push et de crash
			metric_errors.labels(api_name='opensky').inc()
			push_dag_metrics(registry)
			raise AirflowFailException("OpenSky quota exceeded or API down. Stopping DAG run.")
	
		flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
		
		if flights:
			metric_extracted.inc(len(flights))
			
		push_dag_metrics(registry)
		
		if not flights: 
			logging.info("No flights matching the filter found.")
			return []
	
		request_id = str(uuid4())
		for f in flights: f["request_id"] = request_id
		return flights

	@task
	def triage(flights: List[Dict]) -> Dict[str, List[Dict]]:
		registry = CollectorRegistry()
		metric_triage = Counter('etl_triage_total', 'Répartition du triage', ['type'], registry=registry)
		
		if not flights:
			logging.info("Triage received an empty flight list.")
			return {"scrape": [], "direct": []}

		postgrescli = PostgresClient()
		weathercli = WeatherClient() 
		needs_scrape = []
		direct_live = []
	
		try:
			for f in flights:
				current_static = postgrescli.get_static_flight(f["callsign"])
				latest_dynamic = postgrescli.get_latest_dynamic_flight(f["callsign"], f["icao24"])
				
				is_incomplete = False
				if current_static:
					is_incomplete = not all([
						current_static.get("airline_name"),
						current_static.get("origin_code"),
						current_static.get("destination_code")
					])
	
				if not current_static or is_incomplete or postgrescli.needs_refresh(f["callsign"], f["icao24"], f["on_ground"]):
					needs_scrape.append(f)
					metric_triage.labels(type='scrape').inc()
				elif latest_dynamic:
					lat, lon = f.get("latitude"), f.get("longitude")
					if lat and lon:
						f.update(weathercli.get_weather(lat, lon))
	
					f.update({
						"departure_scheduled": latest_dynamic["departure_scheduled"].strftime("%H:%M:%S") if latest_dynamic["departure_scheduled"] else None,
						"flight_date": latest_dynamic["flight_date"],
						"unique_key": latest_dynamic["unique_key"]
					})
					direct_live.append(f)
					metric_triage.labels(type='direct').inc()
	
			push_dag_metrics(registry)
			return {"scrape": needs_scrape, "direct": direct_live}
		finally:
			postgrescli.close()

	@task(pool = "selenium_pool", retries = 2, retry_delay = 30)
	def scraping(flight: Dict) -> Optional[Dict]:
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
			if not static_row: return None
	
			dynamic_row = flightawarecli.parse_dynamic_flight(callsign, icao24)
			if dynamic_row:
				for key in ["departure_scheduled", "departure_actual", "arrival_scheduled", "arrival_actual"]:
					dynamic_row[key] = time_to_str(dynamic_row.get(key))
	
			res = {
				"static_rows": [static_row],
				"dynamic_rows": [dynamic_row] if dynamic_row and dynamic_row.get("departure_scheduled") else [],
				"live_rows": []
			}

			if res["dynamic_rows"]:
				live_row = flight.copy()
				live_row.update({
					"departure_scheduled": dynamic_row["departure_scheduled"],
					"flight_date": dynamic_row["flight_date"],
					"unique_key": dynamic_row["unique_key"]
				})
				res["live_rows"] = [live_row]

			return res
		except Exception as e:
			logging.error(f"Scrape failed for {flight.get('callsign')}: {e}")
			return None
		finally:
			seleniumcli.close()
			postgrescli.close()

	@task
	def loading(scrape_results: List[Optional[Dict]], direct_rows: List[Dict]):
		registry = CollectorRegistry()
		metric_loaded = Counter('etl_loaded_rows_total', 'Lignes chargées en DB', ['table'], registry=registry)
		
		postgrescli = PostgresClient()
		try:
			all_static, all_dynamic, all_live = [], [], []
			
			for r in scrape_results:
				if not r: continue
				all_static.extend(r.get("static_rows", []))
				all_dynamic.extend(r.get("dynamic_rows", []))
				all_live.extend(r.get("live_rows", []))

			if direct_rows:
				all_live.extend(direct_rows)

			if all_static: 
				postgrescli.insert_flight_static(all_static)
				metric_loaded.labels(table='static').inc(len(all_static))
			if all_dynamic: 
				postgrescli.insert_flight_dynamic(all_dynamic)
				metric_loaded.labels(table='dynamic').inc(len(all_dynamic))
			if all_live: 
				postgrescli.insert_live_data(all_live)
				metric_loaded.labels(table='live').inc(len(all_live))
			
			push_dag_metrics(registry)
		finally:
			postgrescli.close()

	# --- Orchestration ---
	triage_results = triage(requesting())

	@task
	def get_scrape_list(res): return res["scrape"]
	
	@task
	def get_direct_list(res): return res["direct"]

	list_to_scrape = get_scrape_list(triage_results)
	list_direct = get_direct_list(triage_results)

	scraped_data = scraping.expand(flight = list_to_scrape)
	
	loading(
		scrape_results = scraped_data, 
		direct_rows = list_direct
	)

flight_data_pipeline()