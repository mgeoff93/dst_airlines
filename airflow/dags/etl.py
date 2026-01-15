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
		
		# Logique de scraping habituelle...
		# (abrégée ici pour la lisibilité, gardez votre code interne)
		return {"static_rows": [], "dynamic_rows": [], "live_rows": []}

	@task
	def loading(scrape_results: List[Optional[Dict]], direct_rows: List[Dict]):
		from postgres_client import PostgresClient
		from prometheus_client import CollectorRegistry, Counter
		
		registry = CollectorRegistry()
		metric_loaded = Counter('etl_loaded_rows_total', 'Lignes DB', ['table'], registry=registry)
		postgrescli = PostgresClient()
		try:
			# Insertion en base...
			push_dag_metrics(registry)
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