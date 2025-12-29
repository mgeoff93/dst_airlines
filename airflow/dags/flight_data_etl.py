import logging
from uuid import uuid4
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from airflow.decorators import dag, task

from opensky_client import OpenskyClient
from weather_client import WeatherClient
from flightaware_client import FlightAwareClient
from postgres_client import PostgresClient
from selenium_client import SeleniumClient

logging.basicConfig(
	format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt="%Y-%m-%dT%H:%M:%S",
	level=logging.INFO
)

# --- Default DAG args ---
default_args = {
	"owner": "DST Airlines",
	"start_date": datetime(2025, 12, 29),  # date fixe
	"retries": 2,
	"retry_delay": timedelta(seconds=30),
}

@dag(
	dag_id="flight_data_etl",
	default_args=default_args,
	schedule="*/5 * * * *",
	catchup=False,
	tags=["airlines", "etl", "production"]
)
def flight_data_pipeline():

	@task
	def extract_flights(airline_filter: str = "AFR") -> List[Dict]:
		logging.info("=== EXTRACT: OpenSky API ===")
		openskycli = OpenskyClient()
		raw = openskycli.get_rawdata()

		if not raw:
			logging.warning("No data from OpenSky")
			return []

		flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
		if not flights:
			logging.warning(f"No flights after filter '{airline_filter}'")
			return []

		request_id = str(uuid4())
		for f in flights:
			f["request_id"] = request_id

		logging.info(f"Extracted {len(flights)} flights (request_id={request_id})")
		return flights

	@task(pool="selenium_pool")
	def scrape_single_flight(flight: Dict) -> Optional[Dict]:
		seleniumcli = SeleniumClient()
		postgrescli = PostgresClient()
		weathercli = WeatherClient()
		flightawarecli = FlightAwareClient(seleniumcli, postgrescli)
	
		try:
			callsign = flight.get("callsign")
			icao24 = flight.get("icao24")
			request_id = flight.get("request_id")
	
			# --- Parsing statique ---
			static_row = flightawarecli.parse_static_flight(callsign)
			if not static_row:
				return None
	
			origin_code = static_row.get("origin_code")
			destination_code = static_row.get("destination_code")
			airline_name = static_row.get("airline_name")
	
			# --- CAS : statique incomplet → flight_static ONLY ---
			if not origin_code or not destination_code or not airline_name:
				static_row["commercial_flight"] = None
				logging.info(
					f"{callsign}: static incomplete "
					f"(origin={origin_code}, dest={destination_code}, airline={airline_name})"
				)
				return {
					"static_rows": [static_row],
					"dynamic_rows": [],
					"live_rows": []
				}
	
			# --- CAS : vol explicitement non commercial ---
			if static_row.get("commercial_flight") is False:
				return {
					"static_rows": [static_row],
					"dynamic_rows": [],
					"live_rows": []
				}
	
			# --- Ajout météo ---
			lat, lon = flight.get("latitude"), flight.get("longitude")
			flight.update(weathercli.get_weather(lat, lon))
	
			# --- Parsing dynamique ---
			dynamic_row = flightawarecli.parse_dynamic_flight(callsign, icao24)
			if not dynamic_row or not dynamic_row.get("departure_scheduled"):
				logging.warning(
					f"Skipping flight {callsign}: missing departure_scheduled or dynamic data"
				)
				return {
					"static_rows": [static_row],
					"dynamic_rows": [],
					"live_rows": []
				}
	
			live_row = flight.copy()
			live_row["request_id"] = request_id
			live_row["departure_scheduled"] = dynamic_row["departure_scheduled"]
			live_row["flight_date"] = dynamic_row["flight_date"]
	
			return {
				"static_rows": [static_row],
				"dynamic_rows": [dynamic_row],
				"live_rows": [live_row]
			}
	
		except Exception as e:
			logging.error(
				f"Scrape failed for {flight.get('callsign', 'UNKNOWN')}: {e}",
				exc_info=True
			)
			raise
		
		finally:
			seleniumcli.close()
			postgrescli.close()

	@task
	def load_all_to_postgres(results: List[Optional[Dict]]):
		logging.info("=== LOAD: PostgreSQL ===")
		postgrescli = PostgresClient()

		try:
			all_static, all_dynamic, all_live = [], [], []

			for r in results:
				if not r:
					continue
				all_static.extend(r["static_rows"])
				all_dynamic.extend(r["dynamic_rows"])
				all_live.extend(r["live_rows"])

			if all_static:
				postgrescli.insert_flight_static(all_static)
			if all_dynamic:
				postgrescli.insert_flight_dynamic(all_dynamic)
			if all_live:
				postgrescli.insert_live_data(all_live)

			logging.info(
				f"Loaded: {len(all_static)} static, "
				f"{len(all_dynamic)} dynamic, "
				f"{len(all_live)} live"
			)

		except Exception as e:
			logging.error(f"Load failed: {e}", exc_info=True)
			raise

		finally:
			postgrescli.close()

	# --- DAG flow ---
	flights_list = extract_flights(airline_filter="AFR")
	scraped_results = scrape_single_flight.expand(flight=flights_list)
	load_all_to_postgres(scraped_results)

flight_data_pipeline()