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

# Configuration du logging
logging.basicConfig(
	format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt = "%Y-%m-%dT%H:%M:%S",
	level = logging.INFO
)

default_args = {
	"owner": "DST Airlines",
	"start_date": datetime(2025, 12, 29),
	"retries": 1, # Réduit pour tenir dans les 5 minutes
	"retry_delay": timedelta(seconds = 30),
}

@dag(
	dag_id = "etl",
	default_args = default_args,
	schedule = None,
	catchup = False,
	tags = ["airlines", "etl"]
)
def flight_data_pipeline():

	@task
	def requesting(airline_filter: str = "AFR") -> List[Dict]:
		logging.info("=== EXTRACT: OpenSky API ===")
		openskycli = OpenskyClient()
		raw = openskycli.get_rawdata()
		if not raw: return []
		flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
		if not flights: return []
		request_id = str(uuid4())
		for f in flights: f["request_id"] = request_id
		return flights

	@task
	def triage(flights: List[Dict]) -> List[Dict]:
		postgrescli = PostgresClient()
		needs_scrape = []
		try:
			for f in flights:
				current_static = postgrescli.get_static_flight(f["callsign"])
	
				# Condition de "complétude"
				is_incomplete = False
				if current_static:
					is_incomplete = not all([
						current_static.get("airline_name"),
						current_static.get("origin_code"),
						current_static.get("destination_code")
					])
	
				if not current_static or is_incomplete:
					logging.info(f"Triage: {f['callsign']} incomplete or unknown. Scrape needed.")
					needs_scrape.append(f)
				elif postgrescli.needs_refresh(f["callsign"], f["icao24"], f["on_ground"]):
					needs_scrape.append(f)
	
			return needs_scrape
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
	def loading(results: List[Optional[Dict]]):
		postgrescli = PostgresClient()
		try:
			all_static, all_dynamic, all_live = [], [], []
			for r in results:
				if not r: continue
				all_static.extend(r.get("static_rows", []))
				all_dynamic.extend(r.get("dynamic_rows", []))
				all_live.extend(r.get("live_rows", []))

			if all_static: postgrescli.insert_flight_static(all_static)
			if all_dynamic: postgrescli.insert_flight_dynamic(all_dynamic)
			if all_live: postgrescli.insert_live_data(all_live)
		finally:
			postgrescli.close()

	loading(scraping.expand(flight = triage(requesting())))

flight_data_pipeline()