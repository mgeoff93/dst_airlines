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
    schedule = None, #"*/5 * * * *",
    catchup = False,
    tags = ["airlines", "etl"]
)
def flight_data_pipeline():

    @task
    def opensky(airline_filter: str = "AFR") -> List[Dict]:
        logging.info("=== EXTRACT: OpenSky API ===")
        openskycli = OpenskyClient()
        raw = openskycli.get_rawdata()

        if not raw:
            return []

        flights = openskycli.normalize_rawdata(raw, filter=airline_filter)
        if not flights:
            return []

        request_id = str(uuid4())
        for f in flights:
            f["request_id"] = request_id

        return flights

    @task
    def triage(flights: List[Dict]) -> List[Dict]:
        """
        FILTRE : Ne garde que les vols nécessitant un appel Selenium.
        C'est l'étape clé pour respecter la contrainte de 5 minutes.
        """
        logging.info(f"=== TRIAGE: Checking refresh needs for {len(flights)} flights ===")
        postgrescli = PostgresClient()
        needs_scrape = []

        try:
            for f in flights:
                # On interroge la logique métier du client Postgres
                if postgrescli.needs_refresh(f["callsign"], f["icao24"], f["on_ground"]):
                    needs_scrape.append(f)
            
            logging.info(f"Triage complete: {len(needs_scrape)} flights to scrape / {len(flights)} total")
            return needs_scrape
        finally:
            postgrescli.close()

    @task(pool="selenium_pool")
    def scraping(flight: Dict) -> Optional[Dict]:
        seleniumcli = SeleniumClient()
        postgrescli = PostgresClient()
        weathercli = WeatherClient()
        flightawarecli = FlightAwareClient(seleniumcli, postgrescli)
    
        def time_to_str(t):
            return t.strftime("%H:%M:%S") if t else None
    
        try:
            callsign = flight.get("callsign")
            icao24 = flight.get("icao24")
            
            # 1. Ajout météo (rapide, fait à chaque fois pour le live)
            lat, lon = flight.get("latitude"), flight.get("longitude")
            flight.update(weathercli.get_weather(lat, lon))
            
            # 2. Parsing statique (uniquement si inconnu)
            # On pourrait optimiser flightawarecli pour ne pas scraper si déjà connu
            static_row = flightawarecli.parse_static_flight(callsign)
            if not static_row:
                return None
    
            # 3. Parsing dynamique
            dynamic_row = flightawarecli.parse_dynamic_flight(callsign, icao24)
            if dynamic_row:
                for key in ["departure_scheduled", "departure_actual", "arrival_scheduled", "arrival_actual"]:
                    dynamic_row[key] = time_to_str(dynamic_row.get(key))
    
            # Construction du résultat
            res = {
                "static_rows": [static_row] if static_row else [],
                "dynamic_rows": [dynamic_row] if dynamic_row and dynamic_row.get("departure_scheduled") else [],
                "live_rows": []
            }

            # Si on a les infos dynamiques, on prépare le live
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
        logging.info("=== LOAD: PostgreSQL ===")
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

    # --- DAG flow ---
    # 1. Extraction
    raw_flights = opensky(airline_filter = "AFR")
    
    # 2. Filtrage (Triage) pour économiser Selenium
    flights_to_scrape = triage(raw_flights)
    
    # 3. Scraping parallèle (uniquement sur le delta)
    scraped_results = scraping.expand(flight = flights_to_scrape)
    
    # 4. Chargement final
    loading(scraped_results)

flight_data_pipeline()