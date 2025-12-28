import logging
from uuid import uuid4
from datetime import datetime, timedelta
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

default_args = {
    "owner": "DST Airlines",
    "start_date": datetime.now() - timedelta(days=1),
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
    def extract_flights(airline_filter="AFR"):
        logging.info("=== EXTRACT: OpenSky API ===")
        
        try:
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
        
        except Exception as e:
            logging.error(f"Extract failed: {e}", exc_info=True)
            raise

    @task(pool="selenium_pool")
    def scrape_single_flight(flight):
        seleniumcli = None
        postgrescli = None
        
        try:
            seleniumcli = SeleniumClient()
            postgrescli = PostgresClient()
            weathercli = WeatherClient()
            flightawarecli = FlightAwareClient(seleniumcli, postgrescli)
            
            callsign = flight.get("callsign")
            icao24 = flight.get("icao24")
            request_id = flight.get("request_id")
            
            static_row = flightawarecli.parse_static_flight(callsign)
            if not static_row:
                return None
            
            if static_row.get("commercial_flight") is False:
                return {
                    "static_rows": [static_row],
                    "dynamic_rows": [],
                    "live_rows": []
                }
            
            lat, lon = flight.get("latitude"), flight.get("longitude")
            flight.update(weathercli.get_weather(lat, lon))
            
            dynamic_row = flightawarecli.parse_dynamic_flight(callsign, icao24)
            flight_id = dynamic_row.get("flight_id") if dynamic_row else None
            
            live_row = flight.copy()
            live_row["flight_id"] = flight_id
            live_row["request_id"] = request_id
            
            return {
                "static_rows": [static_row],
                "dynamic_rows": [dynamic_row] if dynamic_row else [],
                "live_rows": [live_row]
            }
        
        except Exception as e:
            logging.error(f"Scrape failed for {flight.get('callsign', 'UNKNOWN')}: {e}")
            return None
        
        finally:
            if seleniumcli:
                seleniumcli.close()
            if postgrescli:
                postgrescli.close()

    @task
    def load_all_to_postgres(results):
        logging.info("=== LOAD: PostgreSQL ===")
        
        postgrescli = None
        try:
            postgrescli = PostgresClient()
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
                inserts = [r for r in all_dynamic if r.get("operation") == "INSERT"]
                updates = [r for r in all_dynamic if r.get("operation") == "UPDATE"]
                if inserts:
                    postgrescli.insert_flight_dynamic(inserts, "INSERT")
                if updates:
                    postgrescli.insert_flight_dynamic(updates, "UPDATE")
            
            if all_live:
                postgrescli.insert_live_data(all_live)
            
            logging.info(f"Loaded: {len(all_static)} static, {len(all_dynamic)} dynamic, {len(all_live)} live")
        
        except Exception as e:
            logging.error(f"Load failed: {e}", exc_info=True)
            raise
        
        finally:
            if postgrescli:
                postgrescli.close()

    flights_list = extract_flights(airline_filter="AFR")
    scraped_results = scrape_single_flight.expand(flight=flights_list)
    load_all_to_postgres(scraped_results)

flight_data_pipeline()
