import logging
import re
from airflow.models import Variable
from datetime import datetime, timezone
from postgres_client import PostgresClient
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_client import SeleniumClient

class FlightAwareClient:
    FLIGHTAWARE_BASE_URL = "FLIGHTAWARE_BASE_URL"
    SELENIUM_WAIT_TIME = "SELENIUM_WAIT_TIME"
    CODE_REGEX = re.compile(r"^[A-Z]{3}$")

    def __init__(self, selenium_client: SeleniumClient, postgres_client: PostgresClient):
        self.base_url = Variable.get(self.FLIGHTAWARE_BASE_URL)
        self.wait_time = int(Variable.get(self.SELENIUM_WAIT_TIME, default_var=7)) # Réduit à 7s pour plus de nervosité
        self.selenium = selenium_client
        self.postgres = postgres_client

    def _prepare_page(self, callsign, selector="div.flightPageSummary", load_page=True):
        """Prépare la page avec un timeout strict pour respecter les 5 min."""
        url = f"{self.base_url}/{callsign}"
        if load_page:
            try:
                self.selenium.driver.get(url)
            except Exception as e:
                logging.error(f"{callsign}: Échec accès URL {url}: {e}")
                return False

        try:
            wait = WebDriverWait(self.selenium.driver, self.wait_time)
            # On attend un élément global plutôt que deux pour gagner du temps
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            return True
        except TimeoutException:
            logging.warning(f"{callsign}: Timeout sur la page FlightAware.")
            return False

    def parse_static_flight(self, callsign):
        """Optimisé : Ne scrape que si les données ne sont pas déjà en base."""
        # --- OPTIMISATION : COURT-CIRCUIT ---
        # Si on connaît déjà le callsign, on renvoie un dictionnaire minimal
        # Cela évite de parser le DOM pour rien.
        if self.postgres.is_static_known(callsign):
            logging.info(f"{callsign}: Données statiques déjà en base. Skip scraping.")
            return {"callsign": callsign, "already_known": True}

        logging.info(f"{callsign}: Parsing statique nécessaire.")
        if not self._prepare_page(callsign, load_page=True):
            return None

        try:
            airline_name = self.selenium.request("div.flightPageDetails > div:nth-child(9) > div:nth-child(2) > div > div > div:nth-child(2) a")
            origin_raw = self.selenium.request("div.flightPageSummaryOrigin > span.flightPageSummaryAirportCode span")
            dest_raw = self.selenium.request("div.flightPageSummaryDestination > span.flightPageSummaryAirportCode span")
            
            origin_code = self._normalize_airport_code(origin_raw)
            destination_code = self._normalize_airport_code(dest_raw)
            commercial = True if (airline_name and origin_code and destination_code) else False

            return {
                "callsign": callsign,
                "airline_name": airline_name,
                "origin_code": origin_code,
                "destination_code": destination_code,
                "commercial_flight": commercial
            }
        except Exception as e:
            logging.error(f"{callsign}: Erreur parsing statique: {e}")
            return None

    def parse_dynamic_flight(self, callsign, icao24):
        """Optimisé : Utilise la page déjà chargée par parse_static_flight."""
        # load_page=False car la page est déjà là (ou a échoué juste avant)
        if not self._prepare_page(callsign, load_page=False):
            return None

        current_date = datetime.now(timezone.utc).date().isoformat()
        dynamic = self.postgres.get_latest_dynamic_flight(callsign, icao24)

        # Status normalization
        raw_status = self.selenium.request("div.flightPageSummaryStatus")
        curr_status_raw = re.sub("\n", " ", raw_status).lower() if raw_status else ""
        
        status_map = {
            ("expected", "scheduled", "taxiing"): "departing",
            ("en route", "arriving", "ready"): "en route",
            ("just landed", "landed", "arrived"): "arrived"
        }
        
        status = "unknown"
        for keys, val in status_map.items():
            if curr_status_raw.startswith(keys):
                status = val
                break

        # Extraction des horaires (Sélecteurs simplifiés)
        sched_dep = self._get_24h_time_from_string(self.selenium.request("div.flightPageDataTimesParent div.flightPageDataAncillaryText span"))
        
        # Logique de la unique_key
        new_key = f"{callsign}_{icao24}_{current_date}_{sched_dep.strftime('%H:%M') if sched_dep else '00:00'}"

        # Construction de la ligne
        # On réutilise les données existantes si c'est le même vol pour éviter les "trous" de données
        if dynamic and dynamic.get("unique_key") == new_key:
            dynamic_row = {
                **dynamic, # On garde les anciennes valeurs (date de vol, etc.)
                "status": status,
                "departure_actual": dynamic.get("departure_actual") or self._get_actual_time("dep"),
                "arrival_actual": dynamic.get("arrival_actual") or self._get_actual_time("arr") if status == "arrived" else None
            }
        else:
            dynamic_row = {
                "callsign": callsign, "icao24": icao24, "flight_date": current_date,
                "departure_scheduled": sched_dep,
                "departure_actual": self._get_actual_time("dep") if status in ("en route", "arrived") else None,
                "arrival_scheduled": self._get_scheduled_arrival(),
                "arrival_actual": self._get_actual_time("arr") if status == "arrived" else None,
                "status": status, "unique_key": new_key
            }

        if not dynamic_row.get("departure_scheduled"):
            return None

        return dynamic_row

    # --- HELPERS ---
    def _get_actual_time(self, type="dep"):
        idx = 1 if type == "dep" else 2
        selector = f"div:nth-child({2 if type=='dep' else 4}) > div.flightPageDataTimesParent > div:nth-child({idx}) > div.flightPageDataActualTimeText"
        return self._get_24h_time_from_string(self.selenium.request(selector))

    def _get_scheduled_arrival(self):
        return self._get_24h_time_from_string(self.selenium.request("div:nth-child(4) div.flightPageDataAncillaryText span"))

    def _normalize_airport_code(self, value):
        if value:
            val = str(value).strip().upper()
            return val if self.CODE_REGEX.match(val) else None
        return None

    def _get_24h_time_from_string(self, text):
        if not text: return None
        match = re.search(r'\d{1,2}:\d{2}(?:AM|PM)?', text)
        if not match: return None
        try:
            fmt = "%I:%M%p" if ("AM" in match.group(0) or "PM" in match.group(0)) else "%H:%M"
            return datetime.strptime(match.group(0), fmt).time()
        except: return None