import logging
import re
from airflow.models import Variable
from datetime import datetime
from postgres_client import PostgresClient
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_client import SeleniumClient
from uuid import uuid4

logging.basicConfig(
	format="[%(asctime)s.%(msecs)03d+0000] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt="%Y-%m-%dT%H:%M:%S",
	level=logging.INFO
)

class FlightAwareClient:

	FLIGHTAWARE_BASE_URL = "FLIGHTAWARE_BASE_URL"
	SELENIUM_WAIT_TIME = "SELENIUM_WAIT_TIME"

	CODE_REGEX = re.compile(r"^[A-Z]{3}$")

	def __init__(self, selenium_client: SeleniumClient, postgres_client: PostgresClient):
		self.base_url = Variable.get(self.FLIGHTAWARE_BASE_URL)
		self.wait_time = int(Variable.get(self.SELENIUM_WAIT_TIME, default_var=10))
		self.selenium = selenium_client
		self.postgres = postgres_client

	def _prepare_page(self, callsign, selector, load_page):
		url = f"{self.base_url}/{callsign}"
		if load_page:
			try:
				self.selenium.driver.get(url)
			except InvalidSessionIdException:
				logging.error(f"{callsign}: Session Selenium perdue (InvalidSessionIdException).")
				return False
			except Exception as e:
				logging.error(f"{callsign}: Erreur accès URL {url}. {e}")
				return False

		try:
			wait = WebDriverWait(self.selenium.driver, self.wait_time)
			wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
			wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.flightPageSummary")))
			return True
		except TimeoutException:
			logging.warning(f"{callsign}: Page FlightAware non prête (timeout sur '{selector}').")
			return False
		
	def _normalize_airport_code(self, value):
		if not value:
			return None

		value = str(value).strip().upper()

		if self.CODE_REGEX.match(value):
			return value

		return None

	def parse_static_flight(self, callsign):
		logging.info(f"{callsign}: Début du parsing statique.")
		if not self._prepare_page(callsign, selector="div.flightPageDetails", load_page=True):
			return None
	
		try:
			airline_name = self.selenium.request("div.flightPageDetails > div:nth-child(9) > div:nth-child(2) > div > div > div:nth-child(2) a")
			origin_code_raw = self.selenium.request("div.flightPageSummaryOrigin > span.flightPageSummaryAirportCode span")
			destination_code_raw = self.selenium.request("div.flightPageSummaryDestination > span.flightPageSummaryAirportCode span")
		except Exception as e:
			logging.debug(f"{callsign}: Erreur récupération codes/airline. {e}")
			airline_name = origin_code_raw = destination_code_raw = None

		origin_code = self._normalize_airport_code(origin_code_raw)
		destination_code = self._normalize_airport_code(destination_code_raw)
		
		scraping_ok = any([airline_name, origin_code, destination_code])

		if not scraping_ok:
			commercial = None
		elif airline_name and origin_code and destination_code:
			commercial = True
		else:
			commercial = False
	
		origin_airport = destination_airport = origin_city = destination_city = None
	
		if commercial is True:
			origin_airport_raw = self.selenium.request("div.flightPageSummaryOrigin a")
			if origin_airport_raw:
				cleaned = re.sub(rf"\s*-\s*{origin_code}\b", "", origin_airport_raw)
				cleaned = re.sub(r"int\s*[’'‘`´]?\s*l\s*(airport)?", "International Airport", cleaned, flags=re.IGNORECASE)
				origin_airport = cleaned.strip()
	
			destination_airport_raw = self.selenium.request("div.flightPageSummaryDestination a")
			if destination_airport_raw:
				cleaned = re.sub(rf"\s*-\s*{destination_code}\b", "", destination_airport_raw)
				cleaned = re.sub(r"int\s*[’'‘`´]?\s*l\s*(airport)?", "International Airport", cleaned, flags=re.IGNORECASE)
				destination_airport = cleaned.strip()
	
			origin_city = self.selenium.request("div.flightPageSummaryOrigin > span.flightPageSummaryCity")
			if origin_city:
				origin_city = re.sub(r"\s*/\s*", ", ", origin_city).replace("\n", "").strip()
	
			destination_city = self.selenium.request("div.flightPageSummaryDestination > span.flightPageSummaryCity")
			if destination_city:
				destination_city = re.sub(r"\s*/\s*", ", ", destination_city).replace("\n", "").strip()
		elif commercial is False:
			logging.warning(f"{callsign}: Vol non commercial (codes ou airline absents).")
		else:
			logging.warning(f"{callsign}: Statut commercial inconnu (scraping partiel ou page instable).")
	
		# Retourner un dict prêt à l'insertion dans PostgreSQL
		return {
			"callsign": callsign,
			"airline_name": airline_name,
			"origin_code": origin_code,
			"destination_code": destination_code,
			"origin_airport": origin_airport,
			"destination_airport": destination_airport,
			"origin_city": origin_city,
			"destination_city": destination_city,
			"commercial_flight": commercial
		}

	def _get_24h_time_from_string(self, text):
		"""
		Transforme une chaîne type '7:30AM', '19:45' en datetime.time ou None si invalide.
		"""
		if text is None:
			return None
	
		pattern = r'\d{1,2}:\d{2}(?:AM|PM)?'
		match = re.search(pattern, text)
		if not match:
			return None
	
		time_str = match.group(0)
		try:
			if "AM" in time_str or "PM" in time_str:
				dt_object = datetime.strptime(time_str, "%I:%M%p")
			else:
				dt_object = datetime.strptime(time_str, "%H:%M")
			return dt_object.time()
		except ValueError:
			return None
	
	def parse_dynamic_flight(self, callsign, icao24):
		if not self._prepare_page(callsign, selector="div.flightPageDetails", load_page=False):
			return None
	
		current_date = datetime.utcnow().date().isoformat()
	
		# Récupère le dernier vol dynamique enregistré
		dynamic = self.postgres.get_latest_dynamic_flight(callsign, icao24)
		logging.info(f"{callsign} ({icao24}): last dynamic flight: {dynamic}")
	
		# Récupération et normalisation du status FlightAware
		raw_status = self.selenium.request("div.flightPageSummaryStatus")
		current_status = re.sub("\n", " ", raw_status).lower() if raw_status else ""
		logging.info(f"{callsign}: current FlightAware status: '{current_status}'")
	
		if current_status.startswith(("expected", "scheduled", "taxiing")):
			status = "departing"
		elif current_status.startswith(("en route", "arriving", "ready")):
			status = "en route"
		elif current_status.startswith(("just landed", "landed", "arrived")):
			status = "arrived"
		else:
			logging.warning(f"{callsign}: statut inconnu détecté: '{current_status}'")
			status = "unknown"
	
		# Fonctions utilitaires pour récupérer les horaires
		def get_scheduled_departure():
			val = self.selenium.request(
				"div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataAncillaryText > div > span"
			)
			return self._get_24h_time_from_string(val) if val else None
	
		def get_scheduled_arrival():
			val = self.selenium.request(
				"div:nth-child(4) > div.flightPageDataTimesParent > div:nth-child(2) > div.flightPageDataAncillaryText > div > span"
			)
			return self._get_24h_time_from_string(val) if val else None
	
		def get_actual_departure():
			val = self.selenium.request(
				"div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataActualTimeText"
			)
			return self._get_24h_time_from_string(val) if val else None
	
		def get_actual_arrival():
			val = self.selenium.request(
				"div:nth-child(4) > div.flightPageDataTimesParent > div:nth-child(2) > div.flightPageDataActualTimeText"
			)
			return self._get_24h_time_from_string(val) if val else None
	
		dynamic_row = None
		new_key = f"{callsign}_{icao24}_{current_date}_{get_scheduled_departure()}"
	
		# --- CAS 1 : pas de vol précédent ou vol déjà arrivé ---
		if dynamic is None or dynamic.get("status") == "arrived":
			dynamic_row = {
				"callsign": callsign,
				"icao24": icao24,
				"flight_date": current_date,
				"departure_scheduled": get_scheduled_departure(),
				"departure_actual": get_actual_departure() if status in ("en route", "arrived") else None,
				"arrival_scheduled": get_scheduled_arrival(),
				"arrival_actual": get_actual_arrival() if status == "arrived" else None,
				"status": status,
				"unique_key": new_key
			}
	
		# --- CAS 2 : vol réapparaît ou changement de statut ---
		else:
			last_key = dynamic.get("unique_key")
			if new_key == last_key:
				# Même vol → mettre à jour le status et horaires
				dynamic_row = {
					"callsign": callsign,
					"icao24": icao24,
					"flight_date": dynamic["flight_date"],
					"departure_scheduled": dynamic["departure_scheduled"],
					"departure_actual": dynamic.get("departure_actual") or get_actual_departure(),
					"arrival_scheduled": dynamic.get("arrival_scheduled"),
					"arrival_actual": dynamic.get("arrival_actual"),
					"status": status,
					"unique_key": new_key
				}
			elif status != dynamic.get("status") or status not in ("arrived", "unknown"):
				# Nouveau vol → nouvelle ligne
				dynamic_row = {
					"callsign": callsign,
					"icao24": icao24,
					"flight_date": current_date,
					"departure_scheduled": get_scheduled_departure(),
					"departure_actual": get_actual_departure() if status in ("en route", "arrived") else None,
					"arrival_scheduled": get_scheduled_arrival(),
					"arrival_actual": get_actual_arrival() if status == "arrived" else None,
					"status": status,
					"unique_key": new_key
				}
	
		# Vérifie les champs essentiels
		if dynamic_row and (dynamic_row.get("flight_date") is None or dynamic_row.get("departure_scheduled") is None):
			logging.warning(f"{callsign}: dynamic row skipped due to missing flight_date or departure_scheduled")
			return None
	
		return dynamic_row