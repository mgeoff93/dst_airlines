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
	format = "[%(asctime)s.%(msecs)03d+0000] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
	datefmt = "%Y-%m-%dT%H:%M:%S",
	level = logging.INFO
)

class FlightAwareClient:

	FLIGHTAWARE_BASE_URL = "FLIGHTAWARE_BASE_URL"
	SELENIUM_WAIT_TIME = "SELENIUM_WAIT_TIME"

	def __init__(self, selenium_client: SeleniumClient, postgres_client: PostgresClient):
		"""
			selenium_client: instance de SeleniumClient déjà initialisée
		"""
		self.base_url = Variable.get(self.FLIGHTAWARE_BASE_URL)
		self.wait_time = int(Variable.get(self.SELENIUM_WAIT_TIME, default_var = 10)) 
		self.selenium = selenium_client
		self.postgres = postgres_client

	def _prepare_page(self, callsign, selector, load_page):
		url = f"{self.base_url}/{callsign}"
	
		if load_page:
			try:
				self.selenium.driver.get(url)
			except InvalidSessionIdException:
				logging.error(
					f"{callsign}: Session Selenium perdue (InvalidSessionIdException)."
				)
				return False
			except Exception as e:
				logging.error(f"{callsign}: Erreur accès URL {url}. {e}")
				return False
	
		try:
			wait = WebDriverWait(self.selenium.driver, self.wait_time)
	
			# 1️⃣ container principal
			wait.until(
				EC.presence_of_element_located((By.CSS_SELECTOR, selector))
			)
	
			# 2️⃣ élément interne clé (évite DOM partiel)
			wait.until(
				EC.presence_of_element_located(
					(By.CSS_SELECTOR, "div.flightPageSummary")
				)
			)
	
			return True
	
		except TimeoutException:
			logging.warning(
				f"{callsign}: Page FlightAware non prête (timeout sur '{selector}')."
			)
			return False

	def parse_static_flight(self, callsign):
		"""
			Scrape et parse les informations statiques d'un vol depuis FlightAware.
		"""
		logging.info(f"{callsign}: Début du parsing statique.")

		if not self._prepare_page(callsign, selector = "div.flightPageDetails", load_page = True):
			return None

		# --- Récupération des informations statiques ---
		try:
			# Essai de récupération des codes IATA et du nom de la compagnie
			airline_name = self.selenium.request("div.flightPageDetails > div:nth-child(9) > div:nth-child(2) > div > div > div:nth-child(2) a")
			origin_code = self.selenium.request("div.flightPageSummaryOrigin > span.flightPageSummaryAirportCode span")
			destination_code = self.selenium.request("div.flightPageSummaryDestination > span.flightPageSummaryAirportCode span") 
		except Exception as e:
			# Si ces éléments essentiels manquent, on considère les données comme indisponibles
			logging.debug(f"{callsign}: Erreur lors de la récupération des codes et de l'airline. {e}")
			airline_name = origin_code = destination_code = None
			
		# Détermine si c'est un vol commercial (basé sur la présence des codes/airline)
		scraping_ok = any([airline_name, origin_code, destination_code])
		if not scraping_ok: commercial = None  # inconnu
		elif all(v and str(v).strip() for v in (airline_name, origin_code, destination_code)): commercial = True
		else: commercial = False
		
		# Initialisation par défaut au cas où 'commercial' est False
		origin_airport = destination_airport = origin_city = destination_city = None
		
		if commercial is True:
			logging.debug(f"{callsign}: Vol commercial confirmé. Extraction détaillée.")
			
			# --- Extraction et nettoyage des noms complets d'aéroport ---
			
			origin_airport_raw = self.selenium.request("div.flightPageSummaryOrigin a")
			if origin_airport_raw: 
				# Retire le code IATA de la fin du nom d'aéroport
				cleaned = re.sub(rf"\s*-\s*{origin_code}\b", "", origin_airport_raw)
				# Remplace les abréviations communes par 'International Airport'
				cleaned = re.sub(r"int\s*[’'‘`´]?\s*l\s*(airport)?", "International Airport", cleaned, flags = re.IGNORECASE)
				origin_airport = cleaned.strip()
				
			destination_airport_raw = self.selenium.request("div.flightPageSummaryDestination a")
			if destination_airport_raw: 
				# Retire le code IATA de la fin du nom d'aéroport
				cleaned = re.sub(rf"\s*-\s*{destination_code}\b", "", destination_airport_raw)
				# Remplace les abréviations communes par 'International Airport'
				cleaned = re.sub(r"int\s*[’'‘`´]?\s*l\s*(airport)?", "International Airport", cleaned, flags = re.IGNORECASE)
				destination_airport = cleaned.strip()
				
			# --- Extraction des villes ---
			
			origin_city = self.selenium.request("div.flightPageSummaryOrigin > span.flightPageSummaryCity")
			if origin_city: 
				# Nettoyage des caractères spéciaux et des sauts de ligne
				origin_city = re.sub(r"\s*/\s*", ", ", origin_city).replace("\n", "").strip()
				
			destination_city = self.selenium.request("div.flightPageSummaryDestination > span.flightPageSummaryCity")
			if destination_city: 
				# Nettoyage des caractères spéciaux et des sauts de ligne
				destination_city = re.sub(r"\s*/\s*", ", ", destination_city).replace("\n", "").strip()
		elif commercial is False:
			logging.warning(f"{callsign}: Vol non commercial (codes ou airline absents).")
		else:
			logging.warning(f"{callsign}: Statut commercial inconnu (scraping partiel ou page instable).")

		# Retourne le dictionnaire de résultats
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
			Extrait l'heure formatée en HH:MMAM/PM de la chaîne de texte et la convertit en format HH:MM (24 heures).
		"""
		pattern = r'\d{2}:\d{2}(?:AM|PM)'
		match = re.search(pattern, text)
		if match:
			time_12h_str = match.group(0)
			try:
				dt_object = datetime.strptime(time_12h_str, "%I:%M%p")
				return dt_object.strftime("%H:%M")
			except ValueError:
				return None
		else:
			return None

	def parse_dynamic_flight(self, callsign, icao24):
		if not self._prepare_page(callsign, selector = "div.flightPageDetails", load_page = False):
			return None
		
		dynamic = self.postgres.get_latest_dynamic_flight(callsign, icao24)

		raw_status = self.selenium.request("div.flightPageSummaryStatus")
		current_status = re.sub("\n", " ", raw_status).lower()

		if current_status.startswith("expected to depart") or current_status.startswith("scheduled") or current_status.startswith("taxiing for takeoff"):
			normalized_status = "departing"
		elif current_status.startswith("en route") or current_status.startswith("arriving shortly") or current_status.startswith("ready to land"):
			normalized_status = "en route"
		elif current_status.startswith("just landed") or current_status.startswith("landed") or current_status.startswith("just arrived") or current_status.startswith("arrived"):
			normalized_status = "arrived"
		else:
			normalized_status = "unknown"

		if normalized_status == "unknown": 
			return None
		
		dynamic_row = None

		# CAS 1: INSERTION / RÉINITIALISATION (Non modifié, nécessite toujours scheduled_times)
		if dynamic is None or dynamic.get("status") == "arrived":
			scheduled_departure = self.selenium.request("div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataAncillaryText > div > span")
			if scheduled_departure : scheduled_departure = self._get_24h_time_from_string(scheduled_departure)
			scheduled_arrival = self.selenium.request("div:nth-child(4) > div.flightPageDataTimesParent > div:nth-child(2) > div.flightPageDataAncillaryText > div > span")
			if scheduled_arrival : scheduled_arrival = self._get_24h_time_from_string(scheduled_arrival)

			flight_id = str(uuid4())
			dynamic_row = {
				"flight_id": flight_id,
				"callsign": callsign,
				"icao24": icao24,
				"departure_scheduled": scheduled_departure,
				"departure_actual": None,
				"arrival_scheduled": scheduled_arrival,
				"arrival_actual": None,
				"status": normalized_status,
				"operation": "INSERT"
			}
			if normalized_status in ("en route", "arrived"): 
				actual_departure_site = self.selenium.request("div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataActualTimeText")
				if actual_departure_site : actual_departure_site = self._get_24h_time_from_string(actual_departure_site)
				dynamic_row["departure_actual"] = actual_departure_site

				if normalized_status == "arrived":
					actual_arrival_site = self.selenium.request("div:nth-child(4) > div.flightPageDataTimesParent > div:nth-child(2) > div.flightPageDataActualTimeText")
					if actual_arrival_site : actual_arrival_site = self._get_24h_time_from_string(actual_arrival_site)
					dynamic_row["arrival_actual"] = actual_arrival_site

		# CAS 2 : MISE À JOUR - CHANGEMENT DE STATUT (Transition)
		elif normalized_status != dynamic.get("status"):
			# On prépare dynamic_row pour la mise à jour
			dynamic_row = {
				"flight_id": dynamic.get("flight_id"),
				"callsign": callsign,
				"icao24": icao24,
				"departure_actual": dynamic.get("departure_actual"), 
				"arrival_actual": dynamic.get("arrival_actual"),
				"status": normalized_status,
				"operation": "UPDATE"
			}

			# Transition vers EN ROUTE : on doit lire l'heure de départ réelle du site
			if normalized_status == "en route":
				actual_departure_site = self.selenium.request("div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataActualTimeText")
				if actual_departure_site : actual_departure_site = self._get_24h_time_from_string(actual_departure_site)
				dynamic_row["departure_actual"] = actual_departure_site

			# Transition vers ARRIVED : on doit lire l'heure d'arrivée réelle du site
			elif normalized_status == "arrived":
				actual_arrival_site = self.selenium.request("div:nth-child(4) > div.flightPageDataTimesParent > div:nth-child(2) > div.flightPageDataActualTimeText")
				if actual_arrival_site : actual_arrival_site = self._get_24h_time_from_string(actual_arrival_site)
				dynamic_row["arrival_actual"] = actual_arrival_site

				# Si la transition a manqué l'étape "en route", on lit aussi l'heure de départ.
				if dynamic.get("status") == "departing":
					actual_departure_site = self.selenium.request("div:nth-child(2) > div.flightPageDataTimesParent > div:nth-child(1) > div.flightPageDataActualTimeText")
					if actual_departure_site : actual_departure_site = self._get_24h_time_from_string(actual_departure_site)
					dynamic_row["departure_actual"] = actual_departure_site

		# CAS 3 : MISE À JOUR LÉGÈRE - STATUT IDENTIQUE (last_update)
		# On fait cela uniquement si le vol n'est pas déjà terminé ou inconnu
		elif dynamic.get("status") == normalized_status and normalized_status not in ("arrived", "unknown"):
			dynamic_row = {
				"flight_id": dynamic.get("flight_id"), 
				"callsign": callsign, 
				"icao24": icao24, 
				"departure_actual": dynamic.get("departure_actual"),
				"arrival_actual": dynamic.get("arrival_actual"),
				"status": normalized_status, 
				"operation": "UPDATE"}
		
		return dynamic_row