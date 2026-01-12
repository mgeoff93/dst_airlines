import logging
import re
from datetime import datetime, timezone

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from airflow.models import Variable

from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

class FlightAwareClient:
	CODE_REGEX = re.compile(r"^[A-Z]{3}$")

	def __init__(self, selenium_client, postgres_client):
		self.base_url = Variable.get("FLIGHTAWARE_BASE_URL")
		self.wait_time = int(Variable.get("SELENIUM_WAIT_TIME"))
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		self.selenium = selenium_client
		self.postgres = postgres_client

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		
		# Métriques de performance Scraping
		self.metric_selenium_timeouts = Counter(
			'flightaware_selenium_timeouts_total', 
			'Nombre de timeouts lors du chargement des éléments Selenium',
			['callsign'],
			registry=self.registry
		)
		# Métriques métier
		self.metric_flights_parsed = Counter(
			'flightaware_flights_parsed_total', 
			'Nombre total de vols traités par FlightAware',
			['type'], # 'static' ou 'dynamic'
			registry=self.registry
		)
		self.metric_commercial_status = Gauge(
			'flightaware_last_flight_commercial', 
			'1 si le dernier vol traité était commercial, 0 sinon',
			registry=self.registry
		)

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_flightaware', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed for FlightAware: {e}")

	def _prepare_page(self, callsign, selector="div.flightPageSummary", load_page=True):
		if load_page:
			try:
				self.selenium.driver.get(f"{self.base_url}/{callsign}")
			except Exception as e:
				logging.error(f"{callsign}: Page load error: {e}")
				return False
		try:
			WebDriverWait(self.selenium.driver, self.wait_time).until(
				EC.presence_of_element_located((By.CSS_SELECTOR, selector))
			)
			return True
		except TimeoutException:
			self.metric_selenium_timeouts.labels(callsign=callsign).inc()
			self._push_metrics()
			return False

	def parse_static_flight(self, callsign):
		if not self._prepare_page(callsign, selector = "div.flightPageDetails"):
			self.metric_commercial_status.set(0)
			self._push_metrics()
			return {"callsign": callsign, "airline_name": None, "origin_code": None, "destination_code": None, "commercial_flight": False}

		airline_name = self.selenium.request("div.flightPageDetails > div:nth-child(9) > div:nth-child(2) > div > div > div:nth-child(2) a")
		origin_raw = self.selenium.request("div.flightPageSummaryOrigin .flightPageSummaryAirportCode span")
		destination_raw = self.selenium.request("div.flightPageSummaryDestination .flightPageSummaryAirportCode span")

		origin = self._normalize_airport_code(origin_raw)
		destination = self._normalize_airport_code(destination_raw)

		is_commercial = any([airline_name, origin, destination])
		
		# Update métriques
		self.metric_flights_parsed.labels(type='static').inc()
		self.metric_commercial_status.set(1 if is_commercial else 0)
		self._push_metrics()

		return {
			"callsign": callsign,
			"airline_name": airline_name,
			"origin_code": origin,
			"destination_code": destination,
			"commercial_flight": is_commercial
		}

	def parse_dynamic_flight(self, callsign, icao24):
		if not self._prepare_page(callsign, load_page=False):
			return None

		current_date = datetime.now(timezone.utc).date().isoformat()
		dynamic = self.postgres.get_latest_dynamic_flight(callsign, icao24)

		raw_status = self.selenium.request("div.flightPageSummaryStatus")
		curr_status_raw = re.sub("\n", " ", raw_status).lower() if raw_status else ""
		
		status_map = {
			("expected", "scheduled", "taxiing"): "departing",
			("en route", "arriving", "ready"): "en route",
			("just landed", "landed", "arrived"): "arrived"
		}
		
		status = "unknown"
		for keys, val in status_map.items():
			if any(curr_status_raw.startswith(k) for k in keys):
				status = val
				break

		sched_dep = self._get_scheduled_time("departure")
		if not sched_dep: 
			self._push_metrics()
			return None

		new_key = f"{callsign}_{icao24}_{current_date}_{sched_dep.strftime('%H:%M')}"
		
		self.metric_flights_parsed.labels(type='dynamic').inc()
		self._push_metrics()

		if dynamic and dynamic.get("unique_key") == new_key:
			return {
				**dynamic,
				"status": status,
				"departure_actual": dynamic.get("departure_actual") or self._get_actual_time("departure"),
				"arrival_actual": dynamic.get("arrival_actual") or (self._get_actual_time("arrival") if status == "arrived" else None)
			}
		
		return {
			"callsign": callsign, "icao24": icao24, "flight_date": current_date,
			"departure_scheduled": sched_dep,
			"departure_actual": self._get_actual_time("departure") if status in ("en route", "arrived") else None,
			"arrival_scheduled": self._get_scheduled_time("arrival"),
			"arrival_actual": self._get_actual_time("arrival") if status == "arrived" else None,
			"status": status, "unique_key": new_key
		}

	def _get_scheduled_time(self, type = "departure"):
		idx = 2 if type == "departure" else 4
		sub_idx = 1 if type == "departure" else 2
		selector = f"div:nth-child({idx}) > div.flightPageDataTimesParent > div:nth-child({sub_idx}) > div.flightPageDataAncillaryText > div > span"
		return self._get_24h_time_from_string(self.selenium.request(selector))
	
	def _get_actual_time(self, type = "departure"):
		idx = 2 if type == "departure" else 4
		sub_idx = 1 if type == "departure" else 2
		selector = f"div:nth-child({idx}) > div.flightPageDataTimesParent > div:nth-child({sub_idx}) > div.flightPageDataActualTimeText"
		return self._get_24h_time_from_string(self.selenium.request(selector))

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