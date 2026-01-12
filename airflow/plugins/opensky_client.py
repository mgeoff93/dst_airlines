import logging
import requests
import time

from airflow.models import Variable

from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

class OpenskyClient:
	def __init__(self):
		self.api_url = Variable.get("OPENSKY_API_URL")
		self.token_url = Variable.get("OPENSKY_TOKEN_URL")
		self.username = Variable.get("OPENSKY_USERNAME")
		self.password = Variable.get("OPENSKY_PASSWORD")
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		self.token = self._get_token()
		self.headers = {"Authorization": f"Bearer {self.token}"}

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		
		# Métriques
		self.metric_flights_retrieved = Gauge(
			'opensky_flights_retrieved', 
			'Nombre de vols récupérés (brut)', 
			registry=self.registry
		)
		self.metric_api_errors = Counter(
			'opensky_api_errors_total', 
			'Total des erreurs API OpenSky', 
			['status_code'], 
			registry=self.registry
		)
		self.metric_quota_exceeded = Gauge(
			'opensky_quota_status', 
			'1 si quota dépassé, 0 sinon', 
			registry=self.registry
		)

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_opensky', registry=self.registry)
		except Exception as e:
			logging.warning(f"Metrics push failed: {e}")

	def _generate_token(self):
		data = {
			"grant_type": "client_credentials",
			"client_id": self.username,
			"client_secret": self.password,
		}
		response = requests.post(self.token_url, data=data, timeout=10)
		response.raise_for_status()
		token = response.json().get("access_token")
		Variable.set("OPENSKY_TOKEN", token)
		logging.info("New OpenSky token generated")
		return token

	def _get_token(self):
		token = Variable.get("OPENSKY_TOKEN")
		if not token:
			return self._generate_token()
		return token

	def _refresh_token(self):
		token = self._generate_token()
		self.token = token
		self.headers = {"Authorization": f"Bearer {token}"}

	def get_rawdata(self, max_retries=5, backoff_factor=2):
		attempt = 0
		self.metric_quota_exceeded.set(0) # Reset status
		
		while attempt < max_retries:
			try:
				response = requests.get(self.api_url, headers = self.headers, timeout=15)

				# Cas 1 : Quota dépassé
				if response.status_code == 429:
					logging.error("CRITICAL: OpenSky quota exceeded.")
					self.metric_api_errors.labels(status_code='429').inc()
					self.metric_quota_exceeded.set(1)
					self._push_metrics()
					raise RuntimeError("OpenSky Quota Exceeded")

				# Cas 2 : Token expiré
				if response.status_code == 401:
					logging.warning("Token invalid, refreshing")
					self.metric_api_errors.labels(status_code='401').inc()
					self._refresh_token()
					continue 

				# Cas 3 : Erreurs serveur (500, 502, 503, 504)
				if response.status_code in [500, 502, 503, 504]:
					self.metric_api_errors.labels(status_code=str(response.status_code)).inc()
					attempt += 1
					wait_time = backoff_factor ** attempt
					logging.warning(f"Server error {response.status_code} - retry {attempt}/{max_retries}")
					time.sleep(wait_time)
					continue

				response.raise_for_status()
				data = response.json()

				# Vérification de sécurité sur le contenu
				if not data or 'states' not in data:
					logging.warning("OpenSky returned successful response but no states found")
					self.metric_flights_retrieved.set(0)
					self._push_metrics()
					return {"states": []}

				flights_count = len(data.get('states', []))
				logging.info(f"Retrieved {flights_count} flights")
				
				# Update & Push metrics
				self.metric_flights_retrieved.set(flights_count)
				self._push_metrics()
				
				return data

			except requests.RequestException as e:
				self.metric_api_errors.labels(status_code='network_error').inc()
				attempt += 1
				wait_time = backoff_factor ** attempt
				logging.error(f"Network error {attempt}/{max_retries}: {e}")
				time.sleep(wait_time)

		self._push_metrics()
		raise RuntimeError(f"Failed to get OpenSky data after {max_retries} attempts")

	def normalize_rawdata(self, raw_data, filter = None):
		states = raw_data.get("states", [])
		normalized = []

		for s in states:
			callsign = s[1]
			if filter:
				filters = [filter.upper()] if isinstance(filter, str) else [f.upper() for f in filter]
				if not callsign or not any(callsign.upper().strip().startswith(p) for p in filters):
					continue

			longitude, latitude = s[5], s[6]
			if longitude is None or latitude is None:
				continue

			normalized.append({
				"icao24": s[0],
				"callsign": (callsign or "").strip(),
				"longitude": longitude,
				"latitude": latitude,
				"baro_altitude": s[7],
				"geo_altitude": s[13],
				"on_ground": s[8],
				"velocity": s[9],
				"vertical_rate": s[11],
			})

		logging.info(f"Normalized {len(normalized)} flights after filter")
		return normalized