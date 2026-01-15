import logging
import requests
import time
from airflow.models import Variable
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

class OpenskyClient:
	def __init__(self):
		self.api_url = Variable.get("OPENSKY_API_URL")
		self.token_url = Variable.get("OPENSKY_TOKEN_URL")
		self.username = Variable.get("OPENSKY_USERNAME")
		self.password = Variable.get("OPENSKY_PASSWORD")
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")
		
		# Initialisation Prometheus
		self.registry = CollectorRegistry()
		self.metric_api_errors = Counter(
			'opensky_api_errors_total',
			'Total des erreurs API OpenSky',
			['status_code'],
			registry=self.registry
		)
		self.metric_quota_status = Gauge(
			'opensky_quota_status',
			'1 si quota depasse, 0 sinon',
			registry=self.registry
		)

		# Pre-initialisation des labels pour eviter les "NaN" dans Grafana
		for code in ['401', '429', '500', 'network_error', 'auth_error']:
			self.metric_api_errors.labels(status_code=code).inc(0)

		# Authentification
		try:
			self.token = self._get_token()
			self.headers = {"Authorization": f"Bearer {self.token}"}
		except Exception as e:
			self.metric_api_errors.labels(status_code='auth_error').inc()
			self._push_metrics()
			raise e

	def _push_metrics(self):
		"""Envoi systematique pour garantir la visibilite dans le Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_opensky', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed: {e}")

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
		token = Variable.get("OPENSKY_TOKEN", default_var=None)
		if not token:
			return self._generate_token()
		return token

	def _refresh_token(self):
		token = self._generate_token()
		self.token = token
		self.headers = {"Authorization": f"Bearer {token}"}

	def get_rawdata(self, max_retries=5, backoff_factor=2):
		attempt = 0
		self.metric_quota_status.set(0)

		while attempt < max_retries:
			try:
				response = requests.get(self.api_url, headers=self.headers, timeout=15)

				if response.status_code == 429:
					self.metric_api_errors.labels(status_code='429').inc()
					self.metric_quota_status.set(1)
					self._push_metrics()
					raise RuntimeError("OpenSky Quota Exceeded")

				if response.status_code == 401:
					self.metric_api_errors.labels(status_code='401').inc()
					self._refresh_token()
					continue

				if response.status_code >= 500:
					self.metric_api_errors.labels(status_code=str(response.status_code)).inc()
					attempt += 1
					time.sleep(backoff_factor ** attempt)
					continue

				response.raise_for_status()
				data = response.json()
				
				# Succes : on pousse les metriques a 0 erreur
				self._push_metrics()
				return data if 'states' in data else {"states": []}

			except requests.RequestException as e:
				self.metric_api_errors.labels(status_code='network_error').inc()
				attempt += 1
				time.sleep(backoff_factor ** attempt)

		self._push_metrics()
		raise RuntimeError(f"Failed to get OpenSky data after {max_retries} attempts")

	def normalize_rawdata(self, raw_data, filter=None):
		states = raw_data.get("states", []) or []
		normalized = []
		filters = []
		if filter:
			filters = [filter.upper()] if isinstance(filter, str) else [f.upper() for f in filter]

		for s in states:
			callsign = (s[1] or "").strip().upper()
			if filters and not any(callsign.startswith(p) for p in filters):
				continue
			
			if s[5] is None or s[6] is None: continue

			normalized.append({
				"icao24": s[0],
				"callsign": callsign,
				"longitude": s[5],
				"latitude": s[6],
				"baro_altitude": s[7],
				"geo_altitude": s[13],
				"on_ground": s[8],
				"velocity": s[9],
				"vertical_rate": s[11],
			})
		return normalized