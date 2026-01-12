import logging
import requests

from airflow.models import Variable

from prometheus_client import CollectorRegistry, Counter, push_to_gateway

class WeatherClient:
	def __init__(self):
		self.api_url = Variable.get("WEATHER_API_URL")
		self.api_key = Variable.get("WEATHER_API_KEY")
		self.fields = Variable.get("WEATHER_FIELDS", deserialize_json = True)
		self.timeout = int(Variable.get("WEATHER_TIMEOUT"))
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()

		# Métriques
		self.metric_weather_requests = Counter(
			'weather_api_requests_total', 
			'Total des requêtes à l API météo', 
			['status'], # 'success' ou 'error'
			registry=self.registry
		)
		self.metric_weather_null_responses = Counter(
			'weather_api_null_responses_total', 
			'Nombre de fois où l API a renvoyé des données vides ou invalides', 
			registry=self.registry
		)

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job = 'airflow_weather', registry = self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed for Weather: {e}")

	def get_weather(self, lat, lon):
		try:
			params = {"q": f"{lat},{lon}", "key": self.api_key}
			r = requests.get(self.api_url, params=params, timeout=self.timeout)
			
			if r.status_code != 200:
				self.metric_weather_requests.labels(status='error').inc()
				self.metric_weather_null_responses.inc()
				self._push_metrics()
				return {k: None for k in self.fields}
			
			data = r.json().get("current", {})
			
			# Si data est vide malgré un status 200
			if not data:
				self.metric_weather_null_responses.inc()

			result = {
				"temperature": data.get("temp_c"),
				"wind_speed": data.get("wind_kph"),
				"gust_speed": data.get("gust_kph"),
				"visibility": data.get("vis_km"),
				"cloud_coverage": data.get("cloud"),
				"rain": data.get("precip_mm"),
				"global_condition": data.get("condition", {}).get("text"),
			}

			self.metric_weather_requests.labels(status='success').inc()
			self._push_metrics()
			return result

		except Exception as e:
			logging.warning(f"Weather API error: {e}")
			self.metric_weather_requests.labels(status='error').inc()
			self.metric_weather_null_responses.inc()
			self._push_metrics()
			return {k: None for k in self.fields}