import logging
import requests
import time
from airflow.models import Variable

class OpenskyClient:
	def __init__(self):
		self.api_url = Variable.get("OPENSKY_API_URL")
		self.token_url = Variable.get("OPENSKY_TOKEN_URL")
		self.username = Variable.get("OPENSKY_USERNAME")
		self.password = Variable.get("OPENSKY_PASSWORD")
		self.token = self._get_token()
		self.headers = {"Authorization": f"Bearer {self.token}"}

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
		while attempt < max_retries:
			try:
				response = requests.get(self.api_url, headers=self.headers, timeout=15)
				
				# Cas 1 : Quota dépassé
				if response.status_code == 429:
					logging.error("CRITICAL: OpenSky quota exceeded.")
					# On lève une exception spécifique pour Airflow
					raise RuntimeError("OpenSky Quota Exceeded")
				
				# Cas 2 : Token expiré
				if response.status_code == 401:
					logging.warning("Token invalid, refreshing")
					self._refresh_token()
					continue # On retente immédiatement avec le nouveau token
				
				# Cas 3 : Erreurs serveur (503, 502, 504)
				if response.status_code in [500, 502, 503, 504]:
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
					return {"states": []}

				logging.info(f"Retrieved {len(data.get('states', []))} flights")
				return data
			
			except requests.RequestException as e:
				attempt += 1
				wait_time = backoff_factor ** attempt
				logging.error(f"Network error {attempt}/{max_retries}: {e}")
				time.sleep(wait_time)
		
		raise RuntimeError(f"Failed to get OpenSky data after {max_retries} attempts")

	def normalize_rawdata(self, raw_data, filter=None):
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