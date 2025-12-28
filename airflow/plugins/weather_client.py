import logging
import requests
from airflow.models import Variable

class WeatherClient:
    def __init__(self):
        self.api_url = Variable.get("WEATHER_API_URL")
        self.api_key = Variable.get("WEATHER_API_KEY")
        self.fields = Variable.get("WEATHER_FIELDS", deserialize_json=True)
        self.timeout = int(Variable.get("WEATHER_TIMEOUT", default_var=10))

    def get_weather(self, lat, lon):
        try:
            params = {"q": f"{lat},{lon}", "key": self.api_key}
            r = requests.get(self.api_url, params=params, timeout=self.timeout)
            
            if r.status_code != 200:
                return {k: None for k in self.fields}
            
            data = r.json().get("current", {})
            return {
                "temperature": data.get("temp_c"),
                "wind_speed": data.get("wind_kph"),
                "gust_speed": data.get("gust_kph"),
                "visibility": data.get("vis_km"),
                "cloud_coverage": data.get("cloud"),
                "rain": data.get("precip_mm"),
                "global_condition": data.get("condition", {}).get("text"),
            }
        except Exception as e:
            logging.warning(f"Weather API error: {e}")
            return {k: None for k in self.fields}
