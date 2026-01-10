import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

class PostgresClient:
	def __init__(self):
		conn_id = Variable.get("CONNECTION_ID")
		self.hook = PostgresHook(postgres_conn_id=conn_id)
		self.conn = self.hook.get_conn()
		self.cur = self.conn.cursor()

	def get_static_flight(self, callsign: str) -> Optional[Dict]:
		"""Récupère les infos statiques pour le triage (Garde-fou)."""
		query = "SELECT callsign, airline_name, origin_code, destination_code FROM flight_static WHERE callsign = %s"
		result = self.hook.get_first(sql=query, parameters=(callsign,))
		if result:
			return {
				"callsign": result[0],
				"airline_name": result[1],
				"origin_code": result[2],
				"destination_code": result[3]
			}
		return None

	def is_static_known(self, callsign):
		query = "SELECT 1 FROM flight_static WHERE callsign = %s LIMIT 1;"
		result = self.hook.get_first(sql = query, parameters = (callsign,))
		return result is not None

	def needs_refresh(self, callsign, icao24, opensky_on_ground, static = None, dynamic = None):
		"""
		Détermine si un vol a besoin d'un nouveau passage Selenium.
		On passe static et dynamic en paramètres pour éviter les doubles appels SQL.
		"""
		# 1. Si inconnu ou donnée incomplète (Unknown) -> OUI
		if not static or static.get("airline_name") == "Unknown Airline" or not static.get("origin_code"):
			return True

		# 2. Si aucun historique dynamique -> OUI (besoin de la unique_key)
		if not dynamic:
			return True
		
		# 3. Si déjà arrivé -> NON (on ne scrape plus un vol fini)
		if dynamic["status"] == "arrived":
			return False

		# 4. Si OpenSky voit l'avion au sol alors qu'on le pensait en route -> OUI (MAJ statut)
		if opensky_on_ground and dynamic["status"] == "en route":
			return True

		# 5. Rafraîchir toutes les 20 min pour le live
		last_update = dynamic["last_update"]
		if isinstance(last_update, str):
			last_update = datetime.fromisoformat(last_update)
		
		# S'assurer du timezone pour la comparaison
		if last_update.tzinfo is None:
			last_update = last_update.replace(tzinfo=timezone.utc)
			
		now = datetime.now(timezone.utc)
		if now - last_update > timedelta(minutes=20):
			return True

		return False

	def get_latest_dynamic_flight(self, callsign, icao24):
		query = """
			SELECT icao24, callsign, flight_date, departure_scheduled, departure_actual, 
				   arrival_scheduled, arrival_actual, status, last_update, unique_key
			FROM flight_dynamic 
			WHERE callsign = %s AND icao24 = %s
			ORDER BY flight_date DESC, departure_scheduled DESC
			LIMIT 1;
		"""
		result = self.hook.get_first(sql = query, parameters = (callsign, icao24))
		if result is None: return None

		columns = [
			"icao24", "callsign", "flight_date", "departure_scheduled", "departure_actual",
			"arrival_scheduled", "arrival_actual", "status", "last_update", "unique_key"
		]
		dynamic = dict(zip(columns, result))

		if dynamic["status"] in ("en route", "departing"):
			last_update = dynamic["last_update"]
			if last_update.tzinfo is None:
				last_update = last_update.replace(tzinfo=timezone.utc)

			now = datetime.now(timezone.utc)
			if now - last_update > timedelta(minutes = 45):
				update_sql = "UPDATE flight_dynamic SET status = 'arrived' WHERE unique_key = %s"
				self.hook.run(update_sql, parameters = (dynamic["unique_key"],))
				dynamic["status"] = "arrived"
		return dynamic

	def insert_flight_static(self, rows):
		if not rows: return
		query = """
			INSERT INTO flight_static (callsign, airline_name, origin_code, destination_code, commercial_flight)
			VALUES (%(callsign)s, %(airline_name)s, %(origin_code)s, %(destination_code)s, %(commercial_flight)s)
			ON CONFLICT (callsign) DO UPDATE SET
				airline_name = CASE WHEN EXCLUDED.airline_name != 'Unknown Airline' THEN EXCLUDED.airline_name ELSE flight_static.airline_name END,
				origin_code = COALESCE(EXCLUDED.origin_code, flight_static.origin_code),
				destination_code = COALESCE(EXCLUDED.destination_code, flight_static.destination_code);
		"""
		for row in rows:
			try:
				self.cur.execute(query, row)
			except Exception as e:
				self.conn.rollback()
				logging.error(f"Static insert failed: {e}")
		self.conn.commit()

	def insert_flight_dynamic(self, rows):
		if not rows: return
		query = """
			INSERT INTO flight_dynamic (callsign, icao24, flight_date, departure_scheduled, departure_actual,
									   arrival_scheduled, arrival_actual, status, unique_key)
			VALUES (%(callsign)s, %(icao24)s, %(flight_date)s, %(departure_scheduled)s,
					%(departure_actual)s, %(arrival_scheduled)s, %(arrival_actual)s, %(status)s, %(unique_key)s)
			ON CONFLICT (unique_key) DO UPDATE SET
				status = EXCLUDED.status,
				departure_actual = COALESCE(EXCLUDED.departure_actual, flight_dynamic.departure_actual),
				arrival_actual = COALESCE(EXCLUDED.arrival_actual, flight_dynamic.arrival_actual),
				last_update = NOW();
		"""
		for row in rows:
			if not all([row.get("flight_date"), row.get("departure_scheduled"), row.get("unique_key")]): continue
			try:
				self.cur.execute(query, row)
			except Exception as e:
				self.conn.rollback()
				logging.error(f"Dynamic upsert failed: {e}")
		self.conn.commit()

	def insert_live_data(self, rows):
		if not rows: return
		query = """
			INSERT INTO live_data (request_id, callsign, icao24, flight_date, departure_scheduled,
								  longitude, latitude, baro_altitude, geo_altitude, on_ground,
								  velocity, vertical_rate, temperature, wind_speed, gust_speed,
								  visibility, cloud_coverage, rain, global_condition, unique_key)
			VALUES (%(request_id)s, %(callsign)s, %(icao24)s, %(flight_date)s, %(departure_scheduled)s,
					%(longitude)s, %(latitude)s, %(baro_altitude)s, %(geo_altitude)s, %(on_ground)s,
					%(velocity)s, %(vertical_rate)s, %(temperature)s, %(wind_speed)s, %(gust_speed)s,
					%(visibility)s, %(cloud_coverage)s, %(rain)s, %(global_condition)s, %(unique_key)s)
		"""
		for row in rows:
			if not all([row.get("flight_date"), row.get("departure_scheduled"), row.get("unique_key")]): continue
			try:
				self.cur.execute(query, row)
			except Exception as e:
				self.conn.rollback()
				logging.error(f"Live insert failed: {e}")
		self.conn.commit()

	def close(self):
		try:
			self.cur.close()
			self.conn.close()
		except: pass