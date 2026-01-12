import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from prometheus_client import CollectorRegistry, Counter, push_to_gateway

class PostgresClient:
	def __init__(self):
		conn_id = Variable.get("CONNECTION_ID")
		self.hook = PostgresHook(postgres_conn_id = conn_id)
		self.conn = self.hook.get_conn()
		self.cur = self.conn.cursor()
		self.pushgateway_url = Variable.get("PUSHGATEWAY_URL")

		# --- Initialisation Prometheus ---
		self.registry = CollectorRegistry()
		
		# Métriques de performance DB
		self.metric_db_operations = Counter(
			'postgres_operations_total', 
			'Nombre d opérations DB par table et type',
			['table', 'operation', 'status'], # ex: table='live_data', op='insert', status='success'
			registry=self.registry
		)
		# Métrique métier : Nettoyage automatique
		self.metric_auto_closures = Counter(
			'postgres_flights_auto_closed_total', 
			'Nombre de vols clôturés automatiquement par timeout',
			registry=self.registry
		)

	def _push_metrics(self):
		"""Envoie les métriques au Pushgateway."""
		try:
			push_to_gateway(self.pushgateway_url, job='airflow_postgres', registry=self.registry)
		except Exception as e:
			logging.warning(f"Prometheus push failed for Postgres: {e}")

	def get_static_flight(self, callsign: str) -> Optional[Dict]:
		"""Récupère les infos statiques pour le triage."""
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

	def is_static_known(self, callsign: str) -> bool:
		query = "SELECT 1 FROM flight_static WHERE callsign = %s LIMIT 1;"
		result = self.hook.get_first(sql=query, parameters=(callsign,))
		return result is not None

	def needs_refresh(self, callsign: str, icao24: str, on_ground: bool, threshold_minutes: int = 10) -> bool:
		dynamic = self.get_latest_dynamic_flight(callsign, icao24)
		if not dynamic:
			return True

		now = datetime.now(timezone.utc)
		last_upd = dynamic["last_update"]
		if last_upd.tzinfo is None:
			last_upd = last_upd.replace(tzinfo=timezone.utc)

		diff_minutes = (now - last_upd).total_seconds() / 60
		logging.info(f"DEBUG REFRESH: {callsign} | Last update: {diff_minutes:.1f} min ago")
		return diff_minutes > threshold_minutes

	def get_latest_dynamic_flight(self, callsign: str, icao24: str) -> Optional[Dict]:
		query = """
			SELECT icao24, callsign, flight_date, departure_scheduled, departure_actual, 
				   arrival_scheduled, arrival_actual, status, last_update, unique_key
			FROM flight_dynamic 
			WHERE callsign = %s AND icao24 = %s
			ORDER BY flight_date DESC, departure_scheduled DESC
			LIMIT 1;
		"""
		result = self.hook.get_first(sql=query, parameters=(callsign, icao24))
		if result is None: 
			return None

		columns = [
			"icao24", "callsign", "flight_date", "departure_scheduled", "departure_actual",
			"arrival_scheduled", "arrival_actual", "status", "last_update", "unique_key"
		]
		dynamic = dict(zip(columns, result))

		last_update = dynamic["last_update"]
		if last_update.tzinfo is None:
			last_update = last_update.replace(tzinfo=timezone.utc)

		if dynamic["status"] in ("en route", "departing"):
			now = datetime.now(timezone.utc)
			if (now - last_update) > timedelta(minutes=90):
				update_sql = "UPDATE flight_dynamic SET status = 'arrived' WHERE unique_key = %s"
				self.hook.run(update_sql, parameters=(dynamic["unique_key"],))
				dynamic["status"] = "arrived"
				
				# Update métrique auto-clôture
				self.metric_auto_closures.inc()
				self._push_metrics()
				logging.info(f"Auto-closing flight {callsign} (Timeout)")
				
		return dynamic

	def insert_flight_static(self, rows: List[Dict]):
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
				self.conn.commit()
				self.metric_db_operations.labels(table='flight_static', operation='upsert', status='success').inc()
			except Exception as e:
				self.conn.rollback()
				self.metric_db_operations.labels(table='flight_static', operation='upsert', status='error').inc()
				logging.error(f"Static insert failed for {row.get('callsign')}: {e}")
		self._push_metrics()

	def insert_flight_dynamic(self, rows: List[Dict]):
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
			if not all([row.get("flight_date"), row.get("departure_scheduled"), row.get("unique_key")]): 
				continue
			try:
				self.cur.execute(query, row)
				self.conn.commit()
				self.metric_db_operations.labels(table='flight_dynamic', operation='upsert', status='success').inc()
			except Exception as e:
				self.conn.rollback()
				self.metric_db_operations.labels(table='flight_dynamic', operation='upsert', status='error').inc()
				logging.error(f"Dynamic upsert failed for {row.get('unique_key')}: {e}")
		self._push_metrics()

	def insert_live_data(self, rows: List[Dict]):
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
			if not all([row.get("flight_date"), row.get("departure_scheduled"), row.get("unique_key")]):
				continue
			try:
				self.cur.execute(query, row)
				self.conn.commit()
				self.metric_db_operations.labels(table='live_data', operation='insert', status='success').inc()
			except Exception as e:
				self.conn.rollback()
				self.metric_db_operations.labels(table='live_data', operation='insert', status='error').inc()
				logging.error(f"Live insert failed for {row.get('callsign')}: {e}")
		self._push_metrics()

	def close(self):
		try:
			self._push_metrics()
			self.cur.close()
			self.conn.close()
		except: 
			pass