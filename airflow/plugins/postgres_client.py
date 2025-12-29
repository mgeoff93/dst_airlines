import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone

class PostgresClient:
	def __init__(self):
		conn_id = Variable.get("CONNECTION_ID")
		self.hook = PostgresHook(postgres_conn_id=conn_id)
		self.conn = self.hook.get_conn()
		self.cur = self.conn.cursor()

	def get_latest_dynamic_flight(self, callsign, icao24):
		query = """
			SELECT icao24, callsign, flight_date, departure_scheduled, departure_actual, 
				   arrival_scheduled, arrival_actual, status, last_update
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
			"arrival_scheduled", "arrival_actual", "status", "last_update"
		]
		dynamic = dict(zip(columns, result))
	
		# Ajouter la combinaison unique
		dynamic["unique_key"] = (
			dynamic["callsign"], dynamic["icao24"], dynamic["flight_date"], dynamic["departure_scheduled"]
		)
	
		# Garde-fou pour vols en route ou departing
		if dynamic["status"] in ("en route", "departing"):
			last_update = dynamic["last_update"]
			if isinstance(last_update, str):
				last_update = datetime.fromisoformat(last_update)
			if last_update.tzinfo is None:
				last_update = last_update.replace(tzinfo=timezone.utc)
	
			now = datetime.now(timezone.utc)
			if now - last_update > timedelta(minutes=20):
				update_sql = """
					UPDATE flight_dynamic
					SET status = 'arrived'
					WHERE callsign = %s AND icao24 = %s 
					  AND flight_date = %s AND departure_scheduled = %s
				"""
				self.hook.run(update_sql, parameters=(
					callsign, icao24, dynamic["flight_date"], dynamic["departure_scheduled"]
				))
				dynamic["status"] = "arrived"
	
		return dynamic

	def insert_flight_static(self, rows):
		if not rows:
			return
		logging.info(f"Inserting {len(rows)} rows into flight_static")
		query = """
			INSERT INTO flight_static (
				callsign, airline_name, origin_code, destination_code,
				origin_airport, destination_airport, origin_city, destination_city,
				commercial_flight
			) VALUES (
				%(callsign)s, %(airline_name)s, %(origin_code)s, %(destination_code)s,
				%(origin_airport)s, %(destination_airport)s, %(origin_city)s, %(destination_city)s,
				%(commercial_flight)s
			)
			ON CONFLICT (callsign) DO NOTHING;
		"""
		self.cur.executemany(query, rows)
		self.conn.commit()

	def insert_flight_dynamic(self, rows):
		if not rows:
			return
	
		logging.info(f"Upserting {len(rows)} flight_dynamic rows")
		query = """
			INSERT INTO flight_dynamic (
				callsign,
				icao24,
				flight_date,
				departure_scheduled,
				departure_actual,
				arrival_scheduled,
				arrival_actual,
				status
			) VALUES (
				%(callsign)s,
				%(icao24)s,
				%(flight_date)s,
				%(departure_scheduled)s,
				%(departure_actual)s,
				%(arrival_scheduled)s,
				%(arrival_actual)s,
				%(status)s
			)
			ON CONFLICT (callsign, icao24, flight_date, departure_scheduled)
			DO UPDATE SET
				status = EXCLUDED.status,
				departure_actual = COALESCE(EXCLUDED.departure_actual, flight_dynamic.departure_actual),
				arrival_actual = COALESCE(EXCLUDED.arrival_actual, flight_dynamic.arrival_actual),
				last_update = NOW();
		"""
	
		for row in rows:
			if not row.get("flight_date") or not row.get("departure_scheduled"):
				logging.warning(f"Skipping dynamic row for {row.get('callsign')} due to missing flight_date or departure_scheduled")
				continue
			self.cur.execute(query, row)
	
		self.conn.commit()

	def insert_live_data(self, rows):
		if not rows:
			return

		logging.info(f"Inserting {len(rows)} rows into live_data")
		query = """
			INSERT INTO live_data (
				request_id, callsign, icao24, flight_date, departure_scheduled,
				longitude, latitude, baro_altitude, geo_altitude, on_ground,
				velocity, vertical_rate, temperature, wind_speed, gust_speed,
				visibility, cloud_coverage, rain, global_condition
			)
			VALUES (
				%(request_id)s, %(callsign)s, %(icao24)s, %(flight_date)s, %(departure_scheduled)s,
				%(longitude)s, %(latitude)s, %(baro_altitude)s, %(geo_altitude)s, %(on_ground)s,
				%(velocity)s, %(vertical_rate)s, %(temperature)s, %(wind_speed)s, %(gust_speed)s,
				%(visibility)s, %(cloud_coverage)s, %(rain)s, %(global_condition)s
			)
		"""
		filtered_rows = [
			r for r in rows if r.get("flight_date") and r.get("departure_scheduled")
		]
		self.cur.executemany(query, filtered_rows)
		self.conn.commit()

	def close(self):
		try:
			self.cur.close()
			self.conn.close()
			logging.info("Postgres connection closed")
		except Exception as e:
			logging.warning(f"Error closing postgres: {e}")