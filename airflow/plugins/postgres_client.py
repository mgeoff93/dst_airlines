import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresClient:
    def __init__(self):
        conn_id = Variable.get("CONNECTION_ID")
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = self.hook.get_conn()
        self.cur = self.conn.cursor()

    def get_latest_dynamic_flight(self, callsign, icao24):
        query = """
            SELECT flight_id, icao24, callsign, departure_scheduled, departure_actual, 
                   arrival_scheduled, arrival_actual, status, last_update
            FROM flight_dynamic 
            WHERE callsign = %s AND icao24 = %s
            ORDER BY last_update DESC, flight_id DESC
            LIMIT 1;
        """
        result = self.hook.get_first(sql=query, parameters=(callsign, icao24))
        
        if result is None:
            return None
        
        columns = [
            "flight_id", "icao24", "callsign", "departure_scheduled", "departure_actual",
            "arrival_scheduled", "arrival_actual", "status", "last_update"
        ]
        return dict(zip(columns, result))

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

    def insert_flight_dynamic(self, rows, operation):
        if not rows:
            return
        
        logging.info(f"Processing {len(rows)} flight_dynamic with operation: {operation}")
        
        if operation == "INSERT":
            query = """
                INSERT INTO flight_dynamic (
                    flight_id, callsign, icao24, departure_scheduled, departure_actual,
                    arrival_scheduled, arrival_actual, status
                ) VALUES (
                    %(flight_id)s, %(callsign)s, %(icao24)s, %(departure_scheduled)s,
                    %(departure_actual)s, %(arrival_scheduled)s, %(arrival_actual)s,
                    %(status)s
                );
            """
            self.cur.executemany(query, rows)
        elif operation == "UPDATE":
            query = """
                UPDATE flight_dynamic
                SET status = %(status)s,
                    departure_actual = COALESCE(%(departure_actual)s, departure_actual),
                    arrival_actual = COALESCE(%(arrival_actual)s, arrival_actual),
                    last_update = NOW()
                WHERE flight_id = %(flight_id)s;
            """
            for row in rows:
                self.cur.execute(query, row)
        
        self.conn.commit()

    def insert_live_data(self, rows):
        if not rows:
            return
        
        logging.info(f"Inserting {len(rows)} rows into live_data")
        query = """
            INSERT INTO live_data (
                request_id, flight_id, icao24, callsign, longitude, latitude, baro_altitude,
                geo_altitude, on_ground, velocity, vertical_rate, temperature, wind_speed,
                gust_speed, visibility, cloud_coverage, rain, global_condition
            ) VALUES (
                %(request_id)s, %(flight_id)s, %(icao24)s, %(callsign)s, %(longitude)s, %(latitude)s,
                %(baro_altitude)s, %(geo_altitude)s, %(on_ground)s, %(velocity)s, %(vertical_rate)s,
                %(temperature)s, %(wind_speed)s, %(gust_speed)s, %(visibility)s, %(cloud_coverage)s,
                %(rain)s, %(global_condition)s
            )
        """
        self.cur.executemany(query, rows)
        self.conn.commit()

    def close(self):
        try:
            self.cur.close()
            self.conn.close()
            logging.info("Postgres connection closed")
        except Exception as e:
            logging.warning(f"Error closing postgres: {e}")
