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

    def is_static_known(self, callsign):
        query = "SELECT 1 FROM flight_static WHERE callsign = %s LIMIT 1;"
        result = self.hook.get_first(sql = query, parameters = (callsign,))
        return result is not None

    def needs_refresh(self, callsign, icao24, opensky_on_ground):
        # 1. Si on ne connaît pas la compagnie/origine/destination -> OUI
        if not self.is_static_known(callsign):
            return True

        # 2. Récupérer le dernier état dynamique
        dynamic = self.get_latest_dynamic_flight(callsign, icao24)
        
        # Si inconnu en dynamique -> OUI (nouveau vol détecté)
        if not dynamic:
            return True
        
        # Si déjà arrivé -> NON (sauf si OpenSky détecte un nouveau décollage, géré par le cas précédent)
        if dynamic["status"] == "arrived":
            return False

        # Si OpenSky dit "au sol" mais base dit "en route" -> OUI (atterrissage récent)
        if opensky_on_ground and dynamic["status"] == "en route":
            return True

        # Stratégie de temps : rafraîchir toutes les 20 min max pour les vols longs
        last_update = dynamic["last_update"]
        if isinstance(last_update, str):
            last_update = datetime.fromisoformat(last_update)
        
        now = datetime.now(timezone.utc)
        if now - last_update > timedelta(minutes = 20):
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
        if result is None:
            return None

        columns = [
            "icao24", "callsign", "flight_date", "departure_scheduled", "departure_actual",
            "arrival_scheduled", "arrival_actual", "status", "last_update", "unique_key"
        ]
        dynamic = dict(zip(columns, result))

        # Garde-fou pour passer auto en 'arrived' si pas d'update depuis longtemps
        if dynamic["status"] in ("en route", "departing"):
            last_update = dynamic["last_update"]
            if last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            if now - last_update > timedelta(minutes = 45): # Augmenté à 45min
                update_sql = "UPDATE flight_dynamic SET status = 'arrived' WHERE unique_key = %s"
                self.hook.run(update_sql, parameters = (dynamic["unique_key"],))
                dynamic["status"] = "arrived"

        return dynamic

    # --- MÉTHODES D'INSERTION ---

    def insert_flight_static(self, rows):
        if not rows: return
        query = """
            INSERT INTO flight_static (callsign, airline_name, origin_code, destination_code, commercial_flight)
            VALUES (%(callsign)s, %(airline_name)s, %(origin_code)s, %(destination_code)s, %(commercial_flight)s)
            ON CONFLICT (callsign) DO NOTHING;
        """
        for row in rows:
            try:
                self.cur.execute(query, row)
            except Exception as e:
                self.conn.rollback()
                logging.error(f"Static insert failed: {e}")
                continue
        self.conn.commit()

    def insert_flight_dynamic(self, rows):
        if not rows: return
        query = """
            INSERT INTO flight_dynamic (
                callsign, icao24, flight_date, departure_scheduled, departure_actual,
                arrival_scheduled, arrival_actual, status, unique_key
            )
            VALUES (
                %(callsign)s, %(icao24)s, %(flight_date)s, %(departure_scheduled)s,
                %(departure_actual)s, %(arrival_scheduled)s, %(arrival_actual)s,
                %(status)s, %(unique_key)s
            )
            ON CONFLICT (unique_key)
            DO UPDATE SET
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
            except Exception as e:
                self.conn.rollback()
                logging.error(f"Dynamic upsert failed: {e}")
                continue
        self.conn.commit()

    def insert_live_data(self, rows):
        if not rows: return
        query = """
            INSERT INTO live_data (
                request_id, callsign, icao24, flight_date, departure_scheduled,
                longitude, latitude, baro_altitude, geo_altitude, on_ground,
                velocity, vertical_rate, temperature, wind_speed, gust_speed,
                visibility, cloud_coverage, rain, global_condition, unique_key
            )
            VALUES (
                %(request_id)s, %(callsign)s, %(icao24)s, %(flight_date)s, %(departure_scheduled)s,
                %(longitude)s, %(latitude)s, %(baro_altitude)s, %(geo_altitude)s, %(on_ground)s,
                %(velocity)s, %(vertical_rate)s, %(temperature)s, %(wind_speed)s, %(gust_speed)s,
                %(visibility)s, %(cloud_coverage)s, %(rain)s, %(global_condition)s, %(unique_key)s
            )
        """
        for row in rows:
            if not all([row.get("flight_date"), row.get("departure_scheduled"), row.get("unique_key")]):
                continue
            try:
                self.cur.execute(query, row)
            except Exception as e:
                self.conn.rollback()
                logging.error(f"Live insert failed: {e}")
                continue
        self.conn.commit()

    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass