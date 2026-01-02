from fastapi import APIRouter, Query
from typing import Optional
from api.core.database import db
from api.services import flight_features
import pandas as pd

router = APIRouter(tags=["Live metadatas"])

def get_current_subset():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	datasets = flight_features.build_flight_datasets(all_flights)
	return datasets["current"]

# Toutes les métadonnées
@router.get("/live/all_metadatas")
def get_live_all_metadatas():
	current_rows = get_current_subset()

	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	params = [item for t in tuples for item in t]  # aplatir pour psycopg2

	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT *
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
		ORDER BY request_id DESC
	"""
	live_rows = db.query(sql, tuple(params))

	return {"count": len(live_rows), "data": live_rows}

# Métadonnées positionnelles
@router.get("/live/position_metadatas")
def get_live_position_metadatas():
	current_rows = get_current_subset()

	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	params = [item for t in tuples for item in t]  # aplatir pour psycopg2

	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude,
			   baro_altitude, geo_altitude, on_ground, velocity, vertical_rate, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
		ORDER BY request_id DESC
	"""
	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}

# Météo / environnement
@router.get("/live/weather_metadatas")
def get_live_weather_metadatas():
	current_rows = get_current_subset()

	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	params = [item for t in tuples for item in t]  # aplatir pour psycopg2

	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude,
			   wind_speed, gust_speed, visibility, cloud_coverage, rain,
			   global_condition, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
		ORDER BY request_id DESC
	"""
	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}

# Position / vol réel
@router.get("/live/light")
def get_live_light():
	current_rows = get_current_subset()

	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	params = [item for t in tuples for item in t]  # aplatir pour psycopg2

	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude, global_condition, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
		ORDER BY request_id DESC
	"""
	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}