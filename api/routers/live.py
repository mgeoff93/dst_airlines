from fastapi import APIRouter, Query
from typing import Optional
from api.core.database import db
from api.services import flight_features
import pandas as pd
from api.metrics import DB_RECORDS_PROCESSED

router = APIRouter(tags=["Live metadatas"])

def get_current_subset():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	datasets = flight_features.build_flight_datasets(all_flights)
	return datasets["current"]

# Toutes les métadonnées

@router.get("/live/history/all")
def get_live_history_all(
	callsign: Optional[str] = Query(None),
	limit: Optional[int] = Query(None, ge=1)
):
	current_rows = get_current_subset()
	sql = "SELECT * FROM live_data"
	params = []

	if current_rows:
		tuples_to_exclude = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
		in_clause = ",".join(["(%s,%s,%s)"] * len(tuples_to_exclude))
		sql += f" WHERE (unique_key, callsign, icao24) NOT IN ({in_clause})"
		params.extend([item for t in tuples_to_exclude for item in t])

	if callsign:
		sql += " AND callsign = %s" if "WHERE" in sql else " WHERE callsign = %s"
		params.append(callsign)

	sql += " ORDER BY request_id DESC"
	
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	live_rows = db.query(sql, tuple(params))
	DB_RECORDS_PROCESSED.labels(table_name="live_data").inc(len(live_rows))
	return {"count": len(live_rows), "data": live_rows}

# --- Toutes les métadonnées (Current) ---

@router.get("/live/current/all")
def get_live_current_all(
	callsign: Optional[str] = Query(None),
	limit: Optional[int] = Query(None, ge=1)
):
	current_rows = get_current_subset()
	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"SELECT * FROM live_data WHERE (unique_key, callsign, icao24) IN ({in_clause})"
	params = [item for t in tuples for item in t]

	if callsign:
		sql += " AND callsign = %s"
		params.append(callsign)

	sql += " ORDER BY request_id DESC"
	
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}

# --- Position / current ---

@router.get("/live/current/position")
def get_live_current_position(
	callsign: Optional[str] = Query(None),
	limit: Optional[int] = Query(None, ge=1)
):
	current_rows = get_current_subset()
	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude,
			   baro_altitude, geo_altitude, on_ground, velocity, vertical_rate, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
	"""
	params = [item for t in tuples for item in t]

	if callsign:
		sql += " AND callsign = %s"
		params.append(callsign)

	sql += " ORDER BY request_id DESC"
	
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}

# --- Weather / current ---

@router.get("/live/current/weather")
def get_live_current_weather(
	callsign: Optional[str] = Query(None),
	limit: Optional[int] = Query(None, ge=1)
):
	current_rows = get_current_subset()
	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude,
			   wind_speed, gust_speed, visibility, cloud_coverage, rain,
			   global_condition, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
	"""
	params = [item for t in tuples for item in t]

	if callsign:
		sql += " AND callsign = %s"
		params.append(callsign)

	sql += " ORDER BY request_id DESC"
	
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}

# --- Light / current ---

@router.get("/live/current/light")
def get_live_current_light(
	callsign: Optional[str] = Query(None),
	limit: Optional[int] = Query(None, ge=1)
):
	current_rows = get_current_subset()
	if not current_rows:
		return {"count": 0, "data": []}

	tuples = [(r["unique_key"], r["callsign"], r["icao24"]) for r in current_rows]
	in_clause = ",".join(["(%s,%s,%s)"] * len(tuples))
	sql = f"""
		SELECT request_id, callsign, icao24, longitude, latitude, global_condition, unique_key
		FROM live_data
		WHERE (unique_key, callsign, icao24) IN ({in_clause})
	"""
	params = [item for t in tuples for item in t]

	if callsign:
		sql += " AND callsign = %s"
		params.append(callsign)

	sql += " ORDER BY request_id DESC"
	
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	live_rows = db.query(sql, tuple(params))
	return {"count": len(live_rows), "data": live_rows}