from fastapi import APIRouter, HTTPException
from api.core.database import db
from api.services import flight_features
import pandas as pd
from api.metrics import API_RESPONSE_TIME
import time

router = APIRouter(tags=["Merged"])

def get_datasets():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	return flight_features.build_flight_datasets(all_flights)


def get_static_flight(callsign: str):
	sql = """
		SELECT *
		FROM flight_static
		WHERE callsign = %s
	"""
	rows = db.query(sql, (callsign,))
	return rows[0] if rows else None


def get_live_rows(callsign: str, unique_key: str):
	sql = """
		SELECT *
		FROM live_data
		WHERE callsign = %s AND unique_key = %s
		ORDER BY request_id ASC
	"""
	return db.query(sql, (callsign, unique_key))


@router.get("/merged/{callsign}")
def get_merged_flight(callsign: str):
	start_time = time.time()
	# --- STATIC ---
	static_data = get_static_flight(callsign)
	if not static_data:
		raise HTTPException(status_code = 404, detail = "Callsign not found")

	# --- DATASETS ---
	datasets = get_datasets()
	done = datasets.get("done", [])
	current = datasets.get("current", [])

	# --- HISTORY ---
	history = []
	for flight in done:
		if flight["callsign"] != callsign:
			continue

		live_rows = get_live_rows(callsign, flight["unique_key"])

		history.append({
			"unique_key": flight.get("unique_key"),
			"status": flight.get("status"),
			"departure_scheduled_ts": flight.get("departure_scheduled_ts"),
			"departure_actual_ts": flight.get("departure_actual_ts"),
			"arrival_scheduled_ts": flight.get("arrival_scheduled_ts"),
			"arrival_actual_ts": flight.get("arrival_actual_ts"),
			"departure_difference": flight.get("departure_difference"),
			"arrival_difference": flight.get("arrival_difference"),
			"last_update": flight.get("last_update"),
			"live_data": live_rows
		})

	# --- LIVE ---
	live = []
	for flight in current:
		if flight["callsign"] != callsign:
			continue

		live_rows = get_live_rows(callsign, flight["unique_key"])

		live.append({
			"unique_key": flight.get("unique_key"),
			"status": flight.get("status"),  # en route / departing
			"departure_scheduled_ts": flight.get("departure_scheduled_ts"),
			"departure_actual_ts": flight.get("departure_actual_ts"),
			"arrival_scheduled_ts": flight.get("arrival_scheduled_ts"),
			"arrival_actual_ts": flight.get("arrival_actual_ts"),
			"departure_difference": flight.get("departure_difference"),
			"arrival_difference": flight.get("arrival_difference"),
			"last_update": flight.get("last_update"),
			"live_data": live_rows
		})

	API_RESPONSE_TIME.observe(time.time() - start_time)

	# --- RESPONSE ---
	return {
		"callsign": callsign,
		"airline_name": static_data.get("airline_name"),
		"origin_code": static_data.get("origin_code"),
		"destination_code": static_data.get("destination_code"),
		"history": history,
		"live": live
	}