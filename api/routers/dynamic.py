from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from api.core.database import db
from api.services import flight_features
import pandas as pd
from api.metrics import DB_RECORDS_PROCESSED

router = APIRouter(tags=["Dynamic metadatas"])

def get_datasets():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	return flight_features.build_flight_datasets(all_flights)

@router.get("/dynamic")
def get_dynamic_flights(
	status: Optional[str] = None,  # "history" ou "live"
	callsign: Optional[str] = None
):
	if status not in (None, "live", "history"):
		raise HTTPException(status_code=400, detail="status must be 'live' or 'history'")

	datasets = get_datasets()
	total_loaded = len(datasets["current"]) + len(datasets["done"])
	DB_RECORDS_PROCESSED.labels(table_name="flight_dynamic").inc(total_loaded)
	
	# --- sélection du dataset ---
	if status == "live":
		rows = datasets["current"]
	elif status == "history":
		rows = datasets["done"]
	else:
		rows = datasets["current"] + datasets["done"]

	# --- filtre callsign ---
	if callsign:
		rows = [r for r in rows if r.get("callsign") == callsign]

	# --- tri cohérent ---
	rows = sorted(rows, key=lambda r: r.get("last_update") or "", reverse=True)

	return {
		"count": len(rows),
		"data": rows
	}