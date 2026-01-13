from enum import Enum
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import pandas as pd
from api.core.database import db
from api.services import flight_features
from api.metrics import DB_RECORDS_PROCESSED

router = APIRouter(tags = ["Dynamic metadatas"])

# 1. Définition des choix (les valeurs doivent matcher tes conditions if/elif)
class FlightStatus(str, Enum):
	live = "live"
	history = "historical"
	all = "all"

def get_datasets():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	return flight_features.build_flight_datasets(all_flights)

@router.get("/dynamic")
def get_dynamic_flights(
	timeline: FlightStatus = Query(FlightStatus.all),
	callsign: Optional[str] = None,
	limit: Optional[int] = Query(None, ge=1) # 1. Ajout de la limite optionnelle
):
	datasets = get_datasets()
	total_loaded = len(datasets["current"]) + len(datasets["done"])
	DB_RECORDS_PROCESSED.labels(table_name = "flight_dynamic").inc(total_loaded)
	
	# 3. Sélection du dataset
	if timeline == FlightStatus.live:
		rows = datasets["current"]
	elif timeline == FlightStatus.history:
		rows = datasets["done"]
	else:
		rows = datasets["current"] + datasets["done"]

	# --- filtre callsign ---
	if callsign:
		rows = [r for r in rows if r.get("callsign") == callsign]

	# --- tri cohérent ---
	rows = sorted(
		rows, 
		key=lambda r: r.get("last_update") if r.get("last_update") is not None else pd.Timestamp.min, 
		reverse=True
	)

	# --- 2. Application de la limite (Slicing Python) ---
	if limit is not None:
		rows = rows[:limit]

	return {
		"count": len(rows),
		"data": rows
	}