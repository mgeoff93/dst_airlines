from fastapi import APIRouter, Query
from typing import Optional
from api.core.database import db
from api.services import flight_features
import pandas as pd

router = APIRouter(tags=["Static Metadatas"])


def get_current_subset():
	"""
	Récupère flight_dynamic, calcule les datasets et retourne uniquement `current`
	"""
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	datasets = flight_features.build_flight_datasets(all_flights)
	return datasets["current"]  # liste de dicts


@router.get("/static")
def get_static_flights(
	origin_code: Optional[str] = None,
	destination_code: Optional[str] = None,
	airline_name: Optional[str] = None,
	limit: int = Query(100, ge=1, le=1000)
):
	"""
	Retourne les vols statiques (flight_static) uniquement pour les vols en cours,
	avec filtres optionnels.
	"""

	# --- récupérer les callsigns en cours ---
	current_rows = get_current_subset()
	current_callsigns = {row["callsign"] for row in current_rows if row.get("callsign")}

	if not current_callsigns:
		return {"count": 0, "flights": []}

	# --- construction du IN (%s, %s, ...) ---
	in_placeholders = ",".join(["%s"] * len(current_callsigns))

	sql = f"""
		SELECT
			callsign,
			airline_name,
			origin_code,
			destination_code,
			origin_airport,
			destination_airport,
			origin_city,
			destination_city,
			commercial_flight
		FROM flight_static
		WHERE callsign IN ({in_placeholders})
	"""

	# paramètres SQL : d'abord les callsigns
	params = list(current_callsigns)

	# --- filtres optionnels ---
	if origin_code:
		sql += " AND origin_code = %s"
		params.append(origin_code)

	if destination_code:
		sql += " AND destination_code = %s"
		params.append(destination_code)

	if airline_name:
		sql += " AND airline_name = %s"
		params.append(airline_name)

	# --- tri & limite ---
	sql += " ORDER BY callsign LIMIT %s"
	params.append(limit)

	rows = db.query(sql, tuple(params))

	return {
		"count": len(rows),
		"flights": rows
	}