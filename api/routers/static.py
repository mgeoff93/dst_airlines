from fastapi import APIRouter, Query
from typing import Optional
from api.core.database import db
from api.services import flight_features
import pandas as pd

router = APIRouter(tags=["Static"])

def get_current_subset():
	sql = "SELECT * FROM flight_dynamic ORDER BY last_update DESC"
	all_flights = pd.DataFrame(db.query(sql))
	datasets = flight_features.build_flight_datasets(all_flights)
	return datasets["current"]


@router.get("/static")
def get_static_flights(
	origin_code: Optional[str] = None,
	destination_code: Optional[str] = None,
	airline_name: Optional[str] = None,
	limit: Optional[int] = Query(None, ge=1) # 1. Ajout du paramètre optionnel (minimum 1)
):
	# Récupére les callsigns en cours
	current_rows = get_current_subset()
	current_callsigns = {row["callsign"] for row in current_rows if row.get("callsign")}

	if not current_callsigns:
		return {"count": 0, "flights": []}

	in_placeholders = ",".join(["%s"] * len(current_callsigns))
	
	sql = f"""
		SELECT
			callsign,
			airline_name,
			origin_code,
			destination_code
		FROM flight_static
		WHERE callsign IN ({in_placeholders})
	"""

	params = list(current_callsigns)

	# Filtres optionnels
	if origin_code:
		sql += " AND origin_code ILIKE %s"
		params.append(origin_code)

	if destination_code:
		sql += " AND destination_code ILIKE %s"
		params.append(destination_code)

	if airline_name:
		sql += " AND airline_name ILIKE %s"
		params.append(f"%{airline_name}%")

	# Tri
	sql += " ORDER BY callsign"

	# Ajout de la limite
	if limit is not None:
		sql += " LIMIT %s"
		params.append(limit)

	# Exécution
	rows = db.query(sql, tuple(params))

	return {"count": len(rows), "flights": rows}