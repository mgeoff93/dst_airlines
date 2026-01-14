from fastapi import APIRouter, Query
from typing import Optional
from api.core.database import db
import pandas as pd

router = APIRouter(tags=["Geography"])

@router.get("/airports")
def get_airports(
	airport_code: Optional[str] = None,
	airport_name: Optional[str] = None,
	country_code: Optional[str] = None,
	limit: Optional[int] = Query(None, ge = 1)
):
	query_str = "SELECT * FROM airports WHERE 1=1"
	params = {}

	if airport_code:
		query_str += " AND airport_code ILIKE %(airport_code)s"
		params["airport_code"] = airport_code

	if airport_name:
		query_str += " AND airport_name ILIKE %(airport_name)s"
		params["airport_name"] = f"%{airport_name}%"

	if country_code:
		query_str += " AND country_code ILIKE %(country_code)s"
		params["country_code"] = country_code

	if limit:
		query_str += " LIMIT %(limit)s"
		params["limit"] = limit

	airports = pd.DataFrame(db.query(query_str, params))
	return {"count": len(airports), "airports": airports.to_dict(orient = "records")}

@router.get("/countries")
def get_countries( # Correction du nom de la fonction
	country_code: Optional[str] = None,
	country_name: Optional[str] = None,
	limit: Optional[int] = Query(None, ge = 1)
):
	query_str = "SELECT * FROM countries WHERE 1=1"
	params = {}

	if country_code:
		query_str += " AND country_code ILIKE %(country_code)s"
		params["country_code"] = country_code

	if country_name:
		query_str += " AND country_name ILIKE %(country_name)s"
		params["country_name"] = f"%{country_name}%"

	if limit:
		query_str += " LIMIT %(limit)s"
		params["limit"] = limit

	countries = pd.DataFrame(db.query(query_str, params))
	return {"count": len(countries), "countries": countries.to_dict(orient = "records")}