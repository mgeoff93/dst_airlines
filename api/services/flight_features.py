import numpy as np
import pandas as pd

STALE_THRESHOLD = pd.Timedelta(hours = 2)

def dataframe_to_json_safe(df: pd.DataFrame) -> list[dict]:
	df = df.copy()

	# Remplacer inf / -inf / NaN et les floats trop grands
	for col in df.select_dtypes(include=[float, np.float64]):
		df[col] = df[col].apply(lambda x: x if pd.notna(x) and np.isfinite(x) else None)

	# Convertir tous les datetimes en ISO strings
	for col in df.columns:
		if pd.api.types.is_datetime64_any_dtype(df[col]):
			df[col] = df[col].apply(lambda x: x.isoformat() if x is not None else None)

	return df.astype(object).to_dict(orient="records")


def build_flight_datasets(all_flights: pd.DataFrame) -> dict:
	
	# --- timestamps départ ---
	all_flights["departure_scheduled_ts"] = pd.to_datetime(
		all_flights["flight_date"].astype(str) + " " +
		all_flights["departure_scheduled"].astype(str),
		errors="coerce"
	)

	all_flights["departure_actual_ts"] = pd.to_datetime(
		all_flights["flight_date"].astype(str) + " " +
		all_flights["departure_actual"].astype(str),
		errors="coerce"
	)

	departure_next_day = (
		all_flights["departure_actual_ts"].notna()
		& all_flights["departure_scheduled_ts"].notna()
		& (all_flights["departure_actual_ts"] < all_flights["departure_scheduled_ts"])
	)

	all_flights.loc[departure_next_day, "departure_actual_ts"] += pd.Timedelta(days=1)

	# --- timestamps arrivée ---
	arrival_same_day = (
		all_flights["arrival_scheduled"] >= all_flights["departure_scheduled"]
	)

	all_flights["arrival_scheduled_ts"] = pd.to_datetime(
		all_flights["flight_date"].astype(str) + " " +
		all_flights["arrival_scheduled"].astype(str),
		errors="coerce"
	)

	all_flights.loc[~arrival_same_day, "arrival_scheduled_ts"] += pd.Timedelta(days=1)

	all_flights["arrival_actual_ts"] = pd.to_datetime(
		all_flights["flight_date"].astype(str) + " " +
		all_flights["arrival_actual"].astype(str),
		errors="coerce"
	)

	all_flights.loc[
		(~arrival_same_day) & all_flights["arrival_actual_ts"].notna(),
		"arrival_actual_ts"
	] += pd.Timedelta(days=1)

	# --- différences ---
	all_flights["departure_difference"] = (
		all_flights["departure_actual_ts"]
		- all_flights["departure_scheduled_ts"]
	).dt.total_seconds() / 60

	all_flights["arrival_difference"] = (
		all_flights["arrival_actual_ts"]
		- all_flights["arrival_scheduled_ts"]
	).dt.total_seconds() / 60

	# --- statuts ---
	def categorize_delay(x):
		if pd.isna(x):
			return pd.NA
		if x > 10:
			return "late"
		elif x < -10:
			return "early"
		return "on time"

	all_flights["departure_status"] = all_flights["departure_difference"].apply(categorize_delay)
	all_flights["arrival_status"] = all_flights["arrival_difference"].apply(categorize_delay)

	# --- sélection finale ---
	normalized = all_flights[
		[
			"unique_key", "callsign", "icao24", "status",
			"departure_scheduled_ts", "departure_actual_ts",
			"arrival_scheduled_ts", "arrival_actual_ts",
			"departure_difference", "arrival_difference",
			"departure_status", "arrival_status",
			"last_update"
		]
	].copy()

	normalized = normalized.where(normalized.notna(), pd.NA)

	# --- datasets métier ---
	done = normalized[
		(normalized["status"] == "arrived")
		& normalized["arrival_status"].notna()
	].copy()

	now = pd.Timestamp.utcnow().tz_localize(None)

	current = normalized[
		normalized["status"].isin(["departing", "en route"])
	]

	current = current[
		~(
			current["arrival_actual_ts"].isna()
			& (now > current["arrival_scheduled_ts"])
			& ((now - current["last_update"]) > STALE_THRESHOLD)
		)
	].copy()

	def sanitize_for_json(df: pd.DataFrame) -> pd.DataFrame:
		df = df.replace([np.inf, -np.inf], pd.NA)
		return df.where(df.notna(), None)

	return {
		"normalized": sanitize_for_json(normalized),
		"done": sanitize_for_json(done),
		"current": sanitize_for_json(current)
	}