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

	return {
		"normalized": normalized,
		"done": done,
		"current": current
	}

def prepare_train_test(done: pd.DataFrame, current: pd.DataFrame, db) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
	
	# --- Filtrer les unique_key ---
	ids = current["unique_key"].tolist() + done["unique_key"].tolist()
	# --- Charger live_data pour ces vols ---
	sql = "SELECT * FROM live_data WHERE unique_key = ANY(%s)"
	live = pd.DataFrame(db.query(sql, (ids,)))
	# --- Colonnes à récupérer depuis done ---
	cols_to_add = [
		"unique_key",
		"departure_difference",
		"arrival_difference",
		"departure_status",
		"arrival_status"
	]
	# --- Enrichir live_data avec done ---
	live_enriched = live.merge(
		done[cols_to_add],
		on="unique_key",
		how="left"  # garde toutes les lignes de live
	)
	# --- Créer X_train / y_train pour le modèle ---
	X_train = live_enriched[live_enriched["arrival_status"].notna()].copy()
	y_train = X_train["arrival_status"]
	# --- Créer X_test : lignes correspondant à current ---
	current_keys = current["unique_key"].tolist()
	X_test = live_enriched[live_enriched["unique_key"].isin(current_keys)].copy()
	return X_train, y_train, X_test