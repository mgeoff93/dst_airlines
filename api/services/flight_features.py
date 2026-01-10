import pandas as pd
import numpy as np
from api.core.config import STALE_THRESHOLD

def categorize_delay(x):
	"""
	Catégorise le retard :
	- "late" si > 10 min
	- "early" si < -10 min
	- "on time" sinon
	"""
	if pd.isna(x):
		return None # Remplacé pd.NA par None
	if x > 10:
		return "late"
	elif x < -10:
		return "early"
	return "on time"


def dataframe_to_list_of_dicts(df: pd.DataFrame) -> list:
	return df.replace({np.nan: None}).where(pd.notna(df), None).to_dict(orient="records")

def build_flight_datasets(all_flights: pd.DataFrame) -> dict:
	df = all_flights.copy()

	# --- timestamps départ ---
	df["departure_scheduled_ts"] = pd.to_datetime(
		df["flight_date"].astype(str) + " " + df["departure_scheduled"].astype(str),
		errors="coerce"
	)
	df["departure_actual_ts"] = pd.to_datetime(
		df["flight_date"].astype(str) + " " + df["departure_actual"].astype(str),
		errors="coerce"
	)

	departure_next_day = (
		df["departure_actual_ts"].notna() &
		df["departure_scheduled_ts"].notna() &
		(df["departure_actual_ts"] < df["departure_scheduled_ts"])
	)
	df.loc[departure_next_day, "departure_actual_ts"] += pd.Timedelta(days=1)

	# --- timestamps arrivée ---
	arrival_same_day = df["arrival_scheduled"] >= df["departure_scheduled"]

	df["arrival_scheduled_ts"] = pd.to_datetime(
		df["flight_date"].astype(str) + " " + df["arrival_scheduled"].astype(str),
		errors="coerce"
	)
	df.loc[~arrival_same_day, "arrival_scheduled_ts"] += pd.Timedelta(days=1)

	df["arrival_actual_ts"] = pd.to_datetime(
		df["flight_date"].astype(str) + " " + df["arrival_actual"].astype(str),
		errors="coerce"
	)
	df.loc[(~arrival_same_day) & df["arrival_actual_ts"].notna(), "arrival_actual_ts"] += pd.Timedelta(days=1)

	# --- différences ---
	df["departure_difference"] = (df["departure_actual_ts"] - df["departure_scheduled_ts"]).dt.total_seconds() / 60
	df["arrival_difference"] = (df["arrival_actual_ts"] - df["arrival_scheduled_ts"]).dt.total_seconds() / 60

	# --- statuts ---
	df["departure_status"] = df["departure_difference"].apply(categorize_delay)
	df["arrival_status"] = df["arrival_difference"].apply(categorize_delay)

	# --- sélection finale ---
	normalized = df[
		[
			"unique_key", "callsign", "icao24", "status",
			"departure_scheduled_ts", "departure_actual_ts",
			"arrival_scheduled_ts", "arrival_actual_ts",
			"departure_difference", "arrival_difference",
			"departure_status", "arrival_status",
			"last_update"
		]
	].copy()

	# Changement critique : On évite pd.NA qui casse le JSON
	normalized = normalized.where(pd.notna(normalized), None)

# --- datasets métier ---
	normalized = normalized.where(pd.notna(normalized), None)

	done = normalized[
			(normalized["status"] == "arrived") &
			normalized["arrival_status"].notna()
	].copy()

	# FIX : On s'assure que tout est en "naive" (sans TZ) pour la soustraction
	now = pd.Timestamp.utcnow().replace(tzinfo=None) # Force naive
		
	# On force la colonne last_update en datetime naive au cas où
	df["last_update"] = pd.to_datetime(df["last_update"]).dt.tz_localize(None)

	current = normalized[
			normalized["status"].isin(["departing", "en route"])
	].copy()

	# S'assurer que current["last_update"] est bien datetime pour le calcul
	current["last_update"] = pd.to_datetime(current["last_update"]).dt.tz_localize(None)

	current = current[
			~(
					current["arrival_actual_ts"].isna() &
					(now > current["arrival_scheduled_ts"].dt.tz_localize(None)) & # localize(None) ici aussi
					((now - current["last_update"]) > STALE_THRESHOLD)
			)
	].copy()

	return {
		"done": dataframe_to_list_of_dicts(done),
		"current": dataframe_to_list_of_dicts(current)
	}