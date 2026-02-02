import pandas as pd
import numpy as np
from api.core.config import STALE_THRESHOLD

def dataframe_to_list_of_dicts(df: pd.DataFrame) -> list:
	return df.replace({np.nan: None}).where(pd.notna(df), None).to_dict(orient="records")

def build_flight_datasets(all_flights: pd.DataFrame) -> dict:
	df = all_flights.copy()
	
	# Normalisation des formats
	ts_format = "%Y-%m-%d %H:%M:%S"
	for col in ["departure_scheduled", "departure_actual", "arrival_scheduled", "arrival_actual"]:
		df[f"{col}_ts"] = pd.to_datetime(
			df["flight_date"].astype(str) + " " + df[col].astype(str), 
			format=ts_format, 
			errors="coerce"
		)
	
	df["last_update"] = pd.to_datetime(df["last_update"]).dt.tz_localize(None)
	now = pd.Timestamp.utcnow().replace(tzinfo=None)
	
	# Détection de l'état du vol
	df["_day_adjust_state"] = "initial"
	
	# Recalage J-1 global
	is_yesterday_flight = (
		(df["status"].isin(["en route", "arrived"])) & 
		(df["departure_scheduled_ts"] > now + pd.Timedelta(hours=6))
	)
	
	cols_scheduled_ts = ["departure_scheduled_ts", "arrival_scheduled_ts"]
	df.loc[is_yesterday_flight, cols_scheduled_ts] -= pd.Timedelta(days=1)
	df.loc[is_yesterday_flight, "_day_adjust_state"] = "shifted_J-1_global"
	
	# Recalage J-1
	is_yesterday_actual_too = (
		is_yesterday_flight & 
		((df["departure_actual_ts"] > now + pd.Timedelta(hours=6)) | 
		 (df["arrival_actual_ts"] > now + pd.Timedelta(hours=6)))
	)
	cols_actual_ts = ["departure_actual_ts", "arrival_actual_ts"]
	df.loc[is_yesterday_actual_too, cols_actual_ts] -= pd.Timedelta(days=1)
	df.loc[is_yesterday_actual_too, "_day_adjust_state"] = "shifted_J-1_global (scheduled+actual)"
	
	# Corrections individuelles J+1
	not_yesterday = ~is_yesterday_flight
	
	# Départ réel après minuit
	dep_next_day = (
		not_yesterday &
		(df["departure_actual_ts"].notna()) & 
		(df["departure_actual_ts"] < df["departure_scheduled_ts"])
	)
	df.loc[dep_next_day, "departure_actual_ts"] += pd.Timedelta(days=1)
	df.loc[dep_next_day & (df["_day_adjust_state"] == "initial"), "_day_adjust_state"] = "shifted_J+1_departure_actual"
	
	# Arrivée prévue après minuit
	arr_sched_next_day = (
		(df["arrival_scheduled_ts"].notna()) & 
		(df["arrival_scheduled_ts"] < df["departure_scheduled_ts"])
	)
	df.loc[arr_sched_next_day, "arrival_scheduled_ts"] += pd.Timedelta(days=1)
	df.loc[arr_sched_next_day & (df["_day_adjust_state"] == "initial"), "_day_adjust_state"] = "shifted_J+1_arrival_scheduled"
	df.loc[arr_sched_next_day & is_yesterday_flight, "_day_adjust_state"] += " + J+1_arr_sched"
	
	# C. Arrivée réelle après minuit
	arr_actual_next_day = (
		(df["arrival_actual_ts"].notna()) & 
		(df["departure_actual_ts"].notna()) & 
		(df["arrival_actual_ts"] < df["departure_actual_ts"])
	)
	df.loc[arr_actual_next_day, "arrival_actual_ts"] += pd.Timedelta(days=1)
	df.loc[arr_actual_next_day & (df["_day_adjust_state"] == "initial"), "_day_adjust_state"] = "shifted_J+1_arrival_actual"
	df.loc[arr_actual_next_day & is_yesterday_flight, "_day_adjust_state"] += " + J+1_arr_actual"
	
	# Validation métier
	df["_flight_duration_hours"] = (
		(df["arrival_actual_ts"] - df["departure_actual_ts"]).dt.total_seconds() / 3600
	)
	
	suspicious_duration = (df["_flight_duration_hours"] > 24) | (df["_flight_duration_hours"] < 0)
	df.loc[suspicious_duration, "_day_adjust_state"] += " [SUSPICIOUS_DURATION]"
	
	# Calculs des différences
	df["departure_difference"] = (df["departure_actual_ts"] - df["departure_scheduled_ts"]).dt.total_seconds() / 60
	df["arrival_difference"] = (df["arrival_actual_ts"] - df["arrival_scheduled_ts"]).dt.total_seconds() / 60
	
	# Sélection finale
	cols_to_keep = [
		"unique_key", "callsign", "icao24", "status",
		"departure_scheduled_ts", "departure_actual_ts",
		"arrival_scheduled_ts", "arrival_actual_ts",
		"departure_difference", "arrival_difference",
		"last_update"
	]
	normalized = df[cols_to_keep].copy()

	# Séparation et nettoyage JSON
	done = normalized[(normalized["status"] == "arrived") & (normalized["arrival_difference"].notna())].copy()
	current = normalized[normalized["status"].isin(["departing", "en route"])].copy()

	# Filtrage
	seconds_since_update = (now - current["last_update"]).dt.total_seconds()
	threshold_seconds = STALE_THRESHOLD.total_seconds()
	mask_stale = (
		current["arrival_actual_ts"].isna() & 
		(now > current["arrival_scheduled_ts"]) & 
		(seconds_since_update > threshold_seconds)
	)
	current = current[~mask_stale].copy()
	
	return {
		"done": dataframe_to_list_of_dicts(done),
		"current": dataframe_to_list_of_dicts(current)
	}