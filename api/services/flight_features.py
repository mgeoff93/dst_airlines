import pandas as pd
import numpy as np
from api.core.config import STALE_THRESHOLD

def dataframe_to_list_of_dicts(df: pd.DataFrame) -> list:
	return df.replace({np.nan: None}).where(pd.notna(df), None).to_dict(orient="records")

def build_flight_datasets(all_flights: pd.DataFrame) -> dict:
	df = all_flights.copy()
	ts_format = "%Y-%m-%d %H:%M:%S"
	
	# 1. Nettoyage des chaînes
	f_date_clean = df["flight_date"].astype(str).str.strip()
	dep_sched_clean = df["departure_scheduled"].astype(str).str.strip()

	# 2. Conversion en Naive (Sans fuseau) immédiatement pour éviter le crash
	dep_sched_ts = pd.to_datetime(f_date_clean + " " + dep_sched_clean, format=ts_format, errors="coerce")
	# FIX CRITIQUE : On force last_update en naive dès le début
	last_upd_ts = pd.to_datetime(df["last_update"]).dt.tz_localize(None)

	# 3. RECOUVREMENT DU BUG (Comparaison maintenant possible car les deux sont naives)
	mask_fix = (df["status"].isin(["en route", "arrived"])) & \
			   (dep_sched_ts.notna()) & \
			   (last_upd_ts < (dep_sched_ts - pd.Timedelta(hours=2)))
	
	df.loc[mask_fix, "flight_date"] = pd.to_datetime(df["flight_date"]) - pd.Timedelta(days=1)
	
	# 4. Reconstruction finale
	f_date_fixed = df["flight_date"].astype(str).str.strip()
	
	# On retire "format=ts_format" pour permettre la détection automatique
	df["departure_scheduled_ts"] = pd.to_datetime(f_date_fixed + " " + dep_sched_clean, errors="coerce")
	df["departure_actual_ts"] = pd.to_datetime(f_date_fixed + " " + df["departure_actual"].astype(str).str.strip(), errors="coerce")
	df["arrival_scheduled_ts"] = pd.to_datetime(f_date_fixed + " " + df["arrival_scheduled"].astype(str).str.strip(), errors="coerce")
	df["arrival_actual_ts"] = pd.to_datetime(f_date_fixed + " " + df["arrival_actual"].astype(str).str.strip(), errors="coerce")
	
	# On remplace par la version naive nettoyée
	df["last_update"] = last_upd_ts

	# --- 2. Date Wrap (Passage de minuit) ---
	for col in ["departure_actual_ts", "arrival_scheduled_ts"]:
		mask = (df[col].notna()) & (df[col] < df["departure_scheduled_ts"])
		df.loc[mask, col] += pd.Timedelta(days=1)

	mask_arr_act = (df["arrival_actual_ts"].notna()) & (df["departure_actual_ts"].notna()) & (df["arrival_actual_ts"] < df["departure_actual_ts"])
	df.loc[mask_arr_act, "arrival_actual_ts"] += pd.Timedelta(days=1)

	# --- 3. Calculs deltas ---
	df["departure_difference"] = (df["departure_actual_ts"] - df["departure_scheduled_ts"]).dt.total_seconds() / 60
	df["arrival_difference"] = (df["arrival_actual_ts"] - df["arrival_scheduled_ts"]).dt.total_seconds() / 60

	# --- 4. Filtres et Datasets ---
	cols_to_keep = ["unique_key", "callsign", "icao24", "status", "departure_scheduled_ts", 
					"departure_actual_ts", "arrival_scheduled_ts", "arrival_actual_ts", 
					"departure_difference", "arrival_difference", "last_update"]
	
	normalized = df[cols_to_keep].copy()
	done = normalized[(normalized["status"] == "arrived") & (normalized["arrival_difference"].notna())].copy()
	
	# FIX : now() doit être naive pour la soustraction plus bas
	now = pd.Timestamp.now() 
	current = normalized[normalized["status"].isin(["departing", "en route"])].copy()

	if not current.empty:
		seconds_since_update = (now - current["last_update"]).dt.total_seconds()
		threshold_seconds = STALE_THRESHOLD.total_seconds()
		mask_stale = (current["arrival_actual_ts"].isna() & 
					  (now > current["arrival_scheduled_ts"]) & 
					  (seconds_since_update > threshold_seconds))
		current = current[~mask_stale].copy()

	return {
		"done": dataframe_to_list_of_dicts(done),
		"current": dataframe_to_list_of_dicts(current)
	}