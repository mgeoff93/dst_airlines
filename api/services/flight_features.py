import pandas as pd
import numpy as np
from api.core.config import STALE_THRESHOLD

def dataframe_to_list_of_dicts(df: pd.DataFrame) -> list:
	# On remplace les types numériques NaN et les objets NaT par None pour le JSON
	return df.where(pd.notna(df), None).to_dict(orient="records")

def build_flight_datasets(all_flights: pd.DataFrame) -> dict:
	df = all_flights.copy()
	# --- 1. Normalisation des formats et Timestamps de base ---
	# Utilisation du format explicite pour supprimer le UserWarning et gagner en performance
	ts_format = "%Y-%m-%d %H:%M:%S"
	df["departure_scheduled_ts"] = pd.to_datetime(df["flight_date"].astype(str) + " " + df["departure_scheduled"].astype(str), format=ts_format, errors="coerce")
	df["departure_actual_ts"] = pd.to_datetime(df["flight_date"].astype(str) + " " + df["departure_actual"].astype(str), format=ts_format, errors="coerce")
	df["arrival_scheduled_ts"] = pd.to_datetime(df["flight_date"].astype(str) + " " + df["arrival_scheduled"].astype(str), format=ts_format, errors="coerce")
	df["arrival_actual_ts"] = pd.to_datetime(df["flight_date"].astype(str) + " " + df["arrival_actual"].astype(str), format=ts_format, errors="coerce")
	df["last_update"] = pd.to_datetime(df["last_update"]).dt.tz_localize(None)
	# --- 2. Gestion des passages de minuit (Date Wrap) ---
	# A. Départ réel : si l'heure réelle < heure prévue, l'avion est parti après minuit (J+1)
	# Note : Cette règle suppose que le retard n'excède pas 24h.
	dep_next_day = (df["departure_actual_ts"].notna()) & (df["departure_actual_ts"] < df["departure_scheduled_ts"])
	df.loc[dep_next_day, "departure_actual_ts"] += pd.Timedelta(days=1)
	# B. Arrivée prévue : on compare par rapport au départ prévu
	arr_sched_next_day = (df["arrival_scheduled_ts"].notna()) & (df["arrival_scheduled_ts"] < df["departure_scheduled_ts"])
	df.loc[arr_sched_next_day, "arrival_scheduled_ts"] += pd.Timedelta(days=1)
	# C. Arrivée réelle : Correction CRITIQUE. 
	# On compare l'arrivée réelle par rapport au départ réel pour savoir si on a changé de jour.
	arr_actual_next_day = (df["arrival_actual_ts"].notna()) & (df["departure_actual_ts"].notna()) & (df["arrival_actual_ts"] < df["departure_actual_ts"])
	df.loc[arr_actual_next_day, "arrival_actual_ts"] += pd.Timedelta(days=1)
	# --- 3. Calculs des différences et Statuts ---
	df["departure_difference"] = (df["departure_actual_ts"] - df["departure_scheduled_ts"]).dt.total_seconds() / 60
	df["arrival_difference"] = (df["arrival_actual_ts"] - df["arrival_scheduled_ts"]).dt.total_seconds() / 60
	# --- 4. Sélection et Nettoyage final ---
	cols_to_keep = [
		"unique_key", "callsign", "icao24", "status",
		"departure_scheduled_ts", "departure_actual_ts",
		"arrival_scheduled_ts", "arrival_actual_ts",
		"departure_difference", "arrival_difference",
		"last_update"
	]
	normalized = df[cols_to_keep].copy()
	# --- 5. Séparation des Datasets métiers ---
	# Dataset des vols terminés
	done = normalized[
		(normalized["status"] == "arrived") & 
		(normalized["arrival_difference"].notna())
	].copy()
	# Dataset des vols en cours
	now = pd.Timestamp.utcnow().replace(tzinfo=None)
	current = normalized[normalized["status"].isin(["departing", "en route"])].copy()
	# --- FIX : Comparaison des deltas ---
	# 1. On calcule l'ancienneté de la donnée en secondes
	# .dt.total_seconds() transforme la Series de Timedelta en Series de float
	seconds_since_update = (now - current["last_update"]).dt.total_seconds()
	# 2. On récupère le seuil en secondes (1800 dans ton cas)
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