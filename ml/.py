from api.postgres_client import PostgresClient
import pandas as pd
import numpy as np

# --- Chargement des données ---
db = PostgresClient()
sql = "SELECT * FROM flight_dynamic"
all_flights = pd.DataFrame(db.query(sql))

# ============================================================================
# 1. Construction des timestamps de départ
# ============================================================================

# Départ planifié
all_flights["departure_scheduled_ts"] = pd.to_datetime(
	all_flights["flight_date"].astype(str) + " " +
	all_flights["departure_scheduled"].astype(str),
	errors="coerce"
)

# Départ réel (brut)
all_flights["departure_actual_ts"] = pd.to_datetime(
	all_flights["flight_date"].astype(str) + " " +
	all_flights["departure_actual"].astype(str),
	errors="coerce"
)

# 🔧 Correction des départs après minuit
departure_next_day = (
	all_flights["departure_actual_ts"].notna() &
	all_flights["departure_scheduled_ts"].notna() &
	(all_flights["departure_actual_ts"] < all_flights["departure_scheduled_ts"])
)

all_flights.loc[departure_next_day, "departure_actual_ts"] += pd.Timedelta(days=1)

# ============================================================================
# 2. Construction des timestamps d'arrivée
# ============================================================================

arrival_same_day = (
	all_flights["arrival_scheduled"] >= all_flights["departure_scheduled"]
)

# Arrivée planifiée
all_flights["arrival_scheduled_ts"] = pd.to_datetime(
	all_flights["flight_date"].astype(str) + " " +
	all_flights["arrival_scheduled"].astype(str),
	errors="coerce"
)

# +1 jour si arrivée planifiée après minuit
all_flights.loc[~arrival_same_day, "arrival_scheduled_ts"] += pd.Timedelta(days=1)

# Arrivée réelle (brut)
all_flights["arrival_actual_ts"] = pd.to_datetime(
	all_flights["flight_date"].astype(str) + " " +
	all_flights["arrival_actual"].astype(str),
	errors="coerce"
)

# +1 jour si arrivée réelle après minuit
all_flights.loc[
	(~arrival_same_day) & all_flights["arrival_actual_ts"].notna(),
	"arrival_actual_ts"
] += pd.Timedelta(days=1)

# ============================================================================
# 3. Calcul des différences (en minutes)
# ============================================================================

all_flights["departure_difference"] = (
	all_flights["departure_actual_ts"] -
	all_flights["departure_scheduled_ts"]
).dt.total_seconds() / 60

all_flights["arrival_difference"] = (
	all_flights["arrival_actual_ts"] -
	all_flights["arrival_scheduled_ts"]
).dt.total_seconds() / 60

# ============================================================================
# 4. Statuts catégoriels
# ============================================================================

def categorize_delay(x):
	if pd.isna(x):
		return pd.NA
	if x > 10:
		return "late"
	elif x < -10:
		return "early"
	else:
		return "on time"

all_flights["departure_status"] = all_flights["departure_difference"].apply(categorize_delay)
all_flights["arrival_status"] = all_flights["arrival_difference"].apply(categorize_delay)

# ============================================================================
# 5. Sélection des colonnes normalisées
# ============================================================================

normalized_flights = all_flights[
	[
		"unique_key", "callsign", "icao24", "status",
		"departure_scheduled_ts", "departure_actual_ts",
		"arrival_scheduled_ts", "arrival_actual_ts",
		"departure_difference", "arrival_difference",
		"departure_status", "arrival_status",
		"last_update"
	]
].copy()

# ============================================================================
# 6. Homogénéisation des types et des valeurs nulles
# ============================================================================

# Numériques → Float64 nullable
for col in ["departure_difference", "arrival_difference"]:
	normalized_flights[col] = normalized_flights[col].astype("Float64")

# Datetimes → datetime64[ns]
for col in [
	"departure_scheduled_ts", "departure_actual_ts",
	"arrival_scheduled_ts", "arrival_actual_ts",
	"last_update"
]:
	normalized_flights[col] = pd.to_datetime(
		normalized_flights[col],
		errors="coerce"
	)

# Textes → string nullable
for col in [
	"unique_key", "callsign", "icao24", "status",
	"departure_status", "arrival_status"
]:
	normalized_flights[col] = normalized_flights[col].astype("string")

# Harmonisation finale des valeurs nulles → pd.NA
for col in normalized_flights.columns:
	normalized_flights[col] = normalized_flights[col].where(
		normalized_flights[col].notna(),
		pd.NA
	)

# --- Création du dataset des vols réellement arrivés ---
done = normalized_flights[
	(normalized_flights["status"] == "arrived") &
	(normalized_flights["arrival_status"].notna())
].copy()


# --- Création du dataset des vols réellement en cours (ou au départ) ---
now = pd.Timestamp.utcnow().tz_localize(None)
STALE_THRESHOLD = pd.Timedelta(hours=2)  # seuil à ajuster

current = normalized_flights[
	normalized_flights["status"].isin(["departing", "en route"])
]

current = current[
    ~(
        current["arrival_actual_ts"].isna()
        & (now > current["arrival_scheduled_ts"])
        & ((now - current["last_update"]) > STALE_THRESHOLD)
    )
].copy()










done_counts = done["callsign"].value_counts()
repeated_callsigns = done_counts[done_counts > 3].index
current_repeated = current[current["callsign"].isin(repeated_callsigns)]

#                              unique_key callsign  icao24    status departure_scheduled_ts departure_actual_ts arrival_scheduled_ts arrival_actual_ts  departure_difference  arrival_difference departure_status arrival_status                last_update
# 2113  AFR650_394a0f_2025-12-31_12:35:00   AFR650  394a0f  en route    2025-12-31 12:35:00 2025-12-31 13:34:00  2025-12-31 17:45:00               NaT                  59.0                <NA>             late           <NA> 2025-12-31 16:22:28.705755       
# 2164  AFR196_3949e4_2025-12-31_14:10:00   AFR196  3949e4  en route    2025-12-31 14:10:00 2025-12-31 14:20:00  2026-01-01 07:30:00               NaT                  10.0                <NA>          on time           <NA> 2025-12-31 17:30:39.808779       

CALLSIGN = "AFR650"
current_filtered = current[current["callsign"] == CALLSIGN]
done_filtered = done[done["callsign"] == CALLSIGN].sort_values("departure_scheduled_ts")

# --- Chargement des données ---
ids = current_filtered["unique_key"].tolist() + done_filtered["unique_key"].tolist()
sql = "SELECT * FROM live_data WHERE unique_key = ANY(%s)"
live = pd.DataFrame(db.query(sql, (ids,)))

# Colonnes à récupérer depuis done_filtered
cols_to_add = [
    "unique_key", 
    "departure_difference", 
    "arrival_difference", 
    "departure_status", 
    "arrival_status"
]

# Merge sur 'unique_key'
live_enriched = live.merge(
    done_filtered[cols_to_add],
    on="unique_key",
    how="left"  # garde toutes les lignes de live_filtered
)

train = 
test = 
