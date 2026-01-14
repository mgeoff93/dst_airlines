from prometheus_client import Counter, Histogram, Gauge

# --- POINT 4 : MÉTRIQUES APPLICATIVES ---

# 1. Latence de prédiction (P95)
# L'histogramme permet de calculer les percentiles dans Grafana
PREDICTION_LATENCY = Histogram(
	'api_prediction_duration_seconds',
	'Temps de calcul de la prediction',
	['model_alias'],
	buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# 2. Volume de requêtes par Alias (Champion vs Challenger)
PREDICTION_COUNT = Counter(
	'api_predictions_total',
	'Nombre total de predictions effectuees',
	['model_alias', 'model_version']
)

# 3. Distribution des prédictions (Détection de dérive/Drift)
# Permet de voir si le Challenger prédit des valeurs absurdes par rapport au Champion
PREDICTION_OUTPUTS = Histogram(
	'api_prediction_output_value',
	'Distribution des retards predits (minutes)',
	['model_alias'],
	buckets=[-15.0, 0.0, 15.0, 30.0, 60.0, 120.0, 240.0]
)

# 4. Statut de chargement MLflow
# Gauge : 1 = OK, 0 = Erreur
MODEL_LOAD_STATUS = Gauge(
	'api_model_load_status',
	'Statut du chargement des modeles depuis MLflow',
	['model_alias']
)

# 5. État de la base de données (Point 3 - Complément)
DATABASE_STATUS = Gauge(
	'api_database_connected',
	'Statut de la connexion a la base Postgres (0 ou 1)'
)