from prometheus_client import Counter, Histogram, Gauge

# --- POINT 4 : MÉTRIQUES APPLICATIVES ---

# 1. Volume de requêtes par Alias (Champion vs Challenger)
PREDICTION_COUNT = Counter(
	'api_predictions_total',
	'Nombre total de predictions effectuees',
	['model_alias', 'model_version']
)

# 2. Distribution des prédictions (Détection de dérive/Drift)
# Permet de voir si le Challenger prédit des valeurs absurdes par rapport au Champion
PREDICTION_OUTPUTS = Histogram(
	'api_prediction_output_value',
	'Distribution des retards predits (minutes)',
	['model_alias'],
	buckets=[-15.0, 0.0, 15.0, 30.0, 60.0, 120.0, 240.0]
)

# 3. Statut de chargement MLflow
# Gauge : 1 = OK, 0 = Erreur
MODEL_LOAD_STATUS = Gauge(
	'api_model_load_status',
	'Statut du chargement des modeles depuis MLflow',
	['model_alias']
)