from prometheus_client import Counter, Histogram, Gauge

# Volume de requêtes par alias (Champion vs Challenger)
PREDICTION_COUNT = Counter(
	'api_predictions_total',
	'Nombre total de predictions effectuees',
	['model_alias', 'model_version']
)

# Distribution des prédictions
PREDICTION_OUTPUTS = Histogram(
	'api_prediction_output_value',
	'Distribution des retards predits (minutes)',
	['model_alias'],
	buckets=[-15.0, 0.0, 15.0, 30.0, 60.0, 120.0, 240.0]
)

# Statut de chargement MLflow
MODEL_LOAD_STATUS = Gauge(
	'api_model_load_status',
	'Statut du chargement des modeles depuis MLflow',
	['model_alias']
)