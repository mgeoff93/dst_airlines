# import mlflow.sklearn
# import pandas as pd

# # Charger le modèle MLflow Production au démarrage
# MODEL_NAME = "arrival_status_prediction"
# MODEL_STAGE = "Production"

# def load_model():
# 	return mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{MODEL_STAGE}")

# model = load_model()

# def predict(features: pd.DataFrame):
# 	"""
# 	features: dataframe avec colonnes static/dynamic/live comme discuté
# 	retourne la prédiction arrival_status
# 	"""
# 	preds = model.predict(features)
# 	return preds