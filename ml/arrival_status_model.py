import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder

def train_model(done_df: pd.DataFrame):
	df = done_df.copy()
	# Colonnes numériques
	num_features = ["departure_difference", "arrival_difference"]
	X_num = df[num_features].fillna(0)

	# Colonnes catégoriques
	cat_features = ["departure_status", "arrival_status"]
	encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)
	X_cat = encoder.fit_transform(df[cat_features].fillna("unknown"))

	X = np.hstack([X_num.values, X_cat])
	y = df["arrival_status"]

	model = RandomForestClassifier(n_estimators=100, random_state=42)
	model.fit(X, y)

	return model, encoder, num_features

def predict(model, encoder, num_features, df: pd.DataFrame) -> pd.DataFrame:
	X_num = df[num_features].fillna(0)
	cat_features = ["departure_status", "arrival_status"]
	X_cat = encoder.transform(df[cat_features].fillna("unknown"))
	X = np.hstack([X_num.values, X_cat])

	df = df.copy()
	df["arrival_status_pred"] = model.predict(X)
	return df