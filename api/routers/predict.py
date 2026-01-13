from fastapi import APIRouter, HTTPException
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
import pandas as pd
import os
import numpy as np

router = APIRouter(prefix="/prediction", tags=["ML Prediction"])

# 1. Récupération et validation des variables d'env
MLFLOW_URI = os.getenv("MLFLOW_API_URL")
MODEL_NAME = os.getenv("MODEL_NAME")

def get_latest_model():
    # Sécurité : vérifier que les variables sont présentes
    if not MLFLOW_URI or not MODEL_NAME:
        print("❌ Erreur: MLFLOW_API_URL ou MODEL_NAME non définis dans l'environnement")
        return None
        
    try:
        # 2. Configurer l'URI avant toute action MLflow
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()
        
        # 3. Récupérer les versions (Gestion du cas "alias" recommandé par MLflow)
        versions = client.get_latest_versions(MODEL_NAME, stages=["None", "Production"])
        if not versions:
            print(f"⚠️ Aucune version trouvée pour le modèle {MODEL_NAME}")
            return None
            
        latest_version = max(int(v.version) for v in versions)
        model_uri = f"models:/{MODEL_NAME}/{latest_version}"
        
        print(f"🚀 Chargement du modèle {MODEL_NAME} version {latest_version}")
        return mlflow.pyfunc.load_model(model_uri)
    except Exception as e:
        print(f"⚠️ Erreur lors du chargement: {e}")
        return None

# Chargement initial au démarrage de l'API
model = get_latest_model()

@router.post("/arrival-delay")
async def predict_delay(data: dict):
    # Global 'model' pour s'assurer qu'on utilise celui chargé au démarrage
    if model is None:
        raise HTTPException(status_code=503, detail="Modèle non disponible sur le serveur")
    
    try:
        # 4. Conversion en DataFrame
        df = pd.DataFrame([data])
        
        # 5. Prédiction et gestion du type de sortie
        prediction = model.predict(df)
        
        # MLflow/Sklearn renvoient souvent un array numpy. 
        # On extrait la valeur et on convertit en float standard pour JSON.
        result = prediction[0]
        if isinstance(result, (np.ndarray, list)):
            result = result[0]

        return {
            "status": "success",
            "predicted_delay": float(result),
            "model_version": "latest"
        }
    except Exception as e:
        # 6. Log de l'erreur côté serveur pour le debug
        print(f"❌ Erreur prédiction: {e}")
        raise HTTPException(status_code=400, detail=f"Erreur lors de la prédiction: {str(e)}")