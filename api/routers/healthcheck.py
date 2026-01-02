from fastapi import APIRouter, HTTPException
from api.core.database import db

router = APIRouter(tags=["Healthcheck"])

@router.get("/health")
def health_check():
	"""
	Vérifie la connexion à la base PostgreSQL.
	Retourne 503 si la DB n'est pas joignable.
	"""
	try:
		db.query("SELECT 1;")
		return {"status": "healthy", "database": "connected"}
	except Exception as e:
		raise HTTPException(status_code=503, detail=str(e))