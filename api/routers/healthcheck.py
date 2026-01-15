from fastapi import APIRouter, HTTPException
from api.core.database import db
router = APIRouter(tags=["Healthcheck"])

@router.get("/healthcheck")
def healthcheck():
    try:
        db.query("SELECT 1;")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code = 503, detail = str(e))