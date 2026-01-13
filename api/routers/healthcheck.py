from fastapi import APIRouter, HTTPException
from api.core.database import db
from api.metrics import DATABASE_STATUS

router = APIRouter(tags=["Healthcheck"])

@router.get("/healthcheck")
def healthcheck():
    try:
        db.query("SELECT 1;")
        DATABASE_STATUS.set(1)
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        DATABASE_STATUS.set(0)
        raise HTTPException(status_code = 503, detail = str(e))