from fastapi import FastAPI
from api.routers import healthcheck, static, dynamic, live, merged
from prometheus_fastapi_instrumentator import Instrumentator # <--- Import

app = FastAPI(
	title = "DST Airlines API",
	description = "API REST pour suivi des vols en temps réel",
	version = "1.0.0"
)

Instrumentator().instrument(app).expose(app)

# Inclure tous les routers
app.include_router(healthcheck.router)
# app.include_router(merged.router)
app.include_router(static.router)
# app.include_router(dynamic.router)
# app.include_router(live.router)