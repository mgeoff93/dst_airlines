from fastapi import FastAPI
from api.routers import healthcheck, static, dynamic, live, merged

app = FastAPI(
	title = "DST Airlines API",
	description = "API REST pour suivi des vols en temps réel",
	version = "1.0.0"
)

# Inclure tous les routers
app.include_router(healthcheck.router)
app.include_router(merged.router)
app.include_router(static.router)
app.include_router(dynamic.router)
app.include_router(live.router)