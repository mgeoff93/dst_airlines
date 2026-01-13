from fastapi import FastAPI
from api.routers import healthcheck, static, dynamic, live, merged, predict
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title = "DST Airlines API",
    description = "API REST pour suivi des vols en temps réel",
    version = "1.0.0"
)

# 1. Inclure tous les routers (Décommentés !)
app.include_router(healthcheck.router)
app.include_router(static.router)
app.include_router(dynamic.router)
app.include_router(live.router)
app.include_router(merged.router)
app.include_router(predict.router)

# 2. Instrumenter et Exposer
# L'ordre est important : instrumenter d'abord, exposer ensuite.
Instrumentator().instrument(app).expose(app)