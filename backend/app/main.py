from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import storage, queue, mqtt, db, health, publish

app = FastAPI(
    title="DoTwin Backend",
    description=(
        "Proxies the browser's connections to MinIO, RabbitMQ, and MQTT, and "
        "runs ad-hoc SQL for the pipeline builder's SQL Source node. User "
        "accounts stay client-side (IndexedDB) for now — this service holds "
        "no user data of its own."
    ),
    version="0.1.0",
)

origins = ["*"] if settings.cors_origins == "*" else [o.strip() for o in settings.cors_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(storage.router)
app.include_router(queue.router)
app.include_router(mqtt.router)
app.include_router(db.router)
app.include_router(publish.router)

@app.get("/")
async def root():
    return {"service": "dotwin-backend", "docs": "/docs"}
