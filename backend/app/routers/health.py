from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from app.services import minio_client as minio
from app.services import rabbitmq_client as rmq
from app.services import mqtt_bridge

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health():
    minio_status = await run_in_threadpool(minio.health)
    rmq_status = await rmq.health()
    mqtt_status = await mqtt_bridge.health()
    return {
        "ok": minio_status.get("ok") and rmq_status.get("ok") and mqtt_status.get("ok"),
        "minio": minio_status,
        "rabbitmq": rmq_status,
        "mqtt": mqtt_status,
    }
