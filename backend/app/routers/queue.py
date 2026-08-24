from fastapi import APIRouter, Query

from app.models import PublishBody
from app.services import rabbitmq_client as rmq

router = APIRouter(prefix="/api/queue", tags=["queue"])


@router.get("/health")
async def queue_health():
    return await rmq.health()


@router.get("/exchange")
async def exchange_exists(name: str = Query(...), vhost: str | None = Query(None)):
    return await rmq.exchange_exists(name, vhost)


@router.post("/publish")
async def publish(body: PublishBody):
    return await rmq.publish(body.exchange, body.routingKey, body.payload, body.vhost)
