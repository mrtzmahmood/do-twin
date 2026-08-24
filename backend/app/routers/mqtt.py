from fastapi import APIRouter, WebSocket, Query

from app.models import MqttPublishBody
from app.services import mqtt_bridge

router = APIRouter(prefix="/api/mqtt", tags=["mqtt"])


@router.get("/health")
async def mqtt_health():
    return await mqtt_bridge.health()


@router.post("/publish")
async def publish(body: MqttPublishBody):
    return await mqtt_bridge.publish_once(body.topic, body.payload, body.qos)


@router.websocket("/ws")
async def mqtt_ws(websocket: WebSocket, topics: str = Query("")):
    topic_list = [t.strip() for t in topics.split(",") if t.strip()]
    await mqtt_bridge.bridge_websocket(websocket, topic_list or ["#"])
