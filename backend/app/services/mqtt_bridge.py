import asyncio
import json
import time

import aiomqtt
from fastapi import WebSocket, WebSocketDisconnect

from app.config import settings


def _client_kwargs():
    kw = {"hostname": settings.mqtt_host, "port": settings.mqtt_port}
    if settings.mqtt_username:
        kw["username"] = settings.mqtt_username
        kw["password"] = settings.mqtt_password
    return kw


async def publish_once(topic: str, payload: str, qos: int = 0) -> dict:
    try:
        async with aiomqtt.Client(**_client_kwargs()) as client:
            await client.publish(topic, payload=payload, qos=qos)
        return {"ok": True}
    except Exception as e:  # noqa: BLE001 — surface any broker error to the caller
        return {"ok": False, "message": str(e)}


async def health() -> dict:
    try:
        async with aiomqtt.Client(**_client_kwargs()):
            pass
        return {"ok": True}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": str(e)}


async def bridge_websocket(ws: WebSocket, topics: list[str]) -> None:
    """
    Relays a browser WebSocket <-> the real MQTT broker, so the frontend
    never needs its own direct connection (and never hits browser-side
    Origin/CORS/websocket-listener issues — this is a plain server-to-server
    MQTT connection).

    Wire protocol on the WS (both directions are JSON text frames):
      backend -> frontend : {"type":"message","topic":str,"payload":str,"ts":ms}
      backend -> frontend : {"type":"status","state":"online"|"error","message":str}
      frontend -> backend : {"publish":{"topic":str,"payload":str,"qos":0}}
    """
    await ws.accept()
    try:
        async with aiomqtt.Client(**_client_kwargs()) as client:
            for t in topics:
                await client.subscribe(t)
            await ws.send_json({"type": "status", "state": "online", "message": ",".join(topics)})

            async def forward_incoming():
                async for message in client.messages:
                    payload = message.payload.decode(errors="replace") if isinstance(message.payload, (bytes, bytearray)) else str(message.payload)
                    await ws.send_json({"type": "message", "topic": str(message.topic), "payload": payload, "ts": int(time.time() * 1000)})

            async def forward_outgoing():
                while True:
                    raw = await ws.receive_text()
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    pub = msg.get("publish")
                    if pub and pub.get("topic"):
                        await client.publish(pub["topic"], payload=pub.get("payload", ""), qos=int(pub.get("qos", 0)))

            await asyncio.gather(forward_incoming(), forward_outgoing())
    except WebSocketDisconnect:
        pass
    except Exception as e:  # noqa: BLE001
        try:
            await ws.send_json({"type": "status", "state": "error", "message": str(e)})
        except Exception:  # noqa: BLE001
            pass
