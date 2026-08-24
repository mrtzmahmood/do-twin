import base64
import httpx

from app.config import settings

_auth = base64.b64encode(f"{settings.rabbitmq_user}:{settings.rabbitmq_password}".encode()).decode()
_headers = {"Authorization": f"Basic {_auth}", "Content-Type": "application/json"}


async def health() -> dict:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{settings.rabbitmq_api_url}/api/overview", headers=_headers)
            if r.status_code == 401:
                return {"ok": False, "message": "Authentication failed."}
            r.raise_for_status()
            return {"ok": True}
    except httpx.HTTPError as e:
        return {"ok": False, "message": str(e)}


async def exchange_exists(exchange: str, vhost: str | None = None) -> dict:
    vh = vhost or settings.rabbitmq_vhost
    vh_enc = httpx.QueryParams({"": vh})[""]  # reuse httpx's encoder trivially
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(
            f"{settings.rabbitmq_api_url}/api/exchanges/{_encode_vhost(vh)}/{exchange}",
            headers=_headers,
        )
        if r.status_code == 404:
            return {"ok": False, "message": f'Exchange "{exchange}" not found in vhost "{vh}".'}
        r.raise_for_status()
        return {"ok": True}


def _encode_vhost(vhost: str) -> str:
    return "%2F" if vhost in ("/", "") else vhost


async def publish(exchange: str, routing_key: str, payload: str, vhost: str | None = None) -> dict:
    vh = vhost or settings.rabbitmq_vhost
    body = {"properties": {}, "routing_key": routing_key, "payload": payload, "payload_encoding": "string"}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{settings.rabbitmq_api_url}/api/exchanges/{_encode_vhost(vh)}/{exchange}/publish",
            headers=_headers, json=body,
        )
        r.raise_for_status()
        data = r.json()
        return {"ok": True, "routed": data.get("routed", None)}
