from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.models import PutObjectBody
from app.services import minio_client as minio

router = APIRouter(prefix="/api/storage", tags=["storage"])


@router.get("/health")
async def storage_health():
    return await run_in_threadpool(minio.health)


@router.get("/objects")
async def list_objects(prefix: str = Query("")):
    try:
        keys = await run_in_threadpool(minio.list_objects, prefix)
        return {"ok": True, "keys": keys}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/object")
async def get_object(key: str = Query(...)):
    try:
        text = await run_in_threadpool(minio.get_object_text, key)
        return {"ok": True, "key": key, "content": text}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/object")
async def put_object(body: PutObjectBody):
    try:
        return await run_in_threadpool(minio.put_object, body.key, body.content, body.contentType)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/object")
async def delete_object(key: str = Query(...)):
    try:
        return await run_in_threadpool(minio.delete_object, key)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e
