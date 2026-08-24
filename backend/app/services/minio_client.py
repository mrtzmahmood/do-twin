import io
from datetime import datetime

from minio import Minio
from minio.error import S3Error

from app.config import settings

_client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=settings.minio_secure,
)


def ensure_bucket() -> None:
    if not _client.bucket_exists(settings.minio_bucket):
        _client.make_bucket(settings.minio_bucket)


def list_objects(prefix: str = "") -> list[dict]:
    ensure_bucket()
    out = []
    for obj in _client.list_objects(settings.minio_bucket, prefix=prefix, recursive=True):
        out.append({
            "key": obj.object_name,
            "size": obj.size,
            "lastModified": obj.last_modified.isoformat() if isinstance(obj.last_modified, datetime) else None,
        })
    return out


def get_object_text(key: str) -> str:
    ensure_bucket()
    resp = _client.get_object(settings.minio_bucket, key)
    try:
        return resp.read().decode("utf-8")
    finally:
        resp.close()
        resp.release_conn()


def put_object(key: str, content: str, content_type: str = "application/json") -> dict:
    ensure_bucket()
    data = content.encode("utf-8")
    _client.put_object(
        settings.minio_bucket, key, io.BytesIO(data), length=len(data), content_type=content_type,
    )
    return {"ok": True, "key": key, "size": len(data)}


def delete_object(key: str) -> dict:
    ensure_bucket()
    _client.remove_object(settings.minio_bucket, key)
    return {"ok": True, "key": key}


def health() -> dict:
    try:
        ensure_bucket()
        return {"ok": True, "bucket": settings.minio_bucket}
    except S3Error as e:
        return {"ok": False, "message": str(e)}
