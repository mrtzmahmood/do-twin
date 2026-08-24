import io
import os
import re

import pandas as pd
from minio import Minio

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "dotwin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "dotwin12345")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "dotwin")

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)


def get_json(key: str) -> dict:
    import json
    resp = client.get_object(MINIO_BUCKET, key)
    try:
        return json.loads(resp.read().decode("utf-8"))
    finally:
        resp.close()
        resp.release_conn()


def put_json(key: str, doc: dict) -> None:
    import json
    data = json.dumps(doc, indent=2, default=str).encode("utf-8")
    client.put_object(MINIO_BUCKET, key, io.BytesIO(data), length=len(data), content_type="application/json")


def get_csv(key: str) -> pd.DataFrame:
    resp = client.get_object(MINIO_BUCKET, key)
    try:
        return pd.read_csv(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()


def s3_path_to_key(path: str) -> str:
    """'s3://bucket/some/key.csv' -> 'some/key.csv' (bucket name is ignored —
    always read from this worker's own configured MINIO_BUCKET, since the
    frontend and this worker share one bucket in the docker-compose setup)."""
    m = re.match(r"^s3://[^/]+/(.+)$", path)
    return m.group(1) if m else path.lstrip("/")


def user_key_from_path(path: str) -> str:
    """Pulls the {userKey} segment out of a 'users/{userKey}/projects/...' key
    so the worker doesn't need its own copy of the frontend's slug() logic."""
    m = re.search(r"users/([^/]+)/", path)
    return m.group(1) if m else "anon"
