"""
/api/publish/dashboard  و  /api/public/dashboard/{public_id}

دقیقاً همون الگوی app/api/storage.py (یا هر فایلی که آن روتر توش هست):
همون minio_client، همون run_in_threadpool، همون شکل پاسخ {"ok": ...}.

⚠️ نکته‌ی حیاتی برای سیم‌کشی:
مسیر GET /api/public/dashboard/{public_id} باید از هر middleware/dependency
سراسریِ احراز هویتی که روی بقیه‌ی /api/* اعمال می‌شه معاف بشه — بازدیدکننده‌ی
لینک عمومی اصلاً لاگین نکرده. بسته به اینکه auth شما چطور wire شده:
  - اگه با یک FastAPI dependency روی خودِ router/app اعمال می‌شه، این
    روتر رو با include_router جدا (بدون اون dependency) اضافه کنید.
  - اگه با یک middleware سراسریه، باید مسیر /api/public/* رو صریحاً
    در allow-list آن middleware بگذارید.
بقیه‌ی مسیرهای این فایل (publish/unpublish) باید مثل بقیه‌ی /api/storage
پشت همون احراز هویت فعلی بمونن.
"""

import json
import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from app.models import PublishDashboardBody
from app.services import minio_client as minio

router = APIRouter(tags=["publish"])

PUBLISH_PREFIX = "public/dashboards/"


def _object_key(public_id: str) -> str:
    return f"{PUBLISH_PREFIX}{public_id}.json"


def _public_url(public_id: str) -> str:
    # TODO: از تنظیمات واقعی پروژه بخونید (مثلاً app.core.config.settings.PUBLIC_BASE_URL)
    # به‌جای این placeholder — همون دامنه‌ای که این اپ روش سرو می‌شه.
    from app.config import settings  # noqa: PLC0415  (اگه این ماژول/فیلد رو ندارید، جایگزین کنید)
    base = getattr(settings, "PUBLIC_BASE_URL", "").rstrip("/")
    return f"{base}/public/dashboard/{public_id}"


# ================================================================
# POST /api/publish/dashboard   (نیازمند احراز هویت — مثل بقیه‌ی /api/storage)
# ================================================================

@router.post("/api/publish/dashboard")
async def publish_dashboard(body: PublishDashboardBody):
    public_id = secrets.token_urlsafe(16)  # کوتاه، URL-safe، غیرقابل‌حدس

    record = {
        "userKey": body.userKey,
        "projectId": body.projectId,
        "dashboardId": body.dashboardId,
        "snapshot": body.snapshot,
        "publishedAt": datetime.now(timezone.utc).isoformat(),
    }

    try:
        await run_in_threadpool(
            minio.put_object, _object_key(public_id), json.dumps(record), "application/json"
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"ok": True, "publicId": public_id, "url": _public_url(public_id)}


# ================================================================
# GET /api/public/dashboard/{public_id}   (⚠️ بدون احراز هویت — بالا توضیح داده شد)
# ================================================================

@router.get("/api/public/dashboard/{public_id}")
async def get_public_dashboard(public_id: str):
    try:
        text = await run_in_threadpool(minio.get_object_text, _object_key(public_id))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Dashboard not found or unpublished") from e

    try:
        record = json.loads(text)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail="Corrupted published dashboard") from e

    # فقط snapshot و publishedAt عمومی می‌شه — نه userKey/projectId/dashboardId
    # (این‌ها اطلاعات داخلی مالکیت هستن، نباید به بازدیدکننده‌ی ناشناس درز کنن)
    return {"ok": True, "snapshot": record.get("snapshot"), "publishedAt": record.get("publishedAt")}


# ================================================================
# DELETE /api/publish/dashboard/{public_id}   (unpublish — نیازمند احراز هویت)
# ================================================================
# ⚠️ چک مالکیت اینجا ساده‌ست (فقط تطبیق userKey با رکورد ذخیره‌شده) —
# جایگزین احراز هویت واقعی نیست. اگه یک dependency برای گرفتن کاربر
# لاگین‌کرده‌ی فعلی دارید (مثل بقیه‌ی /api/storage)، به‌جای پارامتر
# querystring زیر از همون استفاده کنید تا userKey جعلی قابل ارسال نباشه.

@router.delete("/api/publish/dashboard/{public_id}")
async def unpublish_dashboard(public_id: str, userKey: str):  # noqa: N803 (camelCase برای هم‌خوانی با فرانت)
    try:
        text = await run_in_threadpool(minio.get_object_text, _object_key(public_id))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Dashboard not found or already unpublished") from e

    try:
        record = json.loads(text)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail="Corrupted published dashboard") from e

    if record.get("userKey") != userKey:
        raise HTTPException(status_code=403, detail="Not the owner of this published dashboard")

    try:
        return await run_in_threadpool(minio.delete_object, _object_key(public_id))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e
