"""
storage/artifact.py
────────────────────
ArtifactStore: ذخیره و بارگذاری self-contained pipeline artifact.

ساختار artifact:
  {
    "model"                : bytes (cloudpickle)
    "preprocessor_serial"  : dict  (Ray JSON-serializable)
    "feature_columns"      : list[str]
    "label_column"         : str
    "task_type"            : str
    "classes"              : list | None
    "metrics"              : dict
    "embedded_sources"     : dict[name -> source_str]
  }

فایل نهایی با cloudpickle ذخیره می‌شود:
  - مدل به صورت bytes embed می‌شود
  - preprocessor با Ray serialize → JSON
  - سورس custom preprocessor‌ها داخل artifact است

هنگام load:
  - embedded_sources توسط SourceRegistry.restore() اجرا می‌شوند
  - کلاس‌ها در builtins inject می‌شوند
  - Ray.deserialize() بدون هیچ import اضافه کار می‌کند
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Any

import cloudpickle
from ray.data.preprocessor import Preprocessor

from ..preprocessors.base import SourceRegistry


class ArtifactStore:
    """
    ذخیره و بارگذاری pipeline artifact.

    Parameters
    ----------
    base_dir : str | Path
        پوشه پیش‌فرض برای ذخیره فایل‌ها (اختیاری).
    minio_wrapper : object | None
        اگر داده شود، ذخیره‌سازی روی MinIO انجام می‌شود.
        باید متدهای save_serialized(data, filename, bucket, folder_name)
        و load_serialized(filename, bucket, folder_name) داشته باشد.
    bucket : str
        نام bucket در MinIO.
    """

    def __init__(
        self,
        base_dir: str | Path = "/tmp/ml_artifacts",
        minio_wrapper: Any | None = None,
        bucket: str = "models",
    ) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.minio = minio_wrapper
        self.bucket = bucket

    # ─── save ───────────────────────────────────────────────────────────────

    def save(
        self,
        prepared: dict,
        train_result: dict,
        artifact_name: str = "pipeline",
        folder: str = "",
    ) -> Path:
        """
        ذخیره کل pipeline در یک فایل .pkl self-contained.

        Parameters
        ----------
        prepared     : dict  — خروجی PipelineBuilder.prepare()
        train_result : dict  — خروجی ModelTrainer.train()
        artifact_name: str   — نام فایل بدون پسوند
        folder       : str   — زیرپوشه (برای MinIO folder_name)

        Returns
        -------
        Path  محل ذخیره فایل روی disk
        """
        preprocessor: Preprocessor = prepared["preprocessor"]

        artifact = {
            "model":               cloudpickle.dumps(train_result["model"]),
            "preprocessor_serial": preprocessor.serialize(),
            "feature_columns":     prepared["feature_columns"],
            "label_column":        prepared["label_column"],
            "task_type":           prepared["task_type"],
            "classes":             (
                prepared["classes"].tolist()
                if prepared["classes"] is not None
                else None
            ),
            "metrics":             train_result["metrics"],
            "embedded_sources":    prepared.get("embedded_sources", {}),
        }

        buf = io.BytesIO()
        cloudpickle.dump(artifact, buf)
        payload = buf.getvalue()

        # ─── disk ───────────────────────────────────────────────────────────
        filename = f"{artifact_name}.pkl"
        local_path = self.base_dir / filename
        local_path.write_bytes(payload)

        size_kb = local_path.stat().st_size / 1024
        print(f"[ArtifactStore] saved → {local_path}  ({size_kb:.1f} KB)")

        # ─── MinIO (اختیاری) ────────────────────────────────────────────────
        if self.minio:
            self.minio.save_serialized(
                payload, filename, self.bucket, folder_name=folder
            )
            print(f"[ArtifactStore] uploaded → {self.bucket}/{folder}/{filename}")

        return local_path

    # ─── load ───────────────────────────────────────────────────────────────

    def load(
        self,
        artifact_name: str = "pipeline",
        folder: str = "",
    ) -> dict:
        """
        بارگذاری artifact.

        ترتیب جستجو:
          1. MinIO (اگر minio_wrapper داده شده)
          2. disk: base_dir / artifact_name.pkl

        Returns
        -------
        dict با کلیدهای:
          model, preprocessor, feature_columns, label_column,
          task_type, classes, metrics
        """
        filename = f"{artifact_name}.pkl"

        # ─── بارگذاری payload ───────────────────────────────────────────────
        if self.minio:
            payload = self.minio.load_serialized(
                filename, self.bucket, folder_name=folder
            )
            print(f"[ArtifactStore] loaded from MinIO → {folder}/{filename}")
        else:
            local_path = self.base_dir / filename
            if not local_path.exists():
                raise FileNotFoundError(f"artifact پیدا نشد: {local_path}")
            payload = local_path.read_bytes()
            print(f"[ArtifactStore] loaded from disk → {local_path}")

        # ─── deserialize ────────────────────────────────────────────────────
        artifact: dict = cloudpickle.loads(payload)

        # بازسازی کلاس‌های custom از سورس‌های embed شده
        SourceRegistry.restore(artifact.get("embedded_sources", {}))

        model = cloudpickle.loads(artifact["model"])
        preprocessor = Preprocessor.deserialize(artifact["preprocessor_serial"])

        return dict(
            model=model,
            preprocessor=preprocessor,
            feature_columns=artifact["feature_columns"],
            label_column=artifact["label_column"],
            task_type=artifact["task_type"],
            classes=artifact["classes"],
            metrics=artifact["metrics"],
        )
