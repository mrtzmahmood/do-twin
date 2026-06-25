"""
serve/endpoint.py
──────────────────
deploy_pipeline: deploy artifact روی Ray Serve.

endpoint‌ها:
  POST /predict  — پیش‌بینی با ورودی list[float]
  GET  /info     — اطلاعات مدل

ویژگی‌ها:
  - پیش‌پردازش و مدل کاملاً در RAM اجرا می‌شوند
  - سورس custom preprocessor‌ها از artifact بازسازی می‌شوند
  - نیازی به نصب هیچ ماژول custom در محیط مقصد نیست
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import ray
import ray.serve as serve

from ..storage.artifact import ArtifactStore


# ─── Schema ─────────────────────────────────────────────────────────────────

class PredictRequest(BaseModel):
    features: list[float]


class PredictResponse(BaseModel):
    predictions: list[Any]
    probabilities: list[list[float]] | None = None
    task_type: str


# ─── Deployment ─────────────────────────────────────────────────────────────

def _make_deployment(artifact_path: str):
    """
    Deployment class را داخل تابع تعریف می‌کند تا artifact_path
    بدون global state در __init__ قابل ارسال باشد.
    """
    _app = FastAPI()

    @serve.deployment(num_replicas=2)
    @serve.ingress(_app)
    class PipelineEndpoint:

        def __init__(self) -> None:
            # بارگذاری artifact (سورس custom preprocessor‌ها داخل آن است)
            store = ArtifactStore()
            loaded = store.load.__func__(store)  # نمی‌توان مستقیم path داد، ببینید پایین

        # ─── از ArtifactStore مستقیم لود می‌کنیم ──────────────────────────
        def _load(self, path: str) -> None:
            import cloudpickle
            from ray.data.preprocessor import Preprocessor
            from ..preprocessors.base import SourceRegistry

            with open(path, "rb") as f:
                artifact = cloudpickle.load(f)

            SourceRegistry.restore(artifact.get("embedded_sources", {}))
            self.model = cloudpickle.loads(artifact["model"])
            self.preprocessor = Preprocessor.deserialize(artifact["preprocessor_serial"])
            self.feature_columns: list[str] = artifact["feature_columns"]
            self.task_type: str = artifact["task_type"]
            self.classes: list | None = artifact["classes"]

        @_app.post("/predict", response_model=PredictResponse)
        def predict(self, request: PredictRequest) -> PredictResponse:
            try:
                X = self._preprocess(request.features)
                return self._run_model(X)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc))

        @_app.get("/info")
        def info(self) -> dict:
            return {
                "task_type": self.task_type,
                "feature_columns": self.feature_columns,
                "classes": self.classes,
            }

        # ─── helpers ────────────────────────────────────────────────────────

        def _preprocess(self, features: list[float]) -> np.ndarray:
            df = pd.DataFrame([features], columns=self.feature_columns)
            transformed = self.preprocessor.transform_batch(df)
            return transformed[self.feature_columns].values

        def _run_model(self, X: np.ndarray) -> PredictResponse:
            if self.task_type == "regression":
                preds = self.model.predict(X).tolist()
                return PredictResponse(predictions=preds, task_type=self.task_type)

            pred_idx = self.model.predict(X).tolist()
            proba = self.model.predict_proba(X).tolist()
            labels = (
                [self.classes[i] for i in pred_idx]
                if self.classes else pred_idx
            )
            return PredictResponse(
                predictions=labels,
                probabilities=proba,
                task_type=self.task_type,
            )

    return PipelineEndpoint


# ─── کلاس تمیزتر با artifact_path در __init__ ───────────────────────────────

def deploy_pipeline(
    artifact_path: str,
    endpoint_name: str = "MLPipeline",
    num_replicas: int = 2,
    route_prefix: str = "/predict",
) -> Any:
    """
    Deploy pipeline artifact روی Ray Serve.

    Parameters
    ----------
    artifact_path : str
        مسیر فایل .pkl artifact روی disk.
    endpoint_name : str
        نام deployment در Ray Serve.
    num_replicas  : int
        تعداد replica‌ها.
    route_prefix  : str
        پیشوند مسیر HTTP.

    Returns
    -------
    DeploymentHandle
    """
    import cloudpickle
    from ray.data.preprocessor import Preprocessor
    from ..preprocessors.base import SourceRegistry

    _app = FastAPI()

    @serve.deployment(
        name=endpoint_name,
        num_replicas=num_replicas,
        ray_actor_options={
            "runtime_env": {
                "pip": ["scikit-learn", "cloudpickle", "pandas", "scipy"]
            }
        },
    )
    @serve.ingress(_app)
    class _Endpoint:

        def __init__(self, path: str) -> None:
            with open(path, "rb") as f:
                artifact = cloudpickle.load(f)

            SourceRegistry.restore(artifact.get("embedded_sources", {}))
            self.model = cloudpickle.loads(artifact["model"])
            self.preprocessor = Preprocessor.deserialize(
                artifact["preprocessor_serial"]
            )
            self.feature_columns: list[str] = artifact["feature_columns"]
            self.task_type: str = artifact["task_type"]
            self.classes: list | None = artifact["classes"]
            print(f"[Serve] '{endpoint_name}' ready — task={self.task_type}")

        @_app.post("/predict")
        def predict(self, request: PredictRequest) -> dict:
            try:
                df = pd.DataFrame(
                    [request.features], columns=self.feature_columns
                )
                X = self.preprocessor.transform_batch(df)[
                    self.feature_columns
                ].values

                if self.task_type == "regression":
                    return {
                        "predictions": self.model.predict(X).tolist(),
                        "task_type": self.task_type,
                    }

                pred_idx = self.model.predict(X).tolist()
                proba = self.model.predict_proba(X).tolist()
                labels = (
                    [self.classes[i] for i in pred_idx]
                    if self.classes else pred_idx
                )
                return {
                    "predictions": labels,
                    "probabilities": proba,
                    "task_type": self.task_type,
                }
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc))

        @_app.get("/info")
        def info(self) -> dict:
            return {
                "task_type":       self.task_type,
                "feature_columns": self.feature_columns,
                "classes":         self.classes,
            }

    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    serve.start(detached=True)
    handle = serve.run(
        _Endpoint.bind(artifact_path),
        name=endpoint_name,
        route_prefix=route_prefix,
    )
    print(f"[Serve] http://localhost:8000{route_prefix}/predict")
    return handle
