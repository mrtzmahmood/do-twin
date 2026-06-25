"""
model/trainer.py
─────────────────
ModelTrainer: آموزش مدل sklearn با Ray Tune برای hyperparameter tuning.

جریان کار:
  1. param_space (از model_params + model_param_space) به فرمت Ray Tune تبدیل می‌شود
  2. برای هر trial یک sklearn model ساخته و fit می‌شود
  3. بهترین checkpoint بر اساس task_type انتخاب می‌شود
  4. مدل و متریک‌های بهترین trial برگردانده می‌شوند

task_type → متریک بهینه‌سازی:
  regression  → rmse    (mode: min)
  binary      → logloss (mode: min)
  multiclass  → mlogloss (mode: min)
"""

from __future__ import annotations

import os
import joblib
import tempfile
import shutil
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    log_loss,
    mean_squared_error,
)

import ray
from ray import train, tune
from ray.train import Checkpoint
from ray.tune import Tuner, session


# ─── متریک‌های هر task ──────────────────────────────────────────────────────

_METRIC_MAP = {
    "regression":  ("rmse",     "min"),
    "binary":      ("logloss",  "min"),
    "multiclass":  ("mlogloss", "min"),
}


# ─── Config ─────────────────────────────────────────────────────────────────

@dataclass
class TrainerConfig:
    """
    پارامترهای قابل تنظیم آموزش.

    model_params      : مقادیر پیش‌فرض پارامترهای مدل
                        {"n_estimators": 100, "max_depth": None, ...}
    model_param_space : فضای جستجو برای Tune
                        {"n_estimators": [50, 100, 200], "max_depth": [3, 5, 10]}
                        هر کلیدی که اینجا باشد با grid_search جستجو می‌شود.
    num_to_keep       : تعداد checkpoint‌های نگه‌داشته شده
    """

    model_params: dict = field(default_factory=lambda: {
        "n_estimators": 100,
        "random_state": 42,
    })
    model_param_space: dict = field(default_factory=dict)
    num_to_keep: int = 1


# ─── Trainer ────────────────────────────────────────────────────────────────

class ModelTrainer:
    """
    Parameters
    ----------
    config       : TrainerConfig | None
    model_class  : sklearn estimator class | None
        کلاس مدل (نه instance). مثال: RandomForestClassifier
        اگر None باشد بر اساس task_type به صورت خودکار انتخاب می‌شود.
    """

    def __init__(
        self,
        config: TrainerConfig | None = None,
        model_class: Any | None = None,
    ) -> None:
        self.config = config or TrainerConfig()
        self.model_class = model_class

    # ─── public API ─────────────────────────────────────────────────────────

    def train(self, prepared: dict) -> dict:
        """
        آموزش مدل با Ray Tune.

        Parameters
        ----------
        prepared : dict  — خروجی PipelineBuilder.prepare()

        Returns
        -------
        dict با کلیدهای:
          model, metrics, task_type, best_config, tune_results
        """
        task_type  = prepared["task_type"]
        X_train    = prepared["X_train"]
        y_train    = prepared["y_train"]
        X_valid    = prepared["X_valid"]
        y_valid    = prepared["y_valid"]

        model_class  = self._resolve_model_class(task_type)
        param_space  = self._build_param_space()
        metric, mode = _METRIC_MAP[task_type]

        # ─── تابع آموزش هر trial ────────────────────────────────────────────
        def _train_fn(config: dict) -> None:
            model = model_class(**config)
            model.fit(X_train, y_train)

            checkpoint_dir = session.get_trial_dir()
            checkpoint     = Checkpoint.from_directory(checkpoint_dir)
            joblib.dump(model, os.path.join(checkpoint_dir, "model.pkl"))

            metrics = _compute_metrics(model, X_valid, y_valid, task_type)
            session.report(metrics, checkpoint=checkpoint)

        # ─── Ray Tune ───────────────────────────────────────────────────────
        tuner = Tuner(
            _train_fn,
            param_space=param_space,
            run_config=train.RunConfig(
                checkpoint_config=train.CheckpointConfig(
                    num_to_keep=self.config.num_to_keep,
                ),
            ),
        )
        result_grid = tuner.fit()

        # ─── بهترین trial ───────────────────────────────────────────────────
        best_trial  = result_grid.get_best_result(metric=metric, mode=mode)
        best_config = best_trial.config
        print(f"[Trainer] best_config={best_config}")

        # ─── بارگذاری مدل از checkpoint ─────────────────────────────────────
        with best_trial.checkpoint.as_directory() as ckpt_dir:
            model   = joblib.load(os.path.join(ckpt_dir, "model.pkl"))
            metrics = _compute_metrics(model, X_valid, y_valid, task_type)

        self._log_metrics(task_type, metrics)

        return dict(
            model=model,
            metrics=metrics,
            task_type=task_type,
            best_config=best_config,
            tune_results=result_grid,
        )

    # ─── private ────────────────────────────────────────────────────────────

    def _resolve_model_class(self, task_type: str) -> type:
        if self.model_class is not None:
            return self.model_class
        if task_type == "regression":
            return RandomForestRegressor
        return RandomForestClassifier

    def _build_param_space(self) -> dict:
        """
        ترکیب model_params (پیش‌فرض) با model_param_space (فضای جستجو).

        هر کلیدی که در param_space باشد با tune.grid_search جستجو می‌شود.
        بقیه کلیدها با مقدار پیش‌فرض ثابت می‌مانند.
        """
        cfg     = self.config
        result  = {}

        for key, default_val in cfg.model_params.items():
            if key in cfg.model_param_space:
                result[key] = tune.grid_search(cfg.model_param_space[key])
            else:
                result[key] = default_val

        # کلیدهایی که فقط در param_space هستند (پیش‌فرض ندارند)
        for key, search_vals in cfg.model_param_space.items():
            if key not in result:
                result[key] = tune.grid_search(search_vals)

        return result

    @staticmethod
    def _log_metrics(task_type: str, metrics: dict) -> None:
        tag   = task_type.capitalize()
        parts = "  ".join(f"{k}={v:.4f}" for k, v in metrics.items())
        print(f"[Trainer] {tag} → {parts}")


# ─── محاسبه متریک‌ها (مشترک بین train_fn و ارزیابی نهایی) ──────────────────

def _compute_metrics(
    model: Any,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    task_type: str,
) -> dict:
    """
    محاسبه متریک‌های مناسب بر اساس task_type.

    regression  → rmse, mse
    binary      → logloss, error (1 - accuracy)
    multiclass  → mlogloss, merror (1 - accuracy)
    """
    if task_type == "regression":
        preds = model.predict(X_valid)
        mse   = float(mean_squared_error(y_valid, preds))
        return {"rmse": float(np.sqrt(mse)), "mse": mse}

    y_pred = model.predict(X_valid)
    acc    = float(accuracy_score(y_valid, y_pred))

    if task_type == "binary":
        proba    = model.predict_proba(X_valid)[:, 1]
        val_loss = float(log_loss(y_valid, proba))
        return {"logloss": val_loss, "error": 1.0 - acc}

    # multiclass
    proba    = model.predict_proba(X_valid)
    val_loss = float(log_loss(y_valid, proba))
    return {"mlogloss": val_loss, "merror": 1.0 - acc}
