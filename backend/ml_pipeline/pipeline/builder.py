"""
pipeline/builder.py
────────────────────
PipelineBuilder: ساخت Ray Chain preprocessor و آماده‌سازی داده.

مراحل Chain (به ترتیب اجرا):
  1. SimpleImputer     — جایگزینی مقادیر گمشده
  2. OutlierImputer    — حذف/جایگزینی داده‌های پرت
  3. StandardScaler    — استانداردسازی ستون‌های عددی

خروجی prepare():
  dict با کلیدهای:
    preprocessor, feature_columns, label_column,
    X_train, X_valid, y_train, y_valid,
    task_type ('regression' | 'binary' | 'multiclass'),
    classes (np.ndarray | None), label_encoder
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import ray
import ray.data
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import Chain, SimpleImputer, StandardScaler
from sklearn.preprocessing import LabelEncoder

from ..preprocessors.outlier_imputer import OutlierImputer
from ..preprocessors.base import SourceRegistry


@dataclass
class PipelineConfig:
    """پارامترهای قابل تنظیم پایپ‌لاین."""

    outlier_method: str = "zscore"          # 'zscore' | 'lof'
    outlier_strategy: str = "mean"          # 'mean' | 'median' | 'interpolate'
    outlier_threshold: float = 3.0
    lof_n_neighbors: int = 20
    test_ratio: float = 0.2
    random_seed: int = 42
    # ستون‌های عددی که نباید scale شوند (مثلاً ID)
    exclude_from_scaling: list[str] = field(default_factory=list)


class PipelineBuilder:
    """
    Parameters
    ----------
    config : PipelineConfig
        تنظیمات پایپ‌لاین.
    registry : SourceRegistry | None
        برای embed سورس custom preprocessor‌ها در artifact.
        اگر None باشد یک registry خودکار ساخته می‌شود.
    """

    def __init__(
        self,
        config: PipelineConfig | None = None,
        registry: SourceRegistry | None = None,
    ) -> None:
        self.config = config or PipelineConfig()
        self.registry = registry or SourceRegistry()
        # ثبت خودکار OutlierImputer
        self.registry.register_source(
            "OutlierImputer",
            (
                __file__.replace("builder.py", "")
                and open(
                    __file__.replace("builder.py", "../preprocessors/outlier_imputer.py")
                ).read()
            ),
        )

    # ─── public API ─────────────────────────────────────────────────────────

    def build_preprocessor(self, feature_columns: list[str]) -> Preprocessor:
        """
        ساخت Chain preprocessor.

        Parameters
        ----------
        feature_columns : list[str]
            ستون‌هایی که پیش‌پردازش روی آن‌ها اعمال می‌شود.
        """
        cfg = self.config
        scale_cols = [
            c for c in feature_columns
            if c not in cfg.exclude_from_scaling
        ]

        return Chain(
            SimpleImputer(columns=feature_columns, strategy="mean"),
            OutlierImputer(
                columns=feature_columns,
                method=cfg.outlier_method,
                strategy=cfg.outlier_strategy,
                threshold=cfg.outlier_threshold,
                lof_n_neighbors=cfg.lof_n_neighbors,
            ),
            StandardScaler(columns=scale_cols),
        )

    def prepare(
        self,
        df: pd.DataFrame,
        label_column: str,
        feature_columns: list[str] | None = None,
    ) -> dict:
        """
        آماده‌سازی کامل داده برای supervised learning.

        Returns
        -------
        dict با کلیدهای:
          preprocessor, feature_columns, label_column,
          X_train, X_valid, y_train, y_valid,
          task_type, classes, label_encoder,
          embedded_sources (برای artifact)
        """
        cfg = self.config

        if feature_columns is None:
            feature_columns = [c for c in df.columns if c != label_column]

        # ─── تقسیم train / valid ────────────────────────────────────────────
        rng = np.random.default_rng(cfg.random_seed)
        idx = rng.permutation(len(df))
        split = int(len(df) * (1 - cfg.test_ratio))
        train_df = df.iloc[idx[:split]].reset_index(drop=True)
        valid_df = df.iloc[idx[split:]].reset_index(drop=True)

        # ─── Ray Dataset ────────────────────────────────────────────────────
        train_ds = ray.data.from_pandas(train_df)
        valid_ds = ray.data.from_pandas(valid_df)

        # ─── fit روی train، transform هر دو ────────────────────────────────
        preprocessor = self.build_preprocessor(feature_columns)
        preprocessor.fit(train_ds)

        train_t = preprocessor.transform(train_ds).to_pandas()
        valid_t = preprocessor.transform(valid_ds).to_pandas()

        X_train = train_t[feature_columns].values
        X_valid = valid_t[feature_columns].values
        y_train_raw = train_t[label_column].values
        y_valid_raw = valid_t[label_column].values

        # ─── تشخیص نوع task ────────────────────────────────────────────────
        le = None
        classes = None
        is_numeric_target = pd.api.types.is_numeric_dtype(df[label_column])
        n_unique = df[label_column].nunique()

        if is_numeric_target and n_unique > 10:
            task_type = "regression"
            y_train = y_train_raw.astype(float)
            y_valid = y_valid_raw.astype(float)
        else:
            le = LabelEncoder()
            y_train = le.fit_transform(y_train_raw)
            y_valid = le.transform(y_valid_raw)
            classes = le.classes_
            task_type = "binary" if len(classes) == 2 else "multiclass"

        return dict(
            preprocessor=preprocessor,
            feature_columns=feature_columns,
            label_column=label_column,
            X_train=X_train,
            X_valid=X_valid,
            y_train=y_train,
            y_valid=y_valid,
            task_type=task_type,
            classes=classes,
            label_encoder=le,
            embedded_sources=self.registry.export(),
        )
