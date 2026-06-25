"""
preprocessors/outlier_imputer.py
─────────────────────────────────
OutlierImputer: Ray Preprocessor برای تشخیص و جایگزینی داده‌های پرت.

روش‌های تشخیص:
  - zscore  : Z-score > threshold
  - lof     : Local Outlier Factor

استراتژی‌های جایگزینی:
  - mean        : میانگین ستون
  - median      : میانه ستون
  - interpolate : میانگین interpolation خطی
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import zscore
from sklearn.neighbors import LocalOutlierFactor

from ray.data import Dataset
from ray.data.preprocessor import Preprocessor


class OutlierImputer(Preprocessor):
    """
    Parameters
    ----------
    columns         : list[str] | None
        ستون‌های هدف. None → تمام ستون‌های عددی.
    threshold       : float
        آستانه Z-score (فقط برای method='zscore').
    strategy        : 'mean' | 'median' | 'interpolate'
        روش جایگزینی مقادیر پرت.
    method          : 'zscore' | 'lof'
        الگوریتم تشخیص.
    lof_n_neighbors : int
        تعداد همسایه برای LOF.
    """

    def __init__(
        self,
        columns: list[str] | None = None,
        threshold: float = 3.0,
        strategy: str = "mean",
        method: str = "zscore",
        lof_n_neighbors: int = 20,
    ) -> None:
        self.columns = columns
        self.threshold = threshold
        self.strategy = strategy
        self.method = method
        self.lof_n_neighbors = lof_n_neighbors

    # ─── Ray API ────────────────────────────────────────────────────────────

    def _fit(self, dataset: Dataset) -> "OutlierImputer":
        return self

    def _transform_pandas(self, batch: pd.DataFrame) -> pd.DataFrame:
        cols = (
            self.columns
            if self.columns
            else batch.select_dtypes(include="number").columns.tolist()
        )
        cols = [c for c in cols if c in batch.columns]

        for col in cols:
            outliers = self._detect(batch, col)
            fill = self._fill_value(batch, col)
            batch.loc[outliers, col] = fill

        return batch

    # ─── helpers ────────────────────────────────────────────────────────────

    def _detect(self, df: pd.DataFrame, col: str) -> pd.Series:
        clean = df[col].fillna(df[col].mean())

        if self.method == "zscore":
            return np.abs(zscore(clean)) > self.threshold

        if self.method == "lof":
            preds = LocalOutlierFactor(
                n_neighbors=self.lof_n_neighbors
            ).fit_predict(clean.values.reshape(-1, 1))
            return pd.Series(preds == -1, index=df.index)

        raise ValueError(
            f"method={self.method!r} نامعتبر است. مقادیر مجاز: 'zscore', 'lof'"
        )

    def _fill_value(self, df: pd.DataFrame, col: str) -> float:
        if self.strategy == "mean":
            return float(df[col].mean())
        if self.strategy == "median":
            return float(df[col].median())
        if self.strategy == "interpolate":
            return float(df[col].interpolate(method="linear").mean())
        raise ValueError(
            f"strategy={self.strategy!r} نامعتبر است. "
            "مقادیر مجاز: 'mean', 'median', 'interpolate'"
        )
