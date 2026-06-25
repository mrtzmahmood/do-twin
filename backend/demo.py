"""
demo.py
────────
نمونه کامل end-to-end با Ray Tune hyperparameter search.

اجرا:
  python demo.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import ray

from ml_pipeline.pipeline.builder import PipelineBuilder, PipelineConfig
from ml_pipeline.model.trainer import ModelTrainer, TrainerConfig
from ml_pipeline.storage.artifact import ArtifactStore


def make_sample_data() -> pd.DataFrame:
    from sklearn.datasets import load_iris
    iris = load_iris(as_frame=True)
    df   = iris.frame.copy()
    df.columns = [*iris.feature_names, "target"]
    rng  = np.random.default_rng(0)
    rows = rng.integers(0, len(df), size=10)
    df.loc[rows, "sepal length (cm)"] *= 6
    return df


def main() -> None:
    ray.init(ignore_reinit_error=True)
    df = make_sample_data()

    # ─── ۱. پایپ‌لاین ────────────────────────────────────────────────────────
    print("\n━━━ مرحله ۱: آماده‌سازی داده ━━━")
    builder  = PipelineBuilder(config=PipelineConfig(outlier_method="zscore"))
    prepared = builder.prepare(df, label_column="target")
    print(f"  task_type={prepared['task_type']}  "
          f"X_train={prepared['X_train'].shape}  "
          f"X_valid={prepared['X_valid'].shape}")

    # ─── ۲. آموزش با Ray Tune ────────────────────────────────────────────────
    print("\n━━━ مرحله ۲: Ray Tune hyperparameter search ━━━")
    trainer = ModelTrainer(
        config=TrainerConfig(
            model_params={
                "n_estimators": 100,
                "random_state": 42,
                "max_depth":    None,
            },
            model_param_space={
                "n_estimators": [50, 100, 200],
                "max_depth":    [3, 5, None],
            },
        )
    )
    result = trainer.train(prepared)
    print(f"  best_config : {result['best_config']}")
    print(f"  metrics     : {result['metrics']}")

    # ─── ۳. ذخیره artifact ───────────────────────────────────────────────────
    print("\n━━━ مرحله ۳: ذخیره artifact ━━━")
    store = ArtifactStore(base_dir="/tmp/ml_demo")
    store.save(prepared, result, artifact_name="iris_tuned")

    # ─── ۴. بارگذاری و پیش‌بینی ─────────────────────────────────────────────
    print("\n━━━ مرحله ۴: بارگذاری و پیش‌بینی ━━━")
    loaded = store.load(artifact_name="iris_tuned")
    print(f"  task_type={loaded['task_type']}  metrics={loaded['metrics']}")

    for row in df[prepared["feature_columns"]].iloc[:3].values.tolist():
        df_row   = pd.DataFrame([row], columns=loaded["feature_columns"])
        X        = loaded["preprocessor"].transform_batch(df_row)[loaded["feature_columns"]].values
        pred_idx = loaded["model"].predict(X)[0]
        label    = loaded["classes"][pred_idx] if loaded["classes"] else pred_idx
        print(f"  {[round(v, 2) for v in row]}  →  {label}")

    ray.shutdown()


if __name__ == "__main__":
    main()
