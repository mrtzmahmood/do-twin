"""
Executes one DoTwin pipeline (the same JSON graph the Pipeline Builder saves
and sends in job.json's "config"). Covers the node types currently used by
real runs: csv (source), power_transform / robust_scaler (transform),
xgboost (model), confusion / roc / metrics (evaluate), registry / share
(output). Anything else is reported as a skipped no-op instead of failing
the whole run, so new node types don't hard-crash old workers.
"""

import time

import numpy as np
import pandas as pd
from sklearn.preprocessing import PowerTransformer, RobustScaler
from sklearn.metrics import (
    confusion_matrix, accuracy_score, f1_score, precision_score, recall_score,
    roc_curve, roc_auc_score, r2_score, mean_absolute_error, mean_squared_error,
)
import xgboost as xgb

import storage
import mqtt_report as mq
import feature_engine


def get_param(node: dict, key: str, default=None):
    for p in node.get("params", []):
        if p.get("key") == key:
            return p.get("value", default)
    return default


def topo_order(nodes: dict, edges: list[dict]) -> list[str]:
    incoming = {nid: 0 for nid in nodes}
    children = {nid: [] for nid in nodes}
    for e in edges:
        a, b = e.get("from"), e.get("to")
        if a in nodes and b in nodes:
            children[a].append(b)
            incoming[b] += 1
    queue = [nid for nid, deg in incoming.items() if deg == 0]
    order = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for c in children[nid]:
            incoming[c] -= 1
            if incoming[c] == 0:
                queue.append(c)
    # anything left (a cycle, or a disconnected node) still gets a slot so we
    # don't silently drop it — appended in original graph order.
    for nid in nodes:
        if nid not in order:
            order.append(nid)
    return order


class RunContext:
    def __init__(self, pipeline_id: str, run_id: str):
        self.pipeline_id = pipeline_id
        self.run_id = run_id
        self.train_df: pd.DataFrame | None = None
        self.test_df: pd.DataFrame | None = None
        self.target_col: str | None = None
        self.task: str | None = None          # "binary" | "multiclass" | "regression"
        self.model = None
        self.X_train = None
        self.y_train = None
        self.X_test = None
        self.y_test = None
        self.classes: list | None = None
        self.defer_fit_to_tune: bool = False  # set when a Tune node exists downstream of the model
        self.metrics_doc: dict | None = None  # {"metrics":[...], "chart":{...}, "label":...}
        self.share_visibility: str = "private"
        self.registry_key: str | None = None
        self.formula_deps: dict[str, set[str]] = {}  # formula output name -> names it directly references

    def emit(self, node_id, ev_type, message=""):
        mq.emit_event(self.pipeline_id, self.run_id, node_id, ev_type, message)


# ---------------------------------------------------------------- source
def run_csv(ctx: RunContext, node: dict, config: dict):
    ds = config.get("dataset", {})
    key = storage.s3_path_to_key(ds["path"])
    df = storage.get_csv(key)
    split_pct = float(get_param(node, "split", "20")) / 100.0
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)  # shuffle, deterministic
    cut = max(1, int(len(df) * (1 - split_pct)))
    ctx.train_df, ctx.test_df = df.iloc[:cut].copy(), df.iloc[cut:].copy()
    return f"loaded {len(df)} rows ({len(ctx.train_df)} train / {len(ctx.test_df)} test)"


# ------------------------------------------------------------- transform
def _numeric_columns(df: pd.DataFrame, exclude: set[str]) -> list[str]:
    return [c for c in df.select_dtypes(include=[np.number]).columns if c not in exclude]


def run_power_transform(ctx: RunContext, node: dict, config: dict):
    method = get_param(node, "method", "yeo-johnson")
    standardize = bool(get_param(node, "standardize", True))
    target = ctx.target_col or ""
    cols = _numeric_columns(ctx.train_df, {target})
    pt = PowerTransformer(method=method, standardize=standardize)
    ctx.train_df[cols] = pt.fit_transform(ctx.train_df[cols])
    ctx.test_df[cols] = pt.transform(ctx.test_df[cols])
    return f"transformed {len(cols)} numeric columns ({method})"


def run_robust_scaler(ctx: RunContext, node: dict, config: dict):
    with_centering = bool(get_param(node, "with_centering", True))
    with_scaling = bool(get_param(node, "with_scaling", True))
    target = ctx.target_col or ""
    cols = _numeric_columns(ctx.train_df, {target})
    sc = RobustScaler(with_centering=with_centering, with_scaling=with_scaling)
    ctx.train_df[cols] = sc.fit_transform(ctx.train_df[cols])
    ctx.test_df[cols] = sc.transform(ctx.test_df[cols])
    return f"scaled {len(cols)} numeric columns"


# ------------------------------------------------------------------ model
def resolve_formula_raw_deps(ctx: RunContext, name: str, _seen: set[str] | None = None) -> set[str]:
    """All RAW (non-formula) columns that transitively feed into formula
    `name`, by walking ctx.formula_deps. Used only when a formula is picked
    as the model's target — its own raw ingredients would otherwise still
    be sitting right there as "input" features, leaking the target back in."""
    _seen = _seen or set()
    if name in _seen:
        return set()
    _seen.add(name)
    raw = set()
    for dep in ctx.formula_deps.get(name, set()):
        if dep in ctx.formula_deps:
            raw |= resolve_formula_raw_deps(ctx, dep, _seen)
        else:
            raw.add(dep)
    return raw


def run_xgboost(ctx: RunContext, node: dict, config: dict):
    target_cfg = get_param(node, "target", {}) or {}
    target_col = target_cfg.get("column")
    declared_task = target_cfg.get("task", "classification")  # "classification" | "regression"
    if not target_col:
        raise ValueError('XGBoost node has no target column set ("Target & task" param).')
    ctx.target_col = target_col

    # If the target IS a formula, none of the raw ingredients that formula
    # was built from may also appear as an input feature — otherwise the
    # model would just be learning to invert its own target's formula.
    exclude = {target_col}
    if target_col in ctx.formula_deps:
        exclude |= resolve_formula_raw_deps(ctx, target_col)

    X_train = ctx.train_df.drop(columns=[c for c in exclude if c in ctx.train_df.columns])
    y_train = ctx.train_df[target_col]
    X_test = ctx.test_df.drop(columns=[c for c in exclude if c in ctx.test_df.columns])
    y_test = ctx.test_df[target_col]
    # keep only numeric feature columns — any leftover categorical/text
    # column would need its own encoder node upstream, which this run didn't have
    num_cols = X_train.select_dtypes(include=[np.number]).columns.tolist()
    X_train, X_test = X_train[num_cols], X_test[num_cols]

    n_estimators = int(get_param(node, "n_estimators", "100"))
    max_depth = int(get_param(node, "max_depth", "6"))
    learning_rate = float(get_param(node, "learning_rate", "0.1"))
    subsample = float(get_param(node, "subsample", "1.0"))

    if declared_task == "regression":
        ctx.task = "regression"
        ctx.classes = None
    else:
        # binary vs multiclass, from the ACTUAL number of distinct classes —
        # mirrors the same cardinality-based rule the frontend uses, so the
        # label shown in the builder matches what the model actually is.
        classes = sorted(pd.unique(y_train))
        ctx.classes = classes
        ctx.task = "binary" if len(classes) <= 2 else "multiclass"

    ctx.X_train, ctx.y_train, ctx.X_test, ctx.y_test = X_train, y_train, X_test, y_test
    lineage_note = f" (excluded {len(exclude) - 1} raw column(s) that feed the target formula: {', '.join(sorted(exclude - {target_col}))})" if len(exclude) > 1 else ""

    if ctx.defer_fit_to_tune:
        return f"prepared {len(X_train)} rows, {len(num_cols)} features ({ctx.task}) — fit deferred to Hyperparameter Tuning{lineage_note}"

    if ctx.task == "regression":
        model = xgb.XGBRegressor(n_estimators=n_estimators, max_depth=max_depth,
                                  learning_rate=learning_rate, subsample=subsample)
    else:
        model = xgb.XGBClassifier(n_estimators=n_estimators, max_depth=max_depth,
                                   learning_rate=learning_rate, subsample=subsample,
                                   eval_metric="mlogloss" if ctx.task == "multiclass" else "logloss")
    model.fit(X_train, y_train)
    ctx.model = model
    return f"trained XGBoost ({ctx.task}) on {len(X_train)} rows, {len(num_cols)} features{lineage_note}"


# ------------------------------------------------------------------- tune
_TUNE_PARAM_CAST = {"n_estimators": int, "max_depth": int, "learning_rate": float, "subsample": float}


def run_tune(ctx: RunContext, node: dict, config: dict):
    if ctx.X_train is None or ctx.y_train is None:
        raise ValueError("Hyperparameter Tuning: no upstream model prepared any data (connect this after a model node).")
    model_node = next((n for n in config["nodes"] if n.get("category") == "model"), None)
    if not model_node or model_node.get("type") != "xgboost":
        raise ValueError(f'Hyperparameter Tuning: "{model_node.get("type") if model_node else "this model"}" isn\'t supported yet — only XGBoost has a search space wired up.')

    from sklearn.model_selection import GridSearchCV, RandomizedSearchCV

    method = get_param(node, "method", "grid")
    num_samples = int(get_param(node, "num_samples", "20"))
    metric = get_param(node, "metric", "accuracy")
    mode = get_param(node, "mode", "max")
    cv = int(get_param(node, "cv", "3"))
    space = get_param(node, "space", {}) or {}

    # Any tunable param the user left blank keeps the model node's own fixed
    # value — a 1-item "range", not a param that drops out of the model.
    param_grid = {}
    for key, caster in _TUNE_PARAM_CAST.items():
        raw = (space.get(key) or "").strip()
        if raw:
            param_grid[key] = [caster(v.strip()) for v in raw.split(",") if v.strip()]
        else:
            param_grid[key] = [caster(get_param(model_node, key, "100" if key == "n_estimators" else "6" if key == "max_depth" else "0.1" if key == "learning_rate" else "1.0"))]

    scoring_map = {
        ("accuracy", "max"): "accuracy", ("f1_score", "max"): "f1_weighted",
        ("roc_auc", "max"): "roc_auc_ovr" if ctx.task == "multiclass" else "roc_auc",
        ("r2_score", "max"): "r2",
        ("rmse", "min"): "neg_root_mean_squared_error", ("mae", "min"): "neg_mean_absolute_error",
    }
    scoring = scoring_map.get((metric, mode), "accuracy" if ctx.task != "regression" else "r2")

    base = xgb.XGBRegressor() if ctx.task == "regression" else xgb.XGBClassifier(
        eval_metric="mlogloss" if ctx.task == "multiclass" else "logloss")

    if method == "random":
        n_iter = min(num_samples, int(np.prod([len(v) for v in param_grid.values()])))
        search = RandomizedSearchCV(base, param_grid, n_iter=n_iter, scoring=scoring, cv=cv, random_state=42)
    else:
        search = GridSearchCV(base, param_grid, scoring=scoring, cv=cv)

    search.fit(ctx.X_train, ctx.y_train)
    ctx.model = search.best_estimator_
    n_trials = len(search.cv_results_["params"])
    return (f"{method} search over {n_trials} combination(s), cv={cv}, scoring={scoring} — "
            f"best: {search.best_params_} (score={search.best_score_:.4f})")


# --------------------------------------------------------------- evaluate
def _classification_metrics(y_true, y_pred) -> list[dict]:
    return [
        {"label": "Accuracy", "value": f"{accuracy_score(y_true, y_pred):.3f}"},
        {"label": "F1", "value": f"{f1_score(y_true, y_pred, average='weighted'):.3f}"},
        {"label": "Precision", "value": f"{precision_score(y_true, y_pred, average='weighted', zero_division=0):.3f}"},
        {"label": "Recall", "value": f"{recall_score(y_true, y_pred, average='weighted', zero_division=0):.3f}"},
    ]


def run_confusion(ctx: RunContext, node: dict, config: dict):
    if ctx.model is None or ctx.task == "regression":
        raise ValueError("Confusion Matrix needs an upstream classification model.")
    y_pred = ctx.model.predict(ctx.X_test)
    cm = confusion_matrix(ctx.y_test, y_pred, labels=ctx.classes)
    ctx.metrics_doc = {
        "task": ctx.task,
        "label": f"{'Binary' if ctx.task == 'binary' else 'Multiclass'} classification · test",
        "metrics": _classification_metrics(ctx.y_test, y_pred),
        "chart": {"kind": "confusion", "data": {"matrix": cm.tolist()}},
    }
    return f"confusion matrix computed ({len(ctx.classes)}x{len(ctx.classes)})"


def run_roc(ctx: RunContext, node: dict, config: dict):
    if ctx.model is None or ctx.task != "binary":
        raise ValueError("ROC / AUC needs an upstream BINARY classification model.")
    proba = ctx.model.predict_proba(ctx.X_test)[:, 1]
    y_pred = ctx.model.predict(ctx.X_test)
    fpr, tpr, _ = roc_curve(ctx.y_test, proba, pos_label=ctx.classes[-1])
    auc = roc_auc_score(ctx.y_test, proba)
    metrics = _classification_metrics(ctx.y_test, y_pred)
    metrics.insert(0, {"label": "AUC", "value": f"{auc:.3f}"})
    step = max(1, len(fpr) // 40)  # thin the curve out for a compact payload
    ctx.metrics_doc = {
        "task": ctx.task, "label": "Binary classification · test", "metrics": metrics,
        "chart": {"kind": "roc", "auc": float(auc), "data": {"fpr": fpr[::step].tolist(), "tpr": tpr[::step].tolist()}},
    }
    return f"ROC/AUC computed (AUC={auc:.3f})"


def run_metrics_node(ctx: RunContext, node: dict, config: dict):
    """The generic task-aware "Metrics" node — regression or classification."""
    if ctx.model is None:
        raise ValueError("Metrics node needs an upstream model.")
    if ctx.task == "regression":
        y_pred = ctx.model.predict(ctx.X_test)
        r2 = r2_score(ctx.y_test, y_pred)
        mae = mean_absolute_error(ctx.y_test, y_pred)
        rmse = mean_squared_error(ctx.y_test, y_pred, squared=False)
        ctx.metrics_doc = {
            "task": "regression", "label": "Regression · test",
            "metrics": [{"label": "R²", "value": f"{r2:.3f}"}, {"label": "MAE", "value": f"{mae:.3f}"}, {"label": "RMSE", "value": f"{rmse:.3f}"}],
            "chart": {"kind": "comparison", "data": {"actual": [float(v) for v in ctx.y_test.tolist()], "predicted": [float(v) for v in y_pred.tolist()]}},
        }
        return f"R²={r2:.3f}"
    y_pred = ctx.model.predict(ctx.X_test)
    ctx.metrics_doc = {
        "task": ctx.task, "label": f"{'Binary' if ctx.task == 'binary' else 'Multiclass'} classification · test",
        "metrics": _classification_metrics(ctx.y_test, y_pred),
        "chart": {"kind": "confusion", "data": {"matrix": confusion_matrix(ctx.y_test, y_pred, labels=ctx.classes).tolist()}},
    }
    return "metrics computed"


# ----------------------------------------------------------------- output
def run_registry(ctx: RunContext, node: dict, config: dict, user_key: str):
    if ctx.metrics_doc is None:
        raise ValueError("Register Model needs an upstream evaluation node to have run first.")
    name = get_param(node, "name", "model")
    stage = get_param(node, "stage", "none")
    version = get_param(node, "version", "v1")
    score = None
    for m in ctx.metrics_doc["metrics"]:
        if m["label"] in ("Accuracy", "R²", "AUC"):
            try:
                score = float(m["value"])
            except ValueError:
                pass
            break
    model_type_node = next((n for n in config["nodes"] if n.get("category") == "model"), {})
    doc = {
        "name": name, "version": version, "stage": stage, "task": ctx.task,
        "modelType": model_type_node.get("name", "Model"), "icon": "boxes",
        "visibility": ctx.share_visibility,
        "score": score, "samples": int(len(ctx.train_df) + len(ctx.test_df)) if ctx.train_df is not None else None,
        "pipelineId": ctx.pipeline_id, "runId": ctx.run_id, "registeredAt": time.time() * 1000,
    }
    key = f"users/{user_key}/registry/{_slug(name)}/{version}.json"
    storage.put_json(key, doc)
    ctx.registry_key = key
    return f"registered as {name}/{version} (visibility={ctx.share_visibility})"


def prescan_share_visibility(nodes: dict) -> str:
    """A 'share' output node can sit anywhere in the graph and isn't
    necessarily processed before 'registry' by topo order (they're usually
    siblings) — read it up front so registry always sees the right value."""
    for node in nodes.values():
        if node.get("type") == "share":
            share_cfg = get_param(node, "share", {}) or {}
            mode = share_cfg.get("mode")
            if mode == "public":
                return "public"
            if mode == "selected" and share_cfg.get("users"):
                return "shared"
    return "private"


def prescan_target_column(nodes: dict) -> str | None:
    """The target column is declared on the MODEL node's "target" param, but
    preprocessing nodes (power_transform, robust_scaler, ...) run BEFORE the
    model node in topo order — so without this, ctx.target_col is still None
    when they run and they'd transform/scale the target column right along
    with the real features, which is wrong. Read it up front instead."""
    for node in nodes.values():
        if node.get("category") == "model":
            target_cfg = get_param(node, "target", {}) or {}
            if target_cfg.get("column"):
                return target_cfg["column"]
    return None


def _slug(name: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9._-]+", "_", (name or "model").lower()).strip("_") or "model"


NODE_HANDLERS = {
    "csv": run_csv,
    "power_transform": run_power_transform,
    "robust_scaler": run_robust_scaler,
    "xgboost": run_xgboost,
    "tune": run_tune,
    "confusion": run_confusion,
    "roc": run_roc,
    "metrics": run_metrics_node,
    **feature_engine.HANDLERS,
    # "registry" and "share" are handled specially in run_pipeline() since
    # they need extra context (user_key / each other's output).
}


def run_pipeline(run_id: str, pipeline_id: str, config: dict) -> None:
    ctx = RunContext(pipeline_id, run_id)
    nodes = {n["id"]: n for n in config["nodes"]}
    edges = config.get("edges", [])
    order = topo_order(nodes, edges)
    ctx.share_visibility = prescan_share_visibility(nodes)
    ctx.target_col = prescan_target_column(nodes)  # set BEFORE preprocessing runs
    ctx.defer_fit_to_tune = any(n.get("type") == "tune" for n in nodes.values())
    has_registry_node = any(n.get("type") == "registry" for n in nodes.values())

    ds_path = config.get("dataset", {}).get("path", "")
    user_key = storage.user_key_from_path(ds_path)

    try:
        for node_id in order:
            node = nodes[node_id]
            ctx.emit(node_id, "node_start", f"{node.get('name', node_id)} — running")
            try:
                if node["type"] == "registry":
                    msg = run_registry(ctx, node, config, user_key)
                elif node["type"] == "share":
                    if has_registry_node:
                        # a sibling "Register Model" node already wrote (or will
                        # write) the registry entry — this node just set the
                        # visibility flag it uses (see prescan_share_visibility).
                        msg = f"visibility → {ctx.share_visibility}"
                    else:
                        # "Share Model" used on its own, with no "Register
                        # Model" node anywhere in the graph — it used to be a
                        # no-op here, so nothing ever showed up on the
                        # Federated Learning page. It can register on its own
                        # using sensible defaults (run_registry() already
                        # falls back to "none"/"v1" for params this node
                        # doesn't have). The name defaults to the model
                        # node's name + a run-id suffix rather than a bare
                        # "model", so repeated share-only runs don't all
                        # collide on the same registry key.
                        reg_node = node
                        if not get_param(node, "name"):
                            model_node = next((n for n in config["nodes"] if n.get("category") == "model"), {})
                            default_name = f"{model_node.get('name', 'model')}-{run_id[-6:]}"
                            reg_node = {**node, "params": list(node.get("params", [])) + [{"key": "name", "value": default_name}]}
                        msg = run_registry(ctx, reg_node, config, user_key) + " (registered by Share Model — no Register Model node was present)"
                elif node["type"] in NODE_HANDLERS:
                    msg = NODE_HANDLERS[node["type"]](ctx, node, config)
                else:
                    msg = f"{node.get('type')} isn't implemented in this worker yet — skipped"
                ctx.emit(node_id, "node_done", msg)
            except Exception as e:  # noqa: BLE001
                ctx.emit(node_id, "node_error", str(e))
                raise

        if ctx.metrics_doc is None:
            raise RuntimeError("Pipeline finished without producing an evaluation result (no evaluate node ran).")

        result_key = f"users/{user_key}/projects/_/pipelines/{pipeline_id}/runs/{run_id}/result.json"
        # keep the same project-scoped path shape the frontend expects, derived from the dataset path when possible
        m = None
        import re as _re
        pm = _re.search(r"(users/[^/]+/projects/[^/]+/pipelines/[^/]+/)", ds_path)
        if pm:
            result_key = pm.group(1) + f"runs/{run_id}/result.json"
        storage.put_json(result_key, ctx.metrics_doc)
        mq.emit_done(pipeline_id, run_id, result_key)
    except Exception as e:  # noqa: BLE001
        mq.emit_error(pipeline_id, run_id, str(e))
        raise
