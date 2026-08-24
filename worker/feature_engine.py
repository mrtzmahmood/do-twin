"""
Feature-engineering node handlers, shared by the Pipeline (batch, over the
full train/test split) — and designed so the SAME functions can later back
a streaming Data Flow node once flow execution moves server-side (see the
"Feature Engineering" section of the project README). Each function takes
(ctx, node, config) exactly like the handlers in pipeline_runner.py, mutates
ctx.train_df / ctx.test_df in place, and returns a short status string.
"""

import numpy as np
import pandas as pd


def _cols(raw: str) -> list[str]:
    return [c.strip() for c in (raw or "").split(",") if c.strip()]


def _nums(raw: str) -> list[float]:
    out = []
    for x in (raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.append(float(x))
        except ValueError:
            pass
    return out


# ---------------------------------------------------------- time series ---
def run_resample(ctx, node, config):
    from pipeline_runner import get_param
    dt_col = get_param(node, "datetime_col", "timestamp")
    freq = get_param(node, "freq", "1min")
    method = get_param(node, "method", "mean")

    def resample_one(df):
        if dt_col not in df.columns:
            raise ValueError(f'Resample: datetime column "{dt_col}" not found.')
        d = df.copy()
        d[dt_col] = pd.to_datetime(d[dt_col], errors="coerce")
        d = d.dropna(subset=[dt_col]).set_index(dt_col).sort_index()
        num = d.select_dtypes(include=[np.number])
        rest = d.select_dtypes(exclude=[np.number])
        if method in ("ffill", "bfill", "interpolate"):
            r = num.resample(freq).asfreq()
            r = r.ffill() if method == "ffill" else r.bfill() if method == "bfill" else r.interpolate()
        else:
            r = getattr(num.resample(freq), method)()
        if len(rest.columns):
            r = r.join(rest.resample(freq).first())
        return r.reset_index()

    ctx.train_df = resample_one(ctx.train_df)
    ctx.test_df = resample_one(ctx.test_df)
    return f"resampled to {freq} ({method}) — {len(ctx.train_df)} train / {len(ctx.test_df)} test rows"


def run_rolling_window(ctx, node, config):
    from pipeline_runner import get_param
    cols = _cols(get_param(node, "columns", ""))
    window = int(float(get_param(node, "window", "20")))
    stat = get_param(node, "stat", "mean")
    min_periods = int(float(get_param(node, "min_periods", "1")))
    if not cols:
        raise ValueError("Rolling Window: no columns specified.")

    def apply_one(df):
        for c in cols:
            if c not in df.columns:
                continue
            r = df[c].rolling(window, min_periods=min_periods)
            new_col = f"{c}_roll_{stat}{window}"
            if stat == "ewm":
                df[new_col] = df[c].ewm(span=window, min_periods=min_periods).mean()
            else:
                df[new_col] = getattr(r, stat)()
            df[new_col] = df[new_col].bfill().ffill()
        return df

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    return f"added rolling {stat}({window}) for {len(cols)} column(s)"


def run_lag_diff(ctx, node, config):
    from pipeline_runner import get_param
    cols = _cols(get_param(node, "columns", ""))
    lags = [int(v) for v in _nums(get_param(node, "lags", "1"))]
    do_diff = bool(get_param(node, "diff", True))
    if not cols:
        raise ValueError("Lag & Diff: no columns specified.")

    def apply_one(df):
        for c in cols:
            if c not in df.columns:
                continue
            for lag in lags:
                df[f"{c}_lag{lag}"] = df[c].shift(lag).bfill()
            if do_diff:
                df[f"{c}_diff1"] = df[c].diff().fillna(0)
        return df

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    return f"added lag{lags} " + ("+ diff(1) " if do_diff else "") + f"for {len(cols)} column(s)"


_CYCLE_MAX = {"hour": 24, "weekday": 7, "month": 12, "minute": 60, "dayofyear": 365}


def run_datetime_split(ctx, node, config):
    from pipeline_runner import get_param
    col = get_param(node, "column", None)
    parts = _cols(get_param(node, "parts", "hour, weekday, month"))
    cyclical = bool(get_param(node, "cyclical", True))
    if not col:
        raise ValueError("Datetime Split: pick a datetime column (the \"datetime column\" field).")

    def apply_one(df):
        if col not in df.columns:
            raise ValueError(f'Datetime Split: column "{col}" not found.')
        dt = pd.to_datetime(df[col], errors="coerce")
        for p in parts:
            if p == "is_weekend":
                df[f"{col}_is_weekend"] = (dt.dt.weekday >= 5).astype(int)
                continue
            if not hasattr(dt.dt, p):
                continue
            vals = getattr(dt.dt, p)
            df[f"{col}_{p}"] = vals
            if cyclical and p in _CYCLE_MAX:
                period = _CYCLE_MAX[p]
                df[f"{col}_{p}_sin"] = np.sin(2 * np.pi * vals / period)
                df[f"{col}_{p}_cos"] = np.cos(2 * np.pi * vals / period)
        return df

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    return f"split {col} into {', '.join(parts)}" + (" (+ cyclical sin/cos)" if cyclical else "")


# ------------------------------------------------------- domain formulas ---
def run_formula(ctx, node, config):
    from pipeline_runner import get_param
    import re as _re
    expr = get_param(node, "expression", "")
    output = (get_param(node, "output", "") or "").strip()
    if not expr:
        raise ValueError("Custom Formula: no expression given.")
    if not output:
        raise ValueError("Custom Formula: give this formula a name (the \"formula name\" field) — it becomes the output column.")

    # Which names does this formula actually reference? Detected from the
    # expression text itself (any identifier that matches a real column or
    # an earlier formula's output) — no separate "variables used" list to
    # keep in sync, and a name can appear as often as needed.
    known = set(ctx.train_df.columns)
    tokens = set(_re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr))
    deps = tokens & known
    if output in deps:
        deps.discard(output)

    def apply_one(df):
        try:
            df[output] = df.eval(expr, engine="python")
        except Exception as e:
            raise ValueError(f'Custom Formula "{expr}" failed: {e}') from e
        return df

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    ctx.formula_deps[output] = deps
    return f"{output} = {expr}" + (f"  (uses: {', '.join(sorted(deps))})" if deps else "")


# ------------------------------------------------------ domain libraries ---
def run_fuzzy_logic(ctx, node, config):
    from pipeline_runner import get_param
    inputs_raw = _cols(get_param(node, "inputs", ""))
    output_raw = get_param(node, "output", "FuzzyScore:0:1")
    rules = get_param(node, "rules", []) or []

    def parse_spec(s):
        parts = s.split(":")
        name = parts[0].strip()
        lo = float(parts[1]) if len(parts) > 1 else 0.0
        hi = float(parts[2]) if len(parts) > 2 else 1.0
        return name, lo, hi

    inputs = [parse_spec(s) for s in inputs_raw]
    out_name, out_lo, out_hi = parse_spec(output_raw)
    if not inputs:
        raise ValueError("Fuzzy Inference: no input variables specified.")

    try:
        import skfuzzy as fuzz  # noqa: F401
        HAVE_FUZZY = True
    except ImportError:
        HAVE_FUZZY = False

    if HAVE_FUZZY and rules:
        # A real rule-based Mamdani system, built from the "rules" param —
        # left for a future rule-editor UI to populate; not wired yet.
        raise NotImplementedError("Custom fuzzy rules aren't wired up yet — leave 'rules' empty to use the normalized-average fallback.")

    # Fallback (no rule editor yet): a normalized weighted-average "fuzzy-like"
    # score — each input mapped to its [0,1] position in its declared
    # universe, then averaged. Documented as an approximation, not a real
    # Mamdani inference, until rule authoring is added to the node.
    def apply_one(df):
        acc = pd.Series(0.0, index=df.index)
        n = 0
        for name, lo, hi in inputs:
            if name not in df.columns:
                continue
            norm = ((df[name] - lo) / (hi - lo if hi != lo else 1)).clip(0, 1)
            acc = acc + norm
            n += 1
        if n == 0:
            raise ValueError("Fuzzy Inference: none of the input columns were found.")
        df[out_name] = out_lo + (acc / n) * (out_hi - out_lo)
        return df

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    note = "" if HAVE_FUZZY else " (scikit-fuzzy not installed — used the normalized-average fallback)"
    return f"{out_name} = fuzzy-normalized average of {len(inputs)} input(s){note}"


def run_btyd(ctx, node, config):
    """BG/NBD (purchase frequency) + Gamma-Gamma (monetary value) via the
    `lifetimes` package — customer lifetime value forecasting. Unlike the
    other feature nodes this reshapes the data (one row per customer, not
    per original row), so it's meant to sit right after the source node in
    a customer-analytics pipeline, not mixed into a per-row feature stack."""
    from pipeline_runner import get_param
    customer_col = get_param(node, "customer_col", "customer_id")
    date_col = get_param(node, "date_col", "order_date")
    monetary_col = get_param(node, "monetary_col", "amount")
    horizon = int(float(get_param(node, "horizon_days", "90")))

    from lifetimes.utils import summary_data_from_transaction_data
    from lifetimes import BetaGeoFitter, GammaGammaFitter

    def apply_one(df):
        for c in (customer_col, date_col, monetary_col):
            if c not in df.columns:
                raise ValueError(f'BTYD: column "{c}" not found.')
        summary = summary_data_from_transaction_data(
            df, customer_col, date_col, monetary_value_col=monetary_col, freq="D",
        )
        summary = summary[summary["frequency"] > 0]
        if len(summary) < 5:
            raise ValueError("BTYD: not enough repeat customers in this split to fit a model.")

        bgf = BetaGeoFitter(penalizer_coef=0.01)
        bgf.fit(summary["frequency"], summary["recency"], summary["T"])
        summary["predicted_purchases"] = bgf.conditional_expected_number_of_purchases_up_to_time(
            horizon, summary["frequency"], summary["recency"], summary["T"],
        )

        ggf = GammaGammaFitter(penalizer_coef=0.01)
        ggf.fit(summary["frequency"], summary["monetary_value"])
        summary["predicted_clv"] = ggf.customer_lifetime_value(
            bgf, summary["frequency"], summary["recency"], summary["T"], summary["monetary_value"],
            time=horizon / 30.0, freq="D", discount_rate=0.01,
        )
        out = summary.reset_index()[[customer_col, "predicted_purchases", "predicted_clv"]]
        return df.merge(out, on=customer_col, how="left")

    ctx.train_df = apply_one(ctx.train_df)
    ctx.test_df = apply_one(ctx.test_df)
    return f"predicted_purchases/predicted_clv added (horizon={horizon}d)"


HANDLERS = {
    "resample": run_resample,
    "rolling_window": run_rolling_window,
    "lag_diff": run_lag_diff,
    "datetime_split": run_datetime_split,
    "formula": run_formula,
    "fuzzy_logic": run_fuzzy_logic,
    "btyd": run_btyd,
}
