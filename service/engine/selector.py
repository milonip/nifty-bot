# service/engine/selector.py
"""
Lightweight selector: loads trained models, reads the latest features row,
produces direction probability and quantile predictions.

Looks for models in:
  - ml/registry/active/
  - models/

Expects features at:
  - data/processed/overnight_features.parquet
"""

from __future__ import annotations

import joblib
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Where to search for model artifacts
REGISTRY_DIRS: List[str] = [
    "ml/registry/active",
    "models",
]

FEATURES_DEFAULT = "data/processed/overnight_features.parquet"


# ---------- helpers ----------

def _find(filename: str) -> str | None:
    """Return the first existing path for filename in REGISTRY_DIRS, else None."""
    for d in REGISTRY_DIRS:
        p = Path(d) / filename
        if p.exists():
            return str(p)
    return None


def _latest_row(features_path: str = FEATURES_DEFAULT) -> pd.DataFrame:
    """Return a single-row DataFrame with the most recent features."""
    p = Path(features_path)
    if not p.exists():
        raise FileNotFoundError(
            f"Features parquet not found at {features_path}. "
            "Build it via ml.data.assemble_training_table first."
        )
    df = pd.read_parquet(p)
    if "date" in df.columns:
        df = df.sort_values("date")
    # keep last row only
    return df.tail(1).reset_index(drop=True)


def _align(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Align DF columns to model expectations (missing -> 0.0; drop extras)."""
    return pd.DataFrame({c: (df[c] if c in df.columns else 0.0) for c in cols})


# ---------- loaders ----------

def load_direction() -> Tuple[Any, List[str], Dict[str, Any]]:
    """
    Returns (model, Xcols, metrics)
    direction_xgb.pkl should be a dict with keys: model, Xcols, metrics (optional).
    """
    path = _find("direction_xgb.pkl")
    if not path:
        raise FileNotFoundError(
            "direction_xgb.pkl not found in models/ or ml/registry/active/."
        )
    obj = joblib.load(path)
    return obj["model"], obj["Xcols"], obj.get("metrics", {})


def load_quantiles() -> Tuple[Dict[str, Any], List[str], List[float]]:
    """
    Returns (models_by_quantile, Xcols, quants)
    quantiles_xgb.pkl should be a dict with keys: models, Xcols, quants.
    """
    path = _find("quantiles_xgb.pkl")
    if not path:
        raise FileNotFoundError(
            "quantiles_xgb.pkl not found in models/ or ml/registry/active/."
        )
    obj = joblib.load(path)
    return obj["models"], obj["Xcols"], obj["quants"]


# ---------- public API ----------

def predict(features_path: str = FEATURES_DEFAULT) -> Dict[str, Any]:
    """
    Compute prediction bundle from latest features row.

    Returns dict:
      {
        "direction": "UP|DOWN|NEUTRAL",
        "confidence": float in [0,1],
        "quantiles": { "0.1": x, "0.25": y, ... },
        "q50": float (median move proxy),
        "cv": {... metrics ...},
        "suggested_strikes": []  # filled by API after reading live LTP
      }
    """
    # load artifacts
    dir_model, dir_cols, dir_cv = load_direction()
    q_models, q_cols, quants = load_quantiles()

    # most recent features
    row = _latest_row(features_path)

    # align columns
    Xd = _align(row, dir_cols)
    Xq = _align(row, q_cols)

    # direction probability (UP)
    proba = dir_model.predict_proba(Xd)
    p_up = float(proba[:, 1][0])

    # quantile predictions
    preds = {str(q): float(q_models[str(q)].predict(Xq)[0]) for q in quants}
    # handle string keys like "0.5" vs 0.5 if present
    if "0.5" not in preds and "0.50" in preds:
        preds["0.5"] = preds["0.50"]

    # median move proxy
    q50 = preds.get("0.5")
    if q50 is None and preds:
        # fallback to the middle quantile if naming differs
        mid_k = sorted(preds.keys(), key=lambda s: float(s))[len(preds)//2]
        q50 = preds[mid_k]

    # simple policy thresholding
    direction = "UP" if p_up >= 0.55 else ("DOWN" if p_up <= 0.45 else "NEUTRAL")
    confidence = p_up if direction != "NEUTRAL" else abs(p_up - 0.5) * 2.0

    return {
        "direction": direction,
        "confidence": confidence,
        "quantiles": preds,
        "q50": q50,
        "cv": dir_cv,
        "suggested_strikes": [],
    }
