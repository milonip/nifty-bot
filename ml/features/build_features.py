# ml/features/build_features.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

# columns we’ll try to use if present
CANDIDATE_FEATURES = [
    "open15_ret", "open15_hl_range_bps", "open15_mom_bps", "open15_vol_bp",
    "late28_ret", "late28_hl_range_bps", "late28_mom_bps", "late28_vol_bp",
    # vix
    "vix_close", "vix_delta_pct",
]

LABEL_UP_COL       = "label_up"
LABEL_RET_BPS_COL  = "overnight_ret_bps"

def _safe_get(df: pd.DataFrame, want: str, *fallbacks: str, default=None):
    if want in df.columns:
        return df[want]
    for alt in fallbacks:
        if alt in df.columns:
            return df[alt]
    if default is not None:
        return pd.Series(default, index=df.index)
    raise KeyError(f"Missing required column: {want} (also tried {fallbacks})")

def build(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Accepts the columns produced by assemble_training_table.py in your repo
    and emits:
      - feature columns (subset of CANDIDATE_FEATURES that exist)
      - labels: label_up, overnight_ret_bps
    """
    # --- rename / derive expected inputs ---
    # vix_close wanted; your dataset has vix_close_t
    vix_close = _safe_get(df, "vix_close", "vix_close_t", default=np.nan)
    # vix delta pct (already a pct in your data as vix_delta)
    vix_delta_pct = _safe_get(df, "vix_delta_pct", "vix_delta", default=np.nan)

    # base features already present (open15_*, late28_*)
    feats = pd.DataFrame(index=df.index)
    for col in [
        "open15_ret", "open15_hl_range_bps", "open15_mom_bps", "open15_vol_bp",
        "late28_ret", "late28_hl_range_bps", "late28_mom_bps", "late28_vol_bp",
    ]:
        if col in df.columns:
            feats[col] = df[col]

    feats["vix_close"] = vix_close
    feats["vix_delta_pct"] = vix_delta_pct

    # --- labels ---
    # your dataset already has overnight_ret (fractional)
    if "overnight_ret" not in df.columns:
        raise ValueError("Missing required column 'overnight_ret' in dataset")
    feats[LABEL_RET_BPS_COL] = (df["overnight_ret"].astype(float) * 1e4)
    feats[LABEL_UP_COL]      = (df["overnight_ret"].astype(float) > 0).astype(int)

    # keep date if present (handy for debugging / plotting)
    if "date" in df.columns:
        feats["date"] = pd.to_datetime(df["date"])

    # tidy
    feats = feats.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    meta = {
        "n_rows": int(len(feats)),
        "n_cols": int(feats.shape[1]),
        "used_cols": list(feats.columns),
    }
    return feats, meta

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="inp",  required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    feats, meta = build(df)
    Path(args.outp).parent.mkdir(parents=True, exist_ok=True)
    feats.to_parquet(args.outp, index=False)
    print(f"Saved features → {args.outp} (rows={len(feats)}, cols={feats.shape[1]})")
    print(meta)

if __name__ == "__main__":
    main()
