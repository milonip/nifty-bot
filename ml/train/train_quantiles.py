# ml/train/train_quantiles.py
from __future__ import annotations
import argparse, joblib, numpy as np, pandas as pd
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor

TARGET = "overnight_ret_bps"
DROP_ALWAYS = {"label_up", TARGET, "date"}
QUANTS = [0.10, 0.25, 0.50, 0.75, 0.90]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="inp",  required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    if TARGET not in df.columns:
        raise ValueError(f"Missing target column '{TARGET}' in {args.inp}")

    num = df.select_dtypes(include=[np.number]).copy()
    Xcols = [c for c in num.columns if c not in DROP_ALWAYS]
    if not Xcols:
        raise ValueError("No numeric feature columns left after filtering.")
    X = num[Xcols].astype(float).values
    y = df[TARGET].astype(float).values

    models = {}
    for q in QUANTS:
        m = GradientBoostingRegressor(loss="quantile", alpha=q, n_estimators=400, max_depth=3, learning_rate=0.05)
        m.fit(X, y)
        models[f"{q:.2f}"] = m

    Path(args.outp).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"models": models, "Xcols": Xcols, "quants": QUANTS}, args.outp)
    print(f"Saved quantile bundle → {args.outp} (q={QUANTS})")

if __name__ == "__main__":
    main()
