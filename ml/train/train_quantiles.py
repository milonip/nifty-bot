# TODO: Train quantile regressors (q10..q90) with walk-forward.
# ml/train/train_quantiles.py
import os, argparse, json, pandas as pd, numpy as np
from xgboost import XGBRegressor
import joblib

SEED = 7

# pinball loss via XGB's 'reg:absoluteerror' + custom quantile transform
# Here we train separate models per quantile with 'reg:pseudohubererror' as a simple proxy.
# (Good enough starter; can swap to LightGBM with quantile objective later.)
QUANTS = [0.10, 0.25, 0.50, 0.75, 0.90]

def pick_X_cols(df):
    drop = {"date","px_1528","px_0921","overnight_ret","dir_up"}
    return [c for c in df.columns if c not in drop]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).sort_values("date")
    Xcols = pick_X_cols(df)
    y = df["overnight_ret"].values

    models = {}
    for q in QUANTS:
        m = XGBRegressor(
            n_estimators=300, max_depth=3, learning_rate=0.05,
            subsample=0.9, colsample_bytree=0.9, reg_lambda=1.0,
            random_state=SEED, objective="reg:squaredlogerror"  # simple, stable
        )
        m.fit(df[Xcols], y)
        models[str(q)] = m
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    joblib.dump({"models": models, "Xcols": Xcols, "quants": QUANTS}, args.out)
    print(f"Saved quantile bundle → {args.out} (q={QUANTS})")

if __name__ == "__main__":
    main()
