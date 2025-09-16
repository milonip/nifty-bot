# ml/train/train_direction.py
from __future__ import annotations
import argparse, json, joblib
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, brier_score_loss, balanced_accuracy_score
from xgboost import XGBClassifier

LABEL = "label_up"
DROP_ALWAYS = {LABEL, "overnight_ret_bps", "date"}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="inp",  required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)

    if LABEL not in df.columns:
        raise ValueError(f"Missing label column '{LABEL}' in {args.inp}")

    # keep only numeric columns and drop labels / dates
    num = df.select_dtypes(include=[np.number]).copy()
    Xcols = [c for c in num.columns if c not in DROP_ALWAYS]
    if not Xcols:
        raise ValueError("No numeric feature columns left after filtering.")
    X = num[Xcols].astype(float).values
    y = df[LABEL].astype(int).values

    # tiny dataset: use simple CV that won’t error on small N
    skf = StratifiedKFold(n_splits=min(3, max(2, np.unique(y, return_counts=True)[1].min())), shuffle=True, random_state=42)
    aucs, briers, bals = [], [], []

    for tr, va in skf.split(X, y):
        m = XGBClassifier(
            n_estimators=200,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
            n_jobs=1,
            eval_metric="logloss",
        )
        m.fit(X[tr], y[tr])
        p = m.predict_proba(X[va])[:, 1]
        aucs.append(roc_auc_score(y[va], p))
        briers.append(brier_score_loss(y[va], p))
        bals.append(balanced_accuracy_score(y[va], (p >= 0.5).astype(int)))

    # final fit on all data
    model = XGBClassifier(
        n_estimators=300,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        n_jobs=1,
        eval_metric="logloss",
    )
    model.fit(X, y)

    metrics = {
        "auc": float(np.mean(aucs)) if aucs else None,
        "brier": float(np.mean(briers)) if briers else None,
        "balanced_acc": float(np.mean(bals)) if bals else None,
    }

    Path(args.outp).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "Xcols": Xcols, "metrics": metrics}, args.outp)
    print(f"Saved direction model → {args.outp}")
    print("CV:", metrics)

if __name__ == "__main__":
    main()
