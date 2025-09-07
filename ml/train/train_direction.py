# TODO: Train UP/DOWN classifier with walk-forward expanding windows.
# ml/train/train_direction.py
import os, argparse, json, pandas as pd, numpy as np
from sklearn.metrics import roc_auc_score, brier_score_loss, balanced_accuracy_score
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBClassifier
import joblib

SEED = 7

def pick_X_cols(df):
    drop = {"date","px_1528","px_0921","overnight_ret","dir_up"}
    return [c for c in df.columns if c not in drop]

def evaluate_cv(df, Xcols):
    tscv = TimeSeriesSplit(n_splits=min(5, max(2, len(df)//5)))
    ys, ps = [], []
    for tr, te in tscv.split(df):
        Xtr, Xte = df.iloc[tr][Xcols], df.iloc[te][Xcols]
        ytr, yte = df.iloc[tr]["dir_up"], df.iloc[te]["dir_up"]
        clf = XGBClassifier(
            n_estimators=200, max_depth=3, learning_rate=0.07,
            subsample=0.9, colsample_bytree=0.9, reg_lambda=1.0,
            random_state=SEED, eval_metric="logloss"
        )
        clf.fit(Xtr, ytr)
        p = clf.predict_proba(Xte)[:,1]
        ys.append(yte.values); ps.append(p)
    y = np.concatenate(ys); p = np.concatenate(ps)
    auc = roc_auc_score(y, p)
    brier = brier_score_loss(y, p)
    bal = balanced_accuracy_score(y, (p>=0.5).astype(int))
    return {"auc": float(auc), "brier": float(brier), "balanced_acc": float(bal)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).sort_values("date")
    Xcols = pick_X_cols(df)

    # CV metrics
    metrics = evaluate_cv(df, Xcols)

    # final fit on all data
    model = XGBClassifier(
        n_estimators=300, max_depth=3, learning_rate=0.05,
        subsample=0.9, colsample_bytree=0.9, reg_lambda=1.0,
        random_state=SEED, eval_metric="logloss"
    ).fit(df[Xcols], df["dir_up"])

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    joblib.dump({"model": model, "Xcols": Xcols, "metrics": metrics}, args.out)
    print(f"Saved direction model → {args.out}")
    print("CV:", metrics)

if __name__ == "__main__":
    main()
