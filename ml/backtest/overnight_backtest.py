# TODO: Backtest that mirrors live rules (15:28 BUY if flat, 09:21 SELL if open).
# ml/backtest/overnight_backtest.py
import os, argparse, json, numpy as np, pandas as pd
import joblib

def load_models(dir_path_or_files):
    dirn = os.path.dirname if isinstance(dir_path_or_files, str) else None

def load_direction(path): 
    obj = joblib.load(path); return obj["model"], obj["Xcols"], obj["metrics"]

def load_quants(path): 
    obj = joblib.load(path); return obj["models"], obj["Xcols"], obj["quants"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--direction", required=True)
    ap.add_argument("--quantiles", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.features).sort_values("date").reset_index(drop=True)
    dir_model, dir_cols, dir_cv = load_direction(args.direction)
    q_models, q_cols, quants = load_quants(args.quantiles)

    # sanity: intersect columns if training columns differ
    Xd = df[dir_cols].copy()
    Xq = df[q_cols].copy()

    # predictions
    df["p_up"] = dir_model.predict_proba(Xd)[:,1]
    preds = {}
    for q, m in q_models.items():
        preds[q] = m.predict(Xq)
    df["q50"] = preds.get("0.5", preds.get("0.50", list(preds.values())[0]))

    # naive strategy: go long if p_up>=0.55 else flat
    df["sig"] = (df["p_up"] >= 0.55).astype(int)
    df["ret"] = df["overnight_ret"] * df["sig"]
    df["equity"] = (1.0 + df["ret"]).cumprod()

    report = {
        "n": int(len(df)),
        "sum_ret_bps": float(df["ret"].sum() * 1e4),
        "hit_rate": float(( (df["ret"]>0).sum() / max(1,len(df[df['sig']==1])) ) if (df["sig"]==1).any() else 0.0),
        "final_equity": float(df["equity"].iloc[-1]),
        "dir_cv": dir_cv,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Backtest report → {args.out}")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
