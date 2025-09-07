# TODO: Build leak-free features available by 15:28 IST on T-day.
# ml/features/build_features.py
import os, argparse, pandas as pd, numpy as np

def add_basic_feats(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # direction label (UP=1 if overnight_ret>0)
    out["dir_up"] = (out["overnight_ret"] > 0).astype(int)

    # scale raw features to practical units
    for c in ["open15_mom_bps","late28_mom_bps","open15_hl_range_bps","late28_hl_range_bps","open15_vol_bp","late28_vol_bp"]:
        if c in out.columns:
            # already in bps from assembler; keep as-is
            pass

    # vix features
    out["vix_delta_pct"] = (out["vix_delta"] / out["vix_close_t_1"]).replace([np.inf,-np.inf], np.nan)

    # seasonality
    out["dow"] = pd.to_datetime(out["date"]).dt.dayofweek
    out = pd.get_dummies(out, columns=["dow"], prefix="dow", drop_first=False)

    # safe fills
    out = out.replace([np.inf,-np.inf], np.nan)
    out = out.fillna(0.0)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    feats = add_basic_feats(df)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    feats.to_parquet(args.out, index=False)
    print(f"Saved features → {args.out} (rows={len(feats)}, cols={len(feats.columns)})")

if __name__ == "__main__":
    main()
