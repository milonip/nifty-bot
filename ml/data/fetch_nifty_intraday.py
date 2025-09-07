import os, sys, glob, argparse
import pandas as pd

REQUIRED = {"datetime","open","high","low","close"}

def read_one(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    low = {c.strip().lower(): c for c in df.columns}

    for need in REQUIRED:
        if need not in low:
            raise ValueError(f"{path}: missing required column '{need}'")

    df = df.rename(columns={
        low["datetime"]: "datetime",
        low["open"]: "open",
        low["high"]: "high",
        low["low"]: "low",
        low["close"]: "close",
    })

    # parse datetimes (assume IST if tz-naive)
    dtser = pd.to_datetime(df["datetime"], errors="coerce", utc=False)
    if dtser.dt.tz is None:
        dtser = dtser.dt.tz_localize("Asia/Kolkata")
    df["datetime"] = dtser.dt.tz_convert("UTC")   # store UTC on disk

    df = df.sort_values("datetime").drop_duplicates("datetime")
    return df[["datetime","open","high","low","close"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-vendor-csv", type=str, default="data/vendor/nifty_1m/*.csv",
                    help="glob for 1m CSVs (IST). Columns: datetime,open,high,low,close")
    ap.add_argument("--out", type=str, default="data/raw/nifty_1m.parquet")
    args = ap.parse_args()

    paths = sorted(glob.glob(args.from_vendor_csv))
    if not paths:
        print(f"No CSVs found at: {args.from_vendor_csv}", file=sys.stderr)
        sys.exit(2)

    frames = []
    for p in paths:
        try:
            frames.append(read_one(p))
        except Exception as e:
            print(f"[warn] {p}: {e}", file=sys.stderr)

    if not frames:
        print("No valid minute files ingested.", file=sys.stderr)
        sys.exit(2)

    df = pd.concat(frames, ignore_index=True).drop_duplicates("datetime").sort_values("datetime")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Saved NIFTY minute bars → {args.out} (rows={len(df)})")

if __name__ == "__main__":
    main()
