# ml/data/assemble_training_table.py
import os, sys, argparse
import pandas as pd
import numpy as np

IST = "Asia/Kolkata"

def load_minutes(path: str) -> pd.DataFrame:
    """Parquet saved in UTC; convert to IST for window logic."""
    df = pd.read_parquet(path)
    df["ts_ist"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(IST)
    df = df.set_index("ts_ist").sort_index()
    return df[["open","high","low","close"]]

def load_vix(path: str) -> pd.DataFrame:
    vix = pd.read_parquet(path)
    # ensure tz-aware midnight timestamps in IST
    vix["date"] = (
        pd.to_datetime(vix["date"], errors="coerce")
          .dt.tz_localize(IST)     # add IST tz
          .dt.normalize()          # set time to 00:00:00
    )
    vix = vix.set_index("date").sort_index()
    return vix[["vix_close"]]

def snap(df_idx_ist: pd.DataFrame, date_ist, hh: int, mm: int):
    """Latest bar <= hh:mm IST for the given IST date."""
    start = pd.Timestamp(date_ist, tz=IST)
    cutoff = start + pd.Timedelta(hours=hh, minutes=mm)
    window = df_idx_ist.loc[start:cutoff]
    if window.empty:
        return None
    return window.iloc[-1]

def features_for_day(df: pd.DataFrame, vix: pd.DataFrame, day) -> dict | None:
    # two windows available by 15:28: 09:15–09:30 and 15:00–15:28
    o1 = pd.Timestamp(day, tz=IST) + pd.Timedelta(hours=9, minutes=15)
    o2 = pd.Timestamp(day, tz=IST) + pd.Timedelta(hours=9, minutes=30)
    c1 = pd.Timestamp(day, tz=IST) + pd.Timedelta(hours=15, minutes=0)
    c2 = pd.Timestamp(day, tz=IST) + pd.Timedelta(hours=15, minutes=28)

    w_open  = df.loc[o1:o2]
    w_close = df.loc[c1:c2]
    if w_open.empty or w_close.empty:
        return None

    f = {}
    # basic stats by window (bps)
    for w, p in [(w_open, "open15"), (w_close, "late28")]:
        f[f"{p}_ret"]          = (w["close"].iloc[-1] / w["open"].iloc[0]) - 1.0
        f[f"{p}_hl_range_bps"] = (w["high"].max() / w["low"].min() - 1.0) * 1e4
        f[f"{p}_mom_bps"]      = (w["close"].iloc[-1] / w["close"].iloc[0] - 1.0) * 1e4
        f[f"{p}_vol_bp"]       = (w["close"].pct_change().std() or 0.0) * 1e4

    # VIX (use yesterday as known at 15:28; also include today's if present)
    d0 = pd.Timestamp(day, tz=IST).normalize()
    vix_t   = vix["vix_close"].reindex([d0]).iloc[0] if d0 in vix.index else np.nan
    vix_tm1 = vix["vix_close"].shift(1).reindex([d0]).iloc[0] if d0 in vix.index else np.nan
    f["vix_close_t"]   = float(vix_t)   if pd.notna(vix_t)   else np.nan
    f["vix_close_t_1"] = float(vix_tm1) if pd.notna(vix_tm1) else np.nan
    f["vix_delta"]     = (f["vix_close_t"] - f["vix_close_t_1"]) if (pd.notna(f["vix_close_t"]) and pd.notna(f["vix_close_t_1"])) else np.nan

    s1528 = snap(df, day, 15, 28)
    if s1528 is None:
        return None
    f["px_1528"] = float(s1528["close"])
    return f

def label_for_next_morning(df: pd.DataFrame, day) -> dict | None:
    s1528 = snap(df, day, 15, 28)
    if s1528 is None:
        return None
    next_day = (pd.Timestamp(day) + pd.Timedelta(days=1)).date()
    s0921 = snap(df, next_day, 9, 21)
    if s0921 is None:
        return None
    px_t  = float(s1528["close"])
    px_t1 = float(s0921["close"])
    return {"overnight_ret": (px_t1 / px_t) - 1.0, "px_0921": px_t1}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-min", type=str, default="data/raw/nifty_1m.parquet")
    ap.add_argument("--in-vix", type=str, default="data/raw/vix_eod.parquet")
    ap.add_argument("--out", type=str, default="data/processed/overnight_dataset.parquet")
    args = ap.parse_args()

    if not os.path.exists(args.in_min):
        print(f"Missing minute file: {args.in_min}", file=sys.stderr); sys.exit(2)
    if not os.path.exists(args.in_vix):
        print(f"Missing VIX file: {args.in_vix}", file=sys.stderr); sys.exit(2)

    mins = load_minutes(args.in_min)
    vix  = load_vix(args.in_vix)

    days = pd.date_range(mins.index.date.min(), mins.index.date.max(), freq="D", tz=IST)
    rows = []
    for d in days:
        f = features_for_day(mins, vix, d.date())
        if f is None:
            continue
        lab = label_for_next_morning(mins, d.date())
        if lab is None:
            continue
        f.update(lab)
        f["date"] = pd.Timestamp(d.date())
        rows.append(f)

    if not rows:
        print("No rows assembled (check your minute data covers 09:15–15:28 and next-day 09:21).", file=sys.stderr)
        sys.exit(2)

    out = pd.DataFrame(rows).sort_values("date")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_parquet(args.out, index=False)
    print(f"Saved dataset → {args.out} (rows={len(out)})")

if __name__ == "__main__":
    main()
