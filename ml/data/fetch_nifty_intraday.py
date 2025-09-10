# ml/data/fetch_nifty_intraday.py
# Generate mock 1m NIFTY bars for many days → data/vendor/nifty_1m/*.csv
import argparse, os, math, random
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
from pathlib import Path

IST = ZoneInfo("Asia/Kolkata")

OPEN_T  = time(9, 15)
CLOSE_T = time(15, 30)

def is_weekday(d: date) -> bool:
    return d.weekday() < 5  # Mon=0..Fri=4

def minutes_between(start: datetime, end: datetime) -> list[datetime]:
    ts = []
    t = start
    while t <= end:
        ts.append(t)
        t += timedelta(minutes=1)
    return ts

def make_day(day: date, px0: float, rng: np.random.Generator) -> pd.DataFrame:
    start = datetime.combine(day, OPEN_T, tzinfo=IST)
    end   = datetime.combine(day, CLOSE_T, tzinfo=IST)
    idx   = minutes_between(start, end)

    # simple random-walk in bps (a bit of intraday mean reversion)
    n  = len(idx)
    dr = rng.normal(loc=0.0, scale=0.0009, size=n)  # ~9 bps min-to-min stdev
    # slight trend/noise
    drift = rng.normal(0, 0.00002)  # ~2 bps per minute drift
    dr = dr + drift

    close = [px0]
    for i in range(1, n):
        close.append(close[-1] * (1.0 + dr[i]))

    close = np.array(close)
    high  = close * (1.0 + np.abs(rng.normal(0.0007, 0.0002, n)))
    low   = close * (1.0 - np.abs(rng.normal(0.0007, 0.0002, n)))
    openp = np.concatenate([[px0], close[:-1]])

    df = pd.DataFrame({
        "datetime": idx,
        "open":  openp,
        "high":  np.maximum(high, np.maximum(openp, close)),
        "low":   np.minimum(low,  np.minimum(openp, close)),
        "close": close,
        "volume": rng.integers(2_00_000, 12_00_000, size=n)  # mock vols
    })
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/vendor/nifty_1m",
                    help="Directory for per-day CSVs")
    ap.add_argument("--start", type=str, default=None,
                    help="Start date YYYY-MM-DD (default: N trading days back)")
    ap.add_argument("--days", type=int, default=60,
                    help="Number of trading days to synthesize")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)

    # pick start date
    if args.start:
        cur = datetime.strptime(args.start, "%Y-%m-%d").date()
    else:
        # choose `days` weekdays back from today
        cur = datetime.now(IST).date()
        back = 0
        while back < args.days:
            cur = cur - timedelta(days=1)
            if is_weekday(cur):
                back += 1

    # starting level ~ 24,500–26,500
    px = float(rng.normal(25_500, 800))
    written = 0

    d = cur
    while written < args.days:
        if not is_weekday(d):
            d += timedelta(days=1)
            continue

        out_csv = out_dir / f"nifty_1m_{d.isoformat()}.csv"
        if out_csv.exists():
            print(f"Skip (exists): {out_csv}")
        else:
            df = make_day(d, px, rng)
            # carry forward end price as next day's start anchor
            px = float(df["close"].iloc[-1])
            df.to_csv(out_csv, index=False)
            print(f"Wrote {out_csv} (rows={len(df)})")
        written += 1
        d += timedelta(days=1)

    print(f"Done. Generated {written} trading-day CSVs in {out_dir}")

if __name__ == "__main__":
    main()
