# ml/data/fetch_vix_eod.py
import sys, os, io, time, argparse, datetime as dt
from typing import List
import pandas as pd
import requests

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
}

def month_range(start: dt.date, end: dt.date) -> List[dt.date]:
    cur = dt.date(start.year, start.month, 1)
    out = []
    while cur <= end:
        out.append(cur)
        y, m = cur.year + (cur.month == 12), (cur.month % 12) + 1
        cur = dt.date(y, m, 1)
    return out

def fetch_nse_month(year: int, month: int) -> pd.DataFrame | None:
    url = f"https://archives.nseindia.com/content/vix/hist/indiahisvolidx_{year:04d}{month:02d}.csv"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        return None
    try:
        df = pd.read_csv(io.BytesIO(r.content))
    except Exception:
        return None
    cols = [c.strip().lower() for c in df.columns]
    if "date" not in cols:
        return None
    df.columns = cols
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.date
    if "close" not in df.columns:
        return None
    df = df.rename(columns={"close": "vix_close"})
    return df[["date", "vix_close"]].dropna()

def fetch_yahoo_range(start: dt.date, end: dt.date) -> pd.DataFrame:
    """
    Fallback: Yahoo Finance ^INDIAVIX daily.
    Ensures a DataFrame with columns ['date','vix_close'].
    """
    try:
        import yfinance as yf
    except Exception:
        print("yfinance not installed; installing temporarily...", file=sys.stderr)
        import subprocess, sys as _sys
        subprocess.check_call([_sys.executable, "-m", "pip", "install", "--no-cache-dir", "yfinance"])
        import yfinance as yf  # type: ignore

    # Use history() (more predictable) and then normalize column names
    tk = yf.Ticker("^INDIAVIX")
    # pad end by +2 days; yfinance end is effectively exclusive
    hist = tk.history(start=start, end=end + dt.timedelta(days=2), auto_adjust=False)
    if hist is None or hist.empty:
        raise RuntimeError("Yahoo ^INDIAVIX returned no rows")

    # Reset index to get a 'Date' column, then normalize to 'date'
    df = hist.reset_index().copy()
    # The column may be 'Date' or already datetime index name None
    # After reset_index, ensure a 'date' column exists
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    elif "date" not in df.columns:
        # if index was unnamed, it becomes a column named 'index'
        if "index" in df.columns:
            df = df.rename(columns={"index": "date"})
        else:
            raise RuntimeError(f"Unexpected Yahoo columns: {df.columns.tolist()}")

    # Ensure datetime -> date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Close column can be 'Close' or 'Adj Close' depending on adjustments; prefer 'Close'
    price_col = "Close" if "Close" in df.columns else ("Adj Close" if "Adj Close" in df.columns else None)
    if price_col is None:
        raise RuntimeError(f"Yahoo data missing Close columns: {df.columns.tolist()}")

    df = df.rename(columns={price_col: "vix_close"})
    return df[["date", "vix_close"]].dropna()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="2015-01-01")
    ap.add_argument("--end", type=str, default=None)
    ap.add_argument("--out", type=str, default="data/raw/vix_eod.parquet")
    ap.add_argument("--prefer", type=str, choices=["nse", "yahoo", "auto"], default="auto",
                    help="Data source preference: 'nse' (archives), 'yahoo', or 'auto' (try NSE then Yahoo).")
    args = ap.parse_args()

    start = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    end = dt.date.today() if not args.end else dt.datetime.strptime(args.end, "%Y-%m-%d").date()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    if args.prefer == "yahoo":
        vix = fetch_yahoo_range(start, end)
        vix = vix.drop_duplicates("date").sort_values("date")
        vix.to_parquet(args.out, index=False)
        print(f"Saved VIX EOD (Yahoo) → {args.out} ({len(vix)} rows)")
        return

    months = month_range(start, end)
    frames_nse: list[pd.DataFrame] = []
    missing_months: list[dt.date] = []

    for m0 in months:
        df = fetch_nse_month(m0.year, m0.month)
        if df is None or df.empty:
            missing_months.append(m0)
            print(f"[warn] {m0:%Y-%m}: NSE archive missing; will backfill from Yahoo", file=sys.stderr)
        else:
            frames_nse.append(df)
        time.sleep(0.25)

    if args.prefer == "nse":
        if not frames_nse:
            print("No NSE VIX data fetched; archives may be unavailable for this range.", file=sys.stderr)
            sys.exit(2)
        vix = pd.concat(frames_nse, ignore_index=True).drop_duplicates("date").sort_values("date")
        vix.to_parquet(args.out, index=False)
        print(f"Saved VIX EOD (NSE archives) → {args.out} ({len(vix)} rows)")
        return

    # AUTO: backfill missing with Yahoo
    yahoo_needed = (not frames_nse) or bool(missing_months)
    if yahoo_needed:
        ydf = fetch_yahoo_range(start, end)
    else:
        ydf = pd.DataFrame(columns=["date", "vix_close"])

    parts = []
    if frames_nse:
        parts.append(pd.concat(frames_nse, ignore_index=True))
    if not ydf.empty:
        parts.append(ydf)

    if not parts:
        print("No VIX data fetched (NSE+Yahoo both empty).", file=sys.stderr)
        sys.exit(2)

    vix = pd.concat(parts, ignore_index=True).drop_duplicates("date", keep="last").sort_values("date")
    vix.to_parquet(args.out, index=False)
    print(f"Saved VIX EOD (auto, NSE+Yahoo) → {args.out} ({len(vix)} rows)")

if __name__ == "__main__":
    main()
