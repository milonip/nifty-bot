# scripts/update_instruments.py
import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEST = DATA_DIR / "angel_instruments.json"
URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

def _last_thursday_of_month(dt: datetime) -> datetime:
    nxt = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
    last = nxt - timedelta(days=1)
    while last.weekday() != 3:  # Thu=3
        last -= timedelta(days=1)
    return last

def _current_month_prefix() -> str:
    return _last_thursday_of_month(datetime.now(tz=IST)).strftime("%Y-%m")  # YYYY-MM

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"🔽 Downloading Angel instruments JSON → {DEST}")
    req = Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as r:
        payload = r.read()

    # Write
    DEST.write_bytes(payload)
    print(f"✅ Saved {DEST} ({DEST.stat().st_size/1024/1024:.2f} MB)")

    # Validate & stats
    data = json.loads(DEST.read_text(encoding="utf-8"))
    print(f"📦 Total rows: {len(data):,}")

    # quick stats
    month = _current_month_prefix()
    nifty_opts = [d for d in data
                  if str(d.get("exch_seg","")).upper()=="NFO"
                  and "OPT" in str(d.get("instrumenttype","")).upper()
                  and "NIFTY" in str(d.get("symbol","")).upper()]
    month_nifty_opts = [d for d in nifty_opts if str(d.get("expiry","")).startswith(month)]
    print(f"🧩 NIFTY options in file: {len(nifty_opts):,}")
    print(f"🗓  NIFTY options for {month}: {len(month_nifty_opts):,}")
    if month_nifty_opts[:3]:
        print("🔎 Examples (first 3):")
        for r in month_nifty_opts[:3]:
            print("   ", r.get("symbol"), r.get("token"), r.get("expiry"), r.get("strike"))
    print("Done.")

if __name__ == "__main__":
    main()
