# service/engine/utils.py
import os
import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

IST = timezone(timedelta(hours=5, minutes=30))

ROOT = Path(__file__).resolve().parents[2]  # points to project root
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
LOGS_DIR = ROOT / "logs"

for p in (DATA_DIR, REPORTS_DIR, LOGS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Time ----------
def _utc_now_str() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

def _now_ist() -> datetime:
    return datetime.now(tz=IST)

def _now_ist_str() -> str:
    return _now_ist().strftime("%Y-%m-%d %H:%M:%S")

def _market_window_now_ist() -> Dict[str, Any]:
    """
    Returns whether we are in BUY (15:28 IST) / SQUAREOFF (09:21 IST) windows today.
    Used by scheduler to guard accidental runs.
    """
    now = _now_ist()
    buy_time = now.replace(hour=15, minute=28, second=0, microsecond=0)
    sq_time = now.replace(hour=9, minute=21, second=0, microsecond=0)

    # If it's morning, squareoff is today; if it's afternoon, buy is today.
    return {
        "now": now,
        "is_buy_window": abs((now - buy_time).total_seconds()) < 180,      # ±3 min
        "is_squareoff_window": abs((now - sq_time).total_seconds()) < 180, # ±3 min
        "buy_at": buy_time,
        "squareoff_at": sq_time,
    }

# ---------- Storage helpers ----------
FUNDS_FILE = REPORTS_DIR / "funds.json"
POSITIONS_FILE = REPORTS_DIR / "open_position.json"
TRADES_CSV = REPORTS_DIR / "trades.csv"

def _read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return default

def _write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    tmp.replace(path)

def _append_csv_row(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "ts_ist","action","symbol_ce","symbol_pe",
                "lots_ce","lots_pe","entry_ce","entry_pe",
                "exit_ce","exit_pe","pnl_ce","pnl_pe","pnl_total","note"
            ],
        )
        if new_file:
            w.writeheader()
        w.writerow(row)

# ---------- Domain writes used by positions.py ----------
def _write_funds(balance: float, realized: float, used: float, mtm: Optional[float]) -> None:
    payload = {
        "updated_at_ist": _now_ist_str(),
        "balance": round(float(balance), 2),
        "realized": round(float(realized), 2),
        "used": round(float(used), 2),
        "mtm": None if mtm is None else round(float(mtm), 2),
    }
    _write_json(FUNDS_FILE, payload)

def _write_position_open(position: Dict[str, Any]) -> None:
    position["opened_at_ist"] = _now_ist_str()
    _write_json(POSITIONS_FILE, position)

def _write_position_clear() -> None:
    if POSITIONS_FILE.exists():
        POSITIONS_FILE.unlink()

def _append_trade_row(row: Dict[str, Any]) -> None:
    row["ts_ist"] = _now_ist_str()
    _append_csv_row(TRADES_CSV, row)
