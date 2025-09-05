"""
Quotes client (Broker: SmartAPI for quotes only).
- Absolutely NO order placement here (PAPER mode only).
- Any order-like function must raise a hard exception.
- Implement staleness checks; return mid if bid/ask both present; log symbol/ltp/bid/ask/ts.
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any

class LiveOrderNotAllowed(Exception):
    pass

def place_order(*args, **kwargs):  # guard against accidental use
    raise LiveOrderNotAllowed("LIVE ORDERS ARE DISALLOWED. PAPER MODE ONLY.")

async def get_quote(symbol: str) -> Dict[str, Any]:
    """
    TODO: Implement SmartAPI LTP + bid/ask fetch.
    Return shape:
      {
        "symbol": symbol, "ltp": float, "bid": Optional[float], "ask": Optional[float],
        "mid": Optional[float], "ts_utc": iso, "fresh": bool
      }
    """
    now = datetime.now(timezone.utc).isoformat()
    return {"symbol": symbol, "ltp": None, "bid": None, "ask": None, "mid": None, "ts_utc": now, "fresh": False}
