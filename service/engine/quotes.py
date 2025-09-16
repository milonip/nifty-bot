# service/engine/quotes.py
from __future__ import annotations

# --- ensure vendored packages are importable ---
import os, sys, time, asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import pyotp
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
VENDOR = os.path.join(ROOT, "vendor")
if os.path.isdir(VENDOR) and VENDOR not in sys.path:
    sys.path.insert(0, VENDOR)
# ----------------------------------------------

# Vendored SmartApi (folder name is SmartApi)
from SmartApi import SmartConnect  # type: ignore

load_dotenv(override=True)

class LiveOrderNotAllowed(Exception):
    pass

def place_order(*args, **kwargs):
    raise LiveOrderNotAllowed("LIVE ORDERS ARE DISALLOWED. PAPER MODE ONLY.")

# ---- Env & session cache ----
_API_KEY      = os.getenv("SMARTAPI_API_KEY")
_CLIENT_CODE  = os.getenv("SMARTAPI_CLIENT_CODE")
_PIN          = os.getenv("SMARTAPI_PIN")            # SmartAPI password/PIN
_TOTP_SECRET  = os.getenv("SMARTAPI_TOTP_SECRET")

_smart: Optional[SmartConnect] = None
_session_started_at: Optional[float] = None
_SESSION_TTL_SEC = 40 * 60  # refresh every ~40 minutes

# Serialize ALL access to SmartAPI to avoid thread errors
_API_LOCK = asyncio.Lock()

def _ensure_env() -> Tuple[bool, str]:
    missing = [k for k, v in {
        "SMARTAPI_API_KEY": _API_KEY,
        "SMARTAPI_CLIENT_CODE": _CLIENT_CODE,
        "SMARTAPI_PIN": _PIN,
        "SMARTAPI_TOTP_SECRET": _TOTP_SECRET,
    }.items() if not v]
    if missing:
        return False, f"Missing env: {', '.join(missing)}"
    return True, "ok"

def _try_generate_session(s: SmartConnect, client: str, pin: str, totp: str) -> Tuple[bool, str]:
    # positional
    try:
        s.generateSession(client, pin, totp)
        return True, "generateSession(client,pin,totp) ok"
    except TypeError:
        pass
    except Exception as e:
        return False, f"generateSession(client,pin,totp) error: {e}"

    # named camelCase
    try:
        s.generateSession(clientCode=client, password=pin, totp=totp)
        return True, "generateSession(clientCode=...,password=...,totp=...) ok"
    except TypeError:
        pass
    except Exception as e:
        return False, f"generateSession(named camelCase) error: {e}"

    # lowercase variant
    try:
        s.generateSession(clientcode=client, password=pin, totp=totp)
        return True, "generateSession(clientcode=...,password=...,totp=...) ok"
    except Exception as e:
        return False, f"generateSession(named lowercase) error: {e}"

def _new_session() -> Tuple[Optional[SmartConnect], Dict[str, Any]]:
    ok_env, msg = _ensure_env()
    if not ok_env:
        return None, {"where": "env", "error": msg}
    try:
        s = SmartConnect(api_key=_API_KEY)  # type: ignore
    except Exception as e:
        return None, {"where": "SmartConnect", "error": str(e)}
    try:
        totp = pyotp.TOTP(_TOTP_SECRET).now()
    except Exception as e:
        return None, {"where": "TOTP", "error": f"TOTP gen failed: {e}"}
    ok, detail = _try_generate_session(s, _CLIENT_CODE, _PIN, totp)
    if not ok:
        return None, {"where": "generateSession", "error": detail}
    return s, {"where": "login", "detail": detail}

def _ensure_session(force_new: bool = False) -> Tuple[Optional[SmartConnect], Dict[str, Any]]:
    global _smart, _session_started_at
    ok_env, msg = _ensure_env()
    if not ok_env:
        return None, {"where": "env", "error": msg}

    now = time.time()
    if not force_new and _smart is not None and _session_started_at and (now - _session_started_at) < _SESSION_TTL_SEC:
        return _smart, {"where": "cached", "detail": "session reused"}

    s, meta = _new_session()
    if s is None:
        return None, meta
    _smart = s
    _session_started_at = now
    return _smart, meta

# Simple resolver for indices; explicit triplet also supported
_INDEX_MAP = {
    "NSE:NIFTY":     {"exchange": "NSE", "tradingsymbol": "NIFTY 50",   "symboltoken": "26000"},
    "NSE:BANKNIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY BANK", "symboltoken": "26009"},
}

def _resolve(symbol: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    key = symbol.strip().upper().replace(" ", "")
    if key in ("NSE:NIFTY50", "NSE:NIFTY"):
        return _INDEX_MAP["NSE:NIFTY"], None
    if key in ("NSE:NIFTYBANK", "NSE:BANKNIFTY"):
        return _INDEX_MAP["NSE:BANKNIFTY"], None
    if "|" in symbol:
        try:
            ex, ts, tok = symbol.split("|", 2)
            return {"exchange": ex, "tradingsymbol": ts, "symboltoken": tok}, None
        except Exception:
            return None, "Explicit form must be 'EXCHANGE|TRADINGSYMBOL|TOKEN'"
    return None, f"Unknown symbol '{symbol}'. Try 'NSE:NIFTY' / 'NSE:BANKNIFTY' or 'EX|TS|TOKEN'."

def _looks_like_thread_or_session_error(e: Exception) -> bool:
    m = str(e).lower()
    return ("threads can only be started once" in m) or ("session" in m and "expired" in m) or ("token" in m and "invalid" in m)

async def _fetch_with(s: SmartConnect, spec: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": True,
        "symbol": f"{spec['exchange']}:{spec['tradingsymbol']}",
        "exchange": spec["exchange"],
        "tradingsymbol": spec["tradingsymbol"],
        "token": spec["symboltoken"],
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }
    # LTP
    ltp_resp = s.ltpData(spec["exchange"], spec["tradingsymbol"], spec["symboltoken"])
    out["raw_ltp"] = ltp_resp
    out["ltp"] = (ltp_resp.get("data") or {}).get("ltp")
    # Depth
    try:
        q = s.quote(spec["exchange"], spec["tradingsymbol"], spec["symboltoken"])
        out["raw_quote"] = q
        depth = ((q.get("data") or {}).get("depth") or {})
        buy = depth.get("buy", [])
        sell = depth.get("sell", [])
        bid = buy[0].get("price") if buy else None
        ask = sell[0].get("price") if sell else None
        out["bid"] = bid
        out["ask"] = ask
        out["mid"] = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None
    except Exception as e:
        out["raw_quote_error"] = str(e)
    out["fresh"] = out.get("ltp") is not None
    return out

async def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Serialized + retrying quotes:
      - serialize on _API_LOCK
      - try once on cached/new session
      - on thread/session error, force a new session and retry once
      - NEVER raise; always return dict with ok/error
    """
    spec, err = _resolve(symbol)
    if err:
        return {"ok": False, "where": "resolve", "error": err, "symbol": symbol}

    async with _API_LOCK:
        # Attempt 1 (cached/new)
        s, meta = _ensure_session(force_new=False)
        if s is None:
            meta.update({"ok": False, "symbol": symbol})
            return meta
        try:
            return await _fetch_with(s, spec)
        except Exception as e1:
            if _looks_like_thread_or_session_error(e1):
                # Reset and retry once
                global _smart, _session_started_at
                _smart = None
                _session_started_at = None
                s2, meta2 = _ensure_session(force_new=True)
                if s2 is None:
                    meta2.update({"ok": False, "symbol": symbol})
                    return meta2
                try:
                    return await _fetch_with(s2, spec)
                except Exception as e2:
                    return {"ok": False, "where": "quote-retry", "error": f"{e1} | retry: {e2}"}
            # Non-session error
            return {"ok": False, "where": "quote", "error": str(e1)}
