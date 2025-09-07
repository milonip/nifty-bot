# --- ensure vendored packages are importable ---
import os, sys, time
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
VENDOR = os.path.join(ROOT, "vendor")
if os.path.isdir(VENDOR) and VENDOR not in sys.path:
    sys.path.insert(0, VENDOR)
# ----------------------------------------------

from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import pyotp
from dotenv import load_dotenv

load_dotenv(override=True)

# Use vendored SmartApi (your /vendor shows 'SmartApi/')
from SmartApi import SmartConnect  # type: ignore


class LiveOrderNotAllowed(Exception):
    pass


def place_order(*args, **kwargs):
    raise LiveOrderNotAllowed("LIVE ORDERS ARE DISALLOWED. PAPER MODE ONLY.")


# ---- Env & session cache ----
_API_KEY = os.getenv("SMARTAPI_API_KEY")
_CLIENT_CODE = os.getenv("SMARTAPI_CLIENT_CODE")
_PIN = os.getenv("SMARTAPI_PIN")  # behaves like password for this flow
_TOTP_SECRET = os.getenv("SMARTAPI_TOTP_SECRET")

_smart: Optional[SmartConnect] = None
_session_started_at: Optional[float] = None
_SESSION_TTL_SEC = 40 * 60  # refresh session roughly every 40 min


def _ensure_env() -> Tuple[bool, str]:
    missing = [
        k
        for k, v in {
            "SMARTAPI_API_KEY": _API_KEY,
            "SMARTAPI_CLIENT_CODE": _CLIENT_CODE,
            "SMARTAPI_PIN": _PIN,
            "SMARTAPI_TOTP_SECRET": _TOTP_SECRET,
        }.items()
        if not v
    ]
    if missing:
        return False, f"Missing env: {', '.join(missing)}"
    return True, "ok"


def _try_generate_session(s: SmartConnect, client: str, pin: str, totp: str) -> Tuple[bool, str]:
    """
    Try common SmartApi method signatures; return (ok, detail).
    """
    # 1) Positional (newer builds)
    try:
        s.generateSession(client, pin, totp)
        return True, "generateSession(client, pin, totp) ok"
    except TypeError:
        pass
    except Exception as e:
        return False, f"generateSession(client,pin,totp) error: {e}"

    # 2) Named (camelCase)
    try:
        s.generateSession(clientCode=client, password=pin, totp=totp)
        return True, "generateSession(clientCode=..., password=..., totp=...) ok"
    except TypeError:
        pass
    except Exception as e:
        return False, f"generateSession(named camelCase) error: {e}"

    # 3) Named (lowercase clientcode)
    try:
        s.generateSession(clientcode=client, password=pin, totp=totp)  # some builds
        return True, "generateSession(clientcode=..., password=..., totp=...) ok"
    except Exception as e:
        return False, f"generateSession(named lowercase) error: {e}"


def _ensure_session() -> Tuple[Optional[SmartConnect], Dict[str, Any]]:
    """
    Returns (smart, meta). If smart is None, meta['error'] explains why.
    """
    global _smart, _session_started_at
    ok_env, msg = _ensure_env()
    if not ok_env:
        return None, {"where": "env", "error": msg}

    now = time.time()
    if _smart is not None and _session_started_at and (now - _session_started_at) < _SESSION_TTL_SEC:
        return _smart, {"where": "cached", "detail": "session reused"}

    try:
        s = SmartConnect(api_key=_API_KEY)  # type: ignore
    except Exception as e:
        return None, {"where": "SmartConnect", "error": str(e)}

    # TOTP now
    try:
        totp = pyotp.TOTP(_TOTP_SECRET).now()
    except Exception as e:
        return None, {"where": "TOTP", "error": f"TOTP gen failed: {e}"}

    ok, detail = _try_generate_session(s, _CLIENT_CODE, _PIN, totp)
    if not ok:
        return None, {"where": "generateSession", "error": detail}

    _smart = s
    _session_started_at = now
    return _smart, {"where": "login", "detail": detail}


# simple resolver (index tokens)
_INDEX_MAP = {
    "NSE:NIFTY": {"exchange": "NSE", "tradingsymbol": "NIFTY 50", "symboltoken": "26000"},
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


async def _debug_login_once() -> Tuple[bool, Dict[str, Any]]:
    s, meta = _ensure_session()
    if s is None:
        return False, meta
    return True, meta


async def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Return structured JSON; never raise to FastAPI.
    On errors, returns {"ok": False, "error": "...", "where": "..."} with context.
    """
    spec, err = _resolve(symbol)
    if err:
        return {"ok": False, "where": "resolve", "error": err, "symbol": symbol}

    s, meta = _ensure_session()
    if s is None:
        meta.update({"ok": False, "symbol": symbol})
        return meta

    out: Dict[str, Any] = {
        "ok": True,
        "symbol": symbol,
        "exchange": spec["exchange"],
        "tradingsymbol": spec["tradingsymbol"],
        "token": spec["symboltoken"],
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

    # 1) LTP (works across builds)
    try:
        ltp_resp = s.ltpData(spec["exchange"], spec["tradingsymbol"], spec["symboltoken"])
        out["raw_ltp"] = ltp_resp
        out["ltp"] = (ltp_resp.get("data") or {}).get("ltp")
    except Exception as e:
        out["ok"] = False
        out["where"] = "ltpData"
        out["error"] = str(e)
        return out

    # 2) Try to fetch depth (best bid/ask) with whatever method this build exposes
    try_methods = [
        "getMarketQuote",
        "marketData",
        "getMarketData",
        "getQuote",
        "quote",  # older builds
    ]

    q = None
    for mname in try_methods:
        try:
            m = getattr(s, mname)
        except AttributeError:
            continue

        # Some builds expect a dict payload; others expect positional args
        # Try dict first, then fallback to positional.
        try:
            q = m({
                "exchange": spec["exchange"],
                "tradingsymbol": spec["tradingsymbol"],
                "symboltoken": spec["symboltoken"],
            })
            break
        except TypeError:
            try:
                q = m(spec["exchange"], spec["tradingsymbol"], spec["symboltoken"])
                break
            except Exception as e:
                out[f"{mname}_error"] = str(e)
        except Exception as e:
            out[f"{mname}_error"] = str(e)

    if q:
        out["raw_quote"] = q
        depth = ((q.get("data") or {}).get("depth") or {})
        buy = depth.get("buy", [])
        sell = depth.get("sell", [])
        bid = buy[0].get("price") if buy else None
        ask = sell[0].get("price") if sell else None
        out["bid"] = bid
        out["ask"] = ask
        out["mid"] = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None

    out["fresh"] = out.get("ltp") is not None
    return out
