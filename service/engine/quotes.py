from __future__ import annotations

import os
import time
import json
import logging
from typing import Optional, Dict, Any

import requests
import pyotp

log = logging.getLogger("service.quotes")
BASE = "https://apiconnect.angelone.in"

# Cache session (simple)
_SESSION: Dict[str, Any] = {"jwt": None, "expiry": 0.0}

def _now() -> float:
    return time.time()

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v if v is not None else default

def _headers(jwt: Optional[str] = None) -> Dict[str, str]:
    # Angel expects these client headers
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-SourceID": "WEB",
        "X-ClientType": "USER",
        "X-ClientLocalIP": _env("SMARTAPI_LOCAL_IP", "127.0.0.1"),
        "X-ClientPublicIP": _env("SMARTAPI_PUBLIC_IP", "127.0.0.1"),
        "X-MACAddress": _env("SMARTAPI_MAC", "AA:BB:CC:DD:EE:FF"),
        "X-PrivateKey": _env("SMARTAPI_API_KEY", ""),
    }
    if jwt:
        h["Authorization"] = f"Bearer {jwt}"
    return h

def _ensure_session() -> str:
    """Login (TOTP) if our cached JWT is missing/expired."""
    if _SESSION["jwt"] and _SESSION["expiry"] > _now() + 15:
        return _SESSION["jwt"]

    api_key = _env("SMARTAPI_API_KEY")
    client_code = _env("SMARTAPI_CLIENT_CODE")
    pin = _env("SMARTAPI_PIN")
    totp_secret = _env("SMARTAPI_TOTP_SECRET")

    if not (api_key and client_code and pin and totp_secret):
        raise RuntimeError("SMARTAPI_* env vars are not fully set (API_KEY, CLIENT_CODE, PIN, TOTP_SECRET)")

    # TOTP
    otp = pyotp.TOTP(totp_secret).now()
    payload = {
        "clientcode": client_code,
        "password": pin,
        "totp": otp,
    }
    url = f"{BASE}/rest/auth/angelbroking/user/v1/loginByPassword"
    r = requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=10)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"SmartAPI login failed: HTTP {r.status_code} {r.text[:200]}")

    if not data.get("status"):
        raise RuntimeError(f"SmartAPI login failed: {data}")

    jwt = data["data"]["jwtToken"]
    # Session is usually short; refresh proactively in ~10 minutes
    _SESSION["jwt"] = jwt
    _SESSION["expiry"] = _now() + 600.0
    log.info("SmartAPI login OK for %s", client_code)
    return jwt

def _search_scrip(jwt: str, exchange: str, query: str) -> Optional[Dict[str, str]]:
    """
    Best-effort token discovery. Returns first hit that has a symboltoken and tradingsymbol.
    We match 'tradingsymbol' EXACTLY first; otherwise return top hit from the exchange.
    """
    url = f"{BASE}/rest/secure/angelbroking/order/v1/searchScrip"
    # Angel docs show both 'symbol' and 'searchsymbol' in different places; try both server-side.
    # The backend accepts 'searchsymbol'.
    payload = {"exchange": exchange, "searchsymbol": query}
    r = requests.post(url, headers=_headers(jwt), data=json.dumps(payload), timeout=10)
    data = r.json()
    if not data.get("status"):
        log.warning("searchScrip failed: %s", data)
        return None

    items = data.get("data") or []
    if not items:
        return None

    # Prefer exact tradingsymbol match
    for it in items:
        ts = (it.get("tradingsymbol") or it.get("symbol") or "").upper()
        if ts == query.upper():
            return it

    # Otherwise, return the first in the same exchange
    for it in items:
        if (it.get("exchange") or it.get("exch_seg") or "").upper() == exchange.upper():
            return it

    # Fallback to first item
    return items[0]

def _ltp(jwt: str, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
    url = f"{BASE}/rest/secure/angelbroking/order/v1/getLtpData"
    payload = {
        "exchange": exchange,
        "tradingsymbol": tradingsymbol,
        "symboltoken": str(symboltoken),
    }
    r = requests.post(url, headers=_headers(jwt), data=json.dumps(payload), timeout=10)
    data = r.json()
    if data.get("status") and data.get("data"):
        try:
            return float(data["data"]["ltp"])
        except Exception:
            pass
    log.warning("getLtpData failed for %s/%s token=%s -> %s", exchange, tradingsymbol, symboltoken, data)
    return None

def get_quote(symbol: Dict[str, Any]) -> Optional[float]:
    """
    Unified entry used by the rest of the app.
    Expects a dict with at least:
        {'exchange': 'NFO'|'NSE', 'tradingsymbol': '...', 'symboltoken': '...'}
    Returns float LTP or None.
    """
    exchange = (symbol.get("exchange") or symbol.get("exch_seg") or "").upper()
    ts = (symbol.get("tradingsymbol") or symbol.get("symbol") or "").upper()
    token = str(symbol.get("symboltoken") or symbol.get("token") or "").strip()

    if not exchange or not ts:
        log.error("get_quote missing exchange/tradingsymbol: %r", symbol)
        return None

    jwt = _ensure_session()

    # 1) Try direct (what we prefer)
    if token:
        price = _ltp(jwt, exchange, ts, token)
        if price is not None:
            return price

    # 2) Discover token with search and retry
    found = _search_scrip(jwt, exchange, ts)
    if found:
        new_ts = (found.get("tradingsymbol") or found.get("symbol") or ts).upper()
        new_tok = str(found.get("symboltoken") or found.get("token") or token)
        if new_tok:
            price = _ltp(jwt, exchange, new_ts, new_tok)
            if price is not None:
                return price

    return None
