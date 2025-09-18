# service/engine/instruments.py
"""
Angel One instruments utilities (JSON version).

Download once (or refresh anytime):
  mkdir -p data
  curl -L "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json" \
    -o data/angel_instruments.json
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# ---------- Time / Paths ----------
IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parents[2]
INSTR_JSON = ROOT / "data" / "angel_instruments.json"

# ---------- Model ----------
@dataclass
class Instrument:
    exchange: str           # "NFO", "NSE", ...
    token: str              # "3045"
    tradingsymbol: str      # "NIFTY25SEP25350CE" OR "NIFTY 25 SEP 2025 CE 25350"
    name: str               # "NIFTY"
    instrumenttype: str     # "OPTIDX" / "FUTIDX" / "AMXIDX" / "" ...
    expiry_raw: Optional[str]   # raw expiry string
    expiry_dt: Optional[datetime]  # parsed expiry date (UTC-naive)
    strike: Optional[int]
    lotsize: Optional[int]
    optiontype: Optional[str]  # "CE"/"PE" when detectable

# ---------- Helpers ----------
_RE_MON = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

def _parse_float(x: object) -> Optional[float]:
    try:
        return float(x) if x is not None and f"{x}".strip() != "" else None
    except Exception:
        return None

def _parse_int(x: object) -> Optional[int]:
    try:
        f = _parse_float(x)
        return int(round(f)) if f is not None else None
    except Exception:
        return None

def _strip(s: object) -> str:
    return f"{s}".strip() if s is not None else ""

def _infer_option_type(ts: str, itype: str = ""):
    TS = ts.upper()
    if TS.endswith("CE"): return "CE"
    if TS.endswith("PE"): return "PE"
    IT = (itype or "").upper()
    if "CALL" in IT: return "CE"
    if "PUT" in IT:  return "PE"
    return None

def _try_parse_expiry(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    # Common formats seen in dumps
    fmts = [
        "%Y-%m-%d",
        "%d-%b-%Y", "%d %b %Y",   # 25-Sep-2025 / 25 Sep 2025
        "%d-%B-%Y", "%d %B %Y",   # 25-September-2025
        "%d/%m/%Y", "%m/%d/%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass
    return None

# --- helpers to read symbol text safely from Angel dumps ---
def _ts_of(i):
    """Best-effort tradingsymbol string from Angel rows."""
    for k in ("tradingsymbol", "symbol", "trading_symbol", "ts", "symbolname", "symbol_name"):
        v = getattr(i, k, None)
        if v:
            return str(v)
    parts = [getattr(i, "name", ""), getattr(i, "symbol", ""), getattr(i, "tradingsymbol", "")]
    return " ".join([p for p in parts if p]).strip().upper()


# --- map month from text inside the symbol ---
def _month_key_from_symbol_text(ts: str):
    """Derive YYYY-MM from popular Angel symbol formats."""
    TS = ts.upper()

    # NIFTY<YY><MON>...
    m = re.search(r"NIFTY(?P<yy>\d{2})(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)", TS)
    if m:
        return f"{2000 + int(m.group('yy')):04d}-{_RE_MON[m.group('mon')]:02d}"

    # NIFTY<MON><YY>...
    m = re.search(r"NIFTY(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)(?P<yy>\d{2})", TS)
    if m:
        return f"{2000 + int(m.group('yy')):04d}-{_RE_MON[m.group('mon')]:02d}"

    # DD[- ]?MON[- ]?YYYY anywhere
    m = re.search(r"\b\d{1,2}\s*-?(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)\s*-?(?P<yr>\d{2,4})\b", TS)
    if m:
        yr = int(m.group("yr"))
        if yr < 100:
            yr += 2000
        return f"{yr:04d}-{_RE_MON[m.group('mon')]:02d}"

    return None


def _expiry_from_symbol(ts: str) -> Optional[datetime]:
    """
    Try to derive expiry when Angel doesn't give a date:
    - Monthly OPTIDX often looks like: NIFTY25SEP25350CE  (YY + MON, no day)
      -> interpret as last Thursday of that month.
    - Also supports patterns with explicit day/month/year variants.
    """
    TS = ts.upper()

    # Pattern A: NIFTY25SEP...  (YY + MON, monthly)
    m = re.search(r"NIFTY(?P<yy>\d{2})(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)", TS)
    if m:
        yy = int(m.group("yy"))
        mon = _RE_MON[m.group("mon")]
        year = 2000 + yy
        try:
            # monthly -> last Thursday
            return _last_thursday_of_month(datetime(year, mon, 1))
        except Exception:
            return None

    # Pattern B: DD-MON-YYYY or DD MON YYYY (with or without separators)
    m = re.search(
        r"(?P<day>\d{1,2})\s*-?(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)\s*-?(?P<yy>\d{2,4})",
        TS
    )
    if m:
        day = int(m.group("day"))
        mon = _RE_MON[m.group("mon")]
        yr = int(m.group("yy"))
        if yr < 100:
            yr += 2000
        try:
            return datetime(yr, mon, day)
        except Exception:
            return None

    return None


def _parse_expiry_any(expiry_raw: Optional[str], tradingsymbol: str) -> Optional[datetime]:
    dt = None
    if expiry_raw:
        dt = _try_parse_expiry(expiry_raw)
    if not dt:
        dt = _expiry_from_symbol(tradingsymbol)
    return dt

# ---------- Load instruments ----------
def load_instruments() -> List[Instrument]:
    if not INSTR_JSON.exists():
        raise FileNotFoundError(f"Angel instruments file missing at {INSTR_JSON}")

    with INSTR_JSON.open(encoding="utf-8") as f:
        raw = json.load(f)

    out: List[Instrument] = []
    for row in raw:
        exchange = _strip(row.get("exch_seg"))
        token = _strip(row.get("token"))
        tradingsymbol = _strip(row.get("symbol"))
        name = _strip(row.get("name"))
        instrumenttype = _strip(row.get("instrumenttype"))
        expiry_raw = _strip(row.get("expiry")) or None
        strike = _parse_int(row.get("strike"))
        lotsize = _parse_int(row.get("lotsize"))
        opt = _infer_option_type(tradingsymbol, instrumenttype)
        expiry_dt = _parse_expiry_any(expiry_raw, tradingsymbol)

        out.append(
            Instrument(
                exchange=exchange,
                token=token,
                tradingsymbol=tradingsymbol,
                name=name,
                instrumenttype=instrumenttype,
                expiry_raw=expiry_raw,
                expiry_dt=expiry_dt,
                strike=strike,
                lotsize=lotsize,
                optiontype=opt,
            )
        )
    return out

# ---------- Expiry helpers ----------
def _last_thursday_of_month(dt: datetime) -> datetime:
    nxt = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
    last = nxt - timedelta(days=1)
    while last.weekday() != 3:  # Thu=3
        last -= timedelta(days=1)
    return last

def current_monthly_expiry_ist(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(tz=IST)
    d = _last_thursday_of_month(now)
    return d.strftime("%Y-%m-%d")

# ---------- STRICT NIFTY MONTHLY FILTERS ----------

def _month_key(dt: Optional[datetime]) -> Optional[str]:
    return None if dt is None else dt.strftime("%Y-%m")

def _next_month(d: datetime) -> datetime:
    y, m = d.year, d.month
    if m == 12: return datetime(y+1, 1, 1)
    return datetime(y, m+1, 1)

def _nifty_monthly_pool(instruments, month_key: str):
    """
    NIFTY 50 index options monthly only (strict). Avoids NIFTY NEXT 50, BANKNIFTY, FINNIFTY,
    and sectoral indices that also start with 'NIFTY'.
    """
    BAD_TOKENS = (
        "BANKNIFTY", "FINNIFTY", "MIDCP", "MIDCAP", "SML", "SMALL", "MICRO", "IT ",
        "AUTO", "PHARMA", "PSU", "NXT", "NEXT", "NIFTYNXT", "NIFTY NEXT 50", "NIFTYNXT50",
    )
    pool = []
    for i in instruments:
        if (getattr(i, "exchange", "") or "").upper() != "NFO":
            continue
        itype = (getattr(i, "instrumenttype", "") or "").upper()
        if "OPTIDX" not in itype:
            continue

        ts = _ts_of(i).upper()
        nm = (getattr(i, "name", "") or "").upper()

        is_core_nifty = (
            nm.replace(" ", "") in {"NIFTY", "NIFTY50", "NIFTY-50", "NIFTY_50"}
            or (ts.startswith("NIFTY") and all(tok not in ts for tok in BAD_TOKENS))
        )
        if not is_core_nifty:
            continue

        mk = _month_key(getattr(i, "expiry_dt", None)) or _month_key_from_symbol_text(ts)
        if mk != month_key:
            continue

        pool.append(i)
    return pool



def _month_key_from_symbol(ts: str) -> Optional[str]:
    """
    Return 'YYYY-MM' by reading the month/year from Angel tradingsymbols.
    Handles all of these:
      NIFTY25SEP25350CE      -> 2025-09
      NIFTYSEP25 25350 CE    -> 2025-09
      NIFTY 26-Sep-2025 CE   -> 2025-09
    """
    TS = ts.upper()

    # A) NIFTY<YY><MON>...
    m = re.search(r"NIFTY(?P<yy>\d{2})(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)", TS)
    if m:
        yy = int(m.group("yy"))
        mon = _RE_MON[m.group("mon")]
        year = 2000 + yy
        return f"{year:04d}-{mon:02d}"

    # B) NIFTY<MON><YY>...  (order reversed, also seen in dumps)
    m = re.search(r"NIFTY(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)(?P<yy>\d{2})", TS)
    if m:
        yy = int(m.group("yy"))
        mon = _RE_MON[m.group("mon")]
        year = 2000 + yy
        return f"{year:04d}-{mon:02d}"

    # C) DD[- ]?MON[- ]?YYYY inside the symbol
    m = re.search(
        r"(?P<day>\d{1,2})\s*-?(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)\s*-?(?P<yy>\d{2,4})",
        TS
    )
    if m:
        mon = _RE_MON[m.group("mon")]
        yr = int(m.group("yy"))
        if yr < 100:
            yr += 2000
        return f"{yr:04d}-{mon:02d}"

    return None



def _strike_of(i) -> int | None:
    if getattr(i, "strike", None) is not None:
        try:
            return int(i.strike)
        except Exception:
            pass
    m = re.search(r"(\d{4,6})(?:CE|PE)?\s*$", _ts_of(i))
    return int(m.group(1)) if m else None

def _best_match_option_strict(pool: List[Instrument], opt: str, strike: int) -> Optional[Instrument]:
    opt = opt.upper()
    # CE/PE detection stays robust
    def is_opt(i: Instrument) -> bool:
        t = (i.optiontype or _infer_option_type(i.tradingsymbol, i.instrumenttype) or "").upper()
        return t == opt

    exact = [i for i in pool if is_opt(i) and _strike_of(i) == strike]
    if exact:
        return exact[0]

    near = [i for i in pool if is_opt(i) and (_strike_of(i) is not None)]
    if not near:
        return None
    near.sort(key=lambda i: abs((_strike_of(i) or strike) - strike))
    # only accept within ±150 to avoid silly mismatches
    if abs((_strike_of(near[0]) or strike) - strike) <= 150:
        return near[0]
    return None


# ---------- SmartAPI client (supports both env naming styles) ----------
try:
    from SmartApi import SmartConnect  # type: ignore
except Exception:
    SmartConnect = None  # type: ignore

def _smart_client():
    """
    Supports:
    - SMARTAPI_KEY / SMARTAPI_CLIENT_ID / SMARTAPI_PASSWORD / SMARTAPI_TOTP
    - SMARTAPI_API_KEY / SMARTAPI_CLIENT_CODE / SMARTAPI_PIN / SMARTAPI_TOTP_SECRET
    If SMARTAPI_TOTP absent but SMARTAPI_TOTP_SECRET present -> generate code via pyotp.
    """
    if not SmartConnect:
        return None

    key = os.getenv("SMARTAPI_KEY") or os.getenv("SMARTAPI_API_KEY")
    uid = os.getenv("SMARTAPI_CLIENT_ID") or os.getenv("SMARTAPI_CLIENT_CODE")
    pwd = os.getenv("SMARTAPI_PASSWORD") or os.getenv("SMARTAPI_PIN")
    totp = os.getenv("SMARTAPI_TOTP")
    if not totp:
        secret = os.getenv("SMARTAPI_TOTP_SECRET")
        if secret:
            try:
                import pyotp
                totp = pyotp.TOTP(secret).now()
            except Exception:
                totp = None

    if not (key and uid and pwd):
        return None

    try:
        c = SmartConnect(key)
        if totp:
            c.generateSession(uid, pwd, totp)
        else:
            c.generateSession(uid, pwd)
        return c
    except Exception:
        return None

# ---------- NIFTY spot ----------
def _possible_nifty_index_rows(instruments: Iterable[Instrument]) -> List[Instrument]:
    cands: List[Instrument] = []
    for i in instruments:
        if i.exchange.upper() == "NSE":
            ts = i.tradingsymbol.upper()
            nm = i.name.upper()
            it = i.instrumenttype.upper()
            if ("NIFTY" in ts or "NIFTY" in nm) and it in ("INDEX", "AMXIDX", "FUTIDX", ""):
                if not ts.endswith("-EQ"):
                    cands.append(i)
    return cands

def get_nifty_spot() -> Optional[float]:
    env_ts = os.getenv("NIFTY_SPOT_TRADINGSYMBOL")
    env_token = os.getenv("NIFTY_SPOT_TOKEN")
    env_exch = os.getenv("NIFTY_SPOT_EXCHANGE", "NSE")

    client = _smart_client()
    if not client:
        return None

    if env_ts and env_token:
        try:
            d = client.ltpData(exchange=env_exch, tradingsymbol=env_ts, symboltoken=env_token)
            if d and "data" in d and "ltp" in d["data"]:
                return float(d["data"]["ltp"])
        except Exception:
            pass

    try:
        ins = load_instruments()
        for row in _possible_nifty_index_rows(ins):
            try:
                d = client.ltpData(exchange=row.exchange, tradingsymbol=row.tradingsymbol, symboltoken=row.token)
                if d and "data" in d and "ltp" in d["data"]:
                    ltp = float(d["data"]["ltp"])
                    if ltp > 100:
                        return ltp
            except Exception:
                continue
    except Exception:
        return None
    return None

# ---------- Options picker ----------
def _nearest_50(x: float) -> int:
    return int(round(x / 50.0) * 50)

def _filter_nifty_options_for_month(instruments: Iterable[Instrument], month_key: str) -> List[Instrument]:
    out: List[Instrument] = []
    for i in instruments:
        if i.exchange.upper() != "NFO":
            continue
        if "OPT" not in i.instrumenttype.upper():
            continue
        ts = i.tradingsymbol.upper()
        if "NIFTY" not in ts:
            continue
        # Prefer same-month matches, but include rows with unknown expiry as soft fallback.
        if _month_key(i.expiry_dt) == month_key or i.expiry_dt is None:
            out.append(i)
    return out

def _strike_of(i) -> int | None:
    if getattr(i, "strike", None) is not None:
        try:
            return int(i.strike)
        except Exception:
            pass
    m = re.search(r"(\d{4,6})(?:CE|PE)?\s*$", _ts_of(i))
    return int(m.group(1)) if m else None

def _best_match_option(instruments: List[Instrument], opt: str, strike: int, month_key: str) -> Optional[Instrument]:
    opt = opt.upper()
    strike = int(strike)

    month_pool = _filter_nifty_options_for_month(instruments, month_key)

    def is_opt(i: Instrument) -> bool:
        t = (i.optiontype or _infer_option_type(i.tradingsymbol, i.instrumenttype) or "").upper()
        return t == opt

    # 1) Same-month & exact strike
    exact = [i for i in month_pool if is_opt(i) and _strike_of(i) == strike]
    if exact:
        return exact[0]

    # 2) Same-month & nearest strike within ±100
    near = [i for i in month_pool if is_opt(i) and (s := _strike_of(i)) is not None and abs(s - strike) <= 100]
    if near:
        near.sort(key=lambda i: abs((_strike_of(i) or strike) - strike))
        return near[0]

    # 3) Any expiry & exact strike
    any_exp = [i for i in instruments if is_opt(i) and _strike_of(i) == strike]
    if any_exp:
        return any_exp[0]

    # 4) Any expiry & nearest strike within ±100
    any_near = [i for i in instruments if is_opt(i) and (s := _strike_of(i)) is not None and abs(s - strike) <= 100]
    if any_near:
        any_near.sort(key=lambda i: abs((_strike_of(i) or strike) - strike))
        return any_near[0]

    return None

def pick_monthly_option_symbols(direction: str, offset_points: int = 0):
    """
    Returns (ce_dict, ce_label, pe_dict, pe_label) for NIFTY 50 monthly options.
    Chooses current monthly; if absent tries next month; then month+2.
    Always returns a 4-tuple or raises RuntimeError with diagnostics.
    """
    spot = get_nifty_spot()
    if spot is None:
        raise RuntimeError(
            "Unable to fetch NIFTY spot via SmartAPI. "
            "Check SMARTAPI_* env vars OR set NIFTY_SPOT_TRADINGSYMBOL/NIFTY_SPOT_TOKEN."
        )

    base = int(round(spot / 50.0) * 50) + int(offset_points)

    now = datetime.now(tz=IST)
    month_keys = [
        _month_key(_last_thursday_of_month(now)),
        _month_key(_last_thursday_of_month(_next_month(now))),
        _month_key(_last_thursday_of_month(_next_month(_next_month(now)))),
    ]

    ins = load_instruments()
    diagnostics = {}

    def _pick(pool, opt, strike):
        opt = opt.upper()
        def is_opt(i):
            t = (getattr(i, "optiontype", None) or _infer_option_type(_ts_of(i), getattr(i, "instrumenttype", "")) or "").upper()
            return t == opt

        exact = [i for i in pool if is_opt(i) and _strike_of(i) == strike]
        if exact:
            return exact[0]

        cand = [i for i in pool if is_opt(i) and (_strike_of(i) is not None)]
        if not cand:
            return None

        cand.sort(key=lambda i: abs((_strike_of(i) or strike) - strike))
        return cand[0]  # accept best even if far; dump can be sparse

    for mk in month_keys:
        pool = _nifty_monthly_pool(ins, mk or "")
        diagnostics[mk or "None"] = len(pool)
        if not pool:
            continue

        ce_i = _pick(pool, "CE", base)
        pe_i = _pick(pool, "PE", base)
        if ce_i and pe_i:
            ce_lbl, pe_lbl = _ts_of(ce_i), _ts_of(pe_i)
            ce = {"exchange": ce_i.exchange, "tradingsymbol": ce_lbl, "symboltoken": ce_i.token}
            pe = {"exchange": pe_i.exchange, "tradingsymbol": pe_lbl, "symboltoken": pe_i.token}
            return ce, ce_lbl, pe, pe_lbl

    raise RuntimeError(
        f"Could not find NIFTY 50 monthly OPTIDX near ATM. Pools: {diagnostics}. "
        "Check symbol month parsing & refresh data/angel_instruments.json."
    )



# ---- existing pick_monthly_option_symbols(...) stays as-is above ----

def pick_option_symbols_any_expiry(direction: str, offset_points: int = 0) -> Tuple[Dict, str, Dict, str]:
    """
    Fallback: pick nearest CE/PE across *any* expiry (weekly/monthly), closest to ATM.
    Returns (ce_symbol, ce_label, pe_symbol, pe_label).
    """
    spot = get_nifty_spot()
    if spot is None:
        raise RuntimeError(
            "Unable to fetch NIFTY spot via SmartAPI. "
            "Check SMARTAPI_* env vars OR set NIFTY_SPOT_TRADINGSYMBOL/NIFTY_SPOT_TOKEN."
        )

    base = _nearest_50(spot) + int(offset_points)
    ins = load_instruments()

    def is_opt(i: Instrument, opt: str) -> bool:
        t = (i.optiontype or _infer_option_type(i.tradingsymbol, i.instrumenttype) or "").upper()
        return (
            i.exchange.upper() == "NFO"
            and "OPT" in i.instrumenttype.upper()
            and "NIFTY" in i.tradingsymbol.upper()
            and t == opt.upper()
        )

    def nearest_opt(opt: str) -> Optional[Instrument]:
        cands = [i for i in ins if is_opt(i, opt) and _strike_of(i) is not None]
        if not cands:
            return None
        # choose the one whose strike is closest to base
        cands.sort(key=lambda i: abs((_strike_of(i) or base) - base))
        # soft guard: prefer within ±100 if available
        close = [i for i in cands if abs((_strike_of(i) or base) - base) <= 100]
        return (close[0] if close else cands[0])

    ce_i = nearest_opt("CE")
    pe_i = nearest_opt("PE")
    if not ce_i or not pe_i:
        raise RuntimeError("Could not find any CE/PE contracts near ATM across expiries. Update instruments file.")

    ce = {"exchange": ce_i.exchange, "tradingsymbol": ce_i.tradingsymbol, "symboltoken": ce_i.token}
    pe = {"exchange": pe_i.exchange, "tradingsymbol": pe_i.tradingsymbol, "symboltoken": pe_i.token}
    return ce, ce_i.tradingsymbol, pe, pe_i.tradingsymbol
