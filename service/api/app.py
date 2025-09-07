# --- ensure vendored packages are importable ---
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # repo root
VENDOR = os.path.join(ROOT, "vendor")
if os.path.isdir(VENDOR) and VENDOR not in sys.path:
    sys.path.insert(0, VENDOR)
# ----------------------------------------------

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import sys, pkgutil

from service.engine.positions import (
    init_db,
    get_status,
    paper_buy_stub,
    paper_sell_stub,
    paper_reset,
)
from service.engine.quotes import get_quote
from fastapi import HTTPException


# load environment
load_dotenv(override=True)

# ✅ define app BEFORE using it
app = FastAPI(title="NIFTY Options ML Paper Bot", version="0.1.0")


# --- lifecycle ---
@app.on_event("startup")
async def _startup():
    await init_db()

@app.get("/debug/quotes-login", tags=["debug"])
async def debug_quotes_login():
    # lazy import to avoid circulars
    from service.engine.quotes import _debug_login_once  # type: ignore
    ok, info = await _debug_login_once()
    if ok:
        return {"ok": True, "info": info}
    raise HTTPException(status_code=400, detail=info)

# --- meta endpoints ---
@app.get("/", tags=["meta"])
async def hello():
    return {"hello": "nifty-bot", "mode": "PAPER", "version": "0.1.0"}


@app.get("/sys", tags=["meta"])
def sys_info():
    mods = ["fastapi", "python_dotenv", "sqlalchemy", "pyotp", "smartapi", "SmartApi"]
    return {
        "python": sys.executable,
        "present": {m: bool(pkgutil.find_loader(m)) for m in mods},
    }


# --- paper trading endpoints ---
@app.get("/status", tags=["paper"])
async def status_endpoint():
    return await get_status()


@app.get("/trade-history", tags=["paper"])
async def trade_history():
    # TODO: return recent positions with legs and P&L
    return {"items": [], "todo": "Implement after positions engine"}


@app.post("/paper/buy", tags=["paper"])
async def paper_buy():
    res = await paper_buy_stub()
    return JSONResponse(
        res, status_code=status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_200_OK
    )


@app.post("/paper/sell", tags=["paper"])
async def paper_sell():
    res = await paper_sell_stub()
    return JSONResponse(
        res, status_code=status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_200_OK
    )


@app.post("/paper/reset", tags=["paper"])
async def paper_reset_endpoint():
    res = await paper_reset()
    return JSONResponse(res, status_code=status.HTTP_200_OK)


# --- ML stubs ---
@app.get("/prediction", tags=["ml"])
async def prediction_stub():
    # TODO: plug models + selector
    return {
        "direction": None,
        "confidence": None,
        "suggested_strikes": [],
        "todo": "ML not wired yet",
    }


# --- quotes (only define once) ---
@app.get("/quotes", tags=["quotes"])
async def quotes(symbol: str = "NSE:NIFTY"):
    """
    Read-only quotes tester.
    Accepts:
      - 'NSE:NIFTY' or 'NSE:BANKNIFTY'
      - OR explicit: 'EXCHANGE|TRADINGSYMBOL|TOKEN'
    """
    return await get_quote(symbol)
