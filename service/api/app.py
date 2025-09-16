# service/api/app.py
# --- ensure vendored packages are importable ---
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
VENDOR = os.path.join(ROOT, "vendor")
if VENDOR not in sys.path:
    sys.path.insert(0, VENDOR)
# ----------------------------------------------

from fastapi import FastAPI, status, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from service.engine.positions import (
    init_db,
    get_status,
    get_history,
    paper_reset,
    paper_buy_at_1528,
    paper_sell_at_0921,
    build_atm_legs_1528,
    paper_option_px_at_0921,   # needed by /sell route (explicit exit value)
)
from service.engine.selector import predict as ml_predict
from service.engine.quotes import get_quote
from service.engine.scheduler import start_scheduler, get_next_runs_ist

load_dotenv(override=True)

app = FastAPI(title="NIFTY Options ML – PAPER", version="0.2.0")

# static + templates
STATIC_DIR = os.path.join(ROOT, "service", "api", "static")
TPL_DIR    = os.path.join(ROOT, "service", "api", "templates")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TPL_DIR)


@app.on_event("startup")
async def _startup():
    # init schema + seed funds + clear position row if missing
    await init_db()
    # start the cron scheduler (idempotent)
    start_scheduler(app)


# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---------- Meta ----------
@app.get("/api", tags=["meta"])
async def hello():
    return {"hello": "nifty-bot", "mode": "PAPER", "version": "0.2.0"}


# ---------- Paper engine ----------
@app.get("/status", tags=["paper"])
async def status_endpoint():
    s = await get_status()
    s["next_jobs_IST"] = get_next_runs_ist()
    return s

@app.get("/trade-history", tags=["paper"])
async def trade_history():
    return {"items": await get_history()}

@app.post("/paper/buy", tags=["paper"])
async def paper_buy():
    # get latest prediction + index LTP for ATM
    ml = ml_predict()
    q = await get_quote("NSE:NIFTY")
    ltp = q.get("ltp")
    if not ltp:
        return JSONResponse({"ok": False, "error": "no NIFTY LTP"}, 200)
    atm = int(round(ltp / 50) * 50)
    res = await paper_buy_at_1528(direction=ml.get("direction",""), atm=atm)
    code = status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_400_BAD_REQUEST
    return JSONResponse(res, code)

@app.post("/paper/sell", tags=["paper"])
async def paper_sell():
    res = await paper_sell_at_0921()
    code = status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_400_BAD_REQUEST
    return JSONResponse(res, code)

@app.post("/paper/reset", tags=["paper"])
async def paper_reset_endpoint():
    res = await paper_reset()
    return JSONResponse(res, status_code=status.HTTP_200_OK)


# ---------- ML / Quotes ----------
@app.get("/prediction", tags=["ml"])
async def prediction():
    """
    Uses latest features row + trained models.
    Also suggests simple strikes using live NIFTY LTP (ATM ± 1 step).
    """
    try:
        ml = ml_predict()
    except Exception as e:
        return {"error": f"model/feature error: {e}"}

    # live NIFTY LTP for ATM strike rounding
    q = await get_quote("NSE:NIFTY")
    ltp = q.get("ltp")
    step = 50  # NIFTY strike step
    atm = int(round((ltp or 0) / step) * step) if ltp else None

    # naive suggestions
    if atm:
        if ml["direction"] == "UP":
            ml["suggested_strikes"] = [f"NIFTY {atm} CE", f"NIFTY {atm+step} CE"]
        elif ml["direction"] == "DOWN":
            ml["suggested_strikes"] = [f"NIFTY {atm} PE", f"NIFTY {atm-step} PE"]
        else:
            ml["suggested_strikes"] = [f"NIFTY {atm} CE", f"NIFTY {atm} PE"]

    ml["ltp"] = ltp
    return ml


@app.get("/quotes", tags=["quotes"])
async def quotes(symbol: str = "NSE:NIFTY"):
    return await get_quote(symbol)
