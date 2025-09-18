# service/api/app.py
from __future__ import annotations

import importlib
import pathlib
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

# Scheduler hooks (cron + manual triggers)
from service.engine.scheduler import (
    start_scheduler,
    get_next_runs_ist,
    predict_and_buy_1528,
    squareoff_0921,
)

# -----------------------------------------------------------------------------
# App, static, templates
# -----------------------------------------------------------------------------
ROOT = pathlib.Path(__file__).resolve().parents[2]
load_dotenv(override=True)

app = FastAPI(title="NIFTY Options ML — PAPER", version="0.2.0")

STATIC_DIR = ROOT / "service" / "api" / "static"
TPL_DIR = ROOT / "service" / "api" / "templates"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TPL_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TPL_DIR))

# -----------------------------------------------------------------------------
# Dynamically adapt to whatever functions exist in service.engine.positions
# -----------------------------------------------------------------------------
_positions = importlib.import_module("service.engine.positions")

def _resolve(*candidates):
    for name in candidates:
        fn = getattr(_positions, name, None)
        if callable(fn):
            return fn
    raise ImportError(
        f"Could not find any of {candidates} in service.engine.positions. "
        f"Available: {sorted(n for n in dir(_positions) if not n.startswith('_'))}"
    )

# <-- include 'funds_snapshot' here
get_funds = _resolve("get_funds", "funds", "funds_status", "status", "funds_snapshot")

# -----------------------------------------------------------------------------
# Startup: start APScheduler (guard against double-start on --reload)
# -----------------------------------------------------------------------------
@app.on_event("startup")
def _startup() -> None:
    if getattr(app.state, "scheduler_started", False):
        return
    try:
        start_scheduler(app=app)
        app.state.scheduler_started = True
    except Exception as e:
        # Keep API alive even if scheduler wiring fails
        print(f"[startup] scheduler failed to start: {e}")

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/health")
def api_health():
    return {"ok": True}

@app.get("/api/funds")
def api_funds():
    try:
        data = get_funds()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": f"failed to compute funds: {e}"}, status_code=500)

@app.post("/api/buy")
async def api_buy():
    try:
        res = await predict_and_buy_1528()
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/sell")
async def api_sell():
    try:
        res = await squareoff_0921()
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/jobs")
def api_jobs():
    try:
        return {"next": get_next_runs_ist()}
    except Exception as e:
        return JSONResponse({"error": f"failed to read jobs: {e}"}, status_code=500)
