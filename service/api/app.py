from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from service.engine.positions import init_db, get_status, paper_buy_stub, paper_sell_stub, paper_reset

load_dotenv(override=True)

app = FastAPI(title="NIFTY Options ML Paper Bot", version="0.1.0")

@app.on_event("startup")
async def _startup():
    await init_db()

@app.get("/", tags=["meta"])
async def hello():
    return {"hello": "nifty-bot", "mode": "PAPER", "version": "0.1.0"}

@app.get("/status", tags=["paper"])
async def status_endpoint():
    return await get_status()

@app.get("/trade-history", tags=["paper"])
async def trade_history():
    # TODO: return recent positions with legs and P&L
    return {"items": [], "todo": "Implement after positions engine"}

@app.get("/prediction", tags=["ml"])
async def prediction_stub():
    # TODO: plug models + selector
    return {"direction": None, "confidence": None, "suggested_strikes": [], "todo": "ML not wired yet"}

@app.post("/paper/buy", tags=["paper"])
async def paper_buy():
    res = await paper_buy_stub()
    return JSONResponse(res, status_code=status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_200_OK)

@app.post("/paper/sell", tags=["paper"])
async def paper_sell():
    res = await paper_sell_stub()
    return JSONResponse(res, status_code=status.HTTP_202_ACCEPTED if res.get("ok") else status.HTTP_200_OK)

@app.post("/paper/reset", tags=["paper"])
async def paper_reset_endpoint():
    res = await paper_reset()
    return JSONResponse(res, status_code=status.HTTP_200_OK)
