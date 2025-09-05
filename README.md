# NIFTY Options ML Paper Bot (Codespaces)

Mode: **PAPER only** (quotes only; no live orders).  
TZ: **Asia/Kolkata**.  
Auto rules: BUY @15:28 IST if flat; SELL @09:21 IST next trading day if open.  
DB: SQLite `data/trades.db` with WAL; state persists; only `/paper/reset` wipes.

## Run (Codespaces)
```bash
cp .env.example .env
uvicorn service.api.app:app --host 0.0.0.0 --port 8000 --reload
