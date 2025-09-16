# service/engine/scheduler.py
from __future__ import annotations

import asyncio
from datetime import datetime, date, time, timedelta
from typing import List, Optional

from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from service.engine.positions import paper_buy_at_1528, paper_sell_at_0921, build_atm_legs_1528
from service.engine.quotes import get_quote

IST = ZoneInfo("Asia/Kolkata")
_scheduler: Optional[AsyncIOScheduler] = None

NIFTY_STEP = 50

MKT_OPEN  = time(9, 15)
MKT_CLOSE = time(15, 30)
BUY_HHMM  = (15, 28)
EXIT_HHMM = (9, 21)


def _log(msg: str) -> None:
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[sched] {now} | {msg}")

def _is_weekday(d: datetime) -> bool:
    return d.weekday() < 5  # Mon..Fri

def _within(now: datetime, start: datetime, end: datetime) -> bool:
    return start <= now <= end


async def _predict_and_buy(force: bool = False) -> None:
    """
    Fire the daily BUY:
      - skip if market closed (unless force=True)
      - use ML to decide UP/DOWN
      - compute ATM from NIFTY index LTP (50-pt step)
      - execute paper buy with cash-aware 1:2 sizing inside paper_buy_at_1528
    """
    try:
        _log(f"predict_and_buy fired (force={force})")

        if not force and not _market_window_now_ist():
            _log("abort: outside market window")
            return

        # --- ML signal ---
        ml = ml_predict()
        direction = (ml.get("direction") or "").upper()
        if direction not in ("UP", "DOWN"):
            _log("neutral/invalid signal; skipping")
            return

        # --- Index LTP -> ATM strike ---
        q = await get_quote("NSE:NIFTY")
        ltp = q.get("ltp")
        if not ltp:
            _log("abort: no NIFTY LTP")
            return
        atm = int(round(float(ltp) / NIFTY_STEP) * NIFTY_STEP)
        _log(f"signal={direction}  ltp={ltp}  atm={atm}")

        # --- Execute paper buy (sizes as many lots as cash allows, 1:2 rule) ---
        res = await paper_buy_at_1528(direction=direction, atm=atm)
        _log(f"buy result: {res}")

    except Exception as e:
        _log(f"ERROR in _predict_and_buy: {e}")

async def _squareoff_0921():
    try:
        res = await paper_sell_at_0921()
        _log(f"SELL -> {res}")
    except Exception as e:
        _log(f"ERROR in _squareoff_0921: {e}")


def get_next_runs_ist() -> List[str]:
    global _scheduler
    if not _scheduler:
        return []
    out: List[str] = []
    for j in _scheduler.get_jobs():
        if j.next_run_time:
            n = j.next_run_time.astimezone(IST)
            out.append(f"{j.id}: {n.strftime('%-d/%-m/%Y, %-I:%M:%S %p')}")
    return sorted(out)


def start_scheduler(app=None) -> AsyncIOScheduler:
    """
    Idempotent start. Schedules:
      - predict_and_buy @ 15:28 IST (Mon-Fri)
      - squareoff       @ 09:21 IST (Mon-Fri)
    Includes a 5-min catch-up window if the API boots shortly after 15:28.
    """
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    _scheduler = AsyncIOScheduler(
        timezone=IST,
        job_defaults={"misfire_grace_time": 600, "coalesce": True, "max_instances": 1},
    )

    _scheduler.add_job(
        _predict_and_buy,
        id="predict_and_buy_1528",
        trigger=CronTrigger(day_of_week="mon-fri", hour=BUY_HHMM[0], minute=BUY_HHMM[1], timezone=IST),
        replace_existing=True,
    )
    _scheduler.add_job(
        _squareoff_0921,
        id="squareoff_0921",
        trigger=CronTrigger(day_of_week="mon-fri", hour=EXIT_HHMM[0], minute=EXIT_HHMM[1], timezone=IST),
        replace_existing=True,
    )

    _scheduler.start()
    _log("scheduler started")
    _log("next: " + " · ".join(get_next_runs_ist()))

    # Catch-up if booted within 5 minutes after 15:28
    now = datetime.now(IST)
    if _is_weekday(now):
        start = datetime.combine(date.today(), time(BUY_HHMM[0], BUY_HHMM[1], tzinfo=IST))
        end = start + timedelta(minutes=5)
        if _within(now, start, end):
            _log("booted within BUY window; kicking one immediate BUY task")
            asyncio.get_event_loop().create_task(_predict_and_buy())

    return _scheduler
