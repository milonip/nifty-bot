# service/engine/scheduler.py
from __future__ import annotations

import asyncio
from datetime import datetime, date, time, timedelta
from typing import List

from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from service.engine.selector import predict as ml_predict
from service.engine.quotes import get_quote
from .positions import paper_buy_stub, paper_sell_stub, get_status

# ----- Timezone -----
IST = ZoneInfo("Asia/Kolkata")

# ----- Scheduler singleton -----
_scheduler: AsyncIOScheduler | None = None


# ----- small utils -----
def _log(msg: str) -> None:
    print(f"[sched] {msg}")


def _is_weekday(d: datetime) -> bool:
    """Mon=0..Fri=4."""
    return d.weekday() < 5


def _market_window_now_ist() -> bool:
    """True if now is within NSE market hours 09:15–15:30 IST."""
    now = datetime.now(IST)
    h, m = now.hour, now.minute
    return (h > 9 or (h == 9 and m >= 15)) and (h < 15 or (h == 15 and m <= 30))


# ----- jobs -----
async def _predict_and_buy() -> None:
    """
    Simple wrapper used by the cron + catch-up kick.
    Checks position, calls paper engine (which calls selector+quotes internally).
    """
    _log(f"predict_and_buy fired at {datetime.now(IST).isoformat(timespec='seconds')}")
    st = await get_status()
    if st.get("open_position"):
        _log("skipped: a position is already open")
        return
    await paper_buy_stub()  # paper engine decides what to buy via selector


async def _predict_and_buy_job() -> None:
    """
    Verbose variant (kept for future debugging); not used by the cron at the moment.
    Shows how we'd wire quotes + selector explicitly.
    """
    try:
        _log("predict_and_buy fired (explicit)")
        if not _market_window_now_ist():
            _log("abort: outside market window")
            return

        ml = ml_predict()
        _log(f"ml: {ml}")

        q = await get_quote("NSE:NIFTY")
        _log(f"quote: {q}")
        ltp = q.get("ltp")
        if not ltp:
            _log("abort: no LTP in quote")
            return

        step = 50
        atm = int(round(ltp / step) * step)
        if ml.get("direction") == "UP":
            symbol = f"NIFTY {atm} CE"
        elif ml.get("direction") == "DOWN":
            symbol = f"NIFTY {atm} PE"
        else:
            _log("neutral signal; skipping")
            return

        _log(f"paper BUY {symbol}")
        res = await paper_buy_stub()
        _log(f"buy result: {res}")

    except Exception as e:
        _log(f"ERROR in predict_and_buy: {e}")


async def _squareoff_0921() -> None:
    _log(f"squareoff_0921 fired at {datetime.now(IST).isoformat(timespec='seconds')}")
    st = await get_status()
    if not st.get("open_position"):
        _log("skipped: no open position")
        return
    await paper_sell_stub()


# ----- public helpers -----
def get_next_runs_ist() -> List[str]:
    global _scheduler
    if not _scheduler:
        return []
    out: List[str] = []
    for j in _scheduler.get_jobs():
        n = j.next_run_time
        if n:
            out.append(f"{j.id}: {n.astimezone(IST).strftime('%-d/%-m/%Y, %-I:%M:%S %p')}")
    return sorted(out)


def start_scheduler(app=None) -> AsyncIOScheduler:
    """
    Starts APScheduler with:
    - Cron jobs (Mon–Fri): 15:28 (predict_and_buy), 09:21 (squareoff)
    - Generous misfire window & coalescing so we don't miss a minute
    - A small 'catch-up' run if we started between 15:28:00–15:33:00
    """
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    _scheduler = AsyncIOScheduler(
        timezone=IST,
        job_defaults={"misfire_grace_time": 600, "coalesce": True, "max_instances": 1},
    )

    # Mon–Fri 15:28:00 IST — BUY
    _scheduler.add_job(
        _predict_and_buy,
        id="predict_and_buy_1528",
        trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=28, timezone=IST),
        replace_existing=True,
    )

    # Mon–Fri 09:21:00 IST — Square off
    _scheduler.add_job(
        _squareoff_0921,
        id="squareoff_0921",
        trigger=CronTrigger(day_of_week="mon-fri", hour=9, minute=21, timezone=IST),
        replace_existing=True,
    )

    _scheduler.start()
    _log(f"started; next: {get_next_runs_ist()}")

    # ---- Catch-up: if we started at, say, 15:28:05–15:33 IST, run BUY once ----
    now = datetime.now(IST)
    if _is_weekday(now):
        buy_window_start = datetime.combine(now.date(), time(15, 28, 0, tzinfo=IST))
        buy_window_end = buy_window_start + timedelta(minutes=5)
        if buy_window_start <= now <= buy_window_end:
            _log("in catch-up window; firing immediate BUY once")
            asyncio.get_event_loop().create_task(_predict_and_buy())

    return _scheduler
