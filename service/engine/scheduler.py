# service/engine/scheduler.py
# --- replace your imports block up to IST definition with this ---
from __future__ import annotations

import asyncio
import logging
from typing import Dict, Tuple, Optional, Any, TYPE_CHECKING

from .instruments import pick_monthly_option_symbols  # strict NIFTY monthly only
from .utils import _market_window_now_ist, _now_ist_str
from .selector import predict as ml_predict
from .positions import open_position, close_position

try:
    # Runtime imports (may be missing in paper env)
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception:  # pragma: no cover
    AsyncIOScheduler = None  # type: ignore[assignment]
    CronTrigger = None       # type: ignore[assignment]

# Type-only imports so Pylance has real types without touching runtime vars
if TYPE_CHECKING:  # only evaluated by type checkers
    from apscheduler.schedulers.asyncio import AsyncIOScheduler as _AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger as _CronTrigger

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    from pytz import timezone as ZoneInfo  # type: ignore

logger = logging.getLogger("service.scheduler")
IST = ZoneInfo("Asia/Kolkata")

# Keep a single scheduler for the process
_SCHED: Optional["_AsyncIOScheduler"] = None


# --------- SYMBOL SELECTION (strict monthly NIFTY index options) ---------
def select_symbols_for_prediction(direction: str) -> Tuple[Dict, str, Dict, str]:
    """
    Monthly-only NIFTY 50 index options (OPTIDX). If current month is past expiry,
    it automatically rolls to next month. Never weekly, never stock options.
    """
    return pick_monthly_option_symbols(direction)


def ratio_for(direction: str) -> Tuple[int, int]:
    """2:1 when UP else 1:2 — matches your current strategy."""
    return (2, 1) if direction.upper() == "UP" else (1, 2)


# --------- TASKS (callable both by HTTP and scheduler) ---------
async def predict_and_buy_1528() -> Dict[str, Any]:
    direction, conf = ml_predict()

    sel = select_symbols_for_prediction(direction)
    if not sel or len(sel) != 4:
        raise RuntimeError(
            f"Symbol selection failed (got {sel!r}). "
            "Ensure data/angel_instruments.json has NIFTY 50 monthly OPTIDX and parsing is correct."
        )
    ce, ce_lbl, pe, pe_lbl = sel

    lots_ratio = ratio_for(direction)

    logger.info(
        "[%s] predict_and_buy_1528: dir=%s conf=%.3f ce=%s pe=%s ratio=%s",
        _now_ist_str(), direction, conf, ce_lbl, pe_lbl, lots_ratio,
    )

    res = open_position(direction, ce_symbol=ce, pe_symbol=pe, ratio=lots_ratio)
    payload = {"opened": True, "direction": direction, "confidence": float(conf), "details": res}
    logger.info("Opened position: %s", payload)
    return payload


async def squareoff_0921() -> Dict[str, Any]:
    """
    Close open position at live LTPs and realize P&L.
    """
    logger.info("[%s] squareoff_0921: trying to close any open position", _now_ist_str())
    res = close_position("scheduled_squareoff_0921")
    logger.info("Squareoff result: %s", res)
    return res


# --------- SCHEDULER WIRING ---------
def _ensure_scheduler() -> "_AsyncIOScheduler":
    global _SCHED
    if _SCHED is not None:
        return _SCHED
    if AsyncIOScheduler is None:
        raise RuntimeError("APScheduler not installed. Add 'apscheduler' to requirements.")

    _SCHED = AsyncIOScheduler(timezone=IST)
    return _SCHED


def start_scheduler(app: Optional[Any] = None) -> "_AsyncIOScheduler":
    """
    Create and start the AsyncIOScheduler with the two cron jobs:
      - 15:28 IST (Mon–Fri): predict & buy
      - 09:21 IST (Mon–Fri): squareoff next morning
    Call this once on app startup.
    """
    sched = _ensure_scheduler()

    # Clear duplicate jobs on hot reload
    for job in list(sched.get_jobs()):
        sched.remove_job(job.id)

    # 15:28 IST — Predict & Buy (Mon–Fri)
    sched.add_job(
        func=lambda: asyncio.create_task(_guarded(predict_and_buy_1528)),
        trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=28, second=0, timezone=IST),
        id="predict_and_buy_1528",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # 09:21 IST — Squareoff (Mon–Fri)
    sched.add_job(
        func=lambda: asyncio.create_task(_guarded(squareoff_0921)),
        trigger=CronTrigger(day_of_week="mon-fri", hour=9, minute=21, second=0, timezone=IST),
        id="squareoff_0921",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    if not sched.running:
        sched.start()
        logger.info("AsyncIOScheduler started with IST timezone.")

    # Expose on app.state for diagnostics
    if app is not None:
        try:
            app.state.scheduler = sched
        except Exception:
            pass

    return sched


async def _guarded(coro_fn):
    """
    Wrap a coroutine task in try/except so APScheduler doesn't swallow errors silently.
    """
    try:
        win = _market_window_now_ist()
        logger.info("Window check (IST): %s", {
            "now": win["now"].strftime("%Y-%m-%d %H:%M:%S"),
            "is_buy_window": win["is_buy_window"],
            "is_squareoff_window": win["is_squareoff_window"],
            "buy_at": win["buy_at"].strftime("%H:%M:%S"),
            "squareoff_at": win["squareoff_at"].strftime("%H:%M:%S"),
        })
        return await coro_fn()
    except Exception as e:
        logger.exception("Scheduled task failed: %s", e)


def get_next_runs_ist() -> Dict[str, Optional[str]]:
    """
    Returns next run times for UI display.
      {"predict_and_buy_1528": "2025-09-17 15:28:00", "squareoff_0921": "2025-09-18 09:21:00"}
    """
    sched = _ensure_scheduler()
    out: Dict[str, Optional[str]] = {
        "predict_and_buy_1528": None,
        "squareoff_0921": None,
    }
    for job in sched.get_jobs():
        nxt = job.next_run_time
        if job.id in out:
            out[job.id] = nxt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if nxt else None
    return out


def stop_scheduler() -> None:
    global _SCHED
    if _SCHED and getattr(_SCHED, "running", False):
        _SCHED.shutdown(wait=False)
        _SCHED = None
