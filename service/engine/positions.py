import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

from dotenv import load_dotenv
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, JSON, ForeignKey, select, func, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from zoneinfo import ZoneInfo

load_dotenv(override=True)

# Timezone (fallback to UTC if tzdata missing)
TZ_NAME = os.getenv("TZ", "Asia/Kolkata")
try:
    IST = ZoneInfo(TZ_NAME)
except Exception:
    IST = ZoneInfo("UTC")

DB_PATH = os.getenv("DB_PATH", "data/trades.db")
STARTING_FUNDS = float(os.getenv("STARTING_FUNDS", "500000"))
LOT_SIZE = int(os.getenv("LOT_SIZE", "75"))

Base = declarative_base()

class Ledger(Base):
    __tablename__ = "ledger"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime(timezone=False), nullable=False)  # stored as local naive ts
    kind = Column(String, nullable=False)  # 'seed'|'buy'|'sell'|'reset'|...
    amount = Column(Float, nullable=False)  # +credit / -debit
    balance = Column(Float, nullable=False) # running cash balance
    meta = Column(JSON, nullable=True)

class Position(Base):
    __tablename__ = "positions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    opened_at = Column(DateTime(timezone=False), nullable=True)
    closed_at = Column(DateTime(timezone=False), nullable=True)
    status = Column(String, nullable=False, default="OPEN")  # OPEN | CLOSED
    legs = relationship("Leg", back_populates="position", cascade="all, delete-orphan")

class Leg(Base):
    __tablename__ = "legs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    position_id = Column(Integer, ForeignKey("positions.id"), nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False, default="BUY")  # BUY (paper only)
    lots = Column(Integer, nullable=False, default=1)
    lot_size = Column(Integer, nullable=False, default=LOT_SIZE)

    entry_ltp = Column(Float, nullable=True)
    entry_bid = Column(Float, nullable=True)
    entry_ask = Column(Float, nullable=True)
    entry_iv  = Column(Float, nullable=True)

    exit_ltp = Column(Float, nullable=True)
    exit_bid = Column(Float, nullable=True)
    exit_ask = Column(Float, nullable=True)
    exit_iv  = Column(Float, nullable=True)

    position = relationship("Position", back_populates="legs")

# --- engine / session ---
Path("data").mkdir(parents=True, exist_ok=True)
engine = create_async_engine(f"sqlite+aiosqlite:///{DB_PATH}", echo=False, future=True)
AsyncSessionMaker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # WAL + sane pragmas
        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await conn.execute(text("PRAGMA synchronous=NORMAL;"))
        await conn.execute(text("PRAGMA foreign_keys=ON;"))
    # Seed ledger on first run
    async with AsyncSessionMaker() as s:
        count = await s.scalar(select(func.count()).select_from(Ledger))
        if count == 0:
            now_local = datetime.now(IST).replace(microsecond=0)
            seed = Ledger(
                ts=now_local, kind="seed", amount=STARTING_FUNDS,
                balance=STARTING_FUNDS, meta={"note": "Initial funds"}
            )
            s.add(seed)
            await s.commit()

def _today_at_local(h: int, m: int) -> datetime:
    now = datetime.now(IST)
    return now.replace(hour=h, minute=m, second=0, microsecond=0)

def next_jobs_ist() -> Dict[str, str]:
    now = datetime.now(IST)
    buy = _today_at_local(15, 28)
    sell = _today_at_local(9, 21)

    if now >= buy:
        buy = buy + timedelta(days=1)  # TODO: trading calendar/holidays
    if now >= sell:
        sell = sell + timedelta(days=1)

    return {
        "next_buy_ist": buy.isoformat(sep=" "),
        "next_sell_ist": sell.isoformat(sep=" ")
    }

async def get_status() -> Dict[str, Any]:
    async with AsyncSessionMaker() as s:
        last_bal = await s.scalar(
            select(Ledger.balance).order_by(Ledger.id.desc()).limit(1)
        )
        if last_bal is None:
            last_bal = STARTING_FUNDS
        open_pos = await s.scalar(
            select(func.count()).select_from(Position).where(Position.status == "OPEN")
        )
    return {
        "funds": last_bal,
        "realized": None,    # TODO compute from ledger once SELL implemented
        "unrealized": None,  # TODO compute MTM if open
        "open_position": bool(open_pos),
        "lot_size": LOT_SIZE,
        "next_jobs_IST": next_jobs_ist()
    }

# --- Paper engine stubs (we’ll wire soon) ---
async def paper_buy_stub() -> Dict[str, Any]:
    return {"ok": False, "detail": "BUY stub: selector/quotes not wired yet."}

async def paper_sell_stub() -> Dict[str, Any]:
    return {"ok": False, "detail": "SELL stub: positions not wired yet."}

async def paper_reset() -> Dict[str, Any]:
    async with AsyncSessionMaker() as s:
        # wipe all tables
        await s.execute(text("DELETE FROM legs;"))
        await s.execute(text("DELETE FROM positions;"))
        await s.execute(text("DELETE FROM ledger;"))
        await s.commit()
    # reseed
    await init_db()
    return {"ok": True, "detail": "State wiped and funds reseeded."}
