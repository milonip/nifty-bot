# service/engine/positions.py
from __future__ import annotations

import os, json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import aiosqlite

from service.engine.selector import predict as ml_predict
from service.engine.quotes import get_quote

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(ROOT, "data", "paper.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# --- Config ---
VIRTUAL_BALANCE = float(os.getenv("PAPER_FUNDS", "500000"))
LOT_SIZE        = int(os.getenv("NIFTY_LOT_SIZE", "75"))  # tracks lots, not raw qty
UNIT_LOTS       = int(os.getenv("UNIT_LOTS", "1"))        # base unit to scale 2:1
PRIMARY_HEDGE   = (2, 1)  # 2:1 by default

@dataclass
class Funds:
    cash: float
    realized: float

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS funds (
            id INTEGER PRIMARY KEY CHECK (id=1),
            cash REAL NOT NULL,
            realized REAL NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS position (
            id INTEGER PRIMARY KEY CHECK (id=1),
            ts_utc TEXT NOT NULL,
            lots INTEGER NOT NULL,
            lot_size INTEGER NOT NULL,
            entry_value REAL NOT NULL,
            legs_json TEXT NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            open_ts_utc TEXT NOT NULL,
            close_ts_utc TEXT,
            lots INTEGER NOT NULL,
            lot_size INTEGER NOT NULL,
            entry_value REAL NOT NULL,
            exit_value REAL,
            pnl REAL,
            legs_json TEXT NOT NULL
        )""")
        # seed funds if empty
        cur = await db.execute("SELECT cash, realized FROM funds WHERE id=1")
        row = await cur.fetchone()
        if not row:
            await db.execute("INSERT INTO funds(id, cash, realized) VALUES(1, ?, ?)", (VIRTUAL_BALANCE, 0.0))
        await db.commit()

async def _read_funds(db) -> Funds:
    cur = await db.execute("SELECT cash, realized FROM funds WHERE id=1")
    cash, realized = await cur.fetchone()
    return Funds(cash=cash, realized=realized)

async def _write_funds(db, cash: float, realized: float) -> None:
    await db.execute("UPDATE funds SET cash=?, realized=? WHERE id=1", (cash, realized))

async def _read_position(db) -> Optional[Dict[str, Any]]:
    cur = await db.execute("SELECT ts_utc, lots, lot_size, entry_value, legs_json FROM position WHERE id=1")
    row = await cur.fetchone()
    if not row:
        return None
    ts_utc, lots, lot_size, entry_value, legs_json = row
    return {
        "ts": ts_utc,
        "lots": lots,
        "lot_size": lot_size,
        "entry_value": entry_value,
        "legs": json.loads(legs_json),
    }

async def _delete_position(db) -> None:
    await db.execute("DELETE FROM position WHERE id=1")

def _build_legs(direction: str, atm: int, unit_lots: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Return legs and total lots using 2:1 primary:hedge.
    For now we create:
      - UP:  primary CE @ ATM, hedge PE @ ATM
      - DOWN: primary PE @ ATM, hedge CE @ ATM
    """
    p, h = PRIMARY_HEDGE
    primary = {"side": "BUY", "symbol": f"NIFTY {atm} {'CE' if direction=='UP' else 'PE'}", "lots": p * unit_lots}
    hedge   = {"side": "BUY", "symbol": f"NIFTY {atm} {'PE' if direction=='UP' else 'CE'}", "lots": h * unit_lots}
    legs = [primary, hedge]
    total_lots = primary["lots"] + hedge["lots"]
    return legs, total_lots

async def get_status() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        f  = await _read_funds(db)
        pos = await _read_position(db)
        # unrealized MTM not implemented until live option quotes are wired
        return {
            "funds": {"cash": round(f.cash, 2), "realized": round(f.realized, 2), "unrealized": None},
            "open_position": pos,
        }

async def paper_reset() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM trades")
        await _delete_position(db)
        await _write_funds(db, VIRTUAL_BALANCE, 0.0)
        await db.commit()
    return {"ok": True, "reset": True, "funds": VIRTUAL_BALANCE}

async def paper_buy_stub() -> Dict[str, Any]:
    """
    PAPER BUY @ 15:28 rules (reused by button):
      - read ML direction & live NIFTY LTP
      - form ATM from LTP (round to 50)
      - build 2:1 primary:hedge CE/PE legs
      - use index LTP as TEMP proxy price per leg (until options quotes wired)
      - compute entry_value = sum(leg_price * qty), qty = lots * LOT_SIZE
      - persist position + trade row; deduct cash
    """
    async with aiosqlite.connect(DB_PATH) as db:
        if await _read_position(db):
            return {"ok": False, "error": "position already open"}

        ml = ml_predict()
        direction = ml.get("direction")
        if direction not in ("UP", "DOWN"):
            return {"ok": False, "error": f"neutral/unknown direction: {direction}"}

        q = await get_quote("NSE:NIFTY")
        ltp = q.get("ltp")
        if not ltp:
            return {"ok": False, "error": "no NIFTY LTP"}

        step = 50
        atm = int(round(float(ltp) / step) * step)

        # Build legs (2:1)
        legs, total_lots = _build_legs(direction, atm, UNIT_LOTS)

        # TEMP pricing proxy: use index LTP for each option leg (placeholder!)
        # mark in the record so it's clear this is a proxy
        for leg in legs:
            leg["entry_price_proxy"] = float(ltp)  # TODO: replace with option LTP
            leg["entry_source"] = "INDEX_LTP_PROXY"

        # Cost: sum(price * lots * LOT_SIZE). This is a crude proxy until options quotes are wired.
        entry_value = sum(leg["entry_price_proxy"] * leg["lots"] * LOT_SIZE for leg in legs)

        f = await _read_funds(db)
        if f.cash < entry_value:
            return {"ok": False, "error": "insufficient cash", "need": entry_value, "cash": f.cash}

        legs_json = json.dumps(legs)
        ts = _utcnow_iso()

        await db.execute(
            "INSERT INTO position(id, ts_utc, lots, lot_size, entry_value, legs_json) VALUES(1, ?, ?, ?, ?, ?)",
            (ts, total_lots, LOT_SIZE, entry_value, legs_json),
        )
        await db.execute(
            "INSERT INTO trades(open_ts_utc, lots, lot_size, entry_value, legs_json) VALUES(?, ?, ?, ?, ?)",
            (ts, total_lots, LOT_SIZE, entry_value, legs_json),
        )
        await db.execute("UPDATE funds SET cash=cash-? WHERE id=1", (entry_value,))
        await db.commit()

        return {
            "ok": True,
            "action": "buy",
            "lots": total_lots,
            "lot_size": LOT_SIZE,
            "entry_value": entry_value,
            "legs": legs,
            "atm": atm,
            "direction": direction,
            "note": "Prices are INDEX_LTP proxies until option quotes are wired.",
        }

async def paper_sell_stub() -> Dict[str, Any]:
    """
    PAPER SELL @ 09:21 rules:
      - use fresh index LTP as TEMP proxy for exit price (until options quotes wired)
      - realized P&L = (sum(exit_price*qty) - entry_value)
      - close position; update last trade row
    """
    async with aiosqlite.connect(DB_PATH) as db:
        pos = await _read_position(db)
        if not pos:
            return {"ok": False, "error": "no open position"}

        q = await get_quote("NSE:NIFTY")
        ltp = q.get("ltp")
        if not ltp:
            return {"ok": False, "error": "no NIFTY LTP"}

        exit_price_proxy = float(ltp)
        legs = pos["legs"]
        for leg in legs:
            leg["exit_price_proxy"] = exit_price_proxy
            leg["exit_source"] = "INDEX_LTP_PROXY"

        exit_value = sum(exit_price_proxy * leg["lots"] * pos["lot_size"] for leg in legs)
        pnl = exit_value - float(pos["entry_value"])

        await _delete_position(db)
        await db.execute("""
            UPDATE trades
            SET close_ts_utc=?, exit_value=?, pnl=?, legs_json=?
            WHERE id = (SELECT id FROM trades WHERE close_ts_utc IS NULL ORDER BY id DESC LIMIT 1)
        """, (_utcnow_iso(), exit_value, pnl, json.dumps(legs)))

        # credit back notional + realized
        cur = await db.execute("SELECT cash, realized FROM funds WHERE id=1")
        cash, realized = await cur.fetchone()
        cash += exit_value
        realized += pnl
        await _write_funds(db, cash, realized)
        await db.commit()

        return {
            "ok": True,
            "action": "sell",
            "exit_value": exit_value,
            "pnl": pnl,
            "legs": legs,
            "note": "Prices are INDEX_LTP proxies until option quotes are wired.",
        }

async def get_history(limit: int = 50) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT open_ts_utc, close_ts_utc, lots, lot_size, entry_value, exit_value, pnl, legs_json
            FROM trades ORDER BY id DESC LIMIT ?
        """, (limit,))
        rows = await cur.fetchall()
        items = []
        for r in rows:
            open_ts, close_ts, lots, lot_size, entry_val, exit_val, pnl, legs_json = r
            legs = json.loads(legs_json or "[]")
            items.append({
                "open_ts": open_ts,
                "exit_ts": close_ts,
                "lots": lots,
                "lot_size": lot_size,
                "entry_value": entry_val,
                "exit_value": exit_val,
                "pnl": pnl,
                "legs": legs,
            })
        return {"items": items}
