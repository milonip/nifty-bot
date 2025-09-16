# service/engine/positions.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiosqlite
import pandas as pd

from service.engine.selector import predict as ml_predict
from service.engine.quotes import get_quote

DB_PATH = "data/trades.db"
DATASET_PATH = "data/processed/overnight_dataset.parquet"

NIFTY_STEP = 50
LOT_SIZE = 75  # NIFTY derivatives lot
START_CASH = 500000.0


# ---------- SQLite helpers ----------

async def _db():
    # IMPORTANT: return the coroutine (not awaited) so callers do:
    # async with await _db() as db: ...
    return aiosqlite.connect(DB_PATH)

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    async with await _db() as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS funds(
            id INTEGER PRIMARY KEY CHECK (id=1),
            cash REAL NOT NULL,
            realized REAL NOT NULL,
            unrealized REAL NOT NULL
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS position(
            id INTEGER PRIMARY KEY CHECK (id=1),
            ts_utc TEXT,
            lots INTEGER,
            lot_size INTEGER,
            entry_value REAL,
            legs_json TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_utc TEXT,
            closed_utc TEXT,
            legs_json TEXT,
            lots INTEGER,
            lot_size INTEGER,
            entry_value REAL,
            exit_value REAL,
            pnl REAL
        )
        """)

        # seed single rows
        cur = await db.execute("SELECT 1 FROM funds WHERE id=1")
        if not await cur.fetchone():
            await db.execute(
                "INSERT INTO funds(id, cash, realized, unrealized) VALUES(1, ?, 0, 0)",
                (START_CASH,)
            )
        cur = await db.execute("SELECT 1 FROM position WHERE id=1")
        if not await cur.fetchone():
            await db.execute(
                "INSERT INTO position(id, ts_utc, lots, lot_size, entry_value, legs_json) VALUES(1, NULL, 0, ?, 0, '[]')",
                (LOT_SIZE,)
            )
        await db.commit()


# ---------- snapshots / pricing ----------
def _inr(n: float | None) -> str:
    if n is None:
        return "—"
    try:
        x = round(float(n), 2)
    except Exception:
        return "—"
    s = f"{x:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"₹{s}"

def _read_snapshots() -> Tuple[float, float]:
    """
    Returns (px_1528, px_0921) from the latest row of dataset.
    """
    df = pd.read_parquet(DATASET_PATH)
    df = df.sort_values("date")
    row = df.iloc[-1]
    px_1528 = float(row["px_1528"])
    px_0921 = float(row["px_0921"])
    return px_1528, px_0921

def _parse_symbol(symbol: str) -> Tuple[int, str]:
    # "NIFTY 25000 CE" -> (25000, "CE")
    parts = symbol.strip().split()
    strike = int(parts[1])
    side = parts[2].upper()
    return strike, side

def _approx_option_price(underlying: float, strike: int, side: str) -> float:
    """
    Very simple paper-pricing so we can compute cost/P&L deterministically.
    intrinsic scaled + a small time value.
    """
    if side == "CE":
        intrinsic = max(0.0, underlying - strike)
    else:
        intrinsic = max(0.0, strike - underlying)
    time_value = max(20.0, 0.08 * NIFTY_STEP)  # constant small buffer
    return round(intrinsic * 0.45 + time_value, 2)

def paper_option_px_at_1528(symbol: str) -> float:
    px_1528, _ = _read_snapshots()
    strike, side = _parse_symbol(symbol)
    return _approx_option_price(px_1528, strike, side)

def paper_option_px_at_0921(symbol: str) -> float:
    _, px_0921 = _read_snapshots()
    strike, side = _parse_symbol(symbol)
    return _approx_option_price(px_0921, strike, side)


# ---------- building legs (2:1 or 1:2) ----------

async def _option_ltp(symbol: str) -> float | None:
    """
    symbol example: 'NIFTY 25000 CE'
    We ask quotes for OPTIDX; your quotes.get_quote should accept this alias:
       'NFO:OPTIDX:NIFTY 25000 CE'
    Fallback: try simple 'OPTIDX:NIFTY 25000 CE' then 'NIFTY 25000 CE'.
    """
    choices = [
        f"NFO:OPTIDX:{symbol}",
        f"OPTIDX:{symbol}",
        symbol,
    ]
    for s in choices:
        try:
            q = await get_quote(s)
            ltp = q.get("ltp")
            if ltp and ltp > 0:
                return float(ltp)
        except Exception:
            pass
    return None

async def build_atm_legs_1528() -> List[Dict]:
    """
    Decide ATM and 2:1 ratio (UP => 2*CE + 1*PE ; DOWN => 2*PE + 1*CE),
    then scale the number of 3-lot sets to fit available cash.
    """
    # predict direction
    ml = ml_predict()
    direction = (ml.get("direction") or "").upper()

    # live LTP to round ATM (fallback to px_1528 if needed)
    q = await get_quote("NSE:NIFTY")
    ltp = q.get("ltp")
    if not ltp:
        ltp, _ = _read_snapshots()

    atm = int(round(ltp / NIFTY_STEP) * NIFTY_STEP)

    if direction == "UP":
        primary = f"NIFTY {atm} CE"
        hedge   = f"NIFTY {atm} PE"
        ratio   = (2, 1)  # CE:PE
    elif direction == "DOWN":
        primary = f"NIFTY {atm} PE"
        hedge   = f"NIFTY {atm} CE"
        ratio   = (2, 1)  # PE:CE
    else:
        # neutral => buy straddle 1:1
        primary = f"NIFTY {atm} CE"
        hedge   = f"NIFTY {atm} PE"
        ratio   = (1, 1)

    p_primary = paper_option_px_at_1528(primary)
    p_hedge   = paper_option_px_at_1528(hedge)

    # cost for one "set" respecting the ratio
    set_cost = (ratio[0] * p_primary + ratio[1] * p_hedge) * LOT_SIZE

    # how many sets can we afford?
    async with await _db() as db:
        cur = await db.execute("SELECT cash FROM funds WHERE id=1")
        cash = float((await cur.fetchone())[0])

    n_sets = int(cash // set_cost)
    n_sets = max(1, n_sets)  # buy at least 1 set

    legs = []
    legs.append({"symbol": primary, "lots": ratio[0] * n_sets, "price": p_primary})
    legs.append({"symbol": hedge,   "lots": ratio[1] * n_sets, "price": p_hedge})
    return legs


# ---------- core paper actions ----------

async def paper_buy_at_1528(direction: str, atm: int) -> dict:
    """
    direction: 'UP' or 'DOWN'
    atm: int strike like 25000
    Rule: 1:2 hedge (UP -> 2×CE + 1×PE, DOWN -> 2×PE + 1×CE)
    Use as many lots as cash allows, valued with option LTPs at execution time.
    """
    async with await _db() as db:
        # funds
        f = await _read_funds(db)
        cash = float(f["cash"] or 0.0)

        # build leg template
        if direction.upper() == "UP":
            major = f"NIFTY {atm} CE"
            hedge = f"NIFTY {atm} PE"
            weights = [(major, 2), (hedge, 1)]
        elif direction.upper() == "DOWN":
            major = f"NIFTY {atm} PE"
            hedge = f"NIFTY {atm} CE"
            weights = [(major, 2), (hedge, 1)]
        else:
            return {"ok": False, "error": "neutral signal"}

        # price both legs
        legs_px = []
        for sym, w in weights:
            ltp = await _option_ltp(sym)
            if not ltp:
                return {"ok": False, "error": f"no LTP for {sym}"}
            legs_px.append((sym, w, float(ltp)))

        # cost of 1 "bundle" (2 lots of major + 1 lot of hedge)
        bundle_cost = 0.0
        for _, w, px in legs_px:
            bundle_cost += px * LOT_SIZE * w

        if bundle_cost <= 0:
            return {"ok": False, "error": "invalid bundle_cost"}

        # how many bundles fit in cash (leave tiny buffer)
        max_bundles = int((cash - 100.0) // bundle_cost)
        if max_bundles < 1:
            return {"ok": False, "error": "insufficient cash", "need": bundle_cost, "cash": cash}

        # construct legs (materialized as lots)
        legs = []
        total_value = 0.0
        for sym, w, px in legs_px:
            lots = w * max_bundles
            leg_value = px * LOT_SIZE * lots
            legs.append({
                "symbol": sym,
                "lots": lots,
                "price": px,
                "px": "paper@15:28",
            })
            total_value += leg_value

        # open position row
        ts_utc = _utc_now_str()
        await _write_position_open(
            ts_utc=ts_utc,
            lots=sum(l["lots"] for l in legs),
            lot_size=LOT_SIZE,
            entry_value=total_value,
            legs_json=json.dumps(legs),
        )
        # deduct cash
        await _write_funds(db, cash - total_value, f["realized"], f["unrealized"])

        return {"ok": True, "entry_value": total_value, "legs": legs, "bundles": max_bundles}

async def paper_sell_at_0921() -> dict:
    async with await _db() as db:
        pos = await _read_position(db)
        if not pos.get("open"):
            return {"ok": False, "error": "no open position"}

        legs = json.loads(pos.get("legs_json") or "[]")
        # price legs for exit (use option LTPs again)
        exit_value = 0.0
        for lg in legs:
            px = await _option_ltp(lg["symbol"]) or 0.0
            lots = int(lg.get("lots", 0))
            exit_value += px * LOT_SIZE * lots

        entry_value = float(pos.get("entry_value") or 0.0)
        pnl = exit_value - entry_value

        # move to trades table (one row per position)
        await _append_trade_row(
            opened_utc=pos.get("ts_utc"),
            closed_utc=_utc_now_str(),
            legs_json=pos.get("legs_json"),
            lots=int(pos.get("lots") or 0),
            lot_size=int(pos.get("lot_size") or LOT_SIZE),
            entry_value=entry_value,
            exit_value=exit_value,
            pnl=pnl,
        )

        # clear current position & update funds (add exit cash, realize P&L)
        f = await _read_funds(db)
        new_cash = float(f["cash"] or 0.0) + exit_value
        new_realized = float(f["realized"] or 0.0) + pnl
        await _write_funds(db, new_cash, new_realized, 0.0)
        await _write_position_clear()

        return {
            "ok": True,
            "entry_value": entry_value,
            "exit_value": exit_value,
            "pnl": pnl,
        }


async def paper_reset() -> Dict:
    async with await _db() as db:
        await db.execute("UPDATE funds SET cash=?, realized=0, unrealized=0 WHERE id=1", (START_CASH,))
        await db.execute("UPDATE position SET ts_utc=NULL, lots=0, entry_value=0, legs_json='[]' WHERE id=1")
        await db.commit()
    return {"ok": True}


# ---------- reads for UI ----------

async def _read_funds(db) -> Dict:
    cur = await db.execute("SELECT cash, realized, unrealized FROM funds WHERE id=1")
    row = await cur.fetchone()
    if not row:
        return {"cash": None, "realized": None, "unrealized": None}
    return {"cash": float(row[0]), "realized": float(row[1]), "unrealized": float(row[2])}

async def _read_position(db) -> Dict:
    cur = await db.execute("SELECT ts_utc, lots, lot_size, entry_value, legs_json FROM position WHERE id=1")
    row = await cur.fetchone()
    if not row or not row[0]:
        return {"open": False}
    ts_utc, lots, lot_size, entry_value, legs_json = row
    return {
        "open": True,
        "ts_utc": ts_utc,
        "lots": int(lots),
        "lot_size": int(lot_size),
        "entry_value": float(entry_value),
        "legs": json.loads(legs_json or "[]"),
    }

async def get_status() -> Dict:
    async with await _db() as db:
        funds = await _read_funds(db)
        pos = await _read_position(db)

    cash = funds.get("cash")
    realized = funds.get("realized")
    unrealized = funds.get("unrealized")

    # Build a UI-friendly block while preserving old keys for compatibility
    out = {
        # legacy top-level (kept)
        "cash": cash,
        "realized": realized,
        "unrealized": unrealized,
        "open_position": pos if pos.get("open") else None,

        # new, always-safe display fields
        "funds": {
            "cash": cash,
            "cash_fmt": _inr(cash),
            "realized": realized,
            "realized_fmt": _inr(realized),
            "unrealized": unrealized,
            "unrealized_fmt": _inr(unrealized),
        },
    }

    # enrich open position display (entry price / value)
    if out["open_position"]:
        op = out["open_position"]
        entry_value = op.get("entry_value", 0.0)
        # sum per-leg cost for clarity
        legs = op.get("legs", [])
        per_leg = []
        for lg in legs:
            lots = int(lg.get("lots", 0))
            px = float(lg.get("price", 0.0))
            per_leg.append({
                "symbol": lg.get("symbol"),
                "lots": lots,
                "price": px,
                "price_fmt": _inr(px),
                "px_label": lg.get("px", "paper@15:28"),
                "leg_value": px * op.get("lot_size", 75) * lots,
                "leg_value_fmt": _inr(px * op.get("lot_size", 75) * lots),
            })
        op["entry_value_fmt"] = _inr(entry_value)
        op["legs_detailed"] = per_leg
        # Friendly “Entry” string the UI can show directly
        ts = op.get("ts_utc")
        op["entry_display"] = _inr(entry_value) if entry_value else "—"
        out["open_position"] = op

    return out


async def get_history(limit: int = 50) -> List[Dict]:
    items: List[Dict] = []
    async with await _db() as db:
        cur = await db.execute("""
            SELECT opened_utc, closed_utc, legs_json, lots, lot_size, entry_value, exit_value, pnl
            FROM trades ORDER BY id DESC LIMIT ?
        """, (limit,))
        rows = await cur.fetchall()
    for r in rows:
        entry_value = float(r[5]) if r[5] is not None else None
        exit_value  = float(r[6]) if r[6] is not None else None
        pnl         = float(r[7]) if r[7] is not None else None
        items.append({
            "opened_utc": r[0],
            "closed_utc": r[1],
            "legs": json.loads(r[2] or "[]"),
            "lots": int(r[3]) if r[3] is not None else 0,
            "lot_size": int(r[4]) if r[4] is not None else LOT_SIZE,
            "entry_value": entry_value,
            "entry_value_fmt": _inr(entry_value),
            "exit_value": exit_value,
            "exit_value_fmt": _inr(exit_value),
            "pnl": pnl,
            "pnl_fmt": _inr(pnl),
        })
    return items

