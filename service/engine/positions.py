# service/engine/positions.py
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .utils import (
    _write_funds, _write_position_open, _write_position_clear, _append_trade_row
)
from .quotes import get_quote

LOT_SIZE = 75  # NIFTY monthly lot

@dataclass
class Leg:
    symbol: Dict[str, str]  # {"exchange","tradingsymbol","symboltoken"}
    lots: int
    entry: Optional[float] = None
    exit: Optional[float] = None

@dataclass
class Position:
    ce: Leg
    pe: Leg
    side: str                 # "UP" or "DOWN"
    ratio: Tuple[int,int]     # (ce, pe) lots ratio ex. (2,1) or (1,2)

# ---- Funds state in memory (mirrors reports/funds.json) ----
_balance = 500000.0   # initial virtual balance (paper)
_realized = 0.0
_used = 0.0
_open: Optional[Position] = None

def _calc_used(pos: Position) -> float:
    # Approx used = (entry_price_ce * lots_ce + entry_price_pe * lots_pe) * LOT_SIZE
    e_ce = pos.ce.entry or 0.0
    e_pe = pos.pe.entry or 0.0
    return (e_ce * pos.ce.lots + e_pe * pos.pe.lots) * LOT_SIZE

def _mtm(pos: Position) -> Optional[float]:
    # MTM only when position is open and current LTPs available
    from math import isfinite
    l_ce = get_quote(pos.ce.symbol)
    l_pe = get_quote(pos.pe.symbol)
    if l_ce is None or l_pe is None or pos.ce.entry is None or pos.pe.entry is None:
        return None
    pnl = ((l_ce - pos.ce.entry) * pos.ce.lots + (l_pe - pos.pe.entry) * pos.pe.lots) * LOT_SIZE
    return float(pnl) if isfinite(pnl) else None

def _write_funds_snapshot():
    mtm_val = _mtm(_open) if _open else None
    _write_funds(_balance, _realized, _used, mtm_val)

# ---------- API called by strategy ----------
def open_position(side: str, ce_symbol: Dict[str,str], pe_symbol: Dict[str,str], ratio: Tuple[int,int]) -> Dict:
    """Open both legs using live LTP as entry."""
    global _open, _used

    lots_ce, lots_pe = ratio
    ltp_ce = get_quote(ce_symbol)
    ltp_pe = get_quote(pe_symbol)
    if ltp_ce is None or ltp_pe is None:
        raise RuntimeError("Failed to fetch LTP for CE/PE while opening position")

    _open = Position(
        ce=Leg(symbol=ce_symbol, lots=lots_ce, entry=float(ltp_ce)),
        pe=Leg(symbol=pe_symbol, lots=lots_pe, entry=float(ltp_pe)),
        side=side,
        ratio=ratio,
    )
    _used = _calc_used(_open)
    _write_position_open({
        "side": side,
        "ce": {"symbol": ce_symbol, "lots": lots_ce, "entry": _open.ce.entry},
        "pe": {"symbol": pe_symbol, "lots": lots_pe, "entry": _open.pe.entry},
        "ratio": list(ratio),
        "used": _used,
    })
    _write_funds_snapshot()
    return {
        "status": "ok",
        "entry": {"ce": _open.ce.entry, "pe": _open.pe.entry},
        "used": _used,
    }

def close_position(note: str = "scheduled_squareoff") -> Dict:
    """Close both legs using live LTP as exit and realize P&L."""
    global _open, _balance, _realized, _used

    if not _open:
        return {"status": "noop", "message": "no open position"}

    ltp_ce = get_quote(_open.ce.symbol)
    ltp_pe = get_quote(_open.pe.symbol)
    if ltp_ce is None or ltp_pe is None:
        raise RuntimeError("Failed to fetch LTP for CE/PE while closing position")

    _open.ce.exit = float(ltp_ce)
    _open.pe.exit = float(ltp_pe)

    pnl_ce = (_open.ce.exit - _open.ce.entry) * _open.ce.lots * LOT_SIZE
    pnl_pe = (_open.pe.exit - _open.pe.entry) * _open.pe.lots * LOT_SIZE
    pnl_total = float(pnl_ce + pnl_pe)

    _balance += pnl_total
    _realized += pnl_total

    _append_trade_row({
        "action": "SQUAREOFF",
        "symbol_ce": _open.ce.symbol.get("tradingsymbol"),
        "symbol_pe": _open.pe.symbol.get("tradingsymbol"),
        "lots_ce": _open.ce.lots,
        "lots_pe": _open.pe.lots,
        "entry_ce": _open.ce.entry,
        "entry_pe": _open.pe.entry,
        "exit_ce": _open.ce.exit,
        "exit_pe": _open.pe.exit,
        "pnl_ce": round(pnl_ce, 2),
        "pnl_pe": round(pnl_pe, 2),
        "pnl_total": round(pnl_total, 2),
        "note": note,
    })

    _open = None
    _used = 0.0
    _write_position_clear()
    _write_funds_snapshot()

    return {"status": "ok", "pnl": pnl_total}

def funds_snapshot() -> Dict:
    """Used by API/UI to show Balance, P&L, Used."""
    mtm_val = _mtm(_open) if _open else None
    return {
        "balance": round(_balance, 2),
        "pnl": None if mtm_val is None else round(mtm_val, 2),
        "used": "-" if _used == 0 else round(_used, 2),
        "open": None if not _open else {
            "side": _open.side,
            "ce": {"symbol": _open.ce.symbol.get("tradingsymbol"), "lots": _open.ce.lots, "entry": _open.ce.entry},
            "pe": {"symbol": _open.pe.symbol.get("tradingsymbol"), "lots": _open.pe.lots, "entry": _open.pe.entry},
        }
    }
