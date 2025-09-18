# service/engine/selector.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple, Optional, List

import math
import warnings

import pandas as pd

# Optional: only used if a model.pkl exists
try:
    import joblib  # type: ignore
except Exception:  # pragma: no cover
    joblib = None  # graceful fallback


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
ML_DIR = ROOT / "ml"
MODEL_PATH = ML_DIR / "models" / "model.pkl"

# -------------------------------
# Utility: find a recent OHLC file
# -------------------------------
CANDIDATE_FILENAMES: List[str] = [
    "intraday_nifty.csv",
    "nifty_intraday.csv",
    "nifty_5min.csv",
    "NIFTY_5min.csv",
    "nifty_15min.csv",
    "NIFTY_15min.csv",
    "ohlc.csv",
]

def _find_ohlc_csv() -> Optional[Path]:
    # 1) direct candidates in /data
    for name in CANDIDATE_FILENAMES:
        p = DATA_DIR / name
        if p.exists():
            return p
    # 2) any csv under /data that looks like OHLC
    best: Optional[Path] = None
    latest_mtime = -1.0
    for p in DATA_DIR.rglob("*.csv"):
        try:
            # quick sniff
            head = pd.read_csv(p, nrows=1)
            cols = {c.strip().lower() for c in head.columns}
            if {"open", "high", "low", "close"}.issubset(cols):
                m = p.stat().st_mtime
                if m > latest_mtime:
                    latest_mtime = m
                    best = p
        except Exception:
            continue
    return best

# -------------------------------
# Technical Indicators
# -------------------------------
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(n).mean()
    avg_loss = loss.rolling(n).mean()
    rs = avg_gain / (avg_loss.replace(0, 1e-12))
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist

def _slope(series: pd.Series, window: int = 5) -> float:
    """Simple slope (% change over window)."""
    if len(series) < window + 1:
        return 0.0
    a = series.iloc[-window]
    b = series.iloc[-1]
    if a == 0 or pd.isna(a) or pd.isna(b):
        return 0.0
    return ((b - a) / abs(a)) * 100.0

# -------------------------------
# Feature preparation
# -------------------------------
def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    for need in ["open", "high", "low", "close"]:
        if need not in {c.lower() for c in df.columns}:
            raise ValueError(f"OHLC column '{need}' missing in {list(df.columns)}")
    # normalize column names
    o = cols[[c for c in cols if c.lower() == "open"][0]]
    h = cols[[c for c in cols if c.lower() == "high"][0]]
    l = cols[[c for c in cols if c.lower() == "low"][0]]
    c = cols[[c for c in cols if c.lower() == "close"][0]]

    df = df.copy()
    df.rename(columns={o: "open", h: "high", l: "low", c: "close"}, inplace=True)

    # indicators
    df["ema20"] = _ema(df["close"], 20)
    df["ema50"] = _ema(df["close"], 50)
    df["rsi14"] = _rsi(df["close"], 14)
    macd, sig, hist = _macd(df["close"], 12, 26, 9)
    df["macd"] = macd
    df["macd_signal"] = sig
    df["macd_hist"] = hist

    df["ret1"] = df["close"].pct_change(1)
    df["ret5"] = df["close"].pct_change(5)
    df["ema20_slope"] = df["ema20"].pct_change(5)

    df = df.dropna().reset_index(drop=True)
    return df

# -------------------------------
# Rule-based ensemble (fallback)
# -------------------------------
def _rule_based_signal(df: pd.DataFrame) -> Tuple[str, float]:
    """
    Voting system from EMA50 trend, MACD histogram, RSI band, and EMA20 slope.
    Returns ("UP"|"DOWN", confidence 0..1).
    """
    if len(df) == 0:
        # extreme fallback if no data
        return ("UP", 0.55)

    last = df.iloc[-1]
    votes_up = 0
    votes_down = 0
    total_votes = 0

    # 1) Trend vs EMA50
    total_votes += 1
    if last["close"] > last["ema50"]:
        votes_up += 1
    else:
        votes_down += 1

    # 2) MACD histogram sign
    total_votes += 1
    if last["macd_hist"] > 0:
        votes_up += 1
    else:
        votes_down += 1

    # 3) RSI regime
    total_votes += 1
    if last["rsi14"] >= 55:
        votes_up += 1
    elif last["rsi14"] <= 45:
        votes_down += 1
    else:
        # neutral → use distance to 50 to split fractional vote
        dist = abs(last["rsi14"] - 50) / 10.0  # 0..0.5 influence
        if last["rsi14"] > 50:
            votes_up += dist
        else:
            votes_down += dist

    # 4) Short-term momentum: EMA20 slope
    total_votes += 1
    slope20 = _slope(df["ema20"], 5)
    if slope20 > 0:
        votes_up += 1
    elif slope20 < 0:
        votes_down += 1

    # Direction
    direction = "UP" if votes_up >= votes_down else "DOWN"

    # Confidence: soft-maxed vote margin, bounded [0.55, 0.90]
    margin = abs(votes_up - votes_down) / max(1.0, total_votes)
    conf = 0.55 + min(0.35, margin)  # 0.55..0.90
    conf = float(round(conf, 3))
    return (direction, conf)

# -------------------------------
# ML path (optional)
# -------------------------------
def _ml_predict(df: pd.DataFrame) -> Optional[Tuple[str, float]]:
    """
    If a scikit-learn model exists at ml/models/model.pkl AND joblib is available,
    we use it. The model is expected to have either:
      - predict_proba(X) → [:,1] = prob(UP), or
      - decision_function(X) + a logistic squashing for a pseudo-probability.
    Features used: close, ema20, ema50, rsi14, macd, macd_signal, macd_hist, ret1, ret5, ema20_slope
    """
    if not MODEL_PATH.exists() or joblib is None:
        return None

    try:
        model = joblib.load(MODEL_PATH)
    except Exception:
        return None

    feats = ["close","ema20","ema50","rsi14","macd","macd_signal","macd_hist","ret1","ret5","ema20_slope"]
    X = df[feats].tail(1).values

    prob_up = None
    try:
        if hasattr(model, "predict_proba"):
            prob_up = float(model.predict_proba(df[feats].tail(1))[:, 1][0])
        elif hasattr(model, "decision_function"):
            val = float(model.decision_function(X)[0])
            # logistic squash to 0..1
            prob_up = 1.0 / (1.0 + math.exp(-val))
    except Exception:
        prob_up = None

    if prob_up is None:
        return None

    direction = "UP" if prob_up >= 0.5 else "DOWN"
    # clamp confidence into sensible range [0.55, 0.92] to avoid overconfidence
    conf = 0.55 + (min(0.37, abs(prob_up - 0.5) * 2.0))  # distance from 0.5 → 0..1 → 0..0.37
    conf = float(round(conf, 3))
    return (direction, conf)

# -------------------------------
# Public API
# -------------------------------
def predict() -> Tuple[str, float]:
    """
    Determine market direction ("UP"/"DOWN") and confidence (0..1).
    Priority:
      1) Use local ML model if available (ml/models/model.pkl)
      2) Else use rule-based technical ensemble (deterministic)
    """
    warnings.filterwarnings("ignore")

    csv_path = _find_ohlc_csv()
    if not csv_path:
        # no price data at all — still return something deterministic
        return ("UP", 0.58)

    try:
        df = pd.read_csv(csv_path)
        df = _prepare_features(df)
        if len(df) == 0:
            return ("UP", 0.57)
    except Exception:
        # parsing/feature failure → deterministic fallback
        return ("UP", 0.56)

    # Try ML first
    ml_res = _ml_predict(df)
    if ml_res is not None:
        return ml_res

    # Deterministic technical ensemble
    return _rule_based_signal(df)
