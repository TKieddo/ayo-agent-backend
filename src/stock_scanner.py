"""Stock scanner — ranks STOCK_ASSETS for the Alpaca book only.

Sibling of Pair Hunter, but never touches Binance heat or OKX. Scores each
curated equity by liquidity (dollar volume), ATR%, and HTF trend strength;
top N become the active stock trade set for that session.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.config_loader import CONFIG

logger = logging.getLogger(__name__)

Candle = Sequence[Any]  # [ts_ms, open, high, low, close, volume]


def stock_universe() -> List[str]:
    raw = CONFIG.get("stock_assets") or ""
    return [p.strip().upper() for p in str(raw).replace(",", " ").split() if p.strip()]


def _ema(values: Sequence[float], period: int) -> List[float]:
    if not values or period <= 0:
        return []
    k = 2.0 / (period + 1)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(out[-1] + k * (float(v) - out[-1]))
    return out


def _atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(closes)):
        h, l, pc = float(highs[i]), float(lows[i]), float(closes[i - 1])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


@dataclass
class StockScore:
    symbol: str
    score: float
    atr_pct: float
    dollar_volume: float
    trend_strength: float
    trend: str  # up | down | flat
    price: float
    reasons: List[str] = field(default_factory=list)


def score_symbol(
    symbol: str,
    bars_15m: List[Candle],
    bars_1h: Optional[List[Candle]] = None,
    *,
    min_dollar_volume: float = 5_000_000.0,
    min_atr_pct: float = 0.4,
    max_atr_pct: float = 4.0,
    atr_period: int = 14,
) -> Optional[StockScore]:
    """Score one equity from Alpaca bars. Returns None when hard gates fail."""
    if not bars_15m or len(bars_15m) < atr_period + 5:
        return None

    closes = [float(c[4]) for c in bars_15m]
    highs = [float(c[2]) for c in bars_15m]
    lows = [float(c[3]) for c in bars_15m]
    vols = [float(c[5] or 0) for c in bars_15m]
    price = closes[-1]
    if price <= 0:
        return None

    atr_val = _atr(highs, lows, closes, atr_period)
    if atr_val is None or atr_val <= 0:
        return None
    atr_pct = (atr_val / price) * 100.0

    # Approx dollar volume over last ~20 bars
    look = min(20, len(closes))
    dollar_volume = sum(closes[-look + i] * vols[-look + i] for i in range(look))

    reasons: List[str] = []
    if dollar_volume < min_dollar_volume:
        reasons.append(f"illiquid dv=${dollar_volume:,.0f}")
        return None
    if atr_pct < min_atr_pct:
        reasons.append(f"quiet ATR {atr_pct:.2f}%")
        return None
    if atr_pct > max_atr_pct:
        reasons.append(f"wild ATR {atr_pct:.2f}%")
        return None

    # Prefer mid-range ATR (sweet spot ~1.0–2.0%)
    ideal = 1.5
    vol_score = max(0.0, 100.0 - abs(atr_pct - ideal) * 30.0)

    htf = bars_1h if bars_1h and len(bars_1h) >= 55 else bars_15m
    htf_closes = [float(c[4]) for c in htf]
    fast = _ema(htf_closes, 20)
    slow = _ema(htf_closes, 50)
    if not fast or not slow:
        return None
    sep_pct = abs(fast[-1] - slow[-1]) / price * 100.0
    if fast[-1] > slow[-1] * 1.001:
        trend = "up"
    elif fast[-1] < slow[-1] * 0.999:
        trend = "down"
    else:
        trend = "flat"
    trend_strength = min(100.0, sep_pct * 40.0)
    if trend == "flat":
        trend_strength *= 0.25

    liq_score = min(100.0, math.log10(max(dollar_volume, 1.0)) * 12.0)
    score = 0.35 * liq_score + 0.30 * vol_score + 0.35 * trend_strength
    reasons.append(f"ATR {atr_pct:.2f}% · trend {trend} · dv=${dollar_volume:,.0f}")

    return StockScore(
        symbol=symbol.upper(),
        score=round(score, 2),
        atr_pct=round(atr_pct, 3),
        dollar_volume=round(dollar_volume, 2),
        trend_strength=round(trend_strength, 2),
        trend=trend,
        price=price,
        reasons=reasons,
    )


async def scan_stocks(
    alpaca_client: Any,
    *,
    top_n: Optional[int] = None,
    pool: Optional[List[str]] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Rank the stock pool; return (active symbols, score rows for persistence)."""
    top_n = int(top_n or CONFIG.get("stock_scanner_top_n") or 5)
    pool = pool or stock_universe()
    if not pool:
        logger.warning("Stock scanner: STOCK_ASSETS empty")
        return [], []

    min_dv = float(CONFIG.get("stock_min_dollar_volume") or 5_000_000)
    min_atr = float(CONFIG.get("stock_min_atr_pct") or 0.4)
    max_atr = float(CONFIG.get("stock_max_atr_pct") or 4.0)
    atr_period = int(CONFIG.get("stock_atr_period") or 14)
    interval = str(CONFIG.get("stock_interval") or "15m")
    htf_interval = str(CONFIG.get("stock_htf_interval") or "1h")

    scored: List[StockScore] = []
    for sym in pool:
        try:
            bars_15 = await alpaca_client.get_stock_bars(sym, interval=interval, limit=120)
            bars_1h = await alpaca_client.get_stock_bars(sym, interval=htf_interval, limit=80)
            row = score_symbol(
                sym,
                bars_15 or [],
                bars_1h or [],
                min_dollar_volume=min_dv,
                min_atr_pct=min_atr,
                max_atr_pct=max_atr,
                atr_period=atr_period,
            )
            if row:
                scored.append(row)
        except Exception as e:
            logger.debug(f"Stock scanner skip {sym}: {e}")

    scored.sort(key=lambda s: s.score, reverse=True)
    top = scored[:top_n]
    active = [s.symbol for s in top]
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "asset": s.symbol,
            "book": "stocks",
            "score": s.score,
            "atr_pct": s.atr_pct,
            "dollar_volume": s.dollar_volume,
            "trend_strength": s.trend_strength,
            "trend": s.trend,
            "price": s.price,
            "reason": "; ".join(s.reasons),
            "rank": i + 1,
            "last_updated": now,
            "updated_at": now,
        }
        for i, s in enumerate(top)
    ]
    logger.info(f"📈 STOCK SCANNER top {top_n}: {active}")
    return active, rows
