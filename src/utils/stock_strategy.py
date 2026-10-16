"""Equity trend + pullback strategy for the Alpaca stock book.

Distinct from crypto mechanical scalp:
- Session: US regular hours (unless STOCK_ALLOW_OVERNIGHT)
- Size: fixed notional ($50 default), leverage 1x, effective equity ~$200
- Stop: ATR × STOCK_SL_ATR_MULT, clamped by min/max % and $ risk cap
- TP: atr_rr at STOCK_TP_RR_RATIO (default 1.5R) with optional 50% scale-out at 1R
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.config_loader import CONFIG
from src.utils.stock_session import us_regular_session_open

logger = logging.getLogger(__name__)

Candle = Sequence[Any]


def resolve_stock_config(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Merge env CONFIG with optional DB trading_settings stock_* overrides."""
    s = settings or {}
    def g(key: str, default: Any = None) -> Any:
        if s.get(key) is not None and s.get(key) != "":
            return s.get(key)
        return CONFIG.get(key, default)

    return {
        "notional": float(g("stock_notional_per_position", 50.0) or 50.0),
        "risk_usd": float(g("stock_risk_per_trade_usd", 8.0) or 8.0),
        "sl_atr_mult": float(g("stock_sl_atr_mult", 1.5) or 1.5),
        "tp_mode": str(g("stock_tp_mode", "atr_rr") or "atr_rr"),
        "tp_rr": float(g("stock_tp_rr_ratio", 1.5) or 1.5),
        "scale_out_at_r": float(g("stock_scale_out_at_r", 1.0) or 1.0),
        "scale_out_fraction": float(g("stock_scale_out_fraction", 0.5) or 0.5),
        "max_positions": int(g("stock_max_positions", 3) or 3),
        "leverage": int(g("stock_leverage", 1) or 1),
        "effective_equity": float(g("stock_effective_equity", 200.0) or 200.0),
        "min_stop_pct": float(g("stock_min_stop_price_pct", 0.8) or 0.8),
        "max_stop_pct": float(g("stock_max_stop_price_pct", 3.5) or 3.5),
        "atr_period": int(g("stock_atr_period", 14) or 14),
        "allow_overnight": bool(g("stock_allow_overnight", False)),
        "entry_cutoff_minutes": int(g("stock_entry_cutoff_minutes", 10) or 10),
        "interval": str(g("stock_interval", "15m") or "15m"),
        "htf_interval": str(g("stock_htf_interval", "1h") or "1h"),
        "pullback_lookback": int(g("stock_pullback_lookback", 5) or 5),
        "ema_fast": 20,
        "ema_slow": 50,
    }


def _ema(values: Sequence[float], period: int) -> List[float]:
    if not values:
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
class StockSignal:
    symbol: str
    side: str  # buy | sell
    entry: float
    stop: float
    take_profit: float
    qty: float
    notional: float
    risk_usd: float
    atr: float
    stop_pct: float
    rr: float
    setup: str = "trend_pullback"
    scale_out_at_r: float = 1.0
    scale_out_fraction: float = 0.5
    rationale: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_long(self) -> bool:
        return self.side == "buy"


@dataclass
class StockSkip:
    symbol: str
    reason: str
    metrics: Dict[str, Any] = field(default_factory=dict)


def size_qty(price: float, notional: float, leverage: int = 1) -> float:
    if price <= 0 or notional <= 0:
        return 0.0
    lev = max(1, int(leverage or 1))
    return round((notional * lev) / price, 4)


def geometry(
    entry: float,
    atr: float,
    side: str,
    cfg: Dict[str, Any],
) -> Tuple[float, float, float, float]:
    """Return (stop, take_profit, stop_pct, risk_usd) clamped to stock knobs."""
    mult = float(cfg["sl_atr_mult"])
    min_pct = float(cfg["min_stop_pct"]) / 100.0
    max_pct = float(cfg["max_stop_pct"]) / 100.0
    risk_cap = float(cfg["risk_usd"])
    notional = float(cfg["notional"])
    rr = float(cfg["tp_rr"])

    raw_stop_dist = atr * mult
    stop_pct = raw_stop_dist / entry if entry > 0 else min_pct
    stop_pct = max(min_pct, min(max_pct, stop_pct))

    # Cap $ risk: if stop is wide, shrink effective risk reporting (qty still notional-based)
    qty = size_qty(entry, notional, int(cfg["leverage"]))
    risk_usd = qty * entry * stop_pct
    if risk_usd > risk_cap and qty > 0:
        # Tighten stop to risk cap when still within min_pct
        capped_pct = risk_cap / (qty * entry) if qty * entry > 0 else stop_pct
        if capped_pct >= min_pct:
            stop_pct = min(stop_pct, capped_pct)
            risk_usd = qty * entry * stop_pct
        else:
            # Stop already at floor — accept risk_cap as soft label; size stays $50
            risk_usd = min(risk_usd, risk_cap)

    if side == "buy":
        stop = entry * (1.0 - stop_pct)
        take_profit = entry * (1.0 + stop_pct * rr)
    else:
        stop = entry * (1.0 + stop_pct)
        take_profit = entry * (1.0 - stop_pct * rr)

    return stop, take_profit, stop_pct * 100.0, risk_usd


def evaluate(
    symbol: str,
    bars_15m: List[Candle],
    bars_1h: Optional[List[Candle]] = None,
    *,
    settings: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> StockSignal | StockSkip:
    cfg = resolve_stock_config(settings)
    now = now or datetime.now(timezone.utc)
    metrics: Dict[str, Any] = {"book": "stocks", "venue": "alpaca"}

    open_ok, sess = us_regular_session_open(
        now,
        allow_overnight=bool(cfg["allow_overnight"]),
        entry_cutoff_minutes=int(cfg.get("entry_cutoff_minutes") or 10),
    )
    metrics["session"] = sess
    if not open_ok:
        return StockSkip(symbol, f"session closed ({sess})", metrics)

    if not bars_15m or len(bars_15m) < cfg["atr_period"] + 25:
        return StockSkip(symbol, "insufficient 15m bars", metrics)

    o = [float(c[1]) for c in bars_15m]
    h = [float(c[2]) for c in bars_15m]
    l = [float(c[3]) for c in bars_15m]
    x = [float(c[4]) for c in bars_15m]
    atr_val = _atr(h, l, x, int(cfg["atr_period"]))
    if atr_val is None or atr_val <= 0:
        return StockSkip(symbol, "ATR unavailable", metrics)

    htf = bars_1h if bars_1h and len(bars_1h) >= 55 else bars_15m
    htf_x = [float(c[4]) for c in htf]
    fast = _ema(htf_x, int(cfg["ema_fast"]))
    slow = _ema(htf_x, int(cfg["ema_slow"]))
    if not fast or not slow:
        return StockSkip(symbol, "HTF EMA unavailable", metrics)

    if fast[-1] > slow[-1] * 1.001:
        trend = "up"
        side = "buy"
    elif fast[-1] < slow[-1] * 0.999:
        trend = "down"
        side = "sell"
    else:
        return StockSkip(symbol, "HTF flat — no edge", metrics)
    metrics["htf_trend"] = trend

    # Intraday EMA pullback continuation
    ema20 = _ema(x, 20)
    i = len(x) - 1
    entry = x[i]
    lookback = int(cfg["pullback_lookback"])
    pulled = False
    for j in range(i - 1, max(i - 1 - lookback, 20), -1):
        if side == "buy" and l[j] <= ema20[j] and x[j] >= ema20[j] * 0.995:
            pulled = True
            break
        if side == "sell" and h[j] >= ema20[j] and x[j] <= ema20[j] * 1.005:
            pulled = True
            break
    if not pulled:
        return StockSkip(symbol, "no pullback to EMA20", metrics)

    # Confirm candle with trend
    if side == "buy" and not (x[i] > o[i] and x[i] >= ema20[i]):
        return StockSkip(symbol, "no bullish confirm", metrics)
    if side == "sell" and not (x[i] < o[i] and x[i] <= ema20[i]):
        return StockSkip(symbol, "no bearish confirm", metrics)

    stop, tp, stop_pct, risk_usd = geometry(entry, atr_val, side, cfg)
    qty = size_qty(entry, float(cfg["notional"]), int(cfg["leverage"]))
    if qty <= 0:
        return StockSkip(symbol, "qty too small", metrics)

    metrics.update({
        "atr": round(atr_val, 4),
        "stop_pct": round(stop_pct, 3),
        "risk_usd": round(risk_usd, 2),
        "notional": float(cfg["notional"]),
        "rr": float(cfg["tp_rr"]),
    })

    return StockSignal(
        symbol=symbol.upper(),
        side=side,
        entry=entry,
        stop=stop,
        take_profit=tp,
        qty=qty,
        notional=float(cfg["notional"]),
        risk_usd=risk_usd,
        atr=atr_val,
        stop_pct=stop_pct,
        rr=float(cfg["tp_rr"]),
        scale_out_at_r=float(cfg["scale_out_at_r"]),
        scale_out_fraction=float(cfg["scale_out_fraction"]),
        rationale=f"HTF {trend} pullback; ATR stop {stop_pct:.2f}%; TP {cfg['tp_rr']}R",
        metrics=metrics,
    )


def scale_out_plan(signal: StockSignal) -> List[Dict[str, Any]]:
    """Half off at 1R, remainder at full TP (stop → BE after first rung)."""
    frac = max(0.0, min(1.0, float(signal.scale_out_fraction)))
    if frac <= 0:
        return [{"r": signal.rr, "fraction": 1.0, "price": signal.take_profit}]
    r1 = float(signal.scale_out_at_r)
    stop_dist = abs(signal.entry - signal.stop)
    if signal.is_long:
        r1_px = signal.entry + stop_dist * r1
    else:
        r1_px = signal.entry - stop_dist * r1
    return [
        {"r": r1, "fraction": frac, "price": r1_px, "move_stop_to_be": True},
        {"r": signal.rr, "fraction": 1.0 - frac, "price": signal.take_profit, "move_stop_to_be": False},
    ]
