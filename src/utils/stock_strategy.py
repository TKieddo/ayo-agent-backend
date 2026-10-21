"""Orakelia-style momentum for the Alpaca stock book.

Port of the best hive runner (Orakelia) onto equities:
- Session: US regular hours (unless STOCK_ALLOW_OVERNIGHT)
- Entry: long only when ret24h > 0 and vol_z > 0 (soft HTF: 1h close above EMA20)
- Size: fixed notional ($50 default), leverage 1x
- Stop: ATR × STOCK_SL_ATR_MULT, clamped by min/max % and $ risk cap
- TP: atr_rr at STOCK_TP_RR_RATIO (default 1.5R) with optional 50% scale-out at 1R
- Exit (book cycle): momentum_fade when ret1h < 0 or vol_z < 0
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.config_loader import CONFIG
from src.utils.stock_session import us_regular_session_open

logger = logging.getLogger(__name__)

Candle = Sequence[Any]

# 15m bars ≈ one US cash session (~6.5h) and one clock hour.
_BARS_1H_15M = 4
_BARS_1D_15M = 26
_VOL_Z_LOOKBACK = 20


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
        "ema_fast": 20,
        "ema_slow": 50,
        "require_htf_above_ema": bool(g("stock_require_htf_above_ema", True)),
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


def _ret_pct(closes: Sequence[float], bars_back: int) -> Optional[float]:
    if not closes or bars_back <= 0 or len(closes) <= bars_back:
        return None
    base = float(closes[-(bars_back + 1)])
    last = float(closes[-1])
    if base <= 0:
        return None
    return (last - base) / base * 100.0


def _vol_z(volumes: Sequence[float], lookback: int = _VOL_Z_LOOKBACK) -> Optional[float]:
    if not volumes or len(volumes) < lookback + 1:
        return None
    window = [float(v or 0) for v in volumes[-(lookback + 1) : -1]]
    cur = float(volumes[-1] or 0)
    if not window:
        return None
    mean = sum(window) / len(window)
    var = sum((v - mean) ** 2 for v in window) / len(window)
    std = math.sqrt(var)
    if std <= 1e-12:
        return 0.0 if abs(cur - mean) < 1e-9 else (1.0 if cur > mean else -1.0)
    return (cur - mean) / std


@dataclass
class MomentumSnapshot:
    """Orakelia-style metrics from Alpaca bars (used for entry rank + fade exits)."""

    ret1h: float
    ret24h: float
    vol_z: float
    atr: float
    entry: float
    htf_above_ema: bool
    strength: float  # rank key: ret24h + soft vol boost


def momentum_from_bars(
    bars_15m: List[Candle],
    bars_1h: Optional[List[Candle]] = None,
    *,
    atr_period: int = 14,
) -> Optional[MomentumSnapshot]:
    """Compute ret1h / ret24h / vol_z / ATR from bar lists. None if not enough data."""
    if not bars_15m or len(bars_15m) < max(atr_period + 5, _BARS_1D_15M + 2):
        return None

    h = [float(c[2]) for c in bars_15m]
    l = [float(c[3]) for c in bars_15m]
    x = [float(c[4]) for c in bars_15m]
    v = [float(c[5] or 0) for c in bars_15m]

    atr_val = _atr(h, l, x, atr_period)
    if atr_val is None or atr_val <= 0:
        return None

    ret1h = _ret_pct(x, _BARS_1H_15M)
    ret24h = _ret_pct(x, _BARS_1D_15M)
    # Prefer true 1h bars for ret1h when available.
    if bars_1h and len(bars_1h) >= 3:
        htf_closes = [float(c[4]) for c in bars_1h]
        r1 = _ret_pct(htf_closes, 1)
        if r1 is not None:
            ret1h = r1

    vol_z = _vol_z(v)
    if ret1h is None or ret24h is None or vol_z is None:
        return None

    htf = bars_1h if bars_1h and len(bars_1h) >= 25 else bars_15m
    htf_x = [float(c[4]) for c in htf]
    ema20 = _ema(htf_x, 20)
    htf_above = bool(ema20 and htf_x[-1] >= ema20[-1] * 0.998)

    strength = float(ret24h) + max(0.0, float(vol_z)) * 0.5
    return MomentumSnapshot(
        ret1h=float(ret1h),
        ret24h=float(ret24h),
        vol_z=float(vol_z),
        atr=float(atr_val),
        entry=float(x[-1]),
        htf_above_ema=htf_above,
        strength=strength,
    )


def should_momentum_fade(snap: MomentumSnapshot) -> bool:
    """Orakelia exit: 1h turns down or volume z-score goes cold."""
    return snap.ret1h < 0 or snap.vol_z < 0


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
    setup: str = "orakelia_momentum"
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

    qty = size_qty(entry, notional, int(cfg["leverage"]))
    risk_usd = qty * entry * stop_pct
    if risk_usd > risk_cap and qty > 0:
        capped_pct = risk_cap / (qty * entry) if qty * entry > 0 else stop_pct
        if capped_pct >= min_pct:
            stop_pct = min(stop_pct, capped_pct)
            risk_usd = qty * entry * stop_pct
        else:
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
    """Orakelia momentum: long only when day is green and volume is elevated."""
    cfg = resolve_stock_config(settings)
    now = now or datetime.now(timezone.utc)
    metrics: Dict[str, Any] = {"book": "stocks", "venue": "alpaca", "setup": "orakelia_momentum"}

    open_ok, sess = us_regular_session_open(
        now,
        allow_overnight=bool(cfg["allow_overnight"]),
        entry_cutoff_minutes=int(cfg.get("entry_cutoff_minutes") or 10),
    )
    metrics["session"] = sess
    if not open_ok:
        return StockSkip(symbol, f"session closed ({sess})", metrics)

    snap = momentum_from_bars(bars_15m, bars_1h, atr_period=int(cfg["atr_period"]))
    if snap is None:
        return StockSkip(symbol, "insufficient bars for momentum", metrics)

    metrics.update(
        {
            "ret1h": round(snap.ret1h, 3),
            "ret24h": round(snap.ret24h, 3),
            "vol_z": round(snap.vol_z, 3),
            "strength": round(snap.strength, 3),
            "htf_above_ema": snap.htf_above_ema,
            "atr": round(snap.atr, 4),
        }
    )

    # Long-only Orakelia: green day + elevated volume.
    if snap.ret24h <= 0:
        return StockSkip(symbol, f"ret24h {snap.ret24h:.2f}% <= 0", metrics)
    if snap.vol_z <= 0:
        return StockSkip(symbol, f"vol_z {snap.vol_z:.2f} <= 0", metrics)
    if cfg.get("require_htf_above_ema") and not snap.htf_above_ema:
        return StockSkip(symbol, "HTF below EMA20", metrics)

    side = "buy"
    entry = snap.entry
    stop, tp, stop_pct, risk_usd = geometry(entry, snap.atr, side, cfg)
    qty = size_qty(entry, float(cfg["notional"]), int(cfg["leverage"]))
    if qty <= 0:
        return StockSkip(symbol, "qty too small", metrics)

    metrics.update(
        {
            "stop_pct": round(stop_pct, 3),
            "risk_usd": round(risk_usd, 2),
            "notional": float(cfg["notional"]),
            "rr": float(cfg["tp_rr"]),
        }
    )

    return StockSignal(
        symbol=symbol.upper(),
        side=side,
        entry=entry,
        stop=stop,
        take_profit=tp,
        qty=qty,
        notional=float(cfg["notional"]),
        risk_usd=risk_usd,
        atr=snap.atr,
        stop_pct=stop_pct,
        rr=float(cfg["tp_rr"]),
        setup="orakelia_momentum",
        scale_out_at_r=float(cfg["scale_out_at_r"]),
        scale_out_fraction=float(cfg["scale_out_fraction"]),
        rationale=(
            f"Orakelia long ret24h={snap.ret24h:.2f}% vol_z={snap.vol_z:.2f}; "
            f"ATR stop {stop_pct:.2f}%; TP {cfg['tp_rr']}R"
        ),
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
