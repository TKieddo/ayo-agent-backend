"""Sneaky Pivot (Doug-style) for the Alpaca stock book — long-only v1.

Mechanics (15m RTH):
- Prior session high / low from previous US cash day
- Opening range = first completed 15m bar of today (09:30 ET)
- Sneaky pivot = first confirmed 3-bar swing low after the open that holds
  above prior-day low (not a full breakdown)
- Entry: completed bar closes above the post-pivot swing high (break of sneaky)
- Stop: under sneaky swing low, clamped by ATR / $ risk knobs
- TP: atr_rr (default 1.5R) with optional scale-out at 1R
- Exit (book cycle): structure break — close back below sneaky low
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from src.config_loader import CONFIG
from src.utils.stock_session import us_regular_session_open

logger = logging.getLogger(__name__)

Candle = Sequence[Any]
ET = ZoneInfo("America/New_York")
RTH_OPEN_MIN = 9 * 60 + 30
RTH_CLOSE_MIN = 16 * 60


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
    }


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


def _bar_et(ts_ms: Any) -> Optional[datetime]:
    try:
        ms = int(ts_ms or 0)
        if ms <= 0:
            return None
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(ET)
    except Exception:
        return None


def _is_rth_bar(ts_ms: Any) -> bool:
    dt = _bar_et(ts_ms)
    if dt is None or dt.weekday() >= 5:
        return False
    mins = dt.hour * 60 + dt.minute
    return RTH_OPEN_MIN <= mins < RTH_CLOSE_MIN


def _session_date(ts_ms: Any) -> Optional[Any]:
    dt = _bar_et(ts_ms)
    return dt.date() if dt else None


def _completed_rth_bars(bars: List[Candle], now: datetime) -> List[Candle]:
    """Drop the forming bar and any non-RTH prints."""
    now_et = now.astimezone(ET) if now.tzinfo else now.replace(tzinfo=timezone.utc).astimezone(ET)
    out: List[Candle] = []
    for c in bars:
        if not _is_rth_bar(c[0]):
            continue
        bt = _bar_et(c[0])
        if bt is None:
            continue
        # 15m bar labeled at start — treat as incomplete if less than 14m old.
        age_s = (now_et - bt).total_seconds()
        if age_s < 14 * 60:
            continue
        out.append(c)
    return out


def _split_sessions(bars: List[Candle]) -> Tuple[List[Candle], List[Candle], Optional[Any]]:
    """Return (today_rth_bars, prior_session_bars, today_date)."""
    if not bars:
        return [], [], None
    dates = [_session_date(c[0]) for c in bars]
    today = next((d for d in reversed(dates) if d is not None), None)
    if today is None:
        return [], [], None
    today_bars = [c for c, d in zip(bars, dates) if d == today]
    prior_dates = sorted({d for d in dates if d is not None and d < today})
    if not prior_dates:
        return today_bars, [], today
    prev = prior_dates[-1]
    prior_bars = [c for c, d in zip(bars, dates) if d == prev]
    return today_bars, prior_bars, today


def _swing_low_index(lows: Sequence[float], start: int, end: int) -> Optional[int]:
    """First confirmed 3-bar swing low in [start, end) where end-1 is confirmed."""
    # Need i-1, i, i+1 all inside today; confirmation requires i+1 exists.
    for i in range(max(start, 1), end - 1):
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            return i
    return None


@dataclass
class SneakySnapshot:
    """Structure levels for entry rank + invalidation exits."""

    entry: float
    atr: float
    prior_high: float
    prior_low: float
    or_high: float
    or_low: float
    sneaky_low: float
    break_level: float
    strength: float
    sneaky_idx: int


def sneaky_from_bars(
    bars_15m: List[Candle],
    *,
    atr_period: int = 14,
    now: Optional[datetime] = None,
) -> Optional[SneakySnapshot]:
    """Build Sneaky Pivot levels from 15m bars. None if structure not ready."""
    now = now or datetime.now(timezone.utc)
    completed = _completed_rth_bars(bars_15m, now)
    if len(completed) < atr_period + 5:
        return None

    today, prior, _ = _split_sessions(completed)
    if len(today) < 3 or len(prior) < 2:
        return None

    h_all = [float(c[2]) for c in completed]
    l_all = [float(c[3]) for c in completed]
    x_all = [float(c[4]) for c in completed]
    atr_val = _atr(h_all, l_all, x_all, atr_period)
    if atr_val is None or atr_val <= 0:
        return None

    prior_high = max(float(c[2]) for c in prior)
    prior_low = min(float(c[3]) for c in prior)
    or_bar = today[0]
    or_high = float(or_bar[2])
    or_low = float(or_bar[3])

    highs = [float(c[2]) for c in today]
    lows = [float(c[3]) for c in today]
    closes = [float(c[4]) for c in today]

    # First swing low after the opening bar, holding above prior-day low.
    pivot_i = _swing_low_index(lows, start=1, end=len(today))
    if pivot_i is None:
        return None
    sneaky_low = lows[pivot_i]
    if sneaky_low < prior_low * 0.999:
        return None  # full breakdown — not a sneaky hold

    # Break the sneaky candle's high (not the running max — that never gets "closed through").
    break_level = highs[pivot_i]
    if break_level <= sneaky_low:
        return None

    entry = closes[-1]
    # Rank: how clean the hold is above prior low + room above OR.
    hold_pct = (sneaky_low - prior_low) / prior_low * 100.0 if prior_low > 0 else 0.0
    strength = hold_pct + max(0.0, (entry - or_high) / atr_val if atr_val else 0.0)

    return SneakySnapshot(
        entry=entry,
        atr=float(atr_val),
        prior_high=prior_high,
        prior_low=prior_low,
        or_high=or_high,
        or_low=or_low,
        sneaky_low=sneaky_low,
        break_level=float(break_level),
        strength=float(strength),
        sneaky_idx=pivot_i,
    )


def should_structure_break(snap: SneakySnapshot, *, last_close: Optional[float] = None) -> bool:
    """Exit long when price closes back below the sneaky swing low."""
    px = snap.entry if last_close is None else float(last_close)
    return px < snap.sneaky_low


# Back-compat aliases so older call sites / imports don't explode mid-deploy.
MomentumSnapshot = SneakySnapshot


def momentum_from_bars(
    bars_15m: List[Candle],
    bars_1h: Optional[List[Candle]] = None,
    *,
    atr_period: int = 14,
) -> Optional[SneakySnapshot]:
    del bars_1h  # unused — Sneaky is 15m structure
    return sneaky_from_bars(bars_15m, atr_period=atr_period)


def should_momentum_fade(snap: SneakySnapshot) -> bool:
    return should_structure_break(snap)


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
    setup: str = "sneaky_pivot"
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
    *,
    structure_stop: Optional[float] = None,
) -> Tuple[float, float, float, float]:
    """Return (stop, take_profit, stop_pct%, risk_usd) clamped to stock knobs.

    Prefer structure_stop (sneaky low) when it sits inside min/max %; otherwise ATR.
    """
    mult = float(cfg["sl_atr_mult"])
    min_pct = float(cfg["min_stop_pct"]) / 100.0
    max_pct = float(cfg["max_stop_pct"]) / 100.0
    risk_cap = float(cfg["risk_usd"])
    notional = float(cfg["notional"])
    rr = float(cfg["tp_rr"])

    atr_pct = (atr * mult / entry) if entry > 0 else min_pct
    stop_frac = max(min_pct, min(max_pct, atr_pct))

    if side == "buy" and structure_stop is not None and entry > 0 and structure_stop < entry:
        struct_frac = (entry - float(structure_stop)) / entry
        if min_pct <= struct_frac <= max_pct:
            stop_frac = max(stop_frac, struct_frac)
            stop_frac = min(stop_frac, max_pct)

    qty = size_qty(entry, notional, int(cfg["leverage"]))
    risk_usd = qty * entry * stop_frac
    if risk_usd > risk_cap and qty > 0 and entry > 0:
        capped = risk_cap / (qty * entry)
        if capped >= min_pct:
            stop_frac = min(stop_frac, capped)
            risk_usd = qty * entry * stop_frac
        else:
            risk_usd = min(risk_usd, risk_cap)

    if side == "buy":
        stop = entry * (1.0 - stop_frac)
        if structure_stop is not None and min_pct <= (entry - float(structure_stop)) / entry <= max_pct:
            stop = min(stop, float(structure_stop))
            stop_frac = (entry - stop) / entry
            risk_usd = qty * (entry - stop)
        take_profit = entry + (entry - stop) * rr
    else:
        stop = entry * (1.0 + stop_frac)
        take_profit = entry - (stop - entry) * rr

    return stop, take_profit, stop_frac * 100.0, risk_usd


def evaluate(
    symbol: str,
    bars_15m: List[Candle],
    bars_1h: Optional[List[Candle]] = None,
    *,
    settings: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> StockSignal | StockSkip:
    """Sneaky Pivot long: break of post-open swing after a held sneaky low."""
    del bars_1h
    cfg = resolve_stock_config(settings)
    now = now or datetime.now(timezone.utc)
    metrics: Dict[str, Any] = {"book": "stocks", "venue": "alpaca", "setup": "sneaky_pivot"}

    open_ok, sess = us_regular_session_open(
        now,
        allow_overnight=bool(cfg["allow_overnight"]),
        entry_cutoff_minutes=int(cfg.get("entry_cutoff_minutes") or 10),
    )
    metrics["session"] = sess
    if not open_ok:
        return StockSkip(symbol, f"session closed ({sess})", metrics)

    snap = sneaky_from_bars(bars_15m, atr_period=int(cfg["atr_period"]), now=now)
    if snap is None:
        return StockSkip(symbol, "sneaky structure not ready", metrics)

    metrics.update(
        {
            "prior_high": round(snap.prior_high, 4),
            "prior_low": round(snap.prior_low, 4),
            "or_high": round(snap.or_high, 4),
            "or_low": round(snap.or_low, 4),
            "sneaky_low": round(snap.sneaky_low, 4),
            "break_level": round(snap.break_level, 4),
            "strength": round(snap.strength, 3),
            "atr": round(snap.atr, 4),
        }
    )

    # Need a completed close through the break level (not just a wick).
    if snap.entry < snap.break_level:
        return StockSkip(
            symbol,
            f"no break yet close={snap.entry:.2f} < break={snap.break_level:.2f}",
            metrics,
        )
    # Prefer longs that cleared the opening-range high (sneaky continuation).
    if snap.entry < snap.or_high:
        return StockSkip(
            symbol,
            f"below OR high {snap.or_high:.2f}",
            metrics,
        )

    side = "buy"
    entry = snap.entry
    stop, tp, stop_pct, risk_usd = geometry(
        entry, snap.atr, side, cfg, structure_stop=snap.sneaky_low
    )
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
        setup="sneaky_pivot",
        scale_out_at_r=float(cfg["scale_out_at_r"]),
        scale_out_fraction=float(cfg["scale_out_fraction"]),
        rationale=(
            f"Sneaky Pivot long break={snap.break_level:.2f} sneaky_low={snap.sneaky_low:.2f} "
            f"OR={snap.or_low:.2f}-{snap.or_high:.2f}; stop {stop_pct:.2f}%; TP {cfg['tp_rr']}R"
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
