"""Account-level circuit breakers.

Per-trade risk control does not stop a bad session compounding: a run of losses in a
regime the strategy cannot read will keep opening trades until the balance is gone. These
breakers pause *new entries* on a daily loss limit or a consecutive-loss streak. Existing
positions are never touched, so stops and targets still manage what is already open.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.config_loader import CONFIG
from src.utils.practice_bankroll import risk_equity

logger = logging.getLogger(__name__)


@dataclass
class SessionState:
    day: str = ""
    realized_pnl: float = 0.0
    trades: int = 0
    consecutive_losses: int = 0
    paused_until: Optional[datetime] = None
    pause_reason: str = ""
    closes: List[Dict[str, Any]] = field(default_factory=list)


_STATE = SessionState()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> str:
    return _now().strftime("%Y-%m-%d")


def _setting(trading_settings: Optional[Dict[str, Any]], key: str, default: Any) -> Any:
    settings = trading_settings or {}
    value = settings.get(key)
    if value is None:
        value = CONFIG.get(key, default)
    return default if value is None else value


def _roll_day_if_needed() -> None:
    today = _today()
    if _STATE.day != today:
        if _STATE.day:
            logger.info(
                f"📅 New trading day {today}: resetting session "
                f"(previous day realised ${_STATE.realized_pnl:.2f} over {_STATE.trades} trades)"
            )
        _STATE.day = today
        _STATE.realized_pnl = 0.0
        _STATE.trades = 0
        _STATE.consecutive_losses = 0
        _STATE.closes = []
        # A daily-limit pause expires with the day; a cooldown pause keeps its own deadline.
        if _STATE.pause_reason.startswith("daily"):
            _STATE.paused_until = None
            _STATE.pause_reason = ""


def record_close(
    asset: str,
    pnl_usd: float,
    trading_settings: Optional[Dict[str, Any]] = None,
    equity: Optional[float] = None,
) -> None:
    """Fold a realised close into the session and trip breakers if thresholds are crossed."""
    _roll_day_if_needed()

    pnl = float(pnl_usd or 0.0)
    _STATE.realized_pnl += pnl
    _STATE.trades += 1
    _STATE.closes.append({"asset": asset, "pnl": pnl, "at": _now().isoformat()})

    # Any loss below $0.00 extends the streak. Any profit above $0.00 clears it.
    if pnl < 0:
        _STATE.consecutive_losses += 1
    elif pnl > 0:
        _STATE.consecutive_losses = 0

    max_streak = int(_setting(trading_settings, "max_consecutive_losses", 4) or 0)
    if max_streak > 0 and _STATE.consecutive_losses >= max_streak:
        _STATE.paused_until = _end_of_day()
        _STATE.pause_reason = (
            f"streak: {_STATE.consecutive_losses} losses in a row, paused until next UTC day"
        )
        logger.warning(f"🚦 Entries paused — {_STATE.pause_reason}")
        return

    limit_usd = _resolve_daily_loss_limit(trading_settings, equity)
    if limit_usd and _STATE.realized_pnl <= -abs(limit_usd):
        _STATE.paused_until = _end_of_day()
        _STATE.pause_reason = (
            f"daily loss limit: ${_STATE.realized_pnl:.2f} realised vs ${-abs(limit_usd):.2f} limit"
        )
        logger.warning(f"🚦 Entries paused for the rest of the day — {_STATE.pause_reason}")


def _end_of_day() -> datetime:
    now = _now()
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _resolve_daily_loss_limit(
    trading_settings: Optional[Dict[str, Any]],
    equity: Optional[float],
) -> Optional[float]:
    # Default session stop is 3× per-trade risk. Explicit max_daily_loss_usd wins
    # unless practice-bankroll % is tighter (always size % off $300, not the demo wallet).
    risk = abs(float(_setting(trading_settings, "risk_per_trade_usd", 6) or 6))
    cap = max(risk, 1.0) * 3.0
    explicit = None
    if trading_settings is not None and "max_daily_loss_usd" in trading_settings:
        explicit = trading_settings.get("max_daily_loss_usd")
    else:
        explicit = CONFIG.get("max_daily_loss_usd")
    try:
        if explicit not in (None, "", 0, "0"):
            cap = abs(float(explicit))
    except (TypeError, ValueError):
        pass

    limit_pct = _setting(trading_settings, "max_daily_loss_pct", 0.0)
    bank = risk_equity(equity)
    if limit_pct and bank > 0:
        cap = min(cap, abs(float(bank) * (float(limit_pct) / 100.0)))
    return cap


def check_can_enter(trading_settings: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str]]:
    """Return ``(allowed, reason_if_blocked)`` for opening a new position."""
    _roll_day_if_needed()

    if not _STATE.paused_until:
        return True, None

    remaining = (_STATE.paused_until - _now()).total_seconds()
    if remaining <= 0:
        logger.info(f"🚦 Entry pause lifted (was: {_STATE.pause_reason})")
        _STATE.paused_until = None
        _STATE.pause_reason = ""
        return True, None

    return False, f"{_STATE.pause_reason}; {remaining / 60:.0f}m remaining"


def status() -> Dict[str, Any]:
    _roll_day_if_needed()
    paused = bool(_STATE.paused_until and _STATE.paused_until > _now())
    return {
        "day": _STATE.day,
        "realized_pnl": round(_STATE.realized_pnl, 2),
        "trades": _STATE.trades,
        "consecutive_losses": _STATE.consecutive_losses,
        "paused": paused,
        "pause_reason": _STATE.pause_reason if paused else "",
        "paused_until": _STATE.paused_until.isoformat() if paused else None,
    }


def resume() -> None:
    """Manually lift a pause (dashboard override)."""
    _STATE.paused_until = None
    _STATE.pause_reason = ""
    _STATE.consecutive_losses = 0
    logger.info("🚦 Entry pause manually cleared")


def reset() -> None:
    global _STATE
    _STATE = SessionState()
