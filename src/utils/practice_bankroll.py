"""AYO practices as a $300 book even when the OKX demo wallet is larger.

Risk %, daily-loss %, and the hive race card must never use the full demo
equity (e.g. ~$5200). Margin stays whatever you set in settings ($40/$50).
"""

from __future__ import annotations

from typing import Optional

from src.config_loader import CONFIG

DEFAULT_BANKROLL = 300.0


def practice_bankroll_usd() -> float:
    try:
        val = float(CONFIG.get("practice_bankroll_usd") or DEFAULT_BANKROLL)
    except (TypeError, ValueError):
        val = DEFAULT_BANKROLL
    return val if val > 0 else DEFAULT_BANKROLL


def risk_equity(wallet_equity: Optional[float] = None) -> float:
    """Capital used for risk math. Never larger than the practice bankroll."""
    bank = practice_bankroll_usd()
    try:
        wallet = float(wallet_equity) if wallet_equity is not None else 0.0
    except (TypeError, ValueError):
        wallet = 0.0
    if wallet <= 0:
        return bank
    return min(wallet, bank)
