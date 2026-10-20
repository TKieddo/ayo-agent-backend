"""Utility functions for fetching and applying trading settings (leverage, TP%, SL%)."""

import logging
import aiohttp
import json
import os
import time
from typing import Dict, Any, Optional
from pathlib import Path
from src.config_loader import CONFIG
from src.utils.position_sizing import calculate_position_size, calculate_profit
from src.utils.mechanical_scalp import DEFAULTS as MECH_DEFAULTS, apply_mechanical_overrides, is_mechanical_mode

CACHE_DIR = Path("settings_cache")
CACHE_FILE = CACHE_DIR / "trading_settings_cache.json"


def _coalesce(*values: Any) -> Any:
    """Return the first value that is not None (DB nulls still count as present for dict.get)."""
    for value in values:
        if value is not None:
            return value
    return None


def resolve_max_loss_usd(trading_settings: Dict[str, Any], default: float = 6.0) -> float:
    """Hard max $ loss per trade — what the user set as risk.

    Prefers ``risk_per_trade_usd``. If ``stop_loss_usd`` is also set, uses the
    *tighter* (smaller) of the two so a gap-buffer of -9 never raises a $6 risk to $9.
    """
    candidates: list[float] = []
    for key in ("risk_per_trade_usd", "stop_loss_usd"):
        raw = trading_settings.get(key)
        if raw is None or raw == "":
            continue
        try:
            val = abs(float(raw))
        except (TypeError, ValueError):
            continue
        if val > 0:
            candidates.append(val)
    if candidates:
        return min(candidates)
    return float(default)


def resolve_stop_loss_usd(trading_settings: Dict[str, Any], default: float = -6.0) -> float:
    """Negative USD stop ceiling used by mechanical close checks (hard, no buffer)."""
    return -abs(resolve_max_loss_usd(trading_settings, default=abs(default)))


def projected_loss_at_stop(
    entry_price: float,
    quantity: float,
    stop_price_pct: float,
) -> float:
    """Dollar loss if price moves ``stop_price_pct`` % against the position."""
    try:
        entry = float(entry_price or 0)
        qty = abs(float(quantity or 0))
        stop_pct = abs(float(stop_price_pct or 0))
    except (TypeError, ValueError):
        return 0.0
    if entry <= 0 or qty <= 0 or stop_pct <= 0:
        return 0.0
    notional = entry * qty
    return notional * (stop_pct / 100.0)


def align_take_profit_to_risk(settings: Dict[str, Any], reward_multiple: float = 2.0) -> Dict[str, Any]:
    """Ensure take-profit targets at least ``reward_multiple`` × max loss (fixes $1 wins vs $6 losses).

    Skipped for ``price_percent`` and ``atr_rr`` — those modes already express an intentional
    price target and must not rewrite the user's TAKE_PROFIT_PERCENT.
    """
    try:
        from src.utils.volatility_stops import normalize_tp_mode

        tp_mode = normalize_tp_mode(settings.get("tp_mode") or "roi_percent")
        settings["tp_mode"] = tp_mode
        # User-facing price % and ATR R:R are intentional — never rewrite the %.
        if tp_mode in ("price_percent", "atr_rr"):
            return settings

        max_loss = resolve_max_loss_usd(settings, default=6.0)
        target_tp = max(max_loss * float(reward_multiple or 2.0), max_loss)
        margin = float(settings.get("margin_per_position") or CONFIG.get("margin_per_position") or 30.0)

        if tp_mode == "usd":
            current = settings.get("take_profit_usd")
            try:
                current_f = float(current) if current not in (None, "") else 0.0
            except (TypeError, ValueError):
                current_f = 0.0
            if current_f < target_tp:
                settings["take_profit_usd"] = target_tp
                logging.info(
                    f"🎯 Aligned take_profit_usd to ${target_tp:.2f} "
                    f"(≥ {reward_multiple:g}× max loss ${max_loss:.2f})"
                )
        elif tp_mode == "roi_percent":
            # roi_percent: TP $ ≈ margin × (tp% / 100)
            try:
                tp_pct = float(settings.get("take_profit_percent") or 0)
            except (TypeError, ValueError):
                tp_pct = 0.0
            tp_usd = margin * (tp_pct / 100.0) if margin > 0 else 0.0
            if tp_usd < target_tp and margin > 0:
                needed_pct = (target_tp / margin) * 100.0
                settings["take_profit_percent"] = round(needed_pct, 2)
                logging.info(
                    f"🎯 Aligned take_profit_percent (ROI) to {needed_pct:.1f}% "
                    f"(≈ ${target_tp:.2f} on ${margin:.0f} margin, ≥ {reward_multiple:g}× risk)"
                )
    except Exception as e:
        logging.debug(f"align_take_profit_to_risk skipped: {e}")
    return settings


def tighten_sl_to_max_loss(
    entry_price: float,
    is_long: bool,
    quantity: float,
    sl_price: Optional[float],
    max_loss_usd: float,
    *,
    allow_tighter_than_atr: bool = True,
) -> Optional[float]:
    """Pull SL so dollar loss at the stop ≤ max_loss_usd.

    Default ``allow_tighter_than_atr=True``: the user's $ risk is law. If an ATR stop
    would lose more than max_loss_usd on this size, the stop is yanked closer.
    """
    try:
        entry = float(entry_price or 0)
        qty = abs(float(quantity or 0))
        max_loss = float(max_loss_usd or 0)
    except (TypeError, ValueError):
        return sl_price
    if entry <= 0 or qty <= 0 or max_loss <= 0:
        return sl_price
    delta = max_loss / qty
    usd_sl = entry - delta if is_long else entry + delta
    if sl_price is None:
        return usd_sl
    try:
        existing = float(sl_price)
    except (TypeError, ValueError):
        return usd_sl
    if not allow_tighter_than_atr:
        # Keep the wider (further from entry) stop — ATR room for chop.
        return min(existing, usd_sl) if is_long else max(existing, usd_sl)
    # Tighter = closer to entry (respect $ risk)
    return max(existing, usd_sl) if is_long else min(existing, usd_sl)


def _save_cached_trading_settings(settings: Dict[str, Any]) -> None:
    """Persist latest successful DB settings for outage fallback."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "cached_at": int(time.time()),
            "settings": settings,
        }
        with CACHE_FILE.open("w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception as e:
        logging.debug(f"Could not save trading settings cache: {e}")


def _load_cached_trading_settings() -> Optional[Dict[str, Any]]:
    """Load cached DB settings when API/database is temporarily unavailable."""
    try:
        if not CACHE_FILE.exists():
            return None
        with CACHE_FILE.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        cached = payload.get("settings")
        return cached if isinstance(cached, dict) else None
    except Exception as e:
        logging.debug(f"Could not load trading settings cache: {e}")
        return None


def _apply_env_bool_overrides(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Allow explicit Railway env booleans to override DB/cached values."""
    val = os.getenv("AGENT_MANAGE_EXITS")
    if val is not None:
        settings["agent_manage_exits"] = val.strip().lower() in {"1", "true", "yes", "on"}
    val = os.getenv("ENABLE_STOP_LOSS_ORDERS")
    if val is not None:
        settings["enable_stop_loss_orders"] = val.strip().lower() in {"1", "true", "yes", "on"}
    return settings



def _settings_api_candidates() -> list:
    """Dashboard URLs to try for /api/trading/settings (never prefer agent self-URL first)."""
    ordered = []
    for key in ("DASHBOARD_URL", "NEXT_PUBLIC_BASE_URL", "NEXT_PUBLIC_API_URL"):
        val = os.getenv(key) or CONFIG.get(key) or CONFIG.get(str(key).lower())
        if not val:
            continue
        u = str(val).rstrip("/")
        if u and u not in ordered:
            ordered.append(u)
    if "http://localhost:3001" not in ordered:
        ordered.append("http://localhost:3001")
    return ordered


async def _fetch_trading_settings_row_from_supabase() -> Optional[Dict[str, Any]]:
    """Read trading_settings directly when the dashboard API is unreachable."""
    base = (os.getenv("SUPABASE_URL") or CONFIG.get("SUPABASE_URL") or "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
        or CONFIG.get("SUPABASE_SERVICE_KEY")
        or CONFIG.get("SUPABASE_KEY")
    )
    if not base or not key:
        return None
    url = f"{base}/rest/v1/trading_settings?id=eq.default&select=*"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={
                    "apikey": str(key),
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logging.warning(
                        f"⚠️  Supabase trading_settings fetch failed (status {resp.status})"
                    )
                    return None
                rows = await resp.json()
                if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                    return rows[0]
    except Exception as e:
        logging.warning(f"⚠️  Supabase trading_settings fetch error: {e}")
    return None


async def get_trading_settings() -> Dict[str, Any]:
    """Fetch trading settings from dashboard API, then Supabase, then cache/env.

    Returns:
        Dictionary with all trading settings including leverage, TP%, SL%, assets, strategy, etc.
    """
    data = None
    source = None
    # Prefer DASHBOARD_URL / Vercel base — NEXT_PUBLIC_API_URL on the agent often points at itself.
    for api_url in _settings_api_candidates():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{api_url}/api/trading/settings",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        source = f"dashboard:{api_url}"
                        break
                    logging.warning(
                        f"⚠️  Failed to fetch trading settings from {api_url} (status {resp.status})"
                    )
        except Exception as e:
            logging.warning(f"⚠️  Could not fetch trading settings from {api_url}: {e}")

    if data is None:
        row = await _fetch_trading_settings_row_from_supabase()
        if row is not None:
            data = row
            source = "supabase"

    if data is not None:
        try:
                    margin_per_pos = data.get("margin_per_position")
                    asset_leverage_overrides = data.get("asset_leverage_overrides", {}) or {}
                    asset_timeframes = data.get("asset_timeframes", {}) or {}
                    
                    logging.info(f"✅ Fetched trading settings via {source}: llm_model={data.get('llm_model')}, leverage={data.get('leverage')}, strategy={data.get('strategy')}, exchange={data.get('exchange')}")

                    settings = {
                        # Position sizing
                        "leverage": int(data.get("leverage", 10)),
                        "take_profit_percent": float(
                            _coalesce(data.get("take_profit_percent"), CONFIG.get("take_profit_percent"), 7.0)
                        ),
                        "stop_loss_percent": float(data.get("stop_loss_percent", 3.0)),
                        "target_profit_per_1pct_move": float(data.get("target_profit_per_1pct_move", 1.0)),
                        "allocation_per_position": data.get("allocation_per_position"),
                        "margin_per_position": float(margin_per_pos) if margin_per_pos is not None else float(_coalesce(CONFIG.get("margin_per_position"), 30.0)),
                        "max_positions": int(data.get("max_positions", 6)),
                        "position_sizing_mode": _coalesce(
                            data.get("position_sizing_mode"), CONFIG.get("position_sizing_mode"), "risk"
                        ),
                        "risk_per_trade_usd": _coalesce(
                            data.get("risk_per_trade_usd"), CONFIG.get("risk_per_trade_usd"), 6.0
                        ),
                        "risk_per_trade_pct": float(
                            _coalesce(data.get("risk_per_trade_pct"), CONFIG.get("risk_per_trade_pct"), 0.5)
                        ),
                        "max_notional_per_position": _coalesce(
                            data.get("max_notional_per_position"), CONFIG.get("max_notional_per_position")
                        ),
                        # Volatility-adaptive exits
                        "exit_mode": _coalesce(data.get("exit_mode"), CONFIG.get("exit_mode"), "atr"),
                        "tp_mode": _coalesce(data.get("tp_mode"), CONFIG.get("tp_mode"), "roi_percent"),
                        "take_profit_usd": _coalesce(
                            data.get("take_profit_usd"), CONFIG.get("take_profit_usd"), 12.0
                        ),
                        "sl_atr_mult": float(data.get("sl_atr_mult", CONFIG.get("sl_atr_mult", 2.0))),
                        "tp_rr_ratio": float(data.get("tp_rr_ratio", CONFIG.get("tp_rr_ratio", 2.0))),
                        "atr_period": int(data.get("atr_period", CONFIG.get("atr_period", 14))),
                        "min_stop_price_pct": float(data.get("min_stop_price_pct", CONFIG.get("min_stop_price_pct", 1.5))),
                        "max_stop_price_pct": float(data.get("max_stop_price_pct", CONFIG.get("max_stop_price_pct", 7.0))),
                        # Re-entry control
                        "reentry_cooldown_minutes": float(data.get("reentry_cooldown_minutes", CONFIG.get("reentry_cooldown_minutes", 45.0))),
                        "loss_reentry_cooldown_minutes": float(data.get("loss_reentry_cooldown_minutes", CONFIG.get("loss_reentry_cooldown_minutes", 90.0))),
                        "block_direction_flip": bool(data.get("block_direction_flip", CONFIG.get("block_direction_flip", True))),
                        "enable_breakeven_stop": bool(data.get("enable_breakeven_stop", CONFIG.get("enable_breakeven_stop", True))),
                        "breakeven_trigger_r": float(data.get("breakeven_trigger_r", CONFIG.get("breakeven_trigger_r", 1.0))),
                        "trailing_stop_activation_r": float(data.get("trailing_stop_activation_r", CONFIG.get("trailing_stop_activation_r", 1.0))),
                        "trailing_stop_distance_r": float(data.get("trailing_stop_distance_r", CONFIG.get("trailing_stop_distance_r", 1.0))),
                        "min_notional_per_position": _coalesce(
                            data.get("min_notional_per_position"), CONFIG.get("min_notional_per_position"), 50.0
                        ),
                        # Scale-out ladder
                        "enable_profit_ladder": bool(data.get("enable_profit_ladder", CONFIG.get("enable_profit_ladder", True))),
                        "profit_ladder": data.get("profit_ladder", CONFIG.get("profit_ladder", "1.0:50,2.0:30")),
                        "breakeven_after_first_fill": bool(data.get("breakeven_after_first_fill", CONFIG.get("breakeven_after_first_fill", True))),
                        # Circuit breakers
                        "max_daily_loss_usd": data.get("max_daily_loss_usd", CONFIG.get("max_daily_loss_usd")),
                        "max_daily_loss_pct": float(data.get("max_daily_loss_pct", CONFIG.get("max_daily_loss_pct", 3.0)) or 0.0),
                        "max_consecutive_losses": int(data.get("max_consecutive_losses", CONFIG.get("max_consecutive_losses", 4)) or 0),
                        "loss_streak_pause_minutes": float(data.get("loss_streak_pause_minutes", CONFIG.get("loss_streak_pause_minutes", 120.0)) or 0.0),
                        # Higher-timeframe trend filter
                        "enable_htf_trend_filter": bool(data.get("enable_htf_trend_filter", CONFIG.get("enable_htf_trend_filter", True))),
                        "htf_trend_timeframe": data.get("htf_trend_timeframe", CONFIG.get("htf_trend_timeframe", "1h")),
                        "htf_trend_fast_ema": int(data.get("htf_trend_fast_ema", CONFIG.get("htf_trend_fast_ema", 20))),
                        "htf_trend_slow_ema": int(data.get("htf_trend_slow_ema", CONFIG.get("htf_trend_slow_ema", 50))),
                        "htf_trend_min_separation_pct": float(data.get("htf_trend_min_separation_pct", CONFIG.get("htf_trend_min_separation_pct", 0.15))),
                        "htf_block_when_flat": bool(data.get("htf_block_when_flat", CONFIG.get("htf_block_when_flat", True))),
                        # Analysis timeframes
                        "intraday_timeframe": data.get("intraday_timeframe", CONFIG.get("intraday_timeframe", "15m")),
                        "longterm_timeframe": data.get("longterm_timeframe", CONFIG.get("longterm_timeframe", "4h")),
                        # Trading configuration
                        "multi_exchange_mode": bool(data.get("multi_exchange_mode", False)),
                        "assets": data.get("assets", "BTC ETH SOL"),
                        "interval": data.get("interval", "5m"),
                        "strategy": data.get("strategy", "auto") or "auto",
                        "exchange": data.get("exchange", "binance"),
                        # Alert service
                        "alert_service_enabled": bool(data.get("alert_service_enabled", False)),
                        "alert_risk_per_trade": float(data.get("alert_risk_per_trade", 30.0)),
                        "alert_check_interval": int(data.get("alert_check_interval", 5)),
                        "alert_agent_endpoint": data.get("alert_agent_endpoint", "http://localhost:5000/api/alert/signal"),
                        "alert_assets": data.get("alert_assets", "ZEC,BTC,ETH,SOL,BNB"),
                        "alert_timeframe": data.get("alert_timeframe", "15m"),
                        # Risk management
                        "enable_trailing_stop": bool(data.get("enable_trailing_stop", True)),
                        "trailing_stop_activation_pct": float(data.get("trailing_stop_activation_pct", 5.0)),
                        "trailing_stop_distance_pct": float(data.get("trailing_stop_distance_pct", 3.0)),
                        "max_position_hold_hours": float(data.get("max_position_hold_hours", 48.0)),
                        "enable_drawdown_protection": bool(data.get("enable_drawdown_protection", True)),
                        "max_drawdown_from_peak_pct": float(data.get("max_drawdown_from_peak_pct", 5.0)),
                        # Scalping strategy
                        "scalping_tp_percent": float(data.get("scalping_tp_percent", 5.0)),
                        "scalping_sl_percent": float(data.get("scalping_sl_percent", 5.0)),
                        "auto_strategy_cache_minutes": int(data.get("auto_strategy_cache_minutes", 0)),
                        # Stop loss enforcement
                        # Stop loss enforcement — ATR+risk uses ~1.5× risk as gap buffer (not a 2% yank)
                        "stop_loss_usd": float(
                            _coalesce(data.get("stop_loss_usd"), CONFIG.get("stop_loss_usd"), -6.0)
                        ),
                        "take_profit_strict_enforcement": bool(data.get("take_profit_strict_enforcement", False)),
                        "hard_max_loss_cap_percent": float(data.get("hard_max_loss_cap_percent", 8.0)),
                        "enable_stop_loss_orders": bool(data.get("enable_stop_loss_orders", CONFIG.get("enable_stop_loss_orders", True))),
                        "agent_manage_exits": bool(data.get("agent_manage_exits", CONFIG.get("agent_manage_exits", True))),
                        # Per-asset overrides
                        "asset_leverage_overrides": asset_leverage_overrides,
                        "asset_timeframes": asset_timeframes,
                        # LLM configuration
                        "llm_model": data.get("llm_model", "deepseek-reasoner"),
                        "deepseek_max_tokens": int(data.get("deepseek_max_tokens", 20000)),
                        "decision_mode": data.get("decision_mode"),
                    }
                    # Mechanical scalp knobs pass straight through; defaults live in the helper.
                    for mech_key in MECH_DEFAULTS:
                        if data.get(mech_key) is not None:
                            settings[mech_key] = data.get(mech_key)
                    # Keep hard USD stop aligned to risk_per_trade_usd (never looser).
                    try:
                        risk_f = float(settings.get("risk_per_trade_usd") or 0)
                        if risk_f > 0:
                            desired_stop = -abs(risk_f)
                            current_stop = settings.get("stop_loss_usd")
                            if current_stop is None:
                                settings["stop_loss_usd"] = desired_stop
                            else:
                                try:
                                    cur = float(current_stop)
                                except (TypeError, ValueError):
                                    cur = desired_stop
                                # Use the tighter ceiling (closer to zero / smaller abs loss).
                                if cur >= 0 or abs(cur) > risk_f + 0.01:
                                    settings["stop_loss_usd"] = desired_stop
                    except (TypeError, ValueError):
                        settings["stop_loss_usd"] = -abs(
                            float(settings.get("risk_per_trade_usd") or CONFIG.get("risk_per_trade_usd") or 6.0)
                        )
                    if is_mechanical_mode(settings):
                        # 1:1 book — the 2R alignment must not lift the $ target above the $ risk.
                        settings = apply_mechanical_overrides(settings)
                    else:
                        settings = align_take_profit_to_risk(settings, reward_multiple=2.0)
                    _save_cached_trading_settings(settings)
                    return _apply_env_bool_overrides(settings)
        except Exception as e:
            logging.warning(f"⚠️  Failed to materialize trading settings from {source}: {e}")

    cached_settings = _load_cached_trading_settings()
    if cached_settings:
        logging.warning("⚠️  Using cached trading settings from last successful database fetch.")
        return _apply_env_bool_overrides(cached_settings)
    
    # Fallback to env/config defaults (ONLY if database is unavailable)
    logging.warning("⚠️  Using .env defaults as fallback. Database settings should be used instead!")
    margin_per_pos = CONFIG.get("margin_per_position")
    asset_leverage_overrides = {}
    # Parse per-asset leverage from env (e.g., BTC_LEVERAGE=25)
    for key, value in CONFIG.items():
        if key.endswith("_LEVERAGE") and isinstance(value, (int, float)):
            asset = key.replace("_LEVERAGE", "").upper()
            asset_leverage_overrides[asset] = int(value)
    
    fallback_settings = {
        # Position sizing
        "leverage": CONFIG.get("default_leverage", 10),
        "take_profit_percent": CONFIG.get("take_profit_percent", 7),
        "stop_loss_percent": CONFIG.get("stop_loss_percent", 3),
        "target_profit_per_1pct_move": CONFIG.get("target_profit_per_1pct_move", 1.0),
        "allocation_per_position": CONFIG.get("allocation_per_position"),
        "margin_per_position": float(margin_per_pos) if margin_per_pos is not None else float(CONFIG.get("margin_per_position") or 30.0),
        "max_positions": CONFIG.get("max_positions", 6),
        "position_sizing_mode": CONFIG.get("position_sizing_mode", "risk"),
        "risk_per_trade_usd": CONFIG.get("risk_per_trade_usd", 6.0),
        "risk_per_trade_pct": CONFIG.get("risk_per_trade_pct", 0.5),
        "max_notional_per_position": CONFIG.get("max_notional_per_position"),
        # Volatility-adaptive exits
        "exit_mode": CONFIG.get("exit_mode", "atr"),
        "tp_mode": CONFIG.get("tp_mode", "roi_percent"),
        "take_profit_usd": CONFIG.get("take_profit_usd", 12.0),
        "sl_atr_mult": CONFIG.get("sl_atr_mult", 2.0),
        "tp_rr_ratio": CONFIG.get("tp_rr_ratio", 2.0),
        "atr_period": CONFIG.get("atr_period", 14),
        "min_stop_price_pct": CONFIG.get("min_stop_price_pct", 1.5),
        "max_stop_price_pct": CONFIG.get("max_stop_price_pct", 7.0),
        # Re-entry control
        "reentry_cooldown_minutes": CONFIG.get("reentry_cooldown_minutes", 45.0),
        "loss_reentry_cooldown_minutes": CONFIG.get("loss_reentry_cooldown_minutes", 90.0),
        "block_direction_flip": CONFIG.get("block_direction_flip", True),
        "enable_breakeven_stop": CONFIG.get("enable_breakeven_stop", True),
        "breakeven_trigger_r": CONFIG.get("breakeven_trigger_r", 1.0),
        "trailing_stop_activation_r": CONFIG.get("trailing_stop_activation_r", 1.0),
        "trailing_stop_distance_r": CONFIG.get("trailing_stop_distance_r", 1.0),
        "min_notional_per_position": CONFIG.get("min_notional_per_position", 50.0),
        # Scale-out ladder
        "enable_profit_ladder": CONFIG.get("enable_profit_ladder", True),
        "profit_ladder": CONFIG.get("profit_ladder", "1.0:50,2.0:30"),
        "breakeven_after_first_fill": CONFIG.get("breakeven_after_first_fill", True),
        # Circuit breakers
        "max_daily_loss_usd": CONFIG.get("max_daily_loss_usd"),
        "max_daily_loss_pct": CONFIG.get("max_daily_loss_pct", 3.0),
        "max_consecutive_losses": CONFIG.get("max_consecutive_losses", 4),
        "loss_streak_pause_minutes": CONFIG.get("loss_streak_pause_minutes", 120.0),
        # Higher-timeframe trend filter
        "enable_htf_trend_filter": CONFIG.get("enable_htf_trend_filter", True),
        "htf_trend_timeframe": CONFIG.get("htf_trend_timeframe", "1h"),
        "htf_trend_fast_ema": CONFIG.get("htf_trend_fast_ema", 20),
        "htf_trend_slow_ema": CONFIG.get("htf_trend_slow_ema", 50),
        "htf_trend_min_separation_pct": CONFIG.get("htf_trend_min_separation_pct", 0.15),
        "htf_block_when_flat": CONFIG.get("htf_block_when_flat", True),
        # Analysis timeframes
        "intraday_timeframe": CONFIG.get("intraday_timeframe", "15m"),
        "longterm_timeframe": CONFIG.get("longterm_timeframe", "4h"),
        # Trading configuration
        "multi_exchange_mode": CONFIG.get("MULTI_EXCHANGE_MODE", False),
        "assets": CONFIG.get("assets") or CONFIG.get("ASSETS", "BTC ETH SOL"),
        "interval": CONFIG.get("interval") or CONFIG.get("INTERVAL", "5m"),
        "strategy": CONFIG.get("strategy") or CONFIG.get("STRATEGY", "auto") or "auto",
        "exchange": CONFIG.get("exchange") or CONFIG.get("EXCHANGE", "binance"),
        # Alert service
        "alert_service_enabled": CONFIG.get("ALERT_SERVICE_ENABLED", False),
        "alert_risk_per_trade": CONFIG.get("ALERT_RISK_PER_TRADE", 30.0),
        "alert_check_interval": CONFIG.get("ALERT_CHECK_INTERVAL", 5),
        "alert_agent_endpoint": CONFIG.get("ALERT_AGENT_ENDPOINT", "http://localhost:5000/api/alert/signal"),
        "alert_assets": CONFIG.get("ALERT_ASSETS", "ZEC,BTC,ETH,SOL,BNB"),
        "alert_timeframe": CONFIG.get("ALERT_TIMEFRAME", "15m"),
        # Risk management
        "enable_trailing_stop": CONFIG.get("enable_trailing_stop", True),
        "trailing_stop_activation_pct": CONFIG.get("trailing_stop_activation_pct", 5.0),
        "trailing_stop_distance_pct": CONFIG.get("trailing_stop_distance_pct", 3.0),
        "max_position_hold_hours": CONFIG.get("max_position_hold_hours", 48.0),
        "enable_drawdown_protection": CONFIG.get("enable_drawdown_protection", True),
        "max_drawdown_from_peak_pct": CONFIG.get("max_drawdown_from_peak_pct", 5.0),
        # Scalping strategy
        "scalping_tp_percent": CONFIG.get("scalping_tp_percent", 5.0),
        "scalping_sl_percent": CONFIG.get("scalping_sl_percent", 5.0),
        "auto_strategy_cache_minutes": CONFIG.get("auto_strategy_cache_minutes", 0),
        # Stop loss enforcement
        "stop_loss_usd": CONFIG.get("stop_loss_usd", -6.0),  # Hard $ close = risk_per_trade_usd
        "take_profit_strict_enforcement": CONFIG.get("take_profit_strict_enforcement", False),
        "hard_max_loss_cap_percent": float(CONFIG.get("stop_loss_percent", 8.0) or 8.0),
        "enable_stop_loss_orders": CONFIG.get("enable_stop_loss_orders", True),
        "agent_manage_exits": CONFIG.get("agent_manage_exits", True),
        # Per-asset overrides
        "asset_leverage_overrides": asset_leverage_overrides,
        "asset_timeframes": {},  # Would need to parse from env if needed
        # LLM configuration
        "llm_model": CONFIG.get("llm_model", "deepseek-reasoner"),
        "deepseek_max_tokens": CONFIG.get("deepseek_max_tokens", 20000),
    }
    if is_mechanical_mode(fallback_settings):
        return _apply_env_bool_overrides(apply_mechanical_overrides(fallback_settings))
    return _apply_env_bool_overrides(align_take_profit_to_risk(fallback_settings, reward_multiple=2.0))


async def get_max_leverage_for_asset(exchange_api, asset: str) -> int:
    """Get maximum allowed leverage for an asset from exchange.
    
    Args:
        exchange_api: Exchange API instance (AsterAPI or HyperliquidAPI)
        asset: Asset symbol (e.g., 'BTC')
        
    Returns:
        Maximum leverage allowed for the asset (default: 10 if not found)
    """
    try:
        # Try to get exchange info/metadata
        if hasattr(exchange_api, 'get_meta_and_ctxs'):
            meta = await exchange_api.get_meta_and_ctxs()
            if meta:
                symbol = asset if asset.endswith('USDT') else f"{asset}USDT"
                # Look for leverage bracket in exchange info
                # Aster format may vary, check common patterns
                if isinstance(meta, dict):
                    symbols = meta.get('symbols', [])
                    for sym_info in symbols:
                        if sym_info.get('symbol') == symbol:
                            # Check for leverage bracket or max leverage
                            leverage_bracket = sym_info.get('leverageBracket') or sym_info.get('leverage')
                            if leverage_bracket:
                                if isinstance(leverage_bracket, list) and len(leverage_bracket) > 0:
                                    # Get max leverage from bracket
                                    max_lev = max([int(b.get('leverage', 1)) for b in leverage_bracket if isinstance(b, dict)])
                                    return max_lev
                                elif isinstance(leverage_bracket, (int, float)):
                                    return int(leverage_bracket)
    except Exception as e:
        logging.debug(f"Could not get max leverage for {asset}: {e}")
    
    # Default: return a safe maximum (most exchanges allow at least 10x)
    return 10


def calculate_tp_sl_prices(
    entry_price: float,
    is_long: bool,
    take_profit_percent: float,
    stop_loss_percent: float
) -> tuple:
    """Calculate take profit and stop loss prices from percentages.
    
    Args:
        entry_price: Entry price of the position
        is_long: True if long position, False if short
        take_profit_percent: Take profit percentage (e.g., 5.0 = 5%)
        stop_loss_percent: Stop loss percentage (e.g., 3.0 = 3%)
        
    Returns:
        Tuple of (tp_price, sl_price)
    """
    if is_long:
        # Long: TP above entry, SL below entry
        tp_price = entry_price * (1 + take_profit_percent / 100)
        sl_price = entry_price * (1 - stop_loss_percent / 100)
    else:
        # Short: TP below entry, SL above entry
        tp_price = entry_price * (1 - take_profit_percent / 100)
        sl_price = entry_price * (1 + stop_loss_percent / 100)
    
    return (tp_price, sl_price)


def calculate_risk_based_allocation(
    trading_settings: Dict[str, Any],
    available_balance: float,
    stop_price_pct: float,
    leverage: int,
) -> float:
    """Margin to commit so that hitting the stop loses exactly the configured risk amount.

    Notional is solved from the stop distance (``risk / stop_pct``), then divided by leverage
    to get the margin. This keeps dollar risk constant whether the stop is 0.8% or 3% wide.
    """
    leverage = max(int(leverage or 1), 1)
    stop_price_pct = max(float(stop_price_pct or 0.0), 0.05)

    risk_usd = trading_settings.get("risk_per_trade_usd")
    if risk_usd is None:
        from src.utils.practice_bankroll import risk_equity

        risk_pct = float(trading_settings.get("risk_per_trade_pct") or CONFIG.get("risk_per_trade_pct", 0.5) or 0.5)
        # % of the $300 practice book — never the fat OKX demo wallet.
        risk_usd = risk_equity(available_balance) * (risk_pct / 100.0)
    risk_usd = max(float(risk_usd), 0.0)
    # Never let sizing exceed the hard USD stop ceiling when both are set.
    try:
        stop_ceiling = trading_settings.get("stop_loss_usd")
        if stop_ceiling is not None and float(stop_ceiling) < 0:
            risk_usd = min(risk_usd, abs(float(stop_ceiling)))
    except (TypeError, ValueError):
        pass

    required_notional = risk_usd / (stop_price_pct / 100.0)

    max_notional = trading_settings.get("max_notional_per_position") or CONFIG.get("max_notional_per_position")
    if max_notional:
        required_notional = min(required_notional, float(max_notional))

    # Do NOT silently raise size above the risk budget. If exchange min notional would
    # risk more than max loss, skip the trade (return 0) instead of oversizing.
    min_notional = float(
        trading_settings.get("min_notional_per_position")
        or CONFIG.get("min_notional_per_position", 50.0)
        or 0.0
    )
    if min_notional and required_notional < min_notional:
        actual_risk = min_notional * (stop_price_pct / 100.0)
        if actual_risk > risk_usd + 0.01:
            logging.warning(
                f"⚠️  Risk sizing needs ${required_notional:.2f} notional for ${risk_usd:.2f} risk, "
                f"but min notional ${min_notional:.2f} would risk ${actual_risk:.2f}. Skipping trade."
            )
            return 0.0
        required_notional = min_notional

    margin = required_notional / leverage

    # Never commit more margin than a fair share of the balance across concurrent slots.
    max_positions = max(int(trading_settings.get("max_positions", 6) or 6), 1)
    margin_ceiling = available_balance / max_positions
    if margin > margin_ceiling:
        logging.warning(
            f"⚠️  Risk sizing wants ${margin:.2f} margin (stop {stop_price_pct:.2f}%, risk ${risk_usd:.2f}) "
            f"but slot ceiling is ${margin_ceiling:.2f}. Capping — effective risk will be lower."
        )
        margin = margin_ceiling

    return max(min(margin, available_balance), 0.0)


def calculate_allocation_usd(
    trading_settings: Dict[str, Any],
    available_balance: float,
    current_price: float,
    leverage: int,
    stop_price_pct: float | None = None,
) -> float:
    """Calculate allocation in USD based on position sizing settings.
    
    CRITICAL: This function ensures the allocation is sufficient to achieve 
    target_profit_per_1pct_move (e.g., $1 per 1% price move).
    
    Args:
        trading_settings: Dictionary from get_trading_settings()
        available_balance: Available balance in USD
        current_price: Current price of the asset
        leverage: Leverage to be used for this trade
        stop_price_pct: Stop distance as % of price, required for "risk" sizing mode
        
    Returns:
        Allocation in USD for this position (guaranteed to meet minimum requirement)
    """
    position_sizing_mode = trading_settings.get("position_sizing_mode", "auto")

    if position_sizing_mode == "risk":
        if stop_price_pct and stop_price_pct > 0:
            return calculate_risk_based_allocation(
                trading_settings, available_balance, stop_price_pct, leverage
            )
        logging.warning("⚠️  Risk sizing mode selected but no stop distance supplied; falling back to margin/auto.")
    target_profit = trading_settings.get("target_profit_per_1pct_move", 1.0)
    fixed_allocation = trading_settings.get("allocation_per_position")
    margin_per_position = trading_settings.get("margin_per_position")
    max_positions = trading_settings.get("max_positions", 6)
    
    if position_sizing_mode == "margin" and margin_per_position is not None:
        # Margin mode: use margin_per_position directly as allocation
        # Leverage will be applied when calculating contract quantity
        return min(margin_per_position, available_balance)
    
    if position_sizing_mode == "fixed" and fixed_allocation is not None:
        # Use fixed allocation if specified, but validate it meets minimum requirement
        min_required = calculate_position_size(target_profit, 1.0, leverage)
        if fixed_allocation < min_required:
            logging.warning(f"⚠️  Fixed allocation ${fixed_allocation:.2f} is below minimum ${min_required:.2f} needed for ${target_profit:.2f} per 1% move. Using minimum.")
            return min(min_required, available_balance)
        return min(fixed_allocation, available_balance)
    
    elif position_sizing_mode in ("auto", "target_profit"):
        # CRITICAL: Calculate MINIMUM allocation required to achieve target_profit per 1% move
        # Formula: allocation = target_profit / (0.01 * leverage)
        # Example: $1 target / (0.01 * 10x) = $10 minimum allocation
        min_required_allocation = calculate_position_size(target_profit, 1.0, leverage)
        
        # Calculate max allocation based on available balance and max positions
        max_allocation_per_position = available_balance / max(1, max_positions)
        
        # Use the MAXIMUM of (minimum required, max per position) to ensure we meet the target
        # This guarantees we can make at least target_profit per 1% move
        calculated_allocation = max(min_required_allocation, max_allocation_per_position)
        
        # But don't exceed available balance
        final_allocation = min(calculated_allocation, available_balance)
        
        # Validate the final allocation will achieve the target
        if final_allocation < min_required_allocation:
            logging.warning(f"⚠️  Insufficient balance: Need ${min_required_allocation:.2f} for ${target_profit:.2f} per 1% move, but only ${available_balance:.2f} available. Using ${final_allocation:.2f}.")
        else:
            # Verify profit calculation
            expected_profit = calculate_profit(final_allocation, 1.0, leverage)
            if expected_profit < target_profit * 0.95:  # Allow 5% tolerance
                logging.warning(f"⚠️  Allocation ${final_allocation:.2f} may not achieve target ${target_profit:.2f} per 1% move. Expected: ${expected_profit:.2f}")
        
        return final_allocation
    
    else:
        # Default: calculate minimum required, but ensure it meets target
        min_required = calculate_position_size(target_profit, 1.0, leverage)
        default_allocation = available_balance / max(1, max_positions)
        return max(min_required, default_allocation, min(available_balance))

