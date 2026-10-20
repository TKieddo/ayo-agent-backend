"""Centralized environment variable loading for the trading agent configuration."""

import json
import os
from dotenv import load_dotenv

load_dotenv()


def _get_env(name: str, default: str | None = None, required: bool = False) -> str | None:
    """Fetch an environment variable with optional default and required validation."""
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_float(name: str, default: float | None = None) -> float | None:
    """Get float environment variable."""
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default


def _get_int(name: str, default: int | None = None) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        # Strip whitespace and remove comments (everything after # or parentheses)
        cleaned = raw.strip()
        # Remove comments after # or in parentheses
        if '#' in cleaned:
            cleaned = cleaned.split('#')[0].strip()
        if '(' in cleaned:
            cleaned = cleaned.split('(')[0].strip()
        return int(cleaned)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {raw}") from exc


def _get_json(name: str, default: dict | None = None) -> dict | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Environment variable {name} must be a JSON object")
        return parsed
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON for {name}: {raw}") from exc


def _get_list(name: str, default: list[str] | None = None) -> list[str] | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    raw = raw.strip()
    # Support JSON-style lists
    if raw.startswith("[") and raw.endswith("]"):
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise RuntimeError(f"Environment variable {name} must be a list if using JSON syntax")
            return [str(item).strip().strip('"\'') for item in parsed if str(item).strip()]
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON list for {name}: {raw}") from exc
    # Fallback: comma separated string
    values = []
    for item in raw.split(","):
        cleaned = item.strip().strip('"\'')
        if cleaned:
            values.append(cleaned)
    return values or default


CONFIG = {
    # TAAPI is optional now (replaced with TA-Lib + Binance)
    "taapi_api_key": _get_env("TAAPI_API_KEY"),  # Optional, kept for backwards compatibility
    # Exchange selection (default: aster)
    "exchange": _get_env("EXCHANGE", "aster").lower(),  # Options: "aster", "binance", "alpaca", "ig", "okx"
    # Aster DEX API (default exchange)
    "aster_api_base": _get_env("ASTER_API_BASE", "https://fapi.asterdex.com"),
    "aster_user_address": _get_env("ASTER_USER_ADDRESS"),  # Main wallet address
    "aster_signer_address": _get_env("ASTER_SIGNER_ADDRESS"),  # API wallet address
    "aster_private_key": _get_env("ASTER_PRIVATE_KEY"),  # API wallet private key
    # Binance Futures API
    "binance_api_key": _get_env("BINANCE_API_KEY"),
    "binance_api_secret": _get_env("BINANCE_API_SECRET"),
    "binance_testnet": _get_bool("BINANCE_TESTNET", False),
    "binance_leverage": _get_int("BINANCE_LEVERAGE", 10),
    # Alpaca Trading API (stocks + crypto paper/live)
    # Accept both our names and Alpaca's dashboard export names (APCA_*).
    "alpaca_api_key": (
        (_get_env("ALPACA_API_KEY") or _get_env("APCA_API_KEY_ID") or "").strip() or None
    ),
    "alpaca_api_secret": (
        (_get_env("ALPACA_API_SECRET") or _get_env("APCA_API_SECRET_KEY") or "").strip() or None
    ),
    "alpaca_paper": _get_bool("ALPACA_PAPER", True),  # True = paper-api.alpaca.markets
    "alpaca_leverage": _get_int("ALPACA_LEVERAGE", 1),  # Hint only — Alpaca is not futures leverage
    "alpaca_paper_url": _get_env("ALPACA_PAPER_URL", "https://paper-api.alpaca.markets"),
    "alpaca_live_url": _get_env("ALPACA_LIVE_URL", "https://api.alpaca.markets"),
    "alpaca_data_url": _get_env("ALPACA_DATA_URL", "https://data.alpaca.markets"),
    "stock_assets": _get_env(
        "STOCK_ASSETS",
        "AAPL MSFT NVDA AMZN META GOOGL AVGO TSLA AMD PLTR SMCI ARM BABA TSM ASML CRM NOW SNOW SPY QQQ XLK SOXX",
    ),
    # Stock book (Alpaca only) — distinct from crypto mechanical / ROI% exits
    "enable_stock_scanner": _get_bool("ENABLE_STOCK_SCANNER", True),
    "stock_scanner_top_n": _get_int("STOCK_SCANNER_TOP_N", 5),
    "stock_scanner_refresh_minutes": _get_float("STOCK_SCANNER_REFRESH_MINUTES", 15.0),
    "stock_notional_per_position": _get_float("STOCK_NOTIONAL_PER_POSITION", 50.0),
    "stock_risk_per_trade_usd": _get_float("STOCK_RISK_PER_TRADE_USD", 8.0),
    "stock_sl_atr_mult": _get_float("STOCK_SL_ATR_MULT", 1.5),
    "stock_tp_mode": _get_env("STOCK_TP_MODE", "atr_rr"),
    "stock_tp_rr_ratio": _get_float("STOCK_TP_RR_RATIO", 1.5),
    "stock_scale_out_at_r": _get_float("STOCK_SCALE_OUT_AT_R", 1.0),
    "stock_scale_out_fraction": _get_float("STOCK_SCALE_OUT_FRACTION", 0.5),
    "stock_max_positions": _get_int("STOCK_MAX_POSITIONS", 3),
    "stock_leverage": _get_int("STOCK_LEVERAGE", 1),
    "stock_effective_equity": _get_float("STOCK_EFFECTIVE_EQUITY", 200.0),
    "stock_min_stop_price_pct": _get_float("STOCK_MIN_STOP_PRICE_PCT", 0.8),
    "stock_max_stop_price_pct": _get_float("STOCK_MAX_STOP_PRICE_PCT", 3.5),
    "stock_interval": _get_env("STOCK_INTERVAL", "15m"),
    "stock_htf_interval": _get_env("STOCK_HTF_INTERVAL", "1h"),
    "stock_allow_overnight": _get_bool("STOCK_ALLOW_OVERNIGHT", False),
    # No new stock entries in the last N minutes before the official close (Alpaca next_close).
    "stock_entry_cutoff_minutes": _get_int("STOCK_ENTRY_CUTOFF_MINUTES", 10),
    "stock_cycle_seconds": _get_float("STOCK_CYCLE_SECONDS", 60.0),
    "stock_atr_period": _get_int("STOCK_ATR_PERIOD", 14),
    "stock_min_dollar_volume": _get_float("STOCK_MIN_DOLLAR_VOLUME", 5_000_000.0),
    "stock_min_atr_pct": _get_float("STOCK_MIN_ATR_PCT", 0.4),
    "stock_max_atr_pct": _get_float("STOCK_MAX_ATR_PCT", 4.0),
    # IG Markets CFD API (forex / indices / stock CFDs — demo or live)
    "ig_api_key": _get_env("IG_API_KEY"),
    "ig_username": _get_env("IG_USERNAME"),
    "ig_password": _get_env("IG_PASSWORD"),
    "ig_demo": _get_bool("IG_DEMO", True),
    "ig_account_id": _get_env("IG_ACCOUNT_ID"),  # optional preferred account id
    "ig_currency": _get_env("IG_CURRENCY", "USD"),
    "ig_leverage": _get_int("IG_LEVERAGE", 20),  # hint for ROI/sizing only
    "ig_demo_url": _get_env("IG_DEMO_URL", "https://demo-api.ig.com/gateway/deal"),
    "ig_live_url": _get_env("IG_LIVE_URL", "https://api.ig.com/gateway/deal"),
    "ig_epic_map": _get_env("IG_EPIC_MAP"),  # EURUSD:CS.D.EURUSD.MINI.IP,GBPUSD:...
    "ig_fixed_size": _get_float("IG_FIXED_SIZE"),  # optional fixed CFD size for testing
    "ig_min_size": _get_float("IG_MIN_SIZE", 0.5),
    "ig_max_size": _get_float("IG_MAX_SIZE", 25),
    "ig_size_divisor": _get_float("IG_SIZE_DIVISOR", 5000),
    # OKX (crypto SWAP / perps — demo via x-simulated-trading)
    "okx_api_key": _get_env("OKX_API_KEY"),
    "okx_api_secret": _get_env("OKX_API_SECRET"),
    "okx_passphrase": _get_env("OKX_PASSPHRASE"),
    "okx_demo": _get_bool("OKX_DEMO", True),
    "okx_base_url": _get_env("OKX_BASE_URL", "https://www.okx.com"),
    "okx_leverage": _get_int("OKX_LEVERAGE", 10),
    "okx_td_mode": _get_env("OKX_TD_MODE", "cross"),  # cross | isolated
    "okx_pos_mode": _get_env("OKX_POS_MODE", "net_mode"),  # net_mode | long_short_mode
    # Trading settings (can be overridden via database/frontend)
    "default_leverage": _get_int("DEFAULT_LEVERAGE", 10),  # Default leverage (will be capped by asset max)
    "take_profit_percent": _get_int("TAKE_PROFIT_PERCENT", 7),  # Price % when tp_mode=price_percent (5 = +5% of entry)
    "stop_loss_percent": _get_int("STOP_LOSS_PERCENT", 8),  # Stop loss percentage - 8% for scalping (was 3%)
    "enable_stop_loss_orders": _get_bool("ENABLE_STOP_LOSS_ORDERS", True),  # Recommended ON: creates exchange-native SL protection orders
    "agent_manage_exits": _get_bool("AGENT_MANAGE_EXITS", True),  # If false, disables agent-driven exits (TP/SL-only close mode)
    # Smart profit taking (scalping mode)
    "enable_smart_profit": _get_bool("ENABLE_SMART_PROFIT", True),
    "smart_profit_tier1_pct": _get_int("SMART_PROFIT_TIER1_PCT", 7),  # Close 70% at 7%
    "smart_profit_tier1_size": _get_int("SMART_PROFIT_TIER1_SIZE", 70),
    "smart_profit_tier2_pct": _get_int("SMART_PROFIT_TIER2_PCT", 12),  # Close 20% at 12%
    "smart_profit_tier2_size": _get_int("SMART_PROFIT_TIER2_SIZE", 20),
    "runner_trail_pct": _get_int("RUNNER_TRAIL_PCT", 2),  # Trail last 10% at 2%
    # Position sizing (fallback when database unavailable)
    "target_profit_per_1pct_move": _get_float("TARGET_PROFIT_PER_1PCT_MOVE", 1.0),  # Target profit per 1% price move (e.g., 1.0 = $1 per 1%, 3.0 = $3 per 1%)
    "allocation_per_position": _get_float("ALLOCATION_PER_POSITION"),  # Fixed allocation per position (None = auto)
    "margin_per_position": _get_float("MARGIN_PER_POSITION", 30.0),  # Fixed margin when position_sizing_mode is "margin"
    "max_positions": _get_int("MAX_POSITIONS", 6),  # Maximum concurrent positions
    "position_sizing_mode": _get_env("POSITION_SIZING_MODE", "risk"),  # risk for choppy pairs (wide ATR stop, size shrinks); margin = fixed $
    # Risk knobs: in risk mode these size the trade from the stop; always used as $ loss target.
    "risk_per_trade_usd": _get_float("RISK_PER_TRADE_USD", 6.0),  # Max $ loss if ATR stop is hit
    "risk_per_trade_pct": _get_float("RISK_PER_TRADE_PCT", 0.5),  # Fallback % of equity when USD not set
    "max_notional_per_position": _get_float("MAX_NOTIONAL_PER_POSITION"),  # Optional hard cap on notional exposure
    "min_notional_per_position": _get_float("MIN_NOTIONAL_PER_POSITION", 50.0),  # Floor so risk sizing never emits a sub-minimum order
    # Leftover reduce-only dust (e.g. ~$5) still counts as an open and blocks max_positions.
    "position_dust_notional_usd": _get_float("POSITION_DUST_NOTIONAL_USD", 8.0),
    "stop_loss_usd": _get_float("STOP_LOSS_USD", -6.0),  # Hard $ close = risk; keep in sync with RISK_PER_TRADE_USD
    # LLM via DeepSeek API (replaces OpenRouter)
    "deepseek_api_key": _get_env("DEEPSEEK_API_KEY", required=True),
    "deepseek_base_url": _get_env("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    "llm_model": _get_env("LLM_MODEL", "deepseek-chat"),  # Options: deepseek-chat, deepseek-reasoner
    # DeepSeek-specific settings
    "deepseek_max_tokens": _get_int("DEEPSEEK_MAX_TOKENS", 64000),  # Max for reasoner
    # Runtime controls via env
    # Asset configuration: support separate CRYPTO_ASSETS / STOCK_ASSETS / FOREX_ASSETS, or legacy ASSETS
    "crypto_assets": _get_env("CRYPTO_ASSETS"),  # e.g., "BTC ETH SOL BNB ZEC DOGE AVAX XLM XMR"
    "forex_assets": _get_env("FOREX_ASSETS"),  # e.g., "EURUSD GBPUSD USDJPY AUDUSD"
    "assets": _get_env("ASSETS"),  # Legacy: e.g., "BTC ETH SOL" or "BTC,ETH,SOL" (fallback if CRYPTO/STOCK/FOREX not set)
    "interval": _get_env("INTERVAL"),  # e.g., "5m", "1h" — LLM decision cadence
    # Mechanical TP/SL poll between LLM cycles (seconds). Keeps ROI take-profit accurate on a 15m INTERVAL.
    "exit_check_seconds": _get_float("EXIT_CHECK_SECONDS", 20.0),
    # Strategy selection (default: None/empty uses LLM trend strategy)
    "strategy": _get_env("STRATEGY"),  # Options: "default", "llm_trend", "scalping", "auto", etc.
    # Scalping strategy settings
    "scalping_risk_per_trade": _get_float("SCALPING_RISK_PER_TRADE", 10.0),  # Risk per trade in USD
    "scalping_tp_percent": _get_float("SCALPING_TP_PERCENT", 5.0),  # Take profit percentage for scalping (e.g., 5.0 = 5%) - close immediately at 5%
    "scalping_sl_percent": _get_float("SCALPING_SL_PERCENT", 3.0),  # Stop loss percentage (e.g., 3.0 = 3%) - updated from 0.5%
    "auto_strategy_cache_minutes": _get_int("AUTO_STRATEGY_CACHE_MINUTES", 0),  # 0 = re-evaluate every cycle, >0 = cache for X minutes
    # Volatility-adaptive exits (ATR-based). When enabled, the fixed TP/SL percentages are
    # replaced by distances derived from recent ATR, so the stop sits outside normal noise.
    "exit_mode": _get_env("EXIT_MODE", "atr"),  # "fixed" (legacy TP/SL %) or "atr" — controls STOP
    # price_percent = TAKE_PROFIT_PERCENT is % of entry price (what users mean by 5%/7% TP).
    "tp_mode": _get_env("TP_MODE", "roi_percent"),  # price_percent | roi_percent | usd | atr_rr
    "take_profit_usd": _get_float("TAKE_PROFIT_USD", 12.0),  # Absolute $ profit lock when tp_mode=usd
    "sl_atr_mult": _get_float("SL_ATR_MULT", 2.0),  # Stop distance = SL_ATR_MULT x ATR%
    "tp_rr_ratio": _get_float("TP_RR_RATIO", 2.0),  # Only used when tp_mode=atr_rr
    "atr_period": _get_int("ATR_PERIOD", 14),  # ATR lookback for exit sizing
    "min_stop_price_pct": _get_float("MIN_STOP_PRICE_PCT", 1.5),  # Floor — avoid noise-tight stops on quiet pairs
    "max_stop_price_pct": _get_float("MAX_STOP_PRICE_PCT", 7.0),  # Allow ~7% stops for choppy alts; risk mode shrinks size
    # Advanced position management
    "enable_trailing_stop": _get_bool("ENABLE_TRAILING_STOP", True),  # Enable trailing stop loss
    "trailing_stop_activation_r": _get_float("TRAILING_STOP_ACTIVATION_R", 1.0),  # Start trailing after X R of profit
    "trailing_stop_distance_r": _get_float("TRAILING_STOP_DISTANCE_R", 1.0),  # Trail X R behind peak price
    "enable_breakeven_stop": _get_bool("ENABLE_BREAKEVEN_STOP", True),  # Move stop to entry once in profit
    "breakeven_trigger_r": _get_float("BREAKEVEN_TRIGGER_R", 1.0),  # Move to breakeven at X R of profit
    "trailing_stop_activation_pct": _get_float("TRAILING_STOP_ACTIVATION_PCT", 5.0),  # Legacy fixed-mode ROI trigger
    "trailing_stop_distance_pct": _get_float("TRAILING_STOP_DISTANCE_PCT", 3.0),  # Legacy fixed-mode price distance
    # Partial take-profit ladder, in R multiples: "R:percent_of_original_size, ..."
    # Banking part of the move at 1R is what converts marginal trades into small wins.
    "enable_profit_ladder": _get_bool("ENABLE_PROFIT_LADDER", True),
    "profit_ladder": _get_env("PROFIT_LADDER", "1.0:50,2.0:30"),  # Remainder rides the trailing stop
    "breakeven_after_first_fill": _get_bool("BREAKEVEN_AFTER_FIRST_FILL", True),  # Stop to entry once a rung fills
    # Re-entry control — prevents the same pair being flipped repeatedly in chop
    "reentry_cooldown_minutes": _get_float("REENTRY_COOLDOWN_MINUTES", 45.0),  # Block re-entry on an asset after a close
    "loss_reentry_cooldown_minutes": _get_float("LOSS_REENTRY_COOLDOWN_MINUTES", 90.0),  # Longer block after a losing close
    "block_direction_flip": _get_bool("BLOCK_DIRECTION_FLIP", True),  # Forbid opposite-side entry during cooldown
    # Account-level circuit breakers — pause new entries, never touch open positions
    "max_daily_loss_usd": _get_float("MAX_DAILY_LOSS_USD"),  # Absolute daily realised loss limit (None = use pct)
    "max_daily_loss_pct": _get_float("MAX_DAILY_LOSS_PCT", 3.0),  # Daily realised loss limit as % of equity
    "max_consecutive_losses": _get_int("MAX_CONSECUTIVE_LOSSES", 4),  # Pause after this many losses in a row (0 = off)
    "loss_streak_pause_minutes": _get_float("LOSS_STREAK_PAUSE_MINUTES", 120.0),  # How long a streak pause lasts
    # Higher-timeframe trend agreement — rejects counter-trend entries and range chop
    "enable_htf_trend_filter": _get_bool("ENABLE_HTF_TREND_FILTER", True),
    "htf_trend_timeframe": _get_env("HTF_TREND_TIMEFRAME", "1h"),
    "htf_trend_fast_ema": _get_int("HTF_TREND_FAST_EMA", 20),
    "htf_trend_slow_ema": _get_int("HTF_TREND_SLOW_EMA", 50),
    "htf_trend_min_separation_pct": _get_float("HTF_TREND_MIN_SEPARATION_PCT", 0.15),  # Below this the HTF is a range
    "htf_block_when_flat": _get_bool("HTF_BLOCK_WHEN_FLAT", True),  # Skip entries while the HTF is flat
    # Analysis timeframes (previously hardcoded in the market-data builder)
    "intraday_timeframe": _get_env("INTRADAY_TIMEFRAME", "15m"),
    "longterm_timeframe": _get_env("LONGTERM_TIMEFRAME", "4h"),
    "trading_enabled": _get_bool("TRADING_ENABLED", True),  # Enable/disable trading (when False, skips new entries but still monitors/closes positions)
    "max_position_hold_hours": _get_float("MAX_POSITION_HOLD_HOURS", 24.0),  # Maximum hours to hold a position
    "enable_drawdown_protection": _get_bool("ENABLE_DRAWDOWN_PROTECTION", True),  # Enable drawdown protection
    "max_drawdown_from_peak_pct": _get_float("MAX_DRAWDOWN_FROM_PEAK_PCT", 5.0),  # Close if profit drops X% from peak
    "loss_protection_pct": _get_float("LOSS_PROTECTION_PCT", 5.0),  # Close if position is down X% (backup protection)
    "drawdown_min_peak_profit_pct": _get_float("DRAWDOWN_MIN_PEAK_PROFIT_PCT", 3.0),  # Apply drawdown protection only after peak profit reaches this %
    "drawdown_confirm_cycles": _get_int("DRAWDOWN_CONFIRM_CYCLES", 2),  # Require N consecutive drawdown checks before closing
    "loss_protection_min_hours": _get_float("LOSS_PROTECTION_MIN_HOURS", 1.0),  # Minimum hours open before loss-protection can close
    "loss_protection_confirm_cycles": _get_int("LOSS_PROTECTION_CONFIRM_CYCLES", 2),  # Require N consecutive loss checks before closing
    # Pair Hunter - Dynamic pair discovery (ASSETS remains fallback if hunter fails)
    "enable_pair_hunter": _get_bool("ENABLE_PAIR_HUNTER", False),  # Enable automatic pair discovery
    "pair_hunter_top_n": _get_int("PAIR_HUNTER_TOP_N", 5),  # Number of top pairs to hunt
    "pair_hunter_refresh_interval": _get_int("PAIR_HUNTER_REFRESH_INTERVAL", 5),  # Refresh hunt every N loops
    "pair_hunter_min_volatility": _get_float("PAIR_HUNTER_MIN_VOLATILITY", 1.0),  # Minimum ATR% (too quiet = skip)
    "pair_hunter_max_volatility": _get_float("PAIR_HUNTER_MAX_VOLATILITY", 3.5),  # Cap hyper-volatile meme-style names
    "pair_hunter_ideal_volatility": _get_float("PAIR_HUNTER_IDEAL_VOLATILITY", 2.0),  # Sweet-spot ATR% for scoring
    "pair_hunter_min_volume_24h": _get_float("PAIR_HUNTER_MIN_VOLUME_24H", 50_000_000.0),  # Liquidity floor (USD)
    "pair_hunter_min_trend_strength": _get_float("PAIR_HUNTER_MIN_TREND_STRENGTH", 25.0),  # Reject chop below this
    "pair_hunter_min_price": _get_float("PAIR_HUNTER_MIN_PRICE", 0.01),  # Avoid sub-penny tick-size noise
    "pair_hunter_max_spread_pct": _get_float("PAIR_HUNTER_MAX_SPREAD_PCT", 0.5),  # Max acceptable bid-ask spread
    "pair_hunter_timeframe": _get_env("PAIR_HUNTER_TIMEFRAME", "15m"),  # Timeframe used for scoring candles
    "pair_hunter_blacklist": _get_env(
        "PAIR_HUNTER_BLACKLIST",
        "SHIB PEPE FLOKI BONK WIF MEME DOGE 1000SATS LUNA FTT",
    ),  # Space/comma separated assets never to trade
    "pair_hunter_weight_volatility": _get_float("PAIR_HUNTER_WEIGHT_VOLATILITY", 0.25),
    "pair_hunter_weight_trend": _get_float("PAIR_HUNTER_WEIGHT_TREND", 0.40),
    "pair_hunter_weight_setup": _get_float("PAIR_HUNTER_WEIGHT_SETUP", 0.35),
    "pair_hunter_max_analyze_assets": _get_int("PAIR_HUNTER_MAX_ANALYZE_ASSETS", 8),  # Cap on assets passed to LLM per cycle
    "pair_hunter_perf_min_trades": _get_int("PAIR_HUNTER_PERF_MIN_TRADES", 3),  # Minimum completed trades before using performance score
    "pair_hunter_perf_filter_min_trades": _get_int("PAIR_HUNTER_PERF_FILTER_MIN_TRADES", 3),  # Minimum completed trades before filtering weak pairs
    "pair_hunter_perf_filter_min_win_rate": _get_float("PAIR_HUNTER_PERF_FILTER_MIN_WIN_RATE", 25.0),  # Legacy; filter now keys off expectancy
    "pair_hunter_perf_filter_min_expectancy_usd": _get_float("PAIR_HUNTER_PERF_FILTER_MIN_EXPECTANCY_USD", 0.0),  # Drop a pair once expectancy/trade is below this
    "pair_hunter_data_fail_threshold": _get_int("PAIR_HUNTER_DATA_FAIL_THRESHOLD", 3),  # Consecutive TA-data failures before temporary exclusion
    "pair_hunter_exclusion_cooldown_minutes": _get_int("PAIR_HUNTER_EXCLUSION_COOLDOWN_MINUTES", 180),  # Exclusion window for repeatedly invalid symbols
    # Webhook notifications (WhatsApp, Discord, etc.)
    "webhook_url": _get_env("WEBHOOK_URL"),  # URL to POST trade notifications (e.g., Telegram bot, Discord webhook)
    "enable_webhook_notifications": _get_bool("ENABLE_WEBHOOK_NOTIFICATIONS", False),  # Enable webhook alerts
    "telegram_bot_token": _get_env("TELEGRAM_BOT_TOKEN"),  # Optional native Telegram mode
    "telegram_chat_id": _get_env("TELEGRAM_CHAT_ID"),  # Optional native Telegram mode
    "position_update_interval": _get_int("POSITION_UPDATE_INTERVAL", 6),  # Send position summary every N cycles (6 × 5min = 30min)
    # API server (DigitalOcean App Platform uses PORT, fallback to APP_PORT/API_PORT/3000)
    "api_host": _get_env("API_HOST", "0.0.0.0"),
    "api_port": _get_env("PORT") or _get_env("APP_PORT") or _get_env("API_PORT") or "3000",
    # Alert service configuration
    "ALERT_SERVICE_ENABLED": _get_bool("ALERT_SERVICE_ENABLED", False),
    "ALERT_SERVICE_PORT": _get_int("ALERT_SERVICE_PORT", 8080),
    "ALERT_CHECK_INTERVAL": _get_int("ALERT_CHECK_INTERVAL", 5),
    "ALERT_AGENT_ENDPOINT": _get_env("ALERT_AGENT_ENDPOINT", "http://localhost:5000/api/alert/signal"),
    "ALERT_ASSETS": _get_env("ALERT_ASSETS", "BTC,ETH,SOL"),
    "ALERT_RISK_PER_TRADE": _get_float("ALERT_RISK_PER_TRADE", 20.0),
    "ALERT_TIMEFRAME": _get_env("ALERT_TIMEFRAME", "5m"),  # Default timeframe (5m, 15m, 1h, etc.)
    # Multi-exchange mode (trade on multiple exchanges simultaneously)
    "MULTI_EXCHANGE_MODE": _get_env("MULTI_EXCHANGE_MODE", "false"),  # Set to "true" to enable
    # When false, forex pairs are never analyzed/traded (even if FOREX_ASSETS is set).
    "ENABLE_FOREX": _get_env("ENABLE_FOREX", "false"),
    # Forex-specific overrides (used when MULTI_EXCHANGE_MODE routes pairs to IG)
    "forex_interval": _get_env("FOREX_INTERVAL", "15m"),
    "forex_leverage": _get_int("FOREX_LEVERAGE", 20),
    "forex_risk_per_trade_usd": _get_float("FOREX_RISK_PER_TRADE_USD"),
    "forex_exit_mode": _get_env("FOREX_EXIT_MODE"),  # defaults to global EXIT_MODE when unset
    "forex_sl_atr_mult": _get_float("FOREX_SL_ATR_MULT"),
    "forex_tp_rr_ratio": _get_float("FOREX_TP_RR_RATIO"),
    "skip_llm_without_market_data": _get_bool("SKIP_LLM_WITHOUT_MARKET_DATA", True),
}

def _parse_assets(assets_str: str | None) -> list[str]:
    """Parse assets from environment variable (supports space or comma separated).
    
    Args:
        assets_str: Asset string like "BTC ETH SOL" or "BTC,ETH,SOL"
        
    Returns:
        List of asset symbols (uppercase, stripped)
    """
    if not assets_str:
        return []
    # Support both space and comma separated
    if "," in assets_str:
        return [a.strip().upper() for a in assets_str.split(",") if a.strip()]
    else:
        return [a.strip().upper() for a in assets_str.split(" ") if a.strip()]

# Parse assets list
_assets_list = _parse_assets(CONFIG.get("assets"))
if _assets_list:
    CONFIG["assets"] = " ".join(_assets_list)
    CONFIG["assets_list"] = _assets_list
else:
    CONFIG["assets"] = None
    CONFIG["assets_list"] = []

# Load per-asset leverage settings (e.g., ZEC_LEVERAGE=5, BTC_LEVERAGE=10)
# This scans all environment variables matching {ASSET}_LEVERAGE pattern
_per_asset_leverage = {}
for key, value in os.environ.items():
    if key.endswith("_LEVERAGE") and key != "BINANCE_LEVERAGE":
        asset = key[:-9].upper()  # Remove "_LEVERAGE" suffix
        try:
            leverage = int(value)
            if leverage > 0:
                _per_asset_leverage[asset] = leverage
        except (ValueError, TypeError):
            pass  # Skip invalid values

# Load per-asset timeframe settings (e.g., BTC_TIMEFRAME=15m, ETH_TIMEFRAME=5m)
# This scans all environment variables matching {ASSET}_TIMEFRAME pattern
_per_asset_timeframe = {}
for key, value in os.environ.items():
    if key.endswith("_TIMEFRAME") and key != "ALERT_TIMEFRAME":
        asset = key[:-10].upper()  # Remove "_TIMEFRAME" suffix
        if asset and value:
            _per_asset_timeframe[asset] = value.strip()


def get_leverage_for_asset(asset: str, default_leverage: int) -> int:
    """Get leverage for a specific asset.
    
    Args:
        asset: Asset symbol (e.g., 'ZEC', 'BTC')
        default_leverage: Default leverage to use if no per-asset setting exists
        
    Returns:
        Leverage for the asset (per-asset setting if exists, otherwise default)
    """
    asset_upper = asset.upper()
    # Check exact match first
    if asset_upper in _per_asset_leverage:
        return _per_asset_leverage[asset_upper]
    # Check without USDT suffix if asset has it
    if asset_upper.endswith("USDT"):
        base_asset = asset_upper[:-4]
        if base_asset in _per_asset_leverage:
            return _per_asset_leverage[base_asset]
    return default_leverage


def get_timeframe_for_asset(asset: str, default_timeframe: str) -> str:
    """Get timeframe for a specific asset.
    
    Args:
        asset: Asset symbol (e.g., 'BTC', 'ETH')
        default_timeframe: Default timeframe to use if no per-asset setting exists (e.g., '5m', '15m')
        
    Returns:
        Timeframe for the asset (per-asset setting if exists, otherwise default)
    """
    asset_upper = asset.upper()
    # Check exact match first
    if asset_upper in _per_asset_timeframe:
        return _per_asset_timeframe[asset_upper]
    # Check without USDT suffix if asset has it
    if asset_upper.endswith("USDT"):
        base_asset = asset_upper[:-4]
        if base_asset in _per_asset_timeframe:
            return _per_asset_timeframe[base_asset]
    return default_timeframe
