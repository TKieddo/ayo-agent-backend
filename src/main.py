"""Entry-point script that wires together the trading agent, data feeds, and API."""

import sys
import argparse
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))
from src.strategies.strategy_factory import StrategyFactory
from src.indicators.technical_analysis_client import TechnicalAnalysisClient
from src.config_loader import CONFIG, get_leverage_for_asset
from src.pair_hunter import PairHunter, get_best_pairs, filter_assets_to_trading_venue
from src.webhook_notifier import WebhookNotifier
import asyncio
import logging
import time
from collections import deque, OrderedDict
from datetime import datetime, timezone, timedelta
import math  # For Sharpe
from dotenv import load_dotenv
import os
import json
import fnmatch
from aiohttp import web, ClientSession, ClientTimeout
try:
    import httpx
except ImportError:
    httpx = None  # httpx may not be installed, handle gracefully
from src.utils.formatting import format_number as fmt, format_size as fmt_sz
from src.utils.prompt_utils import json_default, round_or_none, round_series
from src.utils.trading_settings import get_trading_settings, get_max_leverage_for_asset, calculate_tp_sl_prices, calculate_allocation_usd, calculate_risk_based_allocation, resolve_stop_loss_usd, resolve_max_loss_usd, tighten_sl_to_max_loss
from src.utils.volatility_stops import ExitPlan, build_exit_plan, normalize_tp_mode
from src.utils import reentry_guard, profit_ladder, risk_governor
from src.utils import mechanical_scalp as mech
from src.utils import stock_strategy as stock_strat
from src.utils.stock_session import is_stock_market_open_for_entries
from src.stock_scanner import scan_stocks, stock_universe
from src.utils.trend_filter import check_trend_agreement
from src.utils.forex_session import is_forex_session_open, looks_like_forex as _looks_like_forex

load_dotenv()

# Coolify probes / as soon as the container starts. Exchange init takes several
# seconds, so answer health on PORT immediately, then hand the port to aiohttp.
_EARLY_HEALTH = {"server": None}


def _api_bind():
    host = os.getenv("API_HOST") or os.getenv("HOST") or "0.0.0.0"
    port = int(os.getenv("PORT") or os.getenv("APP_PORT") or os.getenv("API_PORT") or "8080")
    return host, port


def start_early_health_server() -> None:
    """Serve GET / and /health before the main aiohttp app is ready."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    host, port = _api_bind()

    class _BootHealth(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = b'{"ok":true,"service":"ayo-trading-agent","booting":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A003
            return

    try:
        server = ThreadingHTTPServer((host, port), _BootHealth)
    except OSError as exc:
        logging.warning("Early health server could not bind %s:%s (%s)", host, port, exc)
        return
    _EARLY_HEALTH["server"] = server
    threading.Thread(target=server.serve_forever, name="early-health", daemon=True).start()
    logging.info("Early health server listening on %s:%s", host, port)


def stop_early_health_server() -> None:
    server = _EARLY_HEALTH.get("server")
    if not server:
        return
    try:
        server.shutdown()
        server.server_close()
    except Exception:
        pass
    _EARLY_HEALTH["server"] = None



def _safe_float(value, default: float = 0.0) -> float:
    """float() that tolerates None / '' / bad LLM JSON (avoids TypeError on NoneType)."""
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


# Exit plan per open position (asset -> ExitPlan), set at entry so the monitor loop enforces
# the same ATR-derived stop/target the position was sized against.
_EXIT_PLANS: dict[str, ExitPlan] = {}

# Latest resolved trading settings, refreshed each loop so helpers outside run_loop()
# (which owns the local `trading_settings`) can still read live values.
_LIVE_SETTINGS: dict = {}

# Most recent account equity, used to express the daily loss limit as a percentage.
_LAST_KNOWN_EQUITY: dict = {}

# Mechanical scalp book state (AI parked). Pending resting entry, per-asset last pullback
# traded, rolling open interest, last skip reason per asset, and the persisted scorecard.
_MECH_STATE: dict = {
    "pending": None,          # {asset, ord_id, side, placed_at, stop, target, contracts, coins, ttl_s}
    "last_signal_ts": {},     # asset -> ms of the pullback candle last traded
    "skips": {},              # asset -> {"reason", "at", "metrics"}
    "last_cycle_at": None,
    "last_pair_refresh_at": None,
    "managed_assets": set(),  # assets whose open position came from this book
    "open_trades": {},        # asset -> fill record (survives active_trades reconciliation)
}

# Alpaca stock book — independent of crypto mechanical / pair hunter
_STOCK_STATE: dict = {
    "active_set": [],         # scanner top-N symbols
    "last_scan_at": None,
    "managed_assets": set(),
    "open_trades": {},        # symbol -> {entry, stop, tp, qty, side, scale_out_done}
    "skips": {},
    "last_cycle_at": None,
    "scanner_rows": [],
}
_MECH_OI = mech.OIHistory()
_MECH_STATS = mech.ScalpStats()

# Store original stdout/stderr BEFORE creating handlers (Railway logging fix)
# This ensures handlers write to the original streams, not redirected ones
original_stdout = sys.stdout
original_stderr = sys.stderr

# Create a log file handler that captures all output
log_file_path = "trading_agent.log"

# Remove existing handlers to avoid duplicates
logging.getLogger().handlers = []

# Create file handler for comprehensive logging
file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Railway logging fix: Route INFO/DEBUG/WARNING to stdout, ERROR/CRITICAL to stderr
# This ensures Railway correctly classifies log levels instead of marking everything as error
# Railway treats: stdout -> "info", stderr -> "error"
# WARNING is not an error, so it should go to stdout (shown as "info" in Railway)
class WarningAndBelowFilter(logging.Filter):
    """Filter to allow DEBUG, INFO, and WARNING level logs (but not ERROR/CRITICAL)."""
    def filter(self, record):
        return record.levelno <= logging.WARNING

# Handler for INFO/DEBUG/WARNING logs -> stdout (Railway will classify as "info")
stdout_handler = logging.StreamHandler(original_stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(WarningAndBelowFilter())
stdout_handler.setFormatter(file_formatter)

# Handler for ERROR/CRITICAL logs -> stderr (Railway will classify as "error")
stderr_handler = logging.StreamHandler(original_stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(file_formatter)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)

# Suppress verbose DEBUG logs from HTTP/2 libraries (httpx, hyper, h2, httpcore)
# These libraries generate very verbose protocol-level logs that aren't useful for normal operation
noisy_libraries = [
    'httpx', 'hyper', 'h2', 'httpcore', 'hpack', 'hstspreload',
    'urllib3.connectionpool', 'urllib3.util.retry', 'requests.packages.urllib3'
]
for lib_name in noisy_libraries:
    lib_logger = logging.getLogger(lib_name)
    lib_logger.setLevel(logging.WARNING)  # Only show WARNING and above from these libraries

# Redirect print statements to logging
# Note: Handlers use original_stdout/stderr, so print() -> logger -> handlers -> original streams (no recursion)
class PrintToLog:
    """Redirect print() calls to logging."""
    def write(self, text):
        if text.strip():  # Only log non-empty lines
            logger.info(text.strip())
    
    def flush(self):
        pass

# Redirect stdout to capture print() statements as INFO logs
# This goes through our logging handlers which write to the original stdout/stderr
sys.stdout = PrintToLog()
# Don't redirect stderr - keep it for actual errors and WARNING/ERROR logs

logging.info("=" * 80)
logging.info("Trading Agent Starting")
logging.info("=" * 80)


def clear_terminal():
    """Clear the terminal screen on Windows or POSIX systems. No-op in Coolify/Docker."""
    if not sys.stdout.isatty() or not os.getenv("TERM"):
        return
    os.system('cls' if os.name == 'nt' else 'clear')


def get_interval_seconds(interval_str):
    """Convert interval strings like '5m', '1h', '1hr', or '1d' to seconds."""
    interval_str = interval_str.strip().lower()  # Normalize input
    
    if interval_str.endswith('m'):
        return int(interval_str[:-1]) * 60
    elif interval_str.endswith('h') or interval_str.endswith('hr'):
        # Handle both 'h' and 'hr' formats (e.g., '1h' or '1hr')
        if interval_str.endswith('hr'):
            return int(interval_str[:-2]) * 3600
        else:
            return int(interval_str[:-1]) * 3600
    elif interval_str.endswith('d'):
        return int(interval_str[:-1]) * 86400
    else:
        raise ValueError(f"Unsupported interval: {interval_str}. Supported formats: '5m', '1h', '1hr', '1d', etc.")

def main():
    """Parse CLI args, bootstrap dependencies, and launch the trading loop."""
    # Answer Coolify/Docker healthchecks before exchange bootstrap blocks the port.
    try:
        start_early_health_server()
    except Exception as e:
        logging.warning("Early health server failed to start: %s", e)
    try:
        clear_terminal()
    except Exception as e:
        print(f"Warning: Could not clear terminal: {e}", file=original_stderr)
    parser = argparse.ArgumentParser(description="LLM-based Trading Agent")
    parser.add_argument("--assets", type=str, nargs="+", required=False, help="Assets to trade, e.g., BTC ETH")
    parser.add_argument("--interval", type=str, required=False, help="Interval period, e.g., 1h")
    args = parser.parse_args()

    # Allow assets/interval via .env (CONFIG) if CLI not provided
    from src.config_loader import CONFIG

    def _split_assets(raw: str) -> list:
        if not raw:
            return []
        sep = "," if "," in raw else " "
        return [a.strip().upper() for a in raw.split(sep) if a.strip()]

    assets_env = CONFIG.get("assets")
    interval_env = CONFIG.get("interval")
    if (not args.assets or len(args.assets) == 0):
        # Hard-split by venue type: never mix stocks into an OKX-only process (or
        # crypto into Alpaca-only). Forex stays out of the base list until IG is live.
        exchange_for_universe = (CONFIG.get("exchange") or "aster").lower()
        multi_flag = str(CONFIG.get("MULTI_EXCHANGE_MODE", "false")).lower() == "true"
        merged = []
        _fx_on = str(CONFIG.get("ENABLE_FOREX", CONFIG.get("enable_forex", "false"))).lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if multi_flag:
            for key in ("crypto_assets", "stock_assets", "assets"):
                merged.extend(_split_assets(CONFIG.get(key) or ""))
            if _fx_on:
                merged.extend(_split_assets(CONFIG.get("forex_assets") or ""))
        elif exchange_for_universe in ("alpaca",):
            merged.extend(_split_assets(CONFIG.get("stock_assets") or ""))
        elif exchange_for_universe in ("ig", "ig.com", "igmarkets"):
            if _fx_on:
                merged.extend(_split_assets(CONFIG.get("forex_assets") or ""))
        else:
            # Crypto venues (okx / binance / aster): CRYPTO_ASSETS + legacy ASSETS only
            merged.extend(_split_assets(CONFIG.get("crypto_assets") or ""))
            merged.extend(_split_assets(CONFIG.get("assets") or ""))
        seen = set()
        args.assets = []
        for a in merged:
            if a not in seen:
                seen.add(a)
                args.assets.append(a)
        if not args.assets and assets_env and exchange_for_universe not in ("alpaca", "ig", "ig.com", "igmarkets"):
            args.assets = _split_assets(assets_env)
    if not args.interval and interval_env:
        args.interval = interval_env

    if not args.assets or not args.interval:
        parser.error(
            "Please provide --assets and --interval, or set ASSETS / STOCK_ASSETS / CRYPTO_ASSETS and INTERVAL in .env"
        )

    # Initialize multi-exchange manager for simultaneous trading
    taapi = TechnicalAnalysisClient()
    exchange_name = CONFIG.get("exchange", "aster").lower()
    
    # Multi-exchange: explicit flag, or auto when a second venue (IG/Alpaca) is configured
    use_multi_exchange = str(CONFIG.get("MULTI_EXCHANGE_MODE", "false")).lower() == "true"
    _enable_forex = str(CONFIG.get("ENABLE_FOREX", CONFIG.get("enable_forex", "false"))).lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if not use_multi_exchange:
        _has_ig = bool(CONFIG.get("ig_api_key") and CONFIG.get("ig_username") and CONFIG.get("ig_password"))
        _has_crypto = bool(
            (CONFIG.get("okx_api_key") and CONFIG.get("okx_api_secret") and CONFIG.get("okx_passphrase"))
            or (CONFIG.get("binance_api_key") and CONFIG.get("binance_api_secret"))
            or (CONFIG.get("aster_user_address") and CONFIG.get("aster_private_key"))
        )
        _has_fx = bool(str(CONFIG.get("forex_assets") or "").strip())
        _has_alpaca = bool(CONFIG.get("alpaca_api_key") and CONFIG.get("alpaca_api_secret"))
        _has_stocks = bool(str(CONFIG.get("stock_assets") or "").strip())
        if _enable_forex and _has_ig and _has_crypto and _has_fx:
            use_multi_exchange = True
            logging.info("🌐 Auto-enabled multi-exchange (crypto venue + IG forex credentials detected)")
        elif _has_alpaca and _has_crypto and _has_stocks:
            use_multi_exchange = True
            logging.info("🌐 Auto-enabled multi-exchange (crypto venue + Alpaca stocks detected)")

    if use_multi_exchange:
        # Multi-exchange mode: trade on multiple exchanges simultaneously
        from src.trading.multi_exchange_manager import MultiExchangeManager
        exchange_manager = MultiExchangeManager()
        logging.info("🌐 Multi-exchange mode enabled - trading on multiple exchanges simultaneously")
        
        # Prefer the configured primary exchange (usually OKX) over insertion order
        preferred = (CONFIG.get("exchange") or "").lower()
        available = exchange_manager.list_exchanges()
        primary_exchange_name = preferred if preferred in available else (available[0] if available else None)
        if primary_exchange_name:
            exchange = exchange_manager.exchanges[primary_exchange_name]
            hyperliquid = exchange  # Backward compatibility
            logging.info(f"📌 Primary exchange (for compatibility): {primary_exchange_name}")
            logging.info(f"📌 Asset routing: {exchange_manager.asset_to_exchange}")
        else:
            raise ValueError("No exchanges available in multi-exchange mode")
    else:
        # Single exchange mode (original behavior)
        if exchange_name == "aster":
            from src.trading.aster_api import AsterAPI
            try:
                exchange = AsterAPI()
                hyperliquid = exchange  # Backward compatibility alias
                logging.info(f"✅ Using Aster DEX (default)")
            except ValueError as e:
                logging.error(f"❌ Failed to initialize Aster: {e}")
                logging.info("💡 Falling back to Binance...")
                from src.trading.binance_api import BinanceAPI
                try:
                    exchange = BinanceAPI()
                    exchange_name = "binance"
                    testnet = CONFIG.get("binance_testnet", False)
                    logging.info(f"✅ Using Binance Futures ({'testnet' if testnet else 'mainnet'})")
                except ValueError as e2:
                    logging.error(f"❌ Failed to initialize Binance: {e2}")
                    raise ValueError(f"Failed to initialize any exchange. Aster: {e}, Binance: {e2}")
        elif exchange_name == "binance":
            from src.trading.binance_api import BinanceAPI
            try:
                exchange = BinanceAPI()
                testnet = CONFIG.get("binance_testnet", False)
                logging.info(f"✅ Using Binance Futures ({'testnet' if testnet else 'mainnet'})")
            except ValueError as e:
                logging.error(f"❌ Failed to initialize Binance: {e}")
                raise ValueError(f"Binance initialization failed: {e}")
        elif exchange_name == "alpaca":
            from src.trading.alpaca_api import AlpacaAPI
            try:
                exchange = AlpacaAPI()
                paper = CONFIG.get("alpaca_paper", True)
                logging.info(f"✅ Using Alpaca ({'paper' if paper else 'LIVE'}) for stocks + crypto")
                if paper:
                    logging.info("📝 Paper mode: no real money. Set ALPACA_PAPER=false only when ready for live.")
            except ValueError as e:
                logging.error(f"❌ Failed to initialize Alpaca: {e}")
                raise ValueError(f"Alpaca initialization failed: {e}")
        elif exchange_name in ("ig", "ig.com", "igmarkets"):
            from src.trading.ig_api import IGAPI
            try:
                exchange = IGAPI()
                exchange_name = "ig"
                demo = CONFIG.get("ig_demo", True)
                logging.info(f"✅ Using IG Markets ({'DEMO' if demo else 'LIVE'}) for forex/CFDs")
                if demo:
                    logging.info("📝 Demo mode: watch positions in IG demo platform / web")
            except ValueError as e:
                logging.error(f"❌ Failed to initialize IG: {e}")
                raise ValueError(f"IG initialization failed: {e}")
        elif exchange_name in ("okx", "okex"):
            from src.trading.okx_api import OKXAPI
            try:
                exchange = OKXAPI()
                exchange_name = "okx"
                demo = CONFIG.get("okx_demo", True)
                logging.info(f"✅ Using OKX ({'DEMO' if demo else 'LIVE'}) for crypto SWAP")
                if demo:
                    logging.info("📝 Demo mode: x-simulated-trading=1 — create keys under Demo Trading API")
            except ValueError as e:
                logging.error(f"❌ Failed to initialize OKX: {e}")
                raise ValueError(f"OKX initialization failed: {e}")
        else:
            raise ValueError(
                f"Unknown exchange: {exchange_name}. Use 'aster', 'binance', 'alpaca', 'ig', or 'okx'"
            )
        
        exchange_manager = None
    
    # Use 'exchange' as the variable name throughout (aliased for backward compatibility)
    hyperliquid = exchange  # Keep for backward compatibility in code
    
    # Initialize trading strategy using factory
    strategy_name = CONFIG.get("strategy")
    strategy_mode = "AUTO" if strategy_name and strategy_name.lower() == "auto" else "MANUAL"
    
    try:
        strategy = StrategyFactory.get_strategy(strategy_name)
        if strategy_mode == "AUTO":
            logging.info(f"🤖 Strategy Mode: AUTO - LLM will select best strategy based on market conditions")
        else:
            selected_name = strategy_name if strategy_name else "default (LLM Trend)"
            logging.info(f"🎯 Strategy Mode: MANUAL - Using strategy: {selected_name}")
            logging.info(f"   Strategy Class: {strategy.get_name()}")
    except ValueError as e:
        logging.error(f"❌ Strategy initialization failed: {e}")
        logging.info("💡 Falling back to default strategy...")
        strategy = StrategyFactory.get_strategy("default")
        strategy_mode = "MANUAL"
        logging.info(f"🎯 Strategy Mode: MANUAL - Using strategy: default (LLM Trend)")
    
    # For backward compatibility, keep 'agent' variable name
    agent = strategy

    # Fetch and display wallet balance on startup
    async def check_wallet_balance():
        try:
            if exchange_name == "aster":
                user_address = exchange.user_address
                signer_address = exchange.signer_address
                logging.info(f"📍 User Address (main): {user_address}")
                logging.info(f"📍 Signer Address (API): {signer_address}")
                logging.info(f"🔗 View on Aster: https://www.asterdex.com/en")
            elif exchange_name == "alpaca":
                paper = CONFIG.get("alpaca_paper", True)
                logging.info(f"🔗 Alpaca dashboard: https://app.alpaca.markets/")
                logging.info(f"📝 Mode: {'PAPER' if paper else 'LIVE'}")
            elif exchange_name == "ig":
                demo = CONFIG.get("ig_demo", True)
                logging.info(f"🔗 IG platform: https://www.ig.com/ (login → demo if IG_DEMO=true)")
                logging.info(f"📝 Mode: {'DEMO' if demo else 'LIVE'}")
            elif exchange_name == "okx":
                demo = CONFIG.get("okx_demo", True)
                logging.info(f"🔗 OKX: https://www.okx.com/ → Trade → Demo Trading")
                logging.info(f"📝 Mode: {'DEMO' if demo else 'LIVE'}")
            state = await hyperliquid.get_user_state()
            balance = state.get('balance', 0)
            total_value = state.get('total_value', 0)
            positions_count = len(state.get('positions', []))
            
            logging.info(f"💰 Account Balance / Buying Power: ${balance:.2f}")
            logging.info(f"💵 Total Account Value: ${total_value:.2f}")
            logging.info(f"📈 Open Positions: {positions_count}")
            if exchange_name == "alpaca":
                meta = state.get("alpaca_account") or {}
                logging.info(
                    f"🦙 Alpaca status={meta.get('status')} PDT={meta.get('pattern_day_trader')} "
                    f"daytrades={meta.get('daytrade_count')}"
                )
            if exchange_name == "ig":
                meta = state.get("ig_account") or {}
                logging.info(f"📊 IG account_id={meta.get('account_id')} demo={meta.get('demo')}")
            if exchange_name == "okx":
                meta = state.get("okx_account") or {}
                logging.info(f"📊 OKX demo={meta.get('demo')} td_mode={meta.get('td_mode')}")
            
            if balance == 0 and total_value == 0:
                logging.warning("⚠️  WARNING: Wallet balance is $0.00 - Make sure you have funds in your wallet!")
            elif balance < 10:
                logging.warning(f"⚠️  WARNING: Low balance (${balance:.2f}) - Consider adding more funds for trading")
            
            # Reminder about API wallet authorization
            if exchange_name == "aster":
                logging.info("💡 Note: Make sure your API wallet is authorized in Aster dashboard")
                logging.info("   If trades fail, check that the API wallet has proper permissions")
            elif exchange_name == "binance":
                logging.info("💡 Note: Make sure your Binance API keys have Futures trading permissions enabled")
                logging.info("   If trades fail, check that your API keys have the correct permissions in Binance settings")
            
            # Show trading status
            trading_enabled = CONFIG.get("trading_enabled", True)
            if trading_enabled:
                logging.info("✅ TRADING ENABLED: Agent will execute new trades and manage positions")
            else:
                logging.info("⏸️  TRADING PAUSED: Agent will monitor positions only (no new entries)")
                logging.info("   Set TRADING_ENABLED=true in Railway/environment to resume trading")
        except Exception as e:
            logging.error(f"❌ Failed to fetch wallet balance on startup: {e}")
            logging.warning("⚠️  Trading agent will continue, but verify your wallet connection")
    
    # Run the balance check synchronously before starting the async server
    asyncio.run(check_wallet_balance())

    start_time = datetime.now(timezone.utc)
    invocation_count = 0
    trade_log = []  # For Sharpe: list of returns
    active_trades = []  # {'asset','is_long','amount','entry_price','tp_oid','sl_oid','exit_plan'}
    recent_events = deque(maxlen=200)
    diary_path = "diary.jsonl"
    pair_hunter_stats_path = "pair_hunter_stats.json"
    initial_account_value = None
    # Perp mid-price history sampled each loop (authoritative, avoids spot/perp basis mismatch)
    price_history = {}
    pair_hunter_stats = {}
    pair_hunter_db_api_disabled = False
    pair_hunter_supabase_client = None
    pair_hunter_supabase_unavailable_logged = False

    logging.info(f"Starting trading agent for assets: {args.assets} at interval: {args.interval}")

    def add_event(msg: str):
        """Log an informational event and push it into the recent events deque."""
        logging.info(msg)
        recent_events.append({"timestamp": datetime.now(timezone.utc).isoformat(), "message": msg})

    def _load_pair_hunter_stats_local():
        """Load local fallback pair-hunter performance stats."""
        nonlocal pair_hunter_stats
        try:
            if os.path.exists(pair_hunter_stats_path):
                with open(pair_hunter_stats_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    pair_hunter_stats = loaded
                    add_event(f"📚 Loaded Pair Hunter performance stats for {len(pair_hunter_stats)} assets")
        except Exception as e:
            logging.warning(f"Could not load pair_hunter_stats.json: {e}")
            pair_hunter_stats = {}

    def _save_pair_hunter_stats_local():
        """Persist local fallback pair-hunter performance stats."""
        try:
            with open(pair_hunter_stats_path, "w", encoding="utf-8") as f:
                json.dump(pair_hunter_stats, f, default=json_default)
        except Exception as e:
            logging.warning(f"Could not save pair_hunter_stats.json: {e}")

    def _get_dashboard_api_url() -> str:
        return (
            os.getenv("NEXT_PUBLIC_API_URL")
            or os.getenv("NEXT_PUBLIC_BASE_URL")
            or os.getenv("DASHBOARD_URL")
            or CONFIG.get("NEXT_PUBLIC_API_URL")
            or CONFIG.get("next_public_base_url")
            or "http://localhost:3001"
        )

    def _get_pair_hunter_supabase_client():
        """Get direct Supabase client for production-safe Pair Hunter stat persistence."""
        nonlocal pair_hunter_supabase_client, pair_hunter_supabase_unavailable_logged
        if pair_hunter_supabase_client is not None:
            return pair_hunter_supabase_client
        try:
            from supabase import create_client, Client
            supabase_url = os.getenv("SUPABASE_URL")
            # Direct fallback writes need service role to bypass RLS in production.
            supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY")
            if not supabase_url or not supabase_service_key:
                if not pair_hunter_supabase_unavailable_logged:
                    logging.warning(
                        "Pair Hunter direct DB fallback unavailable: SUPABASE_URL or SUPABASE_SERVICE_KEY missing. "
                        "Set SUPABASE_SERVICE_KEY for production-safe Pair Hunter DB writes."
                    )
                    pair_hunter_supabase_unavailable_logged = True
                return None
            pair_hunter_supabase_client = create_client(supabase_url, supabase_service_key)
            return pair_hunter_supabase_client
        except Exception as e:
            if not pair_hunter_supabase_unavailable_logged:
                logging.warning(f"Pair Hunter direct DB fallback unavailable: {e}")
                pair_hunter_supabase_unavailable_logged = True
            return None

    async def _load_pair_hunter_stats():
        """Load pair-hunter performance stats from dashboard API, then direct DB, then local file."""
        nonlocal pair_hunter_stats, pair_hunter_db_api_disabled
        loaded_from_remote = False
        if not pair_hunter_db_api_disabled:
            api_url = _get_dashboard_api_url()
            try:
                async with ClientSession() as session:
                    async with session.get(
                        f"{api_url}/api/pair-hunter/stats",
                        timeout=ClientTimeout(total=8)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if isinstance(data, dict) and isinstance(data.get("stats"), dict):
                                pair_hunter_stats = data["stats"]
                                add_event(f"📚 Loaded Pair Hunter stats from DB for {len(pair_hunter_stats)} assets")
                                _save_pair_hunter_stats_local()
                                return
                        if resp.status == 404:
                            pair_hunter_db_api_disabled = True
                            logging.warning(
                                f"Pair Hunter stats endpoint not found at {api_url}/api/pair-hunter/stats (404). "
                                "Falling back to direct Supabase query and local cache."
                            )
                        else:
                            logging.warning(f"Pair Hunter DB stats fetch returned status {resp.status}, trying fallback")
            except Exception as e:
                logging.warning(f"Could not load Pair Hunter stats from DB API: {e}")

        # Production fallback: direct Supabase read if dashboard API is unavailable.
        try:
            supabase_client = _get_pair_hunter_supabase_client()
            if supabase_client:
                result = supabase_client.table("pair_hunter_stats").select("*").execute()
                rows = getattr(result, "data", None) or []
                stats = {}
                for row in rows:
                    asset = str((row or {}).get("asset") or "").upper().strip()
                    if not asset:
                        continue
                    stats[asset] = {
                        "total_trades": int((row or {}).get("total_trades") or 0),
                        "wins": int((row or {}).get("wins") or 0),
                        "losses": int((row or {}).get("losses") or 0),
                        "win_rate": float((row or {}).get("win_rate") or 0.0),
                        "total_pnl_usd": float((row or {}).get("total_pnl_usd") or 0.0),
                        "total_pnl_percent": float((row or {}).get("total_pnl_percent") or 0.0),
                        "expectancy_usd": float((row or {}).get("expectancy_usd") or 0.0),
                        "expectancy_percent": float((row or {}).get("expectancy_percent") or 0.0),
                        "data_fail_count": int((row or {}).get("data_fail_count") or 0),
                        "excluded_until": (row or {}).get("excluded_until"),
                        "exclusion_reason": str((row or {}).get("exclusion_reason") or ""),
                        "last_data_error": str((row or {}).get("last_data_error") or ""),
                        "last_close_reason": str((row or {}).get("last_close_reason") or ""),
                        "last_updated": (row or {}).get("last_updated") or (row or {}).get("updated_at") or datetime.now(timezone.utc).isoformat(),
                    }
                pair_hunter_stats = stats
                add_event(f"📚 Loaded Pair Hunter stats via direct Supabase for {len(pair_hunter_stats)} assets")
                _save_pair_hunter_stats_local()
                loaded_from_remote = True
        except Exception as e:
            logging.warning(f"Could not load Pair Hunter stats via direct Supabase: {e}")

        if loaded_from_remote:
            return
        _load_pair_hunter_stats_local()

    async def _upsert_pair_hunter_stat_to_db(asset: str, stat_row: dict):
        """Persist a single pair stat to dashboard API, with direct Supabase fallback."""
        nonlocal pair_hunter_db_api_disabled
        api_ok = False
        if not pair_hunter_db_api_disabled:
            api_url = _get_dashboard_api_url()
            payload = {
                "asset": asset,
                "stats": stat_row
            }
            try:
                async with ClientSession() as session:
                    async with session.post(
                        f"{api_url}/api/pair-hunter/stats",
                        json=payload,
                        timeout=ClientTimeout(total=8)
                    ) as resp:
                        if resp.status == 404:
                            pair_hunter_db_api_disabled = True
                            logging.warning(
                                f"Pair Hunter stats endpoint not found at {api_url}/api/pair-hunter/stats (404). "
                                "Switching to direct Supabase writes."
                            )
                        elif resp.status in (200, 201):
                            api_ok = True
                        else:
                            body = await resp.text()
                            logging.warning(f"Pair Hunter DB upsert failed ({resp.status}): {body[:200]}")
            except Exception as e:
                logging.warning(f"Could not upsert Pair Hunter stat to DB API: {e}")
        if api_ok:
            return

        # Production fallback: direct Supabase write when dashboard API is unavailable.
        try:
            supabase_client = _get_pair_hunter_supabase_client()
            if not supabase_client:
                return
            row = {
                "asset": asset,
                "total_trades": int(stat_row.get("total_trades", 0) or 0),
                "wins": int(stat_row.get("wins", 0) or 0),
                "losses": int(stat_row.get("losses", 0) or 0),
                "win_rate": float(stat_row.get("win_rate", 0.0) or 0.0),
                "total_pnl_usd": float(stat_row.get("total_pnl_usd", 0.0) or 0.0),
                "total_pnl_percent": float(stat_row.get("total_pnl_percent", 0.0) or 0.0),
                "expectancy_usd": float(stat_row.get("expectancy_usd", 0.0) or 0.0),
                "expectancy_percent": float(stat_row.get("expectancy_percent", 0.0) or 0.0),
                "data_fail_count": int(stat_row.get("data_fail_count", 0) or 0),
                "excluded_until": stat_row.get("excluded_until"),
                "exclusion_reason": str(stat_row.get("exclusion_reason", "") or ""),
                "last_data_error": str(stat_row.get("last_data_error", "") or ""),
                "last_close_reason": str(stat_row.get("last_close_reason", "") or ""),
                "last_updated": stat_row.get("last_updated") or datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            supabase_client.table("pair_hunter_stats").upsert(row, on_conflict="asset").execute()
        except Exception as e:
            logging.warning(f"Could not upsert Pair Hunter stat via direct Supabase: {e}")

    async def _record_pair_hunter_outcome(asset: str, pnl_usd: float, pnl_percent: float, close_reason: str, was_long: bool = True):
        """Update win/loss + expectancy metrics for a closed trade asset."""
        key = (asset or "").upper().strip()
        if not key:
            return

        # Start the re-entry cooldown, fold the result into the session breakers, and
        # retire the per-position exit/ladder state.
        reentry_guard.record_close(key, pnl_usd, was_long, _LIVE_SETTINGS)
        risk_governor.record_close(key, pnl_usd, _LIVE_SETTINGS, equity=_LAST_KNOWN_EQUITY.get("value"))
        profit_ladder.clear(key)
        _EXIT_PLANS.pop(key, None)
        mech_rec = dict(_MECH_STATE.get("open_trades") or {}).get(key) or {}
        setup_tag = str(mech_rec.get("setup") or "mechanical")
        if key in _MECH_STATE["managed_assets"] or mech.is_mechanical_mode(_LIVE_SETTINGS):
            _MECH_STATE["managed_assets"].discard(key)
            _MECH_STATE["open_trades"].pop(key, None)
            try:
                _MECH_STATS.record_close(float(pnl_usd or 0.0))
            except Exception:
                pass
        try:
            pnl_usd = float(pnl_usd or 0.0)
        except Exception:
            pnl_usd = 0.0
        try:
            pnl_percent = float(pnl_percent or 0.0)
        except Exception:
            pnl_percent = 0.0

        if mech.is_mechanical_mode(_LIVE_SETTINGS) or mech_rec:
            try:
                sb = globals().get("supabase") or None
                if sb is None and os.getenv("SUPABASE_URL") and (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")):
                    from supabase import create_client
                    sb = create_client(
                        os.getenv("SUPABASE_URL"),
                        os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY"),
                    )
                if sb is not None:
                    network, _label = _venue_network()
                    entry_px = float(mech_rec.get("entry_price") or 0) or None
                    row = {
                        "symbol": key,
                        "side": "buy" if was_long else "sell",
                        "size": abs(float(mech_rec.get("amount") or 0) or 0) or 0.001,
                        "price": entry_px or 0.0,
                        "fee": 0.0,
                        "pnl": pnl_usd,
                        "executed_at": datetime.now(timezone.utc).isoformat(),
                        "order_id": f"mech-{setup_tag}-{key}-{int(datetime.now(timezone.utc).timestamp())}",
                        "book": "crypto",
                        "venue": "okx",
                    }
                    sb.table("trades").insert(row).execute()
                    add_event(f"💾 Mechanical close logged to trades: {key} [{setup_tag}] pnl=${pnl_usd:.2f} ({close_reason})")
                    _mech_diary({
                        "asset": key,
                        "action": "close_logged",
                        "setup": setup_tag,
                        "pnl": pnl_usd,
                        "reason": close_reason,
                        "network": network,
                    })
            except Exception as e:
                logging.warning(f"Mechanical trade close insert failed for {key}: {e}")

        stats = pair_hunter_stats.get(key, {})
        # Every non-zero close counts. A few cents still changes the account.
        is_win = pnl_usd > 0
        is_loss = pnl_usd < 0
        total_trades = int(stats.get("total_trades", 0) or 0) + 1
        wins = int(stats.get("wins", 0) or 0) + (1 if is_win else 0)
        losses = int(stats.get("losses", 0) or 0) + (1 if is_loss else 0)
        total_pnl_usd = float(stats.get("total_pnl_usd", 0.0) or 0.0) + pnl_usd
        total_pnl_percent = float(stats.get("total_pnl_percent", 0.0) or 0.0) + pnl_percent
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0
        expectancy_usd = total_pnl_usd / total_trades if total_trades > 0 else 0.0
        expectancy_percent = total_pnl_percent / total_trades if total_trades > 0 else 0.0

        now_utc = datetime.now(timezone.utc)
        excluded_until = None
        exclusion_reason = ""
        quarantine_after = int(CONFIG.get("pair_hunter_perf_filter_min_trades", 3) or 3)
        if total_trades >= quarantine_after and expectancy_usd < 0:
            excluded_until = (now_utc + timedelta(hours=24)).isoformat()
            exclusion_reason = (
                f"negative expectancy ${expectancy_usd:.2f} after {total_trades} trades"
            )

        pair_hunter_stats[key] = {
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 2),
            "total_pnl_usd": round(total_pnl_usd, 4),
            "total_pnl_percent": round(total_pnl_percent, 4),
            "expectancy_usd": round(expectancy_usd, 4),
            "expectancy_percent": round(expectancy_percent, 4),
            "data_fail_count": 0,  # Reset TA data failures after a completed trade cycle
            "excluded_until": excluded_until,
            "exclusion_reason": exclusion_reason,
            "last_close_reason": close_reason,
            "last_updated": now_utc.isoformat()
        }
        _save_pair_hunter_stats_local()
        await _upsert_pair_hunter_stat_to_db(key, pair_hunter_stats[key])
        add_event(
            f"📊 Pair Hunter stats {key}: trades={total_trades}, win_rate={win_rate:.1f}%, "
            f"expectancy=${expectancy_usd:.2f}/trade"
        )

    def _is_pair_temporarily_excluded(asset: str) -> tuple[bool, str]:
        """Return exclusion status for an asset based on excluded_until timestamp."""
        key = (asset or "").upper().strip()
        if not key:
            return False, ""
        stats = pair_hunter_stats.get(key, {})
        excluded_until_raw = stats.get("excluded_until")
        if not excluded_until_raw:
            return False, ""
        try:
            excluded_until = datetime.fromisoformat(str(excluded_until_raw).replace("Z", "+00:00"))
            if excluded_until.tzinfo is None:
                excluded_until = excluded_until.replace(tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            if excluded_until > now_utc:
                remaining_mins = int((excluded_until - now_utc).total_seconds() // 60)
                return True, f"{max(1, remaining_mins)}m remaining"
            # exclusion expired; clear it
            stats["excluded_until"] = None
            stats["exclusion_reason"] = ""
            stats["data_fail_count"] = 0
            pair_hunter_stats[key] = stats
            _save_pair_hunter_stats_local()
            return False, ""
        except Exception:
            return False, ""

    async def _record_pair_hunter_data_failure(asset: str, reason: str):
        """Track TA data failures and temporarily exclude repeated offenders."""
        key = (asset or "").upper().strip()
        if not key:
            return

        fail_threshold = int(CONFIG.get("pair_hunter_data_fail_threshold", 3) or 3)
        cooldown_minutes = int(CONFIG.get("pair_hunter_exclusion_cooldown_minutes", 180) or 180)
        now_utc = datetime.now(timezone.utc)

        stats = pair_hunter_stats.get(key, {})
        fail_count = int(stats.get("data_fail_count", 0) or 0) + 1
        stats["data_fail_count"] = fail_count
        stats["last_data_error"] = reason
        stats["last_updated"] = now_utc.isoformat()

        if fail_count >= fail_threshold:
            excluded_until = now_utc + timedelta(minutes=cooldown_minutes)
            stats["excluded_until"] = excluded_until.isoformat()
            stats["exclusion_reason"] = f"TA data unavailable ({reason})"
            add_event(
                f"🚫 Pair Hunter excluding {key} for {cooldown_minutes}m "
                f"(data failures: {fail_count}/{fail_threshold})"
            )

        pair_hunter_stats[key] = stats
        _save_pair_hunter_stats_local()
        await _upsert_pair_hunter_stat_to_db(key, stats)

    async def _record_pair_hunter_data_success(asset: str):
        """Clear temporary data-failure counters when symbol data is valid again."""
        key = (asset or "").upper().strip()
        if not key:
            return
        stats = pair_hunter_stats.get(key, {})
        if int(stats.get("data_fail_count", 0) or 0) == 0 and not stats.get("excluded_until"):
            return
        stats["data_fail_count"] = 0
        stats["excluded_until"] = None
        stats["exclusion_reason"] = ""
        stats["last_data_error"] = ""
        stats["last_updated"] = datetime.now(timezone.utc).isoformat()
        pair_hunter_stats[key] = stats
        _save_pair_hunter_stats_local()
        await _upsert_pair_hunter_stat_to_db(key, stats)

    def _rank_hunted_assets_by_performance(hunted_assets: list[str]) -> list[str]:
        """Re-rank/filter hunted assets using historical trade outcomes."""
        if not hunted_assets:
            return hunted_assets

        min_trades = int(CONFIG.get("pair_hunter_perf_min_trades", 3) or 3)
        filter_min_trades = int(CONFIG.get("pair_hunter_perf_filter_min_trades", 3) or 3)
        filter_min_expectancy_usd = float(CONFIG.get("pair_hunter_perf_filter_min_expectancy_usd", 0.0) or 0.0)

        filtered_assets = []
        for asset in hunted_assets:
            key = (asset or "").upper().strip()
            stats = pair_hunter_stats.get(key, {})
            is_excluded, detail = _is_pair_temporarily_excluded(key)
            if is_excluded:
                add_event(f"🚫 Pair Hunter skipped excluded asset {key} ({detail})")
                continue
            total_trades = int(stats.get("total_trades", 0) or 0)
            win_rate = float(stats.get("win_rate", 0.0) or 0.0)
            expectancy_usd = float(stats.get("expectancy_usd", 0.0) or 0.0)
            # One condition: after a few real closes, negative expectancy is enough.
            # Requiring BOTH a low win rate AND expectancy under -$1 let SAND/SOL
            # keep getting re-selected through a full losing day.
            should_filter = (
                total_trades >= filter_min_trades
                and expectancy_usd < filter_min_expectancy_usd
            )
            if should_filter:
                add_event(
                    f"🚫 Pair Hunter filtered {key} from hunt list "
                    f"(trades={total_trades}, win_rate={win_rate:.1f}%, expectancy=${expectancy_usd:.2f})"
                )
                continue
            filtered_assets.append(key)

        def _rank_tuple(asset: str):
            stats = pair_hunter_stats.get(asset, {})
            total_trades = int(stats.get("total_trades", 0) or 0)
            win_rate = float(stats.get("win_rate", 0.0) or 0.0)
            expectancy_usd = float(stats.get("expectancy_usd", 0.0) or 0.0)
            if total_trades < min_trades:
                return (0, 0.0, 0.0)  # preserve newer/unknown pairs without penalty
            return (1, win_rate, expectancy_usd)

        ranked = sorted(filtered_assets, key=_rank_tuple, reverse=True)
        return ranked

    # Initialize webhook notifier for external alerts (WhatsApp, Discord, etc.)
    webhook_url = CONFIG.get("webhook_url")
    enable_webhook = CONFIG.get("enable_webhook_notifications", False)
    telegram_bot_token = CONFIG.get("telegram_bot_token")
    telegram_chat_id = CONFIG.get("telegram_chat_id")
    webhook_notifier = WebhookNotifier(
        webhook_url if enable_webhook else None,
        telegram_bot_token=telegram_bot_token if enable_webhook else None,
        telegram_chat_id=telegram_chat_id if enable_webhook else None,
    )
    has_native_telegram = bool(telegram_bot_token and telegram_chat_id)
    if enable_webhook and (webhook_url or has_native_telegram):
        channel_desc = []
        if webhook_url:
            channel_desc.append("WEBHOOK_URL")
        if has_native_telegram:
            channel_desc.append("native Telegram")
        add_event(f"🔔 Notifications enabled via {', '.join(channel_desc)}.")
    elif enable_webhook and not webhook_url and not has_native_telegram:
        add_event("⚠️ Notifications enabled but no valid channel configured (set WEBHOOK_URL or TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID).")
    else:
        add_event("🔕 Notifications disabled (ENABLE_WEBHOOK_NOTIFICATIONS=false).")

    def _build_position_sizing_note(trading_settings: dict, default_leverage: int, per_asset_leverage: dict = None) -> str:
        """Build position sizing note for LLM context (risk or margin mode)."""
        sizing_mode = str(trading_settings.get("position_sizing_mode", "risk") or "risk").lower()
        margin_per_position = trading_settings.get("margin_per_position")
        max_positions = trading_settings.get("max_positions", 6)
        risk_usd = trading_settings.get("risk_per_trade_usd")
        risk_pct = trading_settings.get("risk_per_trade_pct", 0.5)
        stop_loss_usd = trading_settings.get("stop_loss_usd")

        per_asset_note = ""
        if per_asset_leverage:
            custom_leverage_assets = {k: v for k, v in per_asset_leverage.items() if v != default_leverage}
            if custom_leverage_assets:
                per_asset_list = ", ".join([f"{asset}: {lev}x" for asset, lev in custom_leverage_assets.items()])
                per_asset_note = f" PER-ASSET LEVERAGE OVERRIDES: {per_asset_list}. These are MANDATORY and will be strictly enforced."

        base_note = (
            f"Use these percentages to calculate TP/SL prices. Default leverage: {default_leverage}x "
            f"(capped by asset max if lower).{per_asset_note} Maximum {max_positions} concurrent positions allowed."
        )

        sizing_mode = str(trading_settings.get("position_sizing_mode", "risk") or "risk").lower()
        if sizing_mode == "risk":
            risk_desc = f"${float(risk_usd):.2f}" if risk_usd is not None else f"{float(risk_pct or 0.5):.2f}% of equity"
            hard_sl = f"${float(stop_loss_usd):.2f}" if stop_loss_usd is not None else "not set"
            return (
                f"{base_note}\n"
                f"🎯 RISK MODE: position size is solved from the ATR stop so each trade risks about {risk_desc}. "
                f"Hard USD stop ceiling: {hard_sl}. You do NOT set allocation_usd — focus on buy/sell/hold only."
            )

        if margin_per_position is None:
            return (
                f"{base_note} ⚠️ CRITICAL: MARGIN_PER_POSITION is not configured. "
                f"Trades will be skipped until it is set (or switch Position Sizing Mode to Risk)."
            )

        notional_preview = float(margin_per_position) * default_leverage
        hard_sl = f"${float(stop_loss_usd):.2f}" if stop_loss_usd is not None else "not set"
        return (
            f"{base_note}\n"
            f"MARGIN MODE: use exactly ${float(margin_per_position):.2f} margin × leverage "
            f"(≈ ${notional_preview:.2f} notional at {default_leverage}x). "
            f"Hard USD stop ceiling: {hard_sl}. Do NOT suggest allocation_usd above margin — system enforces it."
        )

    def _venue_network() -> tuple:
        """(network, label) for the dashboard badge — OKX demo is reported as 'demo'."""
        if exchange_name == "aster":
            return "mainnet", "Aster DEX"
        if exchange_name == "binance":
            testnet = bool(CONFIG.get("binance_testnet", False))
            return ("testnet" if testnet else "mainnet"), f"Binance Futures ({'testnet' if testnet else 'mainnet'})"
        if exchange_name == "okx":
            demo = bool(CONFIG.get("okx_demo", True))
            return ("demo" if demo else "mainnet"), f"OKX ({'DEMO' if demo else 'LIVE'})"
        return "mainnet", exchange_name

    def _ex_for(asset: str):
        """Return the mapped venue for this asset. No cross-venue fallback."""
        if use_multi_exchange and exchange_manager:
            return exchange_manager.get_exchange_for_asset(asset)
        if _looks_like_forex(asset) and str(CONFIG.get("ENABLE_FOREX", "false")).lower() not in ("1", "true", "yes", "on"):
            return None
        # Single-exchange: only trade the class that matches this process
        if exchange_name in ("alpaca",) and _looks_like_forex(asset):
            return None
        if exchange_name in ("okx", "binance", "aster") and _looks_like_forex(asset):
            return None
        stock_set = {
            p.strip().upper()
            for p in str(CONFIG.get("stock_assets") or "").replace(",", " ").split()
            if p.strip()
        }
        crypto_set = {
            p.strip().upper()
            for p in str(CONFIG.get("crypto_assets") or CONFIG.get("assets") or "").replace(",", " ").split()
            if p.strip()
        }
        au = (asset or "").upper()
        if exchange_name == "alpaca":
            return hyperliquid if (not stock_set or au in stock_set) else None
        if exchange_name in ("okx", "binance", "aster"):
            if stock_set and au in stock_set:
                return None
            if crypto_set and au not in crypto_set and au not in stock_set:
                # Allow mechanical universe symbols not listed in CRYPTO_ASSETS
                pass
            return hyperliquid
        return hyperliquid

    def _settings_for_asset(asset: str, base_settings: dict) -> dict:
        """Overlay forex-specific risk/exit knobs when the asset is a currency pair."""
        settings = dict(base_settings or {})
        if not _looks_like_forex(asset):
            return settings
        if CONFIG.get("forex_interval"):
            settings["interval"] = CONFIG.get("forex_interval")
            settings["intraday_timeframe"] = CONFIG.get("forex_interval")
        if CONFIG.get("forex_exit_mode"):
            settings["exit_mode"] = CONFIG.get("forex_exit_mode")
        if CONFIG.get("forex_sl_atr_mult") is not None:
            settings["sl_atr_mult"] = CONFIG.get("forex_sl_atr_mult")
        if CONFIG.get("forex_tp_rr_ratio") is not None:
            settings["tp_rr_ratio"] = CONFIG.get("forex_tp_rr_ratio")
        if CONFIG.get("forex_risk_per_trade_usd") is not None:
            settings["risk_per_trade_usd"] = CONFIG.get("forex_risk_per_trade_usd")
        if CONFIG.get("forex_leverage"):
            settings["leverage"] = CONFIG.get("forex_leverage")
        return settings

    def _section_has_usable_ta(section: dict) -> bool:
        """True when at least one core indicator arrived — empty series means no LLM entry edge."""
        if not section:
            return False
        try:
            px = float(section.get("current_price") or 0)
        except (TypeError, ValueError):
            px = 0.0
        if px <= 0:
            return False
        intra = section.get("intraday") or {}
        return any(intra.get(k) is not None for k in ("ema20", "rsi14", "macd"))

    def _price_decimals(price: float) -> int:
        """Keep sub-cent coins from rounding to 0.00 (which looked like 'no market data')."""
        try:
            p = abs(float(price or 0))
        except (TypeError, ValueError):
            return 6
        if p >= 100:
            return 2
        if p >= 1:
            return 4
        if p >= 0.01:
            return 6
        return 8

    def _has_required_ta_data(asset: str) -> bool:
        """Validate that key TA inputs exist before adding hunted asset to decision universe."""
        try:
            if _looks_like_forex(asset):
                symbol = asset if "/" in asset else f"{asset[:3]}/{asset[3:6]}"
                tf = CONFIG.get("forex_interval") or CONFIG.get("intraday_timeframe") or "15m"
            else:
                # Crypto must be listed on the live trading venue (OKX) before TA probes.
                # Pair Hunter scores Binance heat — without this gate, meme names pass
                # TAAPI/Binance fallback then fail every OKX price/candle fetch.
                venue = str(CONFIG.get("exchange") or "okx").lower().strip()
                if venue == "okx":
                    try:
                        from src.trading.okx_api import fetch_okx_usdt_swap_bases
                        bases = fetch_okx_usdt_swap_bases()
                        base = str(asset or "").strip().upper().replace("USDT", "").replace("-SWAP", "").replace("-", "")
                        if bases and base not in bases:
                            return False
                    except Exception:
                        pass
                symbol = f"{asset}/USDT"
                tf = CONFIG.get("intraday_timeframe") or CONFIG.get("interval") or "15m"
            longterm_tf = CONFIG.get("longterm_timeframe") or "4h"
            intraday_probe = taapi.fetch_series(
                "ema", symbol, tf, results=1, params={"period": 20}, value_key="value",
            )
            longterm_probe = taapi.fetch_series(
                "ema", symbol, longterm_tf, results=1, params={"period": 20}, value_key="value",
            )
            return bool(intraday_probe) and bool(longterm_probe)
        except Exception:
            return False

    def _ta_symbol(asset: str) -> str:
        """TA feed symbol: EUR/USD for forex, BTC/USDT for crypto."""
        if _looks_like_forex(asset):
            a = asset.replace("/", "").replace("_", "").upper()
            return f"{a[:3]}/{a[3:6]}"
        return f"{asset}/USDT"

    def _fetch_atr_percent(asset: str, interval: str, current_price: float, period: int = 14):
        """ATR for `asset` on `interval`, as a percentage of price. None if unavailable."""
        if not current_price or current_price <= 0:
            return None
        try:
            atr_value = taapi.fetch_value(
                "atr", _ta_symbol(asset), interval, params={"period": period}, key="value"
            )
        except Exception as e:
            logging.debug(f"ATR fetch failed for {asset} {interval}: {e}")
            return None
        if not atr_value or atr_value <= 0:
            return None
        return (float(atr_value) / float(current_price)) * 100.0

    def _resolve_exit_plan(asset: str, settings: dict, leverage: float, current_price: float, interval: str):
        """Build (and cache) the stop/target plan for an asset."""
        atr_pct = None
        if str(settings.get("exit_mode", "fixed")).lower() == "atr":
            atr_pct = _fetch_atr_percent(
                asset, interval, current_price, int(settings.get("atr_period", 14) or 14)
            )
        plan = build_exit_plan(settings, leverage, atr_pct)
        _EXIT_PLANS[asset.upper()] = plan
        return plan

    def _compute_position_roi_percent(pos: dict, is_long: bool = True, entry_price=None, current_price=None) -> float:
        """Return margin-based ROI%. Prefer exchange ROI fields; never use notional as primary."""
        unrealized_pnl = float(pos.get("unrealized_pnl") or pos.get("pnl") or pos.get("unRealizedProfit") or 0 or 0)
        initial_margin = float(
            pos.get("initial_margin")
            or pos.get("initialMargin")
            or pos.get("positionInitialMargin")
            or pos.get("position_initial_margin")
            or 0
            or 0
        )

        for key in ("unrealized_pnl_percent", "roiPercent", "roi"):
            raw = pos.get(key)
            if raw is not None:
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    pass

        if initial_margin > 0:
            return (unrealized_pnl / initial_margin) * 100.0

        entry = float(entry_price or pos.get("entry_price") or pos.get("entryPx") or pos.get("entryPrice") or 0 or 0)
        current = float(current_price or pos.get("current_price") or pos.get("markPrice") or 0 or 0)
        if entry > 0 and current > 0:
            if is_long:
                return ((current - entry) / entry) * 100.0
            return ((entry - current) / entry) * 100.0

        # Last resort only: notional understates leveraged ROI.
        size = abs(float(pos.get("quantity") or pos.get("szi") or pos.get("positionAmt") or 0 or 0))
        if entry > 0 and size > 0:
            notional = size * entry
            if notional > 0:
                return (unrealized_pnl / notional) * 100.0
        return 0.0

    async def _close_position_reduce_only(
        asset: str,
        is_long: bool,
        position_size: float,
        reason: str,
        *,
        entry_price=None,
        current_price=None,
        pnl_usd=None,
        pnl_percent=None,
        diary_action: str = None,
    ) -> bool:
        """Flatten a position with a reduce-only market order. Returns True on success."""
        nonlocal active_trades
        size = abs(float(position_size or 0))
        try:
            ex = _ex_for(asset)
            if ex is None:
                add_event(f"⚠️  Skip close for {asset}: no mapped venue ({reason})")
                return False
            # OKX: close the exact live contract size so lot flooring cannot leave ~$5 dust.
            if hasattr(ex, "close_position_market"):
                result = await ex.close_position_market(asset)
                if result.get("already_flat"):
                    add_event(f"✅ {asset} already flat ({reason})")
                    for tr in active_trades[:]:
                        if tr.get("asset") == asset:
                            active_trades.remove(tr)
                    return True
                close_action = "sell" if result.get("closed_side") != "short" else "buy"
                add_event(
                    f"✅ Closed {asset} ({reason}) via {close_action} reduce-only | "
                    f"contracts={result.get('closed_contracts')}"
                )
            else:
                if size <= 0:
                    add_event(f"⚠️  Skip close for {asset}: zero size ({reason})")
                    return False
                close_action = "sell" if is_long else "buy"
                if is_long:
                    await ex.place_sell_order(asset, size, reduce_only=True)
                else:
                    await ex.place_buy_order(asset, size, reduce_only=True)
                add_event(f"✅ Closed {asset} ({reason}) via {close_action} reduce-only | size={size}")
            try:
                await ex.cancel_all_orders(asset)
                add_event(f"🧹 Cancelled all open orders for {asset} (post-close {reason})")
            except Exception as e:
                add_event(f"⚠️  Could not cancel open orders for {asset} (post-close {reason}): {e}")

            for tr in active_trades[:]:
                if tr.get("asset") == asset:
                    active_trades.remove(tr)

            exit_px = current_price
            if exit_px is None:
                try:
                    exit_px = await ex.get_current_price(asset)
                except Exception:
                    exit_px = None

            action_name = diary_action or f"close_{str(reason).lower().replace(' ', '_')[:48]}"
            try:
                with open(diary_path, "a") as f:
                    f.write(json.dumps({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "asset": asset,
                        "action": action_name,
                        "entry_price": entry_price,
                        "exit_price": exit_px,
                        "amount": size,
                        "reason": reason,
                        "pnl": pnl_usd,
                        "pnl_percent": pnl_percent,
                    }) + "\n")
            except Exception as e:
                logging.warning(f"Could not write close diary for {asset}: {e}")

            try:
                await _record_pair_hunter_outcome(
                    asset,
                    float(pnl_usd or 0),
                    float(pnl_percent or 0),
                    action_name,
                    was_long=is_long,
                )
            except Exception:
                pass
            return True
        except Exception as e:
            add_event(f"❌ Failed to close {asset} ({reason}): {e}")
            logging.error(f"Error closing {asset}: {e}", exc_info=True)
            return False

    async def _mechanical_exit_sweep(settings: dict) -> int:
        """Fast TP/SL-only pass. Safe to call anytime (between LLM cycles or from watcher)."""
        closed_n = 0
        if not settings:
            settings = dict(_LIVE_SETTINGS) if _LIVE_SETTINGS else {}
        try:
            if use_multi_exchange and exchange_manager:
                st = await exchange_manager.get_merged_user_state()
            else:
                st = await hyperliquid.get_user_state()
        except Exception as e:
            logging.warning(f"Exit sweep state failed: {e}")
            return 0

        tp_mode = normalize_tp_mode(settings.get("tp_mode") or "roi_percent")
        tp_pct = float(settings.get("take_profit_percent") or CONFIG.get("take_profit_percent") or 7)
        max_loss = resolve_max_loss_usd(settings, default=6.0)
        stop_usd = -abs(max_loss)
        ui_sl = float(settings.get("stop_loss_percent") or CONFIG.get("stop_loss_percent") or 12)
        if mech.is_mechanical_mode(settings):
            targets = mech.resolve_exit_targets(settings, CONFIG)
            tp_mode = normalize_tp_mode(targets["mode"])
            tp_pct = float(targets["tp_pct"])
            # ROI and price-% modes honor user SL%; USD mode uses $ stop only.
            if tp_mode in ("roi_percent", "price_percent"):
                ui_sl = float(targets["sl_pct"])
            else:
                ui_sl = 1e9
            stop_usd = -abs(float(targets["sl_usd"]))
            try:
                tp_usd_f = float(targets["tp_usd"]) if tp_mode == "usd" else 0.0
            except (TypeError, ValueError):
                tp_usd_f = 0.0
        else:
            try:
                tp_usd_f = float(settings.get("take_profit_usd") or 0)
            except (TypeError, ValueError):
                tp_usd_f = 0.0

        for pos in st.get("positions") or []:
            asset = pos.get("symbol") or pos.get("coin")
            if not asset:
                continue
            signed = _safe_float(pos.get("quantity"), 0.0)
            if signed == 0.0:
                signed = _safe_float(pos.get("szi"), 0.0)
            qty = abs(signed)
            if qty <= 0:
                continue
            is_long = signed > 0
            entry = _safe_float(
                pos.get("entry_price") or pos.get("entryPx") or pos.get("entryPrice"), 0.0
            )
            try:
                _px_ex = _ex_for(asset)
                if _px_ex is None:
                    current = _safe_float(pos.get("current_price") or pos.get("markPrice"), 0.0)
                else:
                    current = _safe_float(await _px_ex.get_current_price(asset), 0.0)
            except Exception:
                current = _safe_float(pos.get("current_price") or pos.get("markPrice"), 0.0)
            upl = _safe_float(
                pos.get("unrealized_pnl") or pos.get("pnl") or pos.get("unRealizedProfit"), 0.0
            )
            roi = _compute_position_roi_percent(
                pos, is_long=is_long, entry_price=entry, current_price=current
            )
            # If exchange ROI is missing/zero but we have $ profit and margin, recompute.
            margin = _safe_float(
                pos.get("initial_margin") or pos.get("initialMargin") or pos.get("positionInitialMargin"),
                0.0,
            )
            if margin > 0 and upl != 0:
                roi_from_usd = (upl / margin) * 100.0
                # Use the larger profit / worse loss so we never miss a TP
                if abs(roi_from_usd) >= abs(roi):
                    roi = roi_from_usd
            price_move = None
            if entry > 0 and current > 0:
                price_move = (
                    ((current - entry) / entry) * 100.0 if is_long
                    else ((entry - current) / entry) * 100.0
                )

            reason = None
            diary = "close_stop_loss"
            if upl <= stop_usd:
                reason = f"USD stop ${upl:.2f} <= ${stop_usd:.2f}"
            elif roi <= -ui_sl:
                reason = f"ROI stop {roi:.2f}% <= -{ui_sl:g}%"
            # Take profit: ROI% mode, OR hit absolute $ lock if configured (whichever fires first).
            elif tp_mode == "roi_percent" and roi >= tp_pct:
                reason = f"ROI TP {roi:.2f}% >= {tp_pct:g}%"
                diary = "close_tp"
            elif tp_mode == "price_percent" and price_move is not None and price_move >= tp_pct:
                reason = f"Price TP {price_move:.2f}% >= {tp_pct:g}%"
                diary = "close_tp"
            elif tp_mode == "usd" and tp_usd_f > 0 and upl >= tp_usd_f:
                reason = f"USD TP ${upl:.2f} >= ${tp_usd_f:g}"
                diary = "close_tp"
            elif tp_mode != "roi_percent" and tp_usd_f > 0 and upl >= tp_usd_f:
                # Non-ROI modes may still honor an absolute $ lock when set.
                reason = f"USD TP ${upl:.2f} >= ${tp_usd_f:g}"
                diary = "close_tp"

            if not reason:
                logging.debug(
                    f"Exit sweep hold {asset}: ROI={roi:.2f}% UPL=${upl:.2f} "
                    f"(need ROI>={tp_pct:g}% or UPL>=${tp_usd_f:g})"
                )
                continue
            add_event(f"⚡ FAST EXIT {asset}: {reason}")
            ok = await _close_position_reduce_only(
                asset,
                is_long,
                qty,
                reason,
                entry_price=entry,
                current_price=current,
                pnl_usd=upl,
                pnl_percent=roi,
                diary_action=diary,
            )
            if ok:
                closed_n += 1
        return closed_n

    async def _sweep_dust_positions(max_notional: float = None) -> int:
        """Flatten leftover OKX stubs (~$5) that still count as open slots."""
        if max_notional is None:
            max_notional = float(CONFIG.get("position_dust_notional_usd") or 8.0)
        swept = 0
        try:
            if use_multi_exchange and exchange_manager:
                st = await exchange_manager.get_merged_user_state()
            else:
                st = await hyperliquid.get_user_state()
        except Exception as e:
            logging.warning(f"Dust sweep state failed: {e}")
            return 0
        for pos in st.get("positions") or []:
            asset = str(pos.get("symbol") or pos.get("coin") or "").upper()
            qty = abs(_safe_float(pos.get("quantity") or pos.get("szi"), 0))
            if not asset or qty <= 0:
                continue
            notional = abs(_safe_float(pos.get("notional") or pos.get("notionalUsd"), 0))
            if notional <= 0:
                mark = _safe_float(pos.get("markPrice") or pos.get("current_price") or pos.get("entryPrice"), 0)
                notional = qty * mark
            if notional <= 0 or notional > max_notional:
                continue
            signed = _safe_float(pos.get("quantity") or pos.get("szi"), 0)
            is_long = signed > 0
            add_event(f"🧹 Dust sweep {asset}: notional ${notional:.2f} ≤ ${max_notional:.0f} — flattening orphan")
            ok = await _close_position_reduce_only(
                asset,
                is_long,
                qty,
                f"dust orphan ${notional:.2f}",
                entry_price=_safe_float(pos.get("entryPrice") or pos.get("entry_price"), None),
                current_price=_safe_float(pos.get("markPrice") or pos.get("current_price"), None),
                pnl_usd=_safe_float(pos.get("pnl") or pos.get("unrealized_pnl"), 0),
                diary_action="close_dust",
            )
            if ok:
                swept += 1
        return swept

    async def _exit_watcher_loop():
        """Always-on TP/SL poll — runs even while the LLM cycle is blocked."""
        add_event("⚡ Exit watcher started (TP/SL independent of LLM cycle)")
        while True:
            try:
                # Re-read DB each poll so UI % / $ / tp_mode changes apply within ~20s.
                try:
                    settings = await get_trading_settings()
                    _LIVE_SETTINGS.clear()
                    _LIVE_SETTINGS.update(settings)
                except Exception:
                    settings = dict(_LIVE_SETTINGS) if _LIVE_SETTINGS else {}
                exit_check_s = float(
                    CONFIG.get("exit_check_seconds")
                    or settings.get("exit_check_seconds")
                    or 20
                )
                exit_check_s = max(5.0, exit_check_s)
                n_closed = await _mechanical_exit_sweep(settings)
                n_dust = await _sweep_dust_positions()
                if n_closed or n_dust:
                    add_event(f"⚡ Exit watcher closed {n_closed} position(s), swept {n_dust} dust stub(s)")
                await asyncio.sleep(exit_check_s)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning(f"Exit watcher error: {e}")
                await asyncio.sleep(10)

    # ═════════════════════════════════════════════════════════════════════
    # MECHANICAL SCALP BOOK (AI parked). One resting post-only entry at a time,
    # $-risk 1:1, exits by the always-on watcher plus exchange-side TP/SL algos.
    # ═════════════════════════════════════════════════════════════════════
    def _mech_skip(asset: str, reason: str, metrics: dict | None = None) -> None:
        _MECH_STATE["skips"][asset] = {
            "reason": reason,
            "at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics or {},
        }

    def _mech_diary(entry: dict) -> None:
        try:
            network, _ = _venue_network()
            entry.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
            entry.setdefault("mode", "mechanical")
            entry.setdefault("network", network)
            with open(diary_path, "a") as f:
                f.write(json.dumps(entry, default=json_default) + "\n")
        except Exception as e:
            logging.debug(f"mechanical diary write failed: {e}")

    async def _mech_reconcile_closed(open_assets: set) -> None:
        """A TP/SL algo may have closed a book position on the exchange; book the result."""
        ex = hyperliquid
        for asset in list(_MECH_STATE["managed_assets"]):
            if asset in open_assets:
                continue
            pending = _MECH_STATE.get("pending")
            if pending and pending.get("asset") == asset:
                continue
            record = _MECH_STATE["open_trades"].get(asset) or next(
                (tr for tr in active_trades if tr.get("asset") == asset), None
            )
            opened_at = None
            if record and record.get("opened_at"):
                try:
                    opened_at = datetime.fromisoformat(str(record["opened_at"]).replace("Z", "+00:00"))
                except Exception:
                    opened_at = None
            pnl = 0.0
            fee = 0.0
            exit_px = None
            try:
                fills = await ex.get_recent_fills(limit=50)
            except Exception:
                fills = []
            for f_row in fills:
                if str(f_row.get("coin") or f_row.get("symbol") or "").upper() != asset:
                    continue
                ts_raw = f_row.get("time")
                try:
                    ts_i = int(ts_raw)
                    f_time = datetime.fromtimestamp(ts_i / 1000 if ts_i > 1e12 else ts_i, tz=timezone.utc)
                except Exception:
                    f_time = None
                if opened_at and f_time and f_time < opened_at:
                    continue
                try:
                    pnl += float(f_row.get("realizedPnl") or f_row.get("pnl") or 0)
                    fee += abs(float(f_row.get("fee") or 0))
                    exit_px = exit_px or float(f_row.get("px") or 0) or None
                except (TypeError, ValueError):
                    continue
            was_long = bool(record.get("is_long", True)) if record else True
            entry_px = record.get("entry_price") if record else None
            add_event(f"🧾 Mechanical {asset} closed on exchange (algo TP/SL) — realised ${pnl:.2f}, fees ${fee:.2f}")
            for tr in active_trades[:]:
                if tr.get("asset") == asset:
                    active_trades.remove(tr)
            try:
                await ex.cancel_all_orders(asset)
            except Exception:
                pass
            _mech_diary({
                "asset": asset,
                "action": "close_tp" if pnl >= 0 else "close_stop_loss",
                "entry_price": entry_px,
                "exit_price": exit_px,
                "reason": "exchange algo fill",
                "pnl": round(pnl, 4),
                "fee": round(fee, 4),
            })
            _MECH_STATE["open_trades"].pop(asset, None)
            try:
                await _record_pair_hunter_outcome(asset, pnl, 0.0, "close_exchange_algo", was_long=was_long)
            except Exception:
                _MECH_STATE["managed_assets"].discard(asset)
            try:
                await webhook_notifier.notify_exit(
                    asset=asset,
                    side="LONG" if was_long else "SHORT",
                    entry_price=float(entry_px or exit_px or 0),
                    exit_price=float(exit_px or 0),
                    pnl_percent=0.0,
                    pnl_usd=pnl,
                    reason="exchange algo fill",
                    size=float(record.get("amount") or 0) if record else 0.0,
                )
            except Exception:
                pass

    async def _mech_on_fill(
        pending: dict, order: dict, trading_settings: dict, cfg: dict, *, repair: bool = False
    ) -> None:
        """Entry filled: protect it with exchange-side TP/SL, register the trade, notify."""
        ex = hyperliquid
        asset = pending["asset"]
        is_long = pending["side"] == "buy"
        filled_coins = float(order.get("filled_coins") or 0) if order else 0.0
        if filled_coins <= 0:
            try:
                st = await ex.get_user_state()
                for p in st.get("positions", []):
                    if str(p.get("symbol") or p.get("coin") or "").upper() == asset:
                        filled_coins = abs(_safe_float(p.get("quantity") or p.get("szi"), 0))
                        break
            except Exception:
                filled_coins = 0.0
        if filled_coins <= 0:
            add_event(f"⚠️  Mechanical {asset}: fill reported but no position found; nothing to protect")
            return
        avg_px = float((order or {}).get("avg_price") or pending.get("px") or 0) or float(pending.get("px") or 0)
        # Prefer live exchange size/entry — never trust wick distances for $ TP/SL.
        try:
            st = await ex.get_user_state()
            for p in st.get("positions", []):
                if str(p.get("symbol") or p.get("coin") or "").upper() != asset:
                    continue
                live_qty = abs(_safe_float(p.get("quantity") or p.get("szi"), 0))
                live_entry = _safe_float(p.get("entry_price") or p.get("entryPx"), 0)
                if live_qty > 0:
                    filled_coins = live_qty
                if live_entry > 0:
                    avg_px = live_entry
                break
        except Exception:
            pass
        targets = mech.resolve_exit_targets(trading_settings, cfg)
        tp_usd = float(targets["tp_usd"])
        sl_usd = float(targets["sl_usd"])
        exit_label = str(targets["label"])
        setup_name = str(pending.get("setup") or (pending.get("metrics") or {}).get("setup") or "mechanical")
        exit_style = str(pending.get("exit_style") or (pending.get("metrics") or {}).get("exit_style") or "book")
        if filled_coins <= 0 or avg_px <= 0:
            add_event(f"⚠️  Mechanical {asset}: cannot pin exits ({exit_label}; qty={filled_coins}, px={avg_px})")
            return
        struct_stop = _safe_float(pending.get("stop"), 0)
        struct_target = _safe_float(pending.get("target"), 0)
        if exit_style == "structure_rr" and struct_stop > 0 and struct_target > 0:
            stop_px = struct_stop
            target_px = struct_target
            exit_label = f"{setup_name} structure R:R"
            tp_usd = filled_coins * abs(target_px - avg_px)
            sl_usd = filled_coins * abs(stop_px - avg_px)
        else:
            if targets.get("price_based"):
                dist_tp = avg_px * float(targets["tp_pct"]) / 100.0
                dist_sl = avg_px * float(targets["sl_pct"]) / 100.0
            else:
                dist_tp = tp_usd / filled_coins
                dist_sl = sl_usd / filled_coins
            if struct_stop > 0:
                dist_sl = max(dist_sl, abs(avg_px - struct_stop))
            stop_px = (avg_px - dist_sl) if is_long else (avg_px + dist_sl)
            target_px = (avg_px + dist_tp) if is_long else (avg_px - dist_tp)
            sl_usd = filled_coins * dist_sl
        expect_tp = filled_coins * abs(target_px - avg_px)
        expect_sl = filled_coins * abs(stop_px - avg_px)
        td_mode = pending.get("td_mode") or "isolated"
        try:
            await ex.cancel_all_orders(asset)
        except Exception:
            pass
        sl_oid = tp_oid = None
        try:
            sl_row = await ex.place_stop_loss(asset, is_long, filled_coins, stop_px, td_mode=td_mode)
            sl_oid = (ex.extract_oids(sl_row) or [None])[0]
        except Exception as e:
            add_event(f"⚠️  Mechanical {asset}: could not place exchange SL at {stop_px}: {e}")
        try:
            tp_row = await ex.place_take_profit(asset, is_long, filled_coins, target_px, td_mode=td_mode)
            tp_oid = (ex.extract_oids(tp_row) or [None])[0]
            if not tp_oid:
                add_event(f"⚠️  Mechanical {asset}: TP place returned no oid — exit watcher still enforces {exit_label}")
        except Exception as e:
            add_event(f"⚠️  Mechanical {asset}: could not place exchange TP at {target_px}: {e} — exit watcher still enforces {exit_label}")
        add_event(
            f"📌 Mechanical {asset} exits pinned ({exit_label}): qty={filled_coins:.6g} @ {avg_px:.6g} → "
            f"TP {target_px:.6g} (≈${expect_tp:.2f}) / SL {stop_px:.6g} (≈${expect_sl:.2f})"
        )
        opened_at = (
            (_MECH_STATE["open_trades"].get(asset) or {}).get("opened_at")
            if repair
            else datetime.now(timezone.utc).isoformat()
        ) or datetime.now(timezone.utc).isoformat()
        trade_record = {
            "asset": asset,
            "is_long": is_long,
            "amount": filled_coins,
            "entry_price": avg_px,
            "tp_oid": tp_oid,
            "sl_oid": sl_oid,
            "exit_plan": f"mechanical {exit_label}",
            "opened_at": opened_at,
            "mode": "mechanical",
            "usd_exits_pinned": True,
            "tp_usd": tp_usd,
            "sl_usd": sl_usd,
            "tp_mode": targets["mode"],
            "tp_pct": targets["tp_pct"],
            "sl_pct": targets["sl_pct"],
            "setup": setup_name,
            "exit_style": exit_style,
        }
        if not repair:
            active_trades.append(trade_record)
            _MECH_STATS.record_filled()
            add_event(
                f"✅ Mechanical {'LONG' if is_long else 'SHORT'} {asset} filled @ {avg_px:.6g} × {filled_coins:.6g} — "
                f"SL {stop_px:.6g} / TP {target_px:.6g} ({exit_label})"
            )
            _mech_diary({
                "asset": asset,
                "action": "buy" if is_long else "sell",
                "allocation_usd": round(filled_coins * avg_px / max(int(cfg["mech_leverage"]), 1), 2),
                "amount": filled_coins,
                "entry_price": avg_px,
                "tp_price": target_px,
                "tp_oid": tp_oid,
                "sl_price": stop_px,
                "sl_oid": sl_oid,
                "exit_plan": f"{exit_label}, exchange algo + exit watcher",
                "rationale": pending.get("rationale", "mechanical pullback"),
                "reasoning": json.dumps(pending.get("metrics") or {}, default=json_default),
                "order_result": str(order.get("raw") if order else pending.get("ord_id")),
                "opened_at": opened_at,
                "filled": True,
            })
            try:
                await webhook_notifier.notify_entry(
                    asset=asset,
                    side="LONG" if is_long else "SHORT",
                    price=avg_px,
                    size=filled_coins,
                    leverage=int(cfg["mech_leverage"]),
                    reason=pending.get("rationale", "mechanical pullback"),
                )
            except Exception as e:
                logging.debug(f"Webhook entry notification failed: {e}")
        else:
            # Keep existing active_trades row in sync without double-counting fills.
            for tr in active_trades:
                if tr.get("asset") == asset:
                    tr.update(trade_record)
                    break
            else:
                active_trades.append(trade_record)
        _MECH_STATE["open_trades"][asset] = dict(trade_record)
        _MECH_STATE["managed_assets"].add(asset)

    async def _mechanical_cycle(trading_settings: dict, state: dict, positions: list, candidate_assets: list) -> None:
        """One pass: book exchange-side closes, manage the resting entry, then seek one new setup."""
        ex = hyperliquid
        cfg = mech.resolve_config(trading_settings, CONFIG)
        now = datetime.now(timezone.utc)
        _MECH_STATE["last_cycle_at"] = now.isoformat()
        targets = mech.resolve_exit_targets(trading_settings, cfg)
        risk_usd = float(targets["sl_usd"])
        margin_usd = float(targets["margin_usd"])
        tp_usd = float(targets["tp_usd"])
        sl_usd = float(targets["sl_usd"])
        exit_label = str(targets["label"])
        leverage = int(cfg["mech_leverage"])
        td_mode = "isolated"

        dust_cap = float(CONFIG.get("position_dust_notional_usd") or 8.0)
        await _sweep_dust_positions(dust_cap)

        def _pos_notional(p: dict) -> float:
            raw = abs(_safe_float(p.get("notional") or p.get("notionalUsd"), 0))
            if raw > 0:
                return raw
            qty_abs = abs(_safe_float(p.get("quantity") or p.get("szi"), 0))
            mark = _safe_float(p.get("markPrice") or p.get("current_price") or p.get("entryPrice"), 0)
            return qty_abs * mark

        open_assets = {
            str(p.get("symbol") or p.get("coin") or "").upper()
            for p in positions
            if abs(_safe_float(p.get("quantity") or p.get("szi"), 0)) > 0 and _pos_notional(p) > dust_cap
        }
        await _mech_reconcile_closed(open_assets)

        # Repair open legs when exit targets change (e.g. $2 → 5% ROI).
        for pos in positions:
            asset = str(pos.get("symbol") or pos.get("coin") or "").upper()
            qty = abs(_safe_float(pos.get("quantity") or pos.get("szi"), 0))
            if not asset or qty <= 0:
                continue
            rec = _MECH_STATE["open_trades"].get(asset) or {}
            same_tp = abs(float(rec.get("tp_usd") or 0) - tp_usd) < 1e-9
            same_sl = abs(float(rec.get("sl_usd") or 0) - sl_usd) < 1e-9
            same_mode = str(rec.get("tp_mode") or "") == str(targets["mode"])
            if rec.get("usd_exits_pinned") and same_tp and same_sl and same_mode:
                continue
            entry = _safe_float(pos.get("entry_price") or pos.get("entryPx"), 0)
            signed = _safe_float(pos.get("quantity") or pos.get("szi"), 0)
            is_long = signed > 0
            if entry <= 0:
                continue
            fake_pending = {
                "asset": asset,
                "side": "buy" if is_long else "sell",
                "px": entry,
                "stop": entry,
                "td_mode": td_mode,
                "rationale": "repair exits",
                "metrics": {},
            }
            add_event(f"🔧 Mechanical {asset}: re-pinning exits to {exit_label}")
            await _mech_on_fill(
                fake_pending,
                {"filled_coins": qty, "avg_price": entry},
                trading_settings,
                cfg,
                repair=True,
            )

        # 1. Resting entry: filled → protect; expired → cancel (booking any partial fill).
        pending = _MECH_STATE.get("pending")
        if pending:
            asset = pending["asset"]
            order = await ex.get_order(asset, pending["ord_id"]) if hasattr(ex, "get_order") else {}
            st = str(order.get("state") or "")
            filled_coins = float(order.get("filled_coins") or 0)
            try:
                age_s = (now - datetime.fromisoformat(pending["placed_at"])).total_seconds()
            except Exception:
                age_s = 0.0
            if st == "filled" or (asset in open_assets and st not in ("live", "partially_filled")):
                await _mech_on_fill(pending, order, trading_settings, cfg)
                _MECH_STATE["pending"] = None
            elif st == "canceled":
                add_event(f"↩️  Mechanical {asset} post-only order was cancelled by the exchange (would have crossed)")
                _MECH_STATS.record_cancelled()
                if filled_coins > 0 or asset in open_assets:
                    await _mech_on_fill(pending, order, trading_settings, cfg)
                _MECH_STATE["pending"] = None
            elif age_s >= float(pending.get("ttl_s", 600)):
                try:
                    await ex.cancel_order(asset, pending["ord_id"])
                except Exception as e:
                    add_event(f"⚠️  Mechanical {asset}: cancel failed: {e}")
                add_event(f"⌛ Mechanical {asset} entry not filled within {int(age_s // 60)}m — cancelled")
                _MECH_STATS.record_cancelled()
                _mech_diary({"asset": asset, "action": "entry_cancelled", "reason": "ttl", "ord_id": pending["ord_id"]})
                try:
                    order2 = await ex.get_order(asset, pending["ord_id"])
                except Exception:
                    order2 = {}
                if float((order2 or {}).get("filled_coins") or 0) > 0 or asset in open_assets:
                    await _mech_on_fill(pending, order2, trading_settings, cfg)
                _MECH_STATE["pending"] = None
            else:
                remaining = int(float(pending.get("ttl_s", 600)) - age_s)
                add_event(f"⏳ Mechanical resting {pending['side'].upper()} {asset} @ {pending['px']} — {remaining}s left")
                return

        # 2. Capacity, trading switch, session, breakers.
        if len(open_assets) >= int(cfg["mech_max_positions"]):
            logging.debug(f"Mechanical: {len(open_assets)} position(s) open — not seeking entries")
            return
        if not CONFIG.get("trading_enabled", True):
            add_event("⏸️  Mechanical: TRADING_ENABLED=false — monitoring only")
            return
        ok, why = mech.session_open(now, cfg)
        if not ok:
            _mech_skip("*", why)
            logging.info(f"Mechanical: {why}")
            return
        gov_ok, gov_reason = risk_governor.check_can_enter(trading_settings)
        if not gov_ok:
            _mech_skip("*", f"risk governor: {gov_reason}")
            add_event(f"🚦 Mechanical ENTRY PAUSED: {gov_reason}")
            return

        # 3. Scan the hunted list in rank order; take the first clean setup.
        available = _safe_float(state.get("balance"), 0.0)
        scanned = 0
        for asset in candidate_assets:
            asset = str(asset or "").upper()
            if not asset or asset in open_assets or _looks_like_forex(asset):
                continue
            # Pair hunter can still surface names that are not OKX USDT-SWAP (e.g. QNT) —
            # skip before candle calls so we don't spam OKX 51001 errors.
            try:
                if hasattr(ex, "is_usdt_swap_tradable") and not ex.is_usdt_swap_tradable(asset):
                    _mech_skip(asset, "not listed as OKX USDT-SWAP")
                    continue
            except Exception:
                pass
            scanned += 1
            try:
                c5 = await asyncio.to_thread(ex.fetch_candles, asset, "5m", 120)
                c15 = await asyncio.to_thread(ex.fetch_candles, asset, "15m", 120)
            except Exception as e:
                _mech_skip(asset, f"candles unavailable: {e}")
                continue
            try:
                funding = await ex.get_funding_rate(asset)
                oi = await ex.get_open_interest(asset)
            except Exception:
                funding, oi = None, None
            _MECH_OI.record(asset, oi)
            oi_chg = _MECH_OI.change_pct(asset, float(cfg["mech_oi_window_minutes"]))
            res = mech.evaluate(
                asset, c5, c15,
                funding=funding,
                oi_change_pct=oi_chg,
                cfg=cfg,
                now=now,
                last_signal_ts=_MECH_STATE["last_signal_ts"].get(asset),
            )
            if isinstance(res, mech.ScalpSkip):
                _mech_skip(asset, res.reason, res.metrics)
                continue

            allowed, cooldown_reason = reentry_guard.check_entry(asset, res.is_long, trading_settings)
            if not allowed:
                _mech_skip(asset, cooldown_reason or "re-entry cooldown", res.metrics)
                continue

            bid, ask = await ex.get_book_top(asset)
            entry_px = float(res.entry_price)
            if res.is_long and bid:
                entry_px = min(entry_px, float(bid))
            elif (not res.is_long) and ask:
                entry_px = max(entry_px, float(ask))
            if entry_px <= 0:
                _mech_skip(asset, "invalid entry price", res.metrics)
                continue
            res.entry_price = entry_px

            try:
                specs = ex.get_instrument_specs(asset)
            except Exception as e:
                _mech_skip(asset, f"instrument specs unavailable: {e}", res.metrics)
                continue
            # Margin book: size by margin, then pin SL/TP to resolved $ targets (from ROI% or USD).
            sized = mech.size_position(
                res,
                risk_usd=risk_usd,
                leverage=leverage,
                ct_val=specs["ct_val"],
                lot_sz=specs["lot_sz"],
                min_sz=specs["min_sz"],
                available_balance=available,
                bid=bid,
                ask=ask,
                cfg=cfg,
                margin_usd=margin_usd,
                take_profit_usd=tp_usd,
                stop_loss_usd=sl_usd,
            )
            if isinstance(sized, mech.ScalpSkip):
                _mech_skip(asset, sized.reason, res.metrics)
                continue

            setup_name = getattr(res, "setup", None) or res.metrics.get("setup") or "mechanical"
            rationale = (
                f"[{setup_name}] 15m {res.metrics.get('regime')} "
                f"(ADX {res.metrics.get('adx_15m')}, ATR {res.metrics.get('atr_15m_pct')}%), "
                f"RVOL {res.metrics.get('rvol')}, RSI {res.metrics.get('rsi_5m')}, "
                f"${sized.margin_usd:.0f} margin → TP ${sized.reward_usd:.2f} / SL ${sized.risk_usd:.2f}"
            )
            try:
                row = await ex.place_limit_order(
                    asset, res.side, sized.coin_qty, entry_px,
                    post_only=True, leverage=leverage, td_mode=td_mode,
                )
            except Exception as e:
                add_event(f"❌ Mechanical {asset} entry rejected: {e}")
                _mech_skip(asset, f"order rejected: {e}", res.metrics)
                continue
            ord_id = row.get("ordId") if isinstance(row, dict) else None
            if not ord_id:
                add_event(f"❌ Mechanical {asset}: no ordId returned ({row})")
                _mech_skip(asset, "no order id returned", res.metrics)
                continue
            ttl_s = int(cfg["mech_entry_ttl_candles"]) * 300
            _MECH_STATE["pending"] = {
                "asset": asset,
                "ord_id": str(ord_id),
                "side": res.side,
                "px": row.get("px", entry_px),
                "stop": res.stop_price,
                "target": res.target_price,
                "contracts": sized.contracts,
                "coins": sized.coin_qty,
                "placed_at": now.isoformat(),
                "ttl_s": ttl_s,
                "td_mode": td_mode,
                "rationale": rationale,
                "metrics": res.metrics,
                "setup": setup_name,
                "exit_style": getattr(res, "exit_style", "book"),
            }
            _MECH_STATE["last_signal_ts"][asset] = res.signal_ts
            _MECH_STATE["skips"].pop(asset, None)
            _MECH_STATS.record_placed()
            add_event(
                f"🎯 Mechanical {res.side.upper()} {asset}: post-only @ {row.get('px', entry_px)} × {sized.coin_qty:.6g} "
                f"(${sized.notional_usd:.0f} notional, ${sized.margin_usd:.2f} margin @ {leverage}x isolated) — "
                f"SL {res.stop_price:.6g} (−${sized.risk_usd:.2f}) / TP {res.target_price:.6g} (+${sized.reward_usd:.2f}); "
                f"cancel in {ttl_s // 60}m if unfilled. {rationale}"
            )
            _mech_diary({
                "asset": asset,
                "action": "entry_placed",
                "side": res.side,
                "price": row.get("px", entry_px),
                "amount": sized.coin_qty,
                "sl_price": res.stop_price,
                "tp_price": res.target_price,
                "allocation_usd": round(sized.margin_usd, 2),
                "rationale": rationale,
                "reasoning": json.dumps(res.metrics, default=json_default),
                "ord_id": str(ord_id),
            })
            try:
                await webhook_notifier.notify_system_decision([{
                    "asset": asset,
                    "proposed_action": res.side,
                    "final_status": "placed",
                    "detail": rationale[:140],
                }])
            except Exception:
                pass
            return
        if scanned and not _MECH_STATE.get("pending"):
            logging.info(f"Mechanical: scanned {scanned} asset(s), no clean setup this cycle")

    def _mechanical_snapshot() -> dict:
        settings = dict(_LIVE_SETTINGS) if _LIVE_SETTINGS else {}
        cfg = mech.resolve_config(settings, CONFIG)
        network, label = _venue_network()
        return {
            "mode": "mechanical" if mech.is_mechanical_mode(settings) else "ai",
            "network": network,
            "network_label": label,
            "margin_usd": abs(_safe_float(settings.get("margin_per_position"), cfg.get("mech_margin_usd") or 40.0)),
            "take_profit_usd": abs(_safe_float(settings.get("take_profit_usd"), cfg.get("mech_take_profit_usd") or 2.0)),
            "stop_loss_usd": abs(_safe_float(settings.get("stop_loss_usd"), cfg.get("mech_stop_loss_usd") or 2.0)),
            "risk_usd": abs(_safe_float(settings.get("risk_per_trade_usd"), 2.0)),
            "leverage": int(cfg["mech_leverage"]),
            "cycle_seconds": int(cfg["mech_cycle_seconds"]),
            "session": {
                "start_utc": int(cfg["mech_session_start_utc"]),
                "end_utc": int(cfg["mech_session_end_utc"]),
                "weekends": bool(cfg["mech_trade_weekends"]),
                "open_now": mech.session_open(datetime.now(timezone.utc), cfg)[0],
            },
            "pending": _MECH_STATE.get("pending"),
            "managed_assets": sorted(_MECH_STATE["managed_assets"]),
            "skips": _MECH_STATE.get("skips", {}),
            "last_cycle_at": _MECH_STATE.get("last_cycle_at"),
            "stats": _MECH_STATS.summary(),
            "risk_governor": risk_governor.status(),
            "reentry_cooldowns": reentry_guard.active_cooldowns(),
        }

    def _alpaca_client():
        """Return the Alpaca exchange only — never fall back to OKX for stocks."""
        if use_multi_exchange and exchange_manager and exchange_manager.has_exchange("alpaca"):
            return exchange_manager.exchanges.get("alpaca")
        if exchange_name == "alpaca":
            return hyperliquid
        return None

    async def _persist_stock_scanner_rows(rows: list) -> None:
        if not rows:
            return
        try:
            sb = _get_pair_hunter_supabase_client()
            if not sb:
                return
            for row in rows:
                try:
                    sb.table("stock_scanner_stats").upsert(row, on_conflict="asset").execute()
                except Exception as e:
                    logging.debug(f"stock_scanner_stats upsert failed: {e}")
        except Exception as e:
            logging.debug(f"stock scanner persist skipped: {e}")

    async def _log_stock_trade(symbol: str, side: str, size: float, price: float, pnl: float = 0.0, order_id: str = "") -> None:
        try:
            sb = _get_pair_hunter_supabase_client()
            if not sb:
                return
            row = {
                "symbol": symbol,
                "side": side,
                "size": abs(float(size or 0)) or 0.001,
                "price": float(price or 0),
                "fee": 0.0,
                "pnl": float(pnl or 0),
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "order_id": order_id or f"stock-{symbol}-{int(datetime.now(timezone.utc).timestamp())}",
                "book": "stocks",
                "venue": "alpaca",
            }
            sb.table("trades").insert(row).execute()
        except Exception as e:
            logging.warning(f"Stock trade insert failed for {symbol}: {e}")

    async def _stock_book_cycle(trading_settings: dict) -> None:
        """Alpaca-only stock cycle: scanner → trend/pullback → ATR SL / 1.5R TP + scale-out."""
        alpaca = _alpaca_client()
        if not alpaca:
            return
        pool = stock_universe()
        if not pool:
            return
        if str(CONFIG.get("enable_stock_scanner", True)).lower() in ("0", "false", "no", "off"):
            # Still allow trading the full curated pool when scanner is off
            pass

        cfg = stock_strat.resolve_stock_config(trading_settings)
        now = datetime.now(timezone.utc)
        _STOCK_STATE["last_cycle_at"] = now.isoformat()

        # Refresh scanner on a clock
        refresh_min = float(CONFIG.get("stock_scanner_refresh_minutes") or 15)
        last_scan = _STOCK_STATE.get("last_scan_at")
        need_scan = True
        if last_scan:
            try:
                age = (now - datetime.fromisoformat(str(last_scan))).total_seconds()
                need_scan = age >= refresh_min * 60
            except Exception:
                need_scan = True

        enable_scanner = str(CONFIG.get("enable_stock_scanner", True)).lower() in ("1", "true", "yes", "on")
        if enable_scanner and need_scan:
            try:
                active, rows = await scan_stocks(alpaca, top_n=int(CONFIG.get("stock_scanner_top_n") or 5), pool=pool)
                if active:
                    _STOCK_STATE["active_set"] = active
                    _STOCK_STATE["scanner_rows"] = rows
                    _STOCK_STATE["last_scan_at"] = now.isoformat()
                    await _persist_stock_scanner_rows(rows)
                    add_event(f"📈 STOCK SCANNER: {', '.join(active)}")
                elif not _STOCK_STATE.get("active_set"):
                    _STOCK_STATE["active_set"] = pool[: int(CONFIG.get("stock_scanner_top_n") or 5)]
            except Exception as e:
                add_event(f"⚠️ Stock scanner error: {e}")
                if not _STOCK_STATE.get("active_set"):
                    _STOCK_STATE["active_set"] = pool[: int(CONFIG.get("stock_scanner_top_n") or 5)]
        elif not _STOCK_STATE.get("active_set"):
            _STOCK_STATE["active_set"] = pool[: int(CONFIG.get("stock_scanner_top_n") or 5)]

        # Live Alpaca positions for the stock book only
        try:
            state = await alpaca.get_user_state()
        except Exception as e:
            add_event(f"⚠️ Alpaca state error: {e}")
            return
        positions = state.get("positions") or []
        open_map = {}
        for p in positions:
            sym = str(p.get("symbol") or p.get("coin") or "").upper()
            qty = _safe_float(p.get("quantity") or p.get("szi"), 0)
            if sym and abs(qty) > 0:
                open_map[sym] = p

        # Reconcile managed closes
        for sym in list(_STOCK_STATE["managed_assets"]):
            if sym not in open_map:
                rec = _STOCK_STATE["open_trades"].pop(sym, {}) or {}
                _STOCK_STATE["managed_assets"].discard(sym)
                pnl = _safe_float(rec.get("last_unrealized"), 0)
                add_event(f"📕 Stock closed {sym} (book=stocks)")
                await _log_stock_trade(
                    sym,
                    "sell" if str(rec.get("side") or "buy") == "buy" else "buy",
                    float(rec.get("qty") or 0),
                    float(rec.get("entry") or 0),
                    pnl=pnl,
                )

        # Scale-out at 1R + move stop to breakeven
        for sym, pos in open_map.items():
            rec = _STOCK_STATE["open_trades"].get(sym)
            if not rec or rec.get("scale_out_done"):
                continue
            entry = _safe_float(rec.get("entry") or pos.get("entry_price") or pos.get("entryPx"), 0)
            stop = _safe_float(rec.get("stop"), 0)
            qty = abs(_safe_float(pos.get("quantity") or pos.get("szi"), 0))
            px = _safe_float(pos.get("current_price") or pos.get("markPrice"), 0)
            if entry <= 0 or stop <= 0 or qty <= 0 or px <= 0:
                continue
            stop_dist = abs(entry - stop)
            if stop_dist <= 0:
                continue
            is_long = str(rec.get("side") or "buy") == "buy"
            r_mult = ((px - entry) / stop_dist) if is_long else ((entry - px) / stop_dist)
            trigger_r = float(rec.get("scale_out_at_r") or cfg["scale_out_at_r"])
            frac = float(rec.get("scale_out_fraction") or cfg["scale_out_fraction"])
            if r_mult < trigger_r or frac <= 0:
                continue
            close_qty = max(0.0001, round(qty * frac, 4))
            try:
                if is_long:
                    await alpaca.place_sell_order(sym, close_qty, reduce_only=True)
                else:
                    await alpaca.place_buy_order(sym, close_qty, reduce_only=True)
                # Re-pin stop to breakeven on remainder
                remain = max(0.0, qty - close_qty)
                if remain > 0:
                    try:
                        await alpaca.cancel_all_orders(sym)
                    except Exception:
                        pass
                    await alpaca.place_stop_loss(sym, is_long, remain, entry)
                    tp = _safe_float(rec.get("tp"), 0)
                    if tp > 0:
                        await alpaca.place_take_profit(sym, is_long, remain, tp)
                rec["scale_out_done"] = True
                rec["qty"] = remain
                _STOCK_STATE["open_trades"][sym] = rec
                add_event(f"✂️ Stock scale-out {sym}: {frac*100:.0f}% at {trigger_r}R → BE stop")
            except Exception as e:
                add_event(f"⚠️ Stock scale-out failed {sym}: {e}")

        # Session gate for NEW entries only (exits/scale-out above still run).
        # Alpaca /v2/clock is source of truth (weekends, NYSE holidays, early closes).
        open_ok, sess = await is_stock_market_open_for_entries(
            alpaca,
            now=now,
            allow_overnight=bool(cfg["allow_overnight"]),
            entry_cutoff_minutes=int(cfg.get("entry_cutoff_minutes") or 10),
        )
        _STOCK_STATE["session"] = {"open": open_ok, "reason": sess, "at": now.isoformat()}
        if not open_ok:
            add_event(f"📊 Stocks session CLOSED ({sess}) — exits only, no new entries")
            return

        max_pos = int(cfg["max_positions"])
        if len(open_map) >= max_pos:
            return

        candidates = [s for s in (_STOCK_STATE.get("active_set") or []) if s not in open_map]
        for sym in candidates:
            if len(open_map) + len([a for a in _STOCK_STATE["managed_assets"] if a not in open_map]) >= max_pos:
                break
            if sym in _STOCK_STATE["managed_assets"]:
                continue
            try:
                # Re-check clock immediately before each order (session can flip mid-cycle)
                still_open, sess2 = await is_stock_market_open_for_entries(
                    alpaca,
                    now=datetime.now(timezone.utc),
                    allow_overnight=bool(cfg["allow_overnight"]),
                    entry_cutoff_minutes=int(cfg.get("entry_cutoff_minutes") or 10),
                )
                if not still_open:
                    add_event(f"📊 Stocks session flipped to CLOSED ({sess2}) — aborting entries")
                    break
                bars_15 = await alpaca.get_stock_bars(sym, interval=str(cfg["interval"]), limit=120)
                bars_1h = await alpaca.get_stock_bars(sym, interval=str(cfg["htf_interval"]), limit=80)
                result = stock_strat.evaluate(sym, bars_15 or [], bars_1h or [], settings=trading_settings, now=now)
                if isinstance(result, stock_strat.StockSkip):
                    _STOCK_STATE["skips"][sym] = {"reason": result.reason, "at": now.isoformat()}
                    continue
                sig = result
                # Market entry + exchange-native SL/TP
                if sig.is_long:
                    order = await alpaca.place_buy_order(sym, sig.qty)
                else:
                    order = await alpaca.place_sell_order(sym, sig.qty)
                try:
                    await alpaca.place_stop_loss(sym, sig.is_long, sig.qty, sig.stop)
                    await alpaca.place_take_profit(sym, sig.is_long, sig.qty, sig.take_profit)
                except Exception as e:
                    add_event(f"⚠️ Stock {sym}: entry ok but SL/TP pin failed: {e}")
                oid = ""
                try:
                    oids = alpaca.extract_oids(order or {})
                    oid = oids[0] if oids else ""
                except Exception:
                    pass
                _STOCK_STATE["managed_assets"].add(sym)
                _STOCK_STATE["open_trades"][sym] = {
                    "entry": sig.entry,
                    "stop": sig.stop,
                    "tp": sig.take_profit,
                    "qty": sig.qty,
                    "side": sig.side,
                    "scale_out_at_r": sig.scale_out_at_r,
                    "scale_out_fraction": sig.scale_out_fraction,
                    "scale_out_done": False,
                    "notional": sig.notional,
                    "risk_usd": sig.risk_usd,
                    "book": "stocks",
                    "venue": "alpaca",
                    "opened_at": now.isoformat(),
                }
                open_map[sym] = {"symbol": sym, "quantity": sig.qty if sig.is_long else -sig.qty}
                add_event(
                    f"📗 STOCK {sig.side.upper()} {sym} qty={sig.qty} @~{sig.entry:.2f} "
                    f"SL={sig.stop:.2f} TP={sig.take_profit:.2f} (${sig.notional:.0f} / risk ${sig.risk_usd:.1f}) "
                    f"— {sig.rationale}"
                )
                await _log_stock_trade(sym, sig.side, sig.qty, sig.entry, pnl=0.0, order_id=oid)
                # One new entry per cycle keeps the $200 book calm
                break
            except Exception as e:
                add_event(f"⚠️ Stock entry failed {sym}: {e}")
                logging.error(f"Stock entry error {sym}: {e}", exc_info=True)

    async def run_loop():
        """Main trading loop that gathers data, calls the agent, and executes trades."""
        nonlocal invocation_count, initial_account_value
        
        # Position cache for PnL tracking (updated every 2-20 seconds)
        position_cache = {}  # asset -> {pnl_usd, pnl_percent, timestamp, entry_price, initial_margin}
        last_cache_update = {}  # asset -> timestamp
        pair_hunter_stats_loaded = False
        exit_watcher_task = asyncio.create_task(_exit_watcher_loop())
        
        while True:
            if not pair_hunter_stats_loaded:
                await _load_pair_hunter_stats()
                pair_hunter_stats_loaded = True
            invocation_count += 1
            minutes_since_start = (datetime.now(timezone.utc) - start_time).total_seconds() / 60

            # Global account state
            if use_multi_exchange and exchange_manager:
                state = await exchange_manager.get_merged_user_state()
            else:
                state = await hyperliquid.get_user_state()
            total_value = state.get('total_value') or state['balance'] + sum(p.get('pnl', 0) for p in state['positions'])
            sharpe = calculate_sharpe(trade_log)

            account_value = total_value
            if initial_account_value is None:
                initial_account_value = account_value
            total_return_pct = ((account_value - initial_account_value) / initial_account_value * 100.0) if initial_account_value else 0.0

            positions = []
            # Defined up front: the mechanical branch skips the market-data gather that
            # normally creates this, and the active-trade summary below reads it.
            asset_prices = {}
            current_time = datetime.now(timezone.utc)
            
            for pos_wrap in state['positions']:
                pos = pos_wrap
                coin = pos.get('coin') or pos.get('symbol')
                # Get the appropriate exchange for this asset
                # In multi-exchange mode, use exchange_manager; otherwise use the single exchange
                if use_multi_exchange and exchange_manager and coin:
                    asset_exchange = exchange_manager.get_exchange_for_asset(coin)
                    if asset_exchange is None:
                        # Position may carry its own exchange tag from merged state
                        pos_ex_name = (pos.get("exchange") or "").lower()
                        asset_exchange = exchange_manager.exchanges.get(pos_ex_name) if pos_ex_name else None
                    if asset_exchange is None:
                        logging.debug(f"Skipping mark price for {coin}: no mapped venue")
                        current_px = None
                    else:
                        current_px = await asset_exchange.get_current_price(coin) if coin else None
                else:
                    asset_exchange = exchange
                    current_px = await asset_exchange.get_current_price(coin) if coin else None
                
                # Calculate PnL in dollars and percentage (same logic as PositionsTable)
                unrealized_pnl = round_or_none(pos.get('pnl'), 4) or 0
                entry_price = round_or_none(pos.get('entryPx'), 2) or 0
                position_size = abs(float(pos.get('szi', 0)))
                initial_margin = pos.get('initialMargin') or pos.get('positionInitialMargin')
                leverage = pos.get('leverage')
                
                # Calculate ROI percentage (same as PositionsTable)
                roi_percent = None
                if pos.get('roiPercent') is not None:
                    roi_percent = float(pos.get('roiPercent'))
                elif pos.get('roi') is not None:
                    roi_percent = float(pos.get('roi'))
                elif initial_margin and initial_margin > 0:
                    # Fallback: calculate ROI from PnL / initial margin
                    roi_percent = (unrealized_pnl / float(initial_margin)) * 100
                elif entry_price > 0 and position_size > 0:
                    # Fallback: calculate from notional value
                    notional_value = position_size * entry_price
                    if notional_value > 0:
                        roi_percent = (unrealized_pnl / notional_value) * 100
                
                # Update position cache (every 2-20 seconds)
                cache_interval = 10  # Update cache every 10 seconds
                should_update_cache = (
                    coin not in last_cache_update or 
                    (current_time - last_cache_update[coin]).total_seconds() >= cache_interval
                )
                
                if should_update_cache:
                    position_cache[coin] = {
                        "pnl_usd": unrealized_pnl,
                        "pnl_percent": roi_percent,
                        "timestamp": current_time.isoformat(),
                        "entry_price": entry_price,
                        "initial_margin": initial_margin,
                        "leverage": leverage,
                        "position_size": position_size
                    }
                    last_cache_update[coin] = current_time
                    logging.info(f"📊 Position PnL cached for {coin}: ${unrealized_pnl:.2f} ({roi_percent:.2f}% ROI) | Entry: ${entry_price:.2f} | Size: {position_size:.6f}")
                
                if coin and current_px:
                    asset_prices[coin] = current_px
                positions.append({
                    "symbol": coin,
                    "quantity": round_or_none(pos.get('szi'), 6),
                    "entry_price": entry_price,
                    "current_price": round_or_none(current_px, 2),
                    "liquidation_price": round_or_none(pos.get('liquidationPx') or pos.get('liqPx'), 2),
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_pnl_usd": unrealized_pnl,  # Explicit USD value
                    "unrealized_pnl_percent": roi_percent,  # Percentage value
                    "pnl_sign": "+" if unrealized_pnl >= 0 else "-",  # Sign indicator
                    "pnl_percent_sign": "+" if (roi_percent or 0) >= 0 else "-",  # Percent sign indicator
                    "leverage": leverage,
                    "initial_margin": initial_margin,
                    "roiPercent": roi_percent
                })

            recent_diary = []
            try:
                with open(diary_path, "r") as f:
                    lines = f.readlines()
                    for line in lines[-10:]:
                        entry = json.loads(line)
                        recent_diary.append(entry)
            except Exception:
                pass

            open_orders_struct = []
            try:
                open_orders = await hyperliquid.get_open_orders()
                # Reconcile stale orders: if no live position exists for an asset, clear old TP/SL leftovers.
                try:
                    assets_with_positions = set()
                    for p in positions:
                        sym = p.get("symbol")
                        qty = float(p.get("quantity", 0) or 0)
                        if sym and abs(qty) > 0:
                            assets_with_positions.add(sym)

                    stale_assets = set()
                    for o in open_orders or []:
                        sym = o.get("coin") or o.get("asset") or o.get("symbol")
                        if not sym:
                            continue
                        sym = str(sym).replace("USDT", "").replace("/USDT", "")
                        if sym not in assets_with_positions:
                            stale_assets.add(sym)

                    for stale_asset in sorted(stale_assets):
                        await _safe_cancel_all_orders(stale_asset, "stale-order reconciliation")

                    if stale_assets:
                        # Refresh snapshot after cancellations so downstream logic sees current state.
                        open_orders = await hyperliquid.get_open_orders()
                except Exception as reconcile_err:
                    logging.debug(f"Stale-order reconciliation skipped: {reconcile_err}")

                for o in open_orders[:50]:
                    open_orders_struct.append({
                        "coin": o.get('coin'),
                        "oid": o.get('oid'),
                        "is_buy": o.get('isBuy'),
                        "size": round_or_none(o.get('sz'), 6),
                        "price": round_or_none(o.get('px'), 2),
                        "trigger_price": round_or_none(o.get('triggerPx'), 2),
                        "order_type": o.get('orderType')
                    })
            except Exception:
                open_orders = []

            # Reconcile active trades
            try:
                assets_with_positions = set()
                for pos in state['positions']:
                    try:
                        if abs(float(pos.get('szi') or 0)) > 0:
                            assets_with_positions.add(pos.get('coin'))
                    except Exception:
                        continue
                assets_with_orders = {o.get('coin') for o in (open_orders or []) if o.get('coin')}
                for tr in active_trades[:]:
                    asset = tr.get('asset')
                    if asset not in assets_with_positions and asset not in assets_with_orders:
                        add_event(f"Reconciling stale active trade for {asset} (no position, no orders)")
                        active_trades.remove(tr)
                        with open(diary_path, "a") as f:
                            f.write(json.dumps({
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "asset": asset,
                                "action": "reconcile_close",
                                "reason": "no_position_no_orders",
                                "opened_at": tr.get('opened_at')
                            }) + "\n")
            except Exception:
                pass

            recent_fills_struct = []
            try:
                fills = await hyperliquid.get_recent_fills(limit=50)
                for f_entry in fills[-20:]:
                    try:
                        t_raw = f_entry.get('time') or f_entry.get('timestamp')
                        timestamp = None
                        if t_raw is not None:
                            try:
                                t_int = int(t_raw)
                                if t_int > 1e12:
                                    timestamp = datetime.fromtimestamp(t_int / 1000, tz=timezone.utc).isoformat()
                                else:
                                    timestamp = datetime.fromtimestamp(t_int, tz=timezone.utc).isoformat()
                            except Exception:
                                timestamp = str(t_raw)
                        recent_fills_struct.append({
                            "timestamp": timestamp,
                            "coin": f_entry.get('coin') or f_entry.get('asset'),
                            "is_buy": f_entry.get('isBuy'),
                            "size": round_or_none(f_entry.get('sz') or f_entry.get('size'), 6),
                            "price": round_or_none(f_entry.get('px') or f_entry.get('price'), 2)
                        })
                    except Exception:
                        continue
            except Exception:
                pass

            # Enhance active_trades with position status and exit condition checks
            enhanced_active_trades = []
            for tr in active_trades:
                asset = tr.get('asset')
                exit_plan = tr.get('exit_plan', '')
                
                # Check if position still exists
                has_position = any(p.get('symbol') == asset and abs(float(p.get('quantity', 0))) > 0 for p in positions)
                
                # Create enhanced trade info
                enhanced_tr = {
                    "asset": asset,
                    "is_long": tr.get('is_long'),
                    "amount": round_or_none(tr.get('amount'), 6),
                    "entry_price": round_or_none(tr.get('entry_price'), 2),
                    "tp_oid": tr.get('tp_oid'),
                    "sl_oid": tr.get('sl_oid'),
                    "exit_plan": exit_plan,
                    "opened_at": tr.get('opened_at'),
                    "has_position": has_position,
                    "current_price": round_or_none(asset_prices.get(asset), 2)
                }
                
                # Add current PnL if position exists
                if has_position:
                    for p in positions:
                        if p.get('symbol') == asset:
                            enhanced_tr["unrealized_pnl"] = round_or_none(p.get('unrealized_pnl'), 2)
                            enhanced_tr["current_price"] = round_or_none(p.get('current_price'), 2)
                            break
                
                enhanced_active_trades.append(enhanced_tr)

            # ═════════════════════════════════════════════════════════════════
            # 🔍 PAIR HUNTER: Build decision universe before market-data fetch
            # ═════════════════════════════════════════════════════════════════
            decision_assets = list(args.assets)
            enable_pair_hunter = CONFIG.get("enable_pair_hunter", False)
            pair_hunter_top_n = CONFIG.get("pair_hunter_top_n", 5)
            pair_hunter_refresh_interval = CONFIG.get("pair_hunter_refresh_interval", 5)  # Refresh every N loops
            pair_hunter_min_volatility = CONFIG.get("pair_hunter_min_volatility", 2.0)
            pair_hunter_max_analyze_assets = CONFIG.get("pair_hunter_max_analyze_assets", 8)

            mechanical_universe_mode = mech.is_mechanical_mode(_LIVE_SETTINGS)
            if enable_pair_hunter and not mechanical_universe_mode:
                # Initialize pair hunter tracking
                if not hasattr(run_loop, '_pair_hunter_counter'):
                    run_loop._pair_hunter_counter = 0
                    run_loop._last_hunted_assets = []

                run_loop._pair_hunter_counter += 1

                # Refresh hunted pairs every N iterations (or on first run)
                should_refresh = (
                    run_loop._pair_hunter_counter >= pair_hunter_refresh_interval
                    or len(run_loop._last_hunted_assets) == 0
                )
                # Rescan on a clock (default 15m), not every ~60s mechanical cycle.
                # Do not gate this on is_mechanical_mode(_LIVE_SETTINGS): settings are
                # loaded later in the loop, so that check was skipping the throttle.
                if should_refresh and run_loop._last_hunted_assets:
                    last_refresh = _MECH_STATE.get("last_pair_refresh_at")
                    refresh_min = float(
                        mech.resolve_config(_LIVE_SETTINGS, CONFIG).get(
                            "mech_pair_refresh_minutes", 15
                        )
                    )
                    if last_refresh and (current_time - last_refresh).total_seconds() < refresh_min * 60:
                        should_refresh = False
                        run_loop._pair_hunter_counter = 0

                # Get current positions (always include these)
                positions_assets = set()
                for pos in positions:
                    asset = pos.get('symbol') or pos.get('coin')
                    if asset:
                        positions_assets.add(asset)

                if should_refresh:
                    try:
                        add_event(f"🔍 PAIR HUNTER: Scanning for top {pair_hunter_top_n} opportunities...")
                        hunted_assets = await get_best_pairs(
                            hyperliquid,
                            top_n=pair_hunter_top_n,
                            min_volatility=pair_hunter_min_volatility
                        )
                        if not hunted_assets or not isinstance(hunted_assets, list):
                            logger.warning(f"Pair Hunter returned invalid result: {hunted_assets}. Using fallback.")
                            hunted_assets = []
                        if hunted_assets:
                            hunted_assets = filter_assets_to_trading_venue(
                                hunted_assets, limit=pair_hunter_top_n
                            )
                            if not hunted_assets:
                                add_event("⚠️ Pair Hunter: all candidates filtered out (not on OKX).")
                            hunted_assets = _rank_hunted_assets_by_performance(hunted_assets)
                            validated_hunts = []
                            mechanical_now = mech.is_mechanical_mode(_LIVE_SETTINGS)
                            for hunted_asset in hunted_assets:
                                # Drop anything OKX cannot trade (pair hunter already filters,
                                # but catalog can lag — this is the last gate before the loop).
                                try:
                                    if hasattr(hyperliquid, "is_usdt_swap_tradable") and not hyperliquid.is_usdt_swap_tradable(hunted_asset):
                                        add_event(f"⚠️ Pair Hunter dropped {hunted_asset}: not listed on OKX USDT-SWAP")
                                        continue
                                except Exception:
                                    pass
                                # Mechanical mode reads OKX candles directly; the TAAPI check is for the LLM book.
                                if mechanical_now or _has_required_ta_data(hunted_asset):
                                    await _record_pair_hunter_data_success(hunted_asset)
                                    validated_hunts.append(hunted_asset)
                                else:
                                    await _record_pair_hunter_data_failure(hunted_asset, "missing TA candles")
                                    add_event(f"⚠️ Pair Hunter dropped {hunted_asset}: missing TA candles")
                            # If drops left us short, backfill again from ASSETS / larger hunt pool.
                            if len(validated_hunts) < pair_hunter_top_n:
                                validated_hunts = filter_assets_to_trading_venue(
                                    validated_hunts
                                    + list(args.assets or [])
                                    + list(getattr(run_loop, "_last_hunted_assets", []) or []),
                                    limit=pair_hunter_top_n,
                                )
                            hunted_assets = validated_hunts
                        run_loop._last_hunted_assets = hunted_assets
                        run_loop._pair_hunter_counter = 0
                        _MECH_STATE["last_pair_refresh_at"] = current_time
                    except Exception as e:
                        add_event(f"⚠️ Pair Hunter error: {e}. Using cached/fallback assets.")
                        hunted_assets = []
                else:
                    hunted_assets = filter_assets_to_trading_venue(list(run_loop._last_hunted_assets))
                    run_loop._last_hunted_assets = hunted_assets

                if hunted_assets:
                    merged_assets = list(positions_assets) + [a for a in hunted_assets if a not in positions_assets]
                    decision_assets = merged_assets[:pair_hunter_max_analyze_assets]
                    # Log + Telegram only on an actual rescan (cached cycles stay quiet).
                    if should_refresh:
                        add_event(f"🏆 PAIR HUNTER: top setups ({len(hunted_assets)}): {', '.join(hunted_assets)}")
                        scoreboard = []
                        for sym in hunted_assets[:5]:
                            s = pair_hunter_stats.get(sym, {})
                            if s:
                                scoreboard.append(
                                    f"{sym}(wr={float(s.get('win_rate', 0.0)):.0f}%, exp=${float(s.get('expectancy_usd', 0.0)):.2f}, n={int(s.get('total_trades', 0) or 0)})"
                                )
                        if scoreboard:
                            add_event(f"📈 Pair Hunter scorecard: {', '.join(scoreboard)}")
                        if positions_assets:
                            add_event(f"📊 Monitoring open-position assets: {', '.join(positions_assets)}")
                        try:
                            await webhook_notifier.notify_pair_hunter(
                                top_pairs=hunted_assets,
                                positions=list(positions_assets)
                            )
                        except Exception as e:
                            logging.debug(f"Webhook pair hunter notification failed: {e}")
                else:
                    decision_assets = list(args.assets)
                    add_event("⚠️ Pair Hunter yielded no symbols, using ASSETS fallback.")

            # Always include configured forex pairs when IG is live; skip them while FX is closed
            # or when IG failed to init (never fall through to OKX for currency pairs).
            forex_assets = []
            raw_fx = CONFIG.get("forex_assets") or ""
            if raw_fx:
                forex_assets = [a.strip().upper() for a in str(raw_fx).replace(",", " ").split() if a.strip()]
            enable_forex = str(CONFIG.get("ENABLE_FOREX", CONFIG.get("enable_forex", "false"))).lower() in (
                "1", "true", "yes", "on"
            )
            ig_ready = bool(
                use_multi_exchange and exchange_manager and "ig" in exchange_manager.exchanges
            )
            # Strip any FX / stock symbols that leaked into the crypto/pair-hunter list.
            _stock_set_decision = {
                p.strip().upper()
                for p in str(CONFIG.get("stock_assets") or "").replace(",", " ").split()
                if p.strip()
            }
            decision_assets = [
                a for a in decision_assets
                if not _looks_like_forex(a) and (a or "").upper() not in _stock_set_decision
            ]
            if not enable_forex:
                add_event("💱 Forex PAUSED (ENABLE_FOREX=false) — crypto only this cycle")
            elif forex_assets and not ig_ready:
                add_event(
                    "💱 Forex PAUSED — IG not available (login/rate-limit). "
                    "Pairs will not be sent to OKX."
                )
            elif forex_assets and ig_ready:
                if is_forex_session_open():
                    for fx in forex_assets:
                        if fx not in decision_assets:
                            decision_assets.append(fx)
                    add_event(f"💱 Forex session OPEN — watching: {', '.join(forex_assets)}")
                else:
                    # Drop flat FX pairs, but keep any open IG positions for mechanical exits.
                    open_fx = {
                        (p.get("symbol") or p.get("coin") or "").upper()
                        for p in positions
                        if _looks_like_forex(p.get("symbol") or p.get("coin") or "")
                    }
                    for fx in open_fx:
                        if fx not in decision_assets:
                            decision_assets.append(fx)
                    add_event(
                        "💱 Forex session CLOSED (Fri 22:00–Sun 22:00 UTC) — "
                        f"skipping new FX entries"
                        + (f"; still managing open: {', '.join(sorted(open_fx))}" if open_fx else "")
                    )

            dashboard = {
                "total_return_pct": round(total_return_pct, 2),
                "balance": round_or_none(state['balance'], 2),
                "account_value": round_or_none(account_value, 2),
                "sharpe_ratio": round_or_none(sharpe, 3),
                "positions": positions,
                "active_trades": enhanced_active_trades,
                "open_orders": open_orders_struct,
                "recent_diary": recent_diary,
                "recent_fills": recent_fills_struct,
                "position_summary": {
                    "assets_with_positions": list(assets_with_positions) if 'assets_with_positions' in locals() else [],
                    "assets_without_positions": [a for a in decision_assets if a not in (assets_with_positions if 'assets_with_positions' in locals() else set())]
                }
            }

            # Fetch trading settings BEFORE market data — the gather loop needs the
            # configured timeframes, and a NameError here previously emptied market_data.
            trading_settings = await get_trading_settings()
            _LIVE_SETTINGS.clear()
            _LIVE_SETTINGS.update(trading_settings)
            _LAST_KNOWN_EQUITY["value"] = float(account_value or 0.0) or _LAST_KNOWN_EQUITY.get("value")
            default_leverage = trading_settings["leverage"]

            # ═════════════════════════════════════════════════════════════════
            # STOCK BOOK (Alpaca only): scanner + trend/pullback. Runs even when
            # crypto mechanical mode is on — never mixes into OKX / pair hunter.
            # ═════════════════════════════════════════════════════════════════
            try:
                await _stock_book_cycle(trading_settings)
            except Exception as e:
                import traceback
                add_event(f"❌ Stock book cycle error: {e}")
                logging.error(f"Stock book cycle error: {traceback.format_exc()}")

            # ═════════════════════════════════════════════════════════════════
            # MECHANICAL MODE: DeepSeek is parked. No TAAPI pulls, no prompt, no
            # LLM call — the scalp helper decides from OKX candles and the cycle
            # runs every ~60s so a 5m setup is taken on the next candle.
            # ═════════════════════════════════════════════════════════════════
            if mech.is_mechanical_mode(trading_settings):
                mech_cfg = mech.resolve_config(trading_settings, CONFIG)
                if not getattr(run_loop, "_mech_announced", False):
                    network_now, label_now = _venue_network()
                    univ = ", ".join(mech.liquid_universe(mech_cfg)[:8])
                    add_event(
                        f"🤖 MECHANICAL multi-setup on {label_now}: AI parked. "
                        f"{mech.resolve_exit_targets(trading_settings, mech_cfg)['label']}, "
                        f"${float(mech_cfg['mech_margin_usd']):.0f} margin ×{int(mech_cfg['mech_max_positions'])} max, "
                        f"{int(mech_cfg['mech_leverage'])}x isolated, cycle {int(mech_cfg['mech_cycle_seconds'])}s. "
                        f"Setups: session_impulse / range_mr / breakout_retest. Universe: {univ}…"
                    )
                    run_loop._mech_announced = True
                # Mechanical is crypto/OKX-only — never pull Alpaca stocks or IG forex into the loop
                _mech_stock_set = {
                    p.strip().upper()
                    for p in str(CONFIG.get("stock_assets") or "").replace(",", " ").split()
                    if p.strip()
                }
                _CRYPTO_VENUES = ("okx", "binance", "aster")

                def _mech_allowed(sym: str) -> bool:
                    au = (sym or "").upper()
                    if not au or _looks_like_forex(au) or au in _mech_stock_set:
                        return False
                    if use_multi_exchange and exchange_manager:
                        mapped = exchange_manager.get_exchange_name_for_asset(au)
                        if mapped and mapped not in _CRYPTO_VENUES:
                            return False
                    return True

                candidates = [a for a in mech.liquid_universe(mech_cfg) if _mech_allowed(a)]
                for pos in positions:
                    a = str(pos.get("symbol") or pos.get("coin") or "").upper()
                    pos_ex = str(pos.get("exchange") or "").lower()
                    if pos_ex and pos_ex not in _CRYPTO_VENUES:
                        continue
                    if a and a not in candidates and _mech_allowed(a):
                        candidates.insert(0, a)
                try:
                    await _mechanical_cycle(trading_settings, state, positions, candidates)
                except Exception as e:
                    import traceback
                    add_event(f"❌ Mechanical cycle error: {e}")
                    logging.error(f"Mechanical cycle error: {traceback.format_exc()}")
                await asyncio.sleep(max(15, int(mech_cfg["mech_cycle_seconds"])))
                continue
            run_loop._mech_announced = False

            # Gather data for ALL assets first
            market_sections = []
            asset_prices = {}
            for asset in decision_assets:
                try:
                    asset_exchange = _ex_for(asset)
                    if asset_exchange is None:
                        add_event(f"⏭️  Skipping {asset}: no mapped venue (hard-split)")
                        continue
                    current_price = await asset_exchange.get_current_price(asset)
                    if not current_price or float(current_price) <= 0:
                        add_event(f"⏭️  Skipping {asset}: no usable price on venue")
                        await _record_pair_hunter_data_failure(asset, "no venue price")
                        continue
                    asset_prices[asset] = current_price
                    if asset not in price_history:
                        price_history[asset] = deque(maxlen=60)
                    px_decimals = _price_decimals(current_price)
                    price_history[asset].append({
                        "t": datetime.now(timezone.utc).isoformat(),
                        "mid": round_or_none(current_price, px_decimals),
                    })

                    is_fx = _looks_like_forex(asset)
                    if is_fx:
                        oi = None
                        funding = None
                        ta_symbol = asset if "/" in asset else f"{asset[:3]}/{asset[3:6]}"
                        asset_settings = _settings_for_asset(asset, trading_settings)
                        intraday_tf = asset_settings.get("intraday_timeframe") or CONFIG.get("forex_interval") or "15m"
                        longterm_tf = asset_settings.get("longterm_timeframe") or CONFIG.get("longterm_timeframe") or "4h"
                    else:
                        oi = await asset_exchange.get_open_interest(asset) if hasattr(asset_exchange, "get_open_interest") else None
                        funding = await asset_exchange.get_funding_rate(asset) if hasattr(asset_exchange, "get_funding_rate") else None
                        ta_symbol = f"{asset}/USDT"
                        intraday_tf = trading_settings.get("intraday_timeframe") or CONFIG.get("intraday_timeframe", "15m")
                        longterm_tf = trading_settings.get("longterm_timeframe") or CONFIG.get("longterm_timeframe", "4h")

                    ema_series = taapi.fetch_series("ema", ta_symbol, intraday_tf, results=10, params={"period": 20}, value_key="value")
                    macd_series = taapi.fetch_series("macd", ta_symbol, intraday_tf, results=10, value_key="valueMACD")
                    rsi7_series = taapi.fetch_series("rsi", ta_symbol, intraday_tf, results=10, params={"period": 7}, value_key="value")
                    rsi14_series = taapi.fetch_series("rsi", ta_symbol, intraday_tf, results=10, params={"period": 14}, value_key="value")

                    lt_ema20 = taapi.fetch_value("ema", ta_symbol, longterm_tf, params={"period": 20}, key="value")
                    lt_ema50 = taapi.fetch_value("ema", ta_symbol, longterm_tf, params={"period": 50}, key="value")
                    lt_atr3 = taapi.fetch_value("atr", ta_symbol, longterm_tf, params={"period": 3}, key="value")
                    lt_atr14 = taapi.fetch_value("atr", ta_symbol, longterm_tf, params={"period": 14}, key="value")
                    lt_macd_series = taapi.fetch_series("macd", ta_symbol, longterm_tf, results=10, value_key="valueMACD")
                    lt_rsi_series = taapi.fetch_series("rsi", ta_symbol, longterm_tf, results=10, params={"period": 14}, value_key="value")

                    recent_mids = [entry["mid"] for entry in list(price_history.get(asset, []))[-10:]]
                    funding_annualized = round(funding * 24 * 365 * 100, 2) if funding else None

                    market_sections.append({
                        "asset": asset,
                        "venue": "ig" if is_fx else (CONFIG.get("exchange") or "okx"),
                        "current_price": round_or_none(current_price, px_decimals),
                        "intraday": {
                            "ema20": round_or_none(ema_series[-1], px_decimals) if ema_series else None,
                            "macd": round_or_none(macd_series[-1], px_decimals) if macd_series else None,
                            "rsi7": round_or_none(rsi7_series[-1], 2) if rsi7_series else None,
                            "rsi14": round_or_none(rsi14_series[-1], 2) if rsi14_series else None,
                            "series": {
                                "ema20": round_series(ema_series, px_decimals),
                                "macd": round_series(macd_series, px_decimals),
                                "rsi7": round_series(rsi7_series, 2),
                                "rsi14": round_series(rsi14_series, 2)
                            }
                        },
                        "long_term": {
                            "ema20": round_or_none(lt_ema20, px_decimals),
                            "ema50": round_or_none(lt_ema50, px_decimals),
                            "atr3": round_or_none(lt_atr3, px_decimals),
                            "atr14": round_or_none(lt_atr14, px_decimals),
                            "macd_series": round_series(lt_macd_series, px_decimals),
                            "rsi_series": round_series(lt_rsi_series, 2)
                        },
                        "open_interest": round_or_none(oi, 2),
                        "funding_rate": round_or_none(funding, 8),
                        "funding_annualized_pct": funding_annualized,
                        "recent_mid_prices": recent_mids
                    })
                except Exception as e:
                    add_event(f"Data gather error {asset}: {e}")
                    continue

            # Identify which assets are flat (no positions) - these need active entry hunting
            assets_with_positions_set = set()
            for pos in positions:
                if pos.get('symbol') and abs(float(pos.get('quantity', 0))) > 0:
                    assets_with_positions_set.add(pos.get('symbol'))
            
            flat_assets = [a for a in decision_assets if a not in assets_with_positions_set]
            tp_percent = trading_settings["take_profit_percent"]
            sl_percent = trading_settings["stop_loss_percent"]
            scalping_tp_percent = trading_settings.get("scalping_tp_percent", 5.0)
            scalping_sl_percent = trading_settings.get("scalping_sl_percent", 5.0)
            take_profit_strict_enforcement = trading_settings.get("take_profit_strict_enforcement", False)
            stop_loss_usd = resolve_stop_loss_usd(trading_settings, default=-6.0)
            # Hard $ close = risk_per_trade_usd exactly (no 1.5× buffer — that was letting losses run).
            max_loss_usd = resolve_max_loss_usd(trading_settings, default=6.0)
            stop_loss_usd = -abs(max_loss_usd)
            exit_mode_now = str(trading_settings.get("exit_mode") or "atr").lower()
            sizing_now = str(trading_settings.get("position_sizing_mode") or "risk").lower()
            enable_stop_loss_orders = trading_settings.get("enable_stop_loss_orders", True)  # Enable automatic SL orders on exchange
            agent_manage_exits = bool(trading_settings.get("agent_manage_exits", CONFIG.get("agent_manage_exits", True)))
            if not agent_manage_exits:
                add_event("🛡️  TP/SL-only close mode active: agent-driven exits are disabled.")
            add_event(
                f"🛡️  HARD max loss: ${max_loss_usd:.2f} "
                f"(mode={sizing_now}/{exit_mode_now}, "
                f"UI SL%={sl_percent}, max_stop={trading_settings.get('max_stop_price_pct')}%)"
            )
            # Position sizing settings (target_profit_per_1pct_move, max_positions, position_sizing_mode) are in trading_settings dict
            # These come from database or .env file (TARGET_PROFIT_PER_1PCT_MOVE, MAX_POSITIONS, POSITION_SIZING_MODE)
            
            # ========================================================================
            # CRITICAL: ENFORCE STOP LOSS BEFORE AI DECISION
            # Mechanical SL from UI settings is always respected (agent or TP/SL-only).
            # ========================================================================
            logging.info(f"🛡️  Checking stop loss for {len(positions)} positions before AI decision...")
            positions_to_close = []  # Track positions that hit stop loss
            
            for pos in positions:
                asset = pos.get('symbol')
                if not asset:
                    continue
                
                position_size = abs(float(pos.get('quantity', 0)))
                if position_size <= 0:
                    continue
                
                # Get position data
                raw_size = float(pos.get('quantity', 0) or 0)
                is_long = raw_size > 0
                entry_price = float(pos.get('entry_price') or 0)
                current_price = float(pos.get('current_price') or 0)
                unrealized_pnl = float(pos.get('unrealized_pnl') or pos.get('pnl') or 0)
                initial_margin = float(pos.get('initial_margin') or 0)

                # Prefer more conservative (more negative) of exchange UPL vs price×size estimate.
                if entry_price > 0 and current_price > 0 and position_size > 0:
                    derived_pnl = (
                        (current_price - entry_price) * position_size if is_long
                        else (entry_price - current_price) * position_size
                    )
                    unrealized_pnl = min(unrealized_pnl, derived_pnl)

                # Prefer margin ROI; for stop-loss use the more conservative (more negative) of ROI vs price-move
                roi_percent = _compute_position_roi_percent(
                    pos, is_long=is_long, entry_price=entry_price, current_price=current_price
                )
                pnl_candidates = [roi_percent]
                if entry_price > 0 and current_price > 0:
                    if is_long:
                        pnl_candidates.append(((current_price - entry_price) / entry_price) * 100.0)
                    else:
                        pnl_candidates.append(((entry_price - current_price) / entry_price) * 100.0)

                # Use the most conservative loss estimate so SL cannot be bypassed by bad data.
                pnl_percent = min(pnl_candidates) if pnl_candidates else 0.0
                
                # Determine which strategy is active for this position
                current_strategy_name = agent.get_name()
                is_scalping = "scalping" in current_strategy_name.lower()
                
                # Get UI stop loss % (scalping or regular) — ALWAYS enforced on margin ROI.
                ui_sl_percent = float(scalping_sl_percent if is_scalping else sl_percent)

                # Refresh exit plan if missing (e.g. after redeploy) so ATR stop + TP mode stay applied.
                exit_plan = _EXIT_PLANS.get(asset.upper())
                if exit_plan is None and entry_price > 0 and current_price > 0:
                    try:
                        lev = float(
                            pos.get("leverage")
                            or trading_settings.get("leverage")
                            or CONFIG.get("default_leverage")
                            or 10
                        )
                        interval = trading_settings.get("interval") or CONFIG.get("interval") or "15m"
                        exit_plan = _resolve_exit_plan(asset, trading_settings, lev, current_price, interval)
                    except Exception as e:
                        logging.debug(f"Could not rebuild exit plan for {asset}: {e}")

                atr_stop_active = (
                    exit_plan is not None
                    and exit_plan.source == "atr"
                    and entry_price > 0
                    and current_price > 0
                )
                price_move_pct = None
                if entry_price > 0 and current_price > 0:
                    price_move_pct = (
                        ((current_price - entry_price) / entry_price) * 100.0 if is_long
                        else ((entry_price - current_price) / entry_price) * 100.0
                    )

                # Hard safety ROI cap (even with ATR) so 15–20% margin losses cannot run.
                hard_max_loss_cap_percent = float(
                    trading_settings.get("hard_max_loss_cap_percent")
                    or ui_sl_percent
                    or 8.0
                )
                effective_roi_sl = min(ui_sl_percent, hard_max_loss_cap_percent) if hard_max_loss_cap_percent > 0 else ui_sl_percent

                # Check stop loss in PERCENTAGE — margin ROI against UI SL% (always).
                sl_breached_percent = False
                logging.info(
                    f"📊 Position {asset} PnL check: ${unrealized_pnl:.2f} "
                    f"(ROI {roi_percent:.2f}%, price {price_move_pct if price_move_pct is not None else 0:.2f}%) | "
                    f"UI SL -{effective_roi_sl:g}% ROI"
                    + (
                        f" | ATR stop -{exit_plan.stop_price_pct:.2f}% price"
                        if atr_stop_active
                        else ""
                    )
                )
                if roi_percent <= -effective_roi_sl:
                    sl_breached_percent = True
                    pnl_percent = roi_percent
                    add_event(
                        f"🛑 STOP LOSS BREACHED (ROI %) for {asset}: {roi_percent:.2f}% "
                        f"(threshold: -{effective_roi_sl:g}%). Closing immediately!"
                    )
                    logging.warning(
                        f"🛑 STOP LOSS BREACHED (ROI %) for {asset}: {roi_percent:.2f}% <= -{effective_roi_sl:g}%"
                    )

                # ATR price stop (additional) — only if ROI check did not already fire.
                if (
                    not sl_breached_percent
                    and atr_stop_active
                    and price_move_pct is not None
                    and price_move_pct <= -float(exit_plan.stop_price_pct or 0)
                ):
                    sl_breached_percent = True
                    pnl_percent = price_move_pct
                    add_event(
                        f"🛑 STOP LOSS BREACHED (ATR price %) for {asset}: {price_move_pct:.2f}% "
                        f"(threshold: -{exit_plan.stop_price_pct:.2f}%). Closing immediately!"
                    )

                # Check stop loss in USD — hard close at risk_per_trade_usd (no buffer).
                sl_breached_usd = False
                if stop_loss_usd is not None and stop_loss_usd < 0:
                    logging.info(f"📊 Position {asset} USD SL check: ${unrealized_pnl:.2f} vs threshold: ${stop_loss_usd:.2f}")
                    if unrealized_pnl <= stop_loss_usd:
                        sl_breached_usd = True
                        add_event(
                            f"🛑 STOP LOSS BREACHED (USD) for {asset}: ${unrealized_pnl:.2f} "
                            f"(threshold: ${stop_loss_usd:.2f}). Closing immediately!"
                        )
                        logging.warning(
                            f"🛑 STOP LOSS BREACHED (USD) for {asset}: ${unrealized_pnl:.2f} <= ${stop_loss_usd:.2f}"
                        )
                # If stop loss is breached, mark for immediate closure
                if sl_breached_percent or sl_breached_usd:
                    positions_to_close.append({
                        "asset": asset,
                        "is_long": is_long,
                        "position_size": position_size,
                        "reason": f"Stop Loss: {pnl_percent:.2f}% / ${unrealized_pnl:.2f}",
                        "pnl_percent": pnl_percent,
                        "pnl_usd": unrealized_pnl,
                        "entry_price": entry_price,
                        "current_price": current_price,
                    })
            
            # Always enforce mechanical UI stop-loss (agent mode and TP/SL-only mode).
            for pos_to_close in positions_to_close:
                asset = pos_to_close["asset"]
                is_long = pos_to_close["is_long"]
                position_size = pos_to_close["position_size"]
                reason = pos_to_close["reason"]
                pnl_percent = pos_to_close["pnl_percent"]
                pnl_usd = pos_to_close["pnl_usd"]
                entry_price = pos_to_close.get("entry_price", 0)
                current_price = pos_to_close.get("current_price", 0)
                
                try:
                    add_event(f"🛑 FORCE CLOSING {asset} due to stop loss: {reason}")
                    closed = await _close_position_reduce_only(
                        asset, is_long, position_size, reason,
                        entry_price=entry_price, current_price=current_price,
                        pnl_usd=pnl_usd, pnl_percent=pnl_percent,
                        diary_action="close_stop_loss",
                    )
                    if closed:
                        add_event(f"✅ Force closed {asset} position (Stop Loss) - Loss: {pnl_percent:.2f}% (${pnl_usd:.2f})")
                        positions = [p for p in positions if p.get('symbol') != asset]
                except Exception as e:
                    add_event(f"❌ Failed to force close {asset} position (Stop Loss): {e}")
                    import traceback
                    logging.error(f"Stop loss closure error: {traceback.format_exc()}")

            # Build per-asset leverage map for context and enforcement
            # Forex defaults to FOREX_LEVERAGE / IG_LEVERAGE instead of crypto DEFAULT_LEVERAGE.
            per_asset_leverage = {}
            for asset in decision_assets:
                asset_defaults = _settings_for_asset(asset, trading_settings)
                base_lev = int(asset_defaults.get("leverage") or default_leverage)
                asset_leverage = get_leverage_for_asset(asset, base_lev)
                per_asset_leverage[asset] = asset_leverage
                if asset_leverage != default_leverage:
                    add_event(f"📌 Per-asset leverage for {asset}: {asset_leverage}x (default: {default_leverage}x)")

            # Single LLM call with all assets
            context_payload = OrderedDict([
                ("invocation", {
                    "minutes_since_start": round(minutes_since_start, 2),
                    "current_time": datetime.now(timezone.utc).isoformat(),
                    "invocation_count": invocation_count
                }),
                ("trading_settings", {
                    "default_leverage": default_leverage,
                    "per_asset_leverage": per_asset_leverage,  # Per-asset leverage overrides (MANDATORY)
                    "take_profit_percent": tp_percent,
                    "stop_loss_percent": sl_percent,
                    "scalping_tp_percent": scalping_tp_percent,  # Scalping strategy TP%
                    "scalping_sl_percent": scalping_sl_percent,  # Scalping strategy SL%
                    "take_profit_strict_enforcement": take_profit_strict_enforcement,  # If true, TP must be strictly enforced
                    "agent_manage_exits": agent_manage_exits,  # If false, do not close positions from agent logic except TP/SL checks
                    "margin_per_position": trading_settings.get("margin_per_position"),
                    "risk_per_trade_usd": trading_settings.get("risk_per_trade_usd"),
                    "risk_per_trade_pct": trading_settings.get("risk_per_trade_pct"),
                    "stop_loss_usd": trading_settings.get("stop_loss_usd"),
                    "max_positions": trading_settings.get("max_positions", 5),
                    "position_sizing_mode": str(trading_settings.get("position_sizing_mode", "risk") or "risk").lower(),
                    "note": _build_position_sizing_note(trading_settings, default_leverage, per_asset_leverage),
                    "⚠️_CRITICAL_RULES": {
                        "position_sizing_mode": str(trading_settings.get("position_sizing_mode", "risk") or "risk").lower(),
                        "risk_per_trade_usd": trading_settings.get("risk_per_trade_usd"),
                        "stop_loss_usd": trading_settings.get("stop_loss_usd"),
                        "margin_per_position": trading_settings.get("margin_per_position"),
                        "default_leverage": default_leverage,
                        "per_asset_leverage": per_asset_leverage,
                        "enforcement": "System sizes positions; agent focuses on buy/sell/hold",
                    }
                }),
                ("account", dashboard),
                ("market_data", market_sections),
                ("position_status", {
                    "assets_with_positions": list(assets_with_positions_set),
                    "flat_assets": flat_assets,
                    "critical_note": f"Assets {flat_assets} have NO positions - ACTIVELY LOOK FOR ENTRY OPPORTUNITIES using technical analysis. Do not default to 'hold' for flat assets unless you've analyzed and found NO viable setups."
                }),
                ("positions_data", {
                    "positions": [
                        {
                            "asset": p.get('symbol'),
                            "side": "long" if float(p.get('quantity', 0)) > 0 else "short",
                            "entry_price": p.get('entry_price'),
                            "current_price": p.get('current_price'),
                            "pnl_usd": p.get('unrealized_pnl_usd'),
                            "pnl_percent": p.get('unrealized_pnl_percent'),
                            "pnl_sign": p.get('pnl_sign'),  # "+" or "-"
                            "pnl_percent_sign": p.get('pnl_percent_sign'),  # "+" or "-"
                            "leverage": p.get('leverage'),
                            "size": abs(float(p.get('quantity', 0))),
                            "initial_margin": p.get('initial_margin'),
                            "cached_pnl": position_cache.get(p.get('symbol'), {})  # Cached data for redundancy
                        }
                        for p in positions if p.get('symbol')
                    ],
                    "note": "Each position includes PnL in USD and percentage with sign indicators. Use cached_pnl if current data is unavailable."
                }),
                ("instructions", {
                    "assets": decision_assets,
                    "requirement": "Decide actions for all assets and return a strict JSON array matching the schema.",
                    "priority": f"PRIORITIZE finding entries for flat assets: {flat_assets}. These have no positions - actively scan for trading opportunities.",
                    "tp_sl_guidance": f"Calculate TP/SL prices using configured percentages: TP={tp_percent}%, SL={sl_percent}%. If not provided, system will calculate automatically."
                })
            ])
            # Log position data being passed to AI
            positions_data = context_payload.get("positions_data", {}).get("positions", [])
            if positions_data:
                logging.info(f"📤 Passing {len(positions_data)} position(s) with PnL data to AI agent:")
                for pos_data in positions_data:
                    asset = pos_data.get("asset", "UNKNOWN")
                    pnl_usd = pos_data.get("pnl_usd", 0)
                    pnl_percent = pos_data.get("pnl_percent", 0)
                    pnl_sign = pos_data.get("pnl_sign", "+")
                    pnl_percent_sign = pos_data.get("pnl_percent_sign", "+")
                    logging.info(f"   • {asset}: {pnl_sign}${abs(pnl_usd):.2f} ({pnl_percent_sign}{abs(pnl_percent):.2f}%) | Side: {pos_data.get('side', 'N/A')} | Entry: ${pos_data.get('entry_price', 0):.2f} | Current: ${pos_data.get('current_price', 0):.2f}")
            else:
                logging.info("📤 No open positions - passing empty positions_data to AI agent")
            
            context = json.dumps(context_payload, default=json_default)
            add_event(f"Combined prompt length: {len(context)} chars for {len(decision_assets)} assets")
            with open("prompts.log", "a") as f:
                f.write(f"\n\n--- {datetime.now()} - ALL ASSETS ---\n{json.dumps(context_payload, indent=2, default=json_default)}\n")

            def _is_failed_outputs(outs):
                """Return True when outputs are missing or clearly invalid."""
                if not isinstance(outs, dict):
                    return True
                decisions = outs.get("trade_decisions")
                if not isinstance(decisions, list) or not decisions:
                    return True
                try:
                    return all(
                        isinstance(o, dict)
                        and (o.get('action') == 'hold')
                        and ('parse error' in (o.get('rationale', '').lower()))
                        for o in decisions
                    )
                except Exception:
                    return True

            # ═════════════════════════════════════════════════════════════════
            # 📊 PERIODIC POSITION UPDATES: Send P&L summary every N iterations
            # ═════════════════════════════════════════════════════════════════
            position_update_interval = CONFIG.get("position_update_interval", 6)  # Every 6 cycles = 30 min at 5m interval
            if not hasattr(run_loop, '_position_update_counter'):
                run_loop._position_update_counter = 0
            
            run_loop._position_update_counter += 1
            
            if run_loop._position_update_counter >= position_update_interval and positions:
                # Build position summary
                position_summary = []
                total_pnl_usd = 0
                total_pnl_pct = 0
                
                for pos in positions:
                    asset = pos.get('symbol') or pos.get('coin')
                    if not asset:
                        continue
                    
                    pnl_usd = float(pos.get('unrealized_pnl', 0) or pos.get('unrealizedPnl', 0) or 0)
                    pnl_pct = float(pos.get('unrealized_pnl_percent', 0) or 0)
                    entry_price = float(pos.get('entry_price') or pos.get('entryPx') or pos.get('entryPrice') or 0)
                    current_price = asset_prices.get(asset, 0)
                    
                    position_summary.append({
                        'asset': asset,
                        'pnl_usd': pnl_usd,
                        'pnl_pct': pnl_pct,
                        'entry': entry_price,
                        'current': current_price
                    })
                    total_pnl_usd += pnl_usd
                    total_pnl_pct += pnl_pct
                
                if position_summary:
                    # Send webhook notification
                    try:
                        await webhook_notifier.send_notification("POSITION_UPDATE", {
                            "positions": position_summary,
                            "total_pnl_usd": round(total_pnl_usd, 2),
                            "total_pnl_pct": round(total_pnl_pct, 2),
                            "count": len(position_summary),
                            "timestamp": str(datetime.now(timezone.utc))
                        })
                    except Exception as e:
                        logging.debug(f"Position update webhook failed: {e}")
                    
                    # Log to console
                    add_event(f"📊 Position Update: {len(position_summary)} positions, Total P&L: ${total_pnl_usd:.2f} ({total_pnl_pct:.1f}%)")
                
                run_loop._position_update_counter = 0  # Reset counter
            
            # Visibility: print the active analysis universe every cycle
            if enable_pair_hunter:
                hunted_snapshot = list(getattr(run_loop, "_last_hunted_assets", []))
                add_event(f"🔎 Pair Hunter cached symbols ({len(hunted_snapshot)}): {', '.join(hunted_snapshot) if hunted_snapshot else 'none'}")
            # Skip DeepSeek when every flat asset lacks indicators. Mechanical exits already
            # ran above this point, so open positions stay protected without an LLM call.
            skip_llm = bool(CONFIG.get("skip_llm_without_market_data", True))
            sections_by_asset = {s.get("asset"): s for s in market_sections}
            usable_flat = [
                a for a in flat_assets
                if _section_has_usable_ta(sections_by_asset.get(a))
            ]
            # Agent-managed exits still need the model when it is allowed to close positions.
            needs_agent_exit_review = bool(agent_manage_exits and assets_with_positions_set)
            will_call_llm = not (skip_llm and not usable_flat and not needs_agent_exit_review)
            if will_call_llm:
                add_event(f"🧠 LLM analyzing {len(decision_assets)} assets this cycle: {', '.join(decision_assets)}")
            if skip_llm and not usable_flat and not needs_agent_exit_review:
                missing = [a for a in flat_assets if a not in sections_by_asset or not _section_has_usable_ta(sections_by_asset.get(a))]
                add_event(
                    f"💤 Skipping DeepSeek this cycle — no usable market data for flat assets"
                    f"{(' (' + ', '.join(missing) + ')') if missing else ''}. "
                    f"Mechanical TP/SL still active."
                )
                outputs = {
                    "reasoning": "Skipped DeepSeek — no usable market data for flat assets; mechanical TP/SL still active.",
                    "trade_decisions": [
                        {
                            "asset": a,
                            "action": "hold",
                            "allocation_usd": 0,
                            "tp_price": None,
                            "sl_price": None,
                            "exit_plan": "",
                            "rationale": "no usable market data",
                        }
                        for a in decision_assets
                    ],
                }
            else:
                current_strategy_name = agent.get_name()
                if not hasattr(run_loop, '_last_strategy_name'):
                    run_loop._last_strategy_name = None
                if strategy_mode == "MANUAL":
                    if not hasattr(run_loop, '_last_strategy_log') or run_loop._last_strategy_log != invocation_count:
                        logging.info(f"📊 Using strategy: {current_strategy_name}")
                        run_loop._last_strategy_log = invocation_count
                elif strategy_mode == "AUTO":
                    if run_loop._last_strategy_name and run_loop._last_strategy_name != current_strategy_name:
                        logging.warning(f"🔄 Strategy changed: {run_loop._last_strategy_name} → {current_strategy_name}")
                run_loop._last_strategy_name = current_strategy_name

                try:
                    outputs = agent.decide_trade(decision_assets, context)
                    if not isinstance(outputs, dict):
                        add_event(f"Invalid output format (expected dict): {outputs}")
                        outputs = {}
                except Exception as e:
                    import traceback
                    add_event(f"Agent error: {e}")
                    add_event(f"Traceback: {traceback.format_exc()}")
                    outputs = {}

                # Retry once on failure/parse error with a stricter instruction prefix
                if _is_failed_outputs(outputs):
                    add_event("Retrying LLM once due to invalid/parse-error output")
                    context_retry_payload = OrderedDict([
                        ("retry_instruction", "Return ONLY the JSON array per schema with no prose."),
                        ("original_context", context_payload)
                    ])
                    context_retry = json.dumps(context_retry_payload, default=json_default)
                    try:
                        outputs = agent.decide_trade(decision_assets, context_retry)
                        if not isinstance(outputs, dict):
                            add_event(f"Retry invalid format: {outputs}")
                            outputs = {}
                    except Exception as e:
                        import traceback
                        add_event(f"Retry agent error: {e}")
                        add_event(f"Retry traceback: {traceback.format_exc()}")
                        outputs = {}

            reasoning_text = outputs.get("reasoning", "") if isinstance(outputs, dict) else ""
            if reasoning_text:
                add_event(f"LLM reasoning summary: {reasoning_text}")
            if isinstance(outputs, dict):
                try:
                    decisions = outputs.get("trade_decisions", [])
                    action_counts = {}
                    proposals = []
                    for d in decisions if isinstance(decisions, list) else []:
                        if not isinstance(d, dict):
                            continue
                        action = (d.get("action") if isinstance(d, dict) else None) or "unknown"
                        action_counts[action] = action_counts.get(action, 0) + 1
                        proposals.append({
                            "asset": d.get("asset"),
                            "action": action,
                            "allocation_usd": d.get("allocation_usd"),
                            "rationale": d.get("rationale") or "",
                            "tp_price": d.get("tp_price"),
                            "sl_price": d.get("sl_price"),
                        })
                    add_event(f"🧾 LLM decision summary: {action_counts} across {len(decision_assets)} analyzed assets")
                    await webhook_notifier.notify_agent_proposal(
                        analyzed_assets=decision_assets,
                        proposals=proposals,
                        action_counts=action_counts,
                    )
                except Exception as e:
                    logging.debug(f"Decision summary notification failed: {e}")

            # Check if strategy changed (for auto mode) and adjust existing positions accordingly
            if strategy_mode == "AUTO" and hasattr(run_loop, '_last_strategy_name') and run_loop._last_strategy_name:
                previous_strategy_name = run_loop._last_strategy_name
                if previous_strategy_name != current_strategy_name:
                    add_event(f"🔄 Strategy changed: {previous_strategy_name} → {current_strategy_name}")
                    # Adjust TP/SL for existing positions based on new strategy
                    for pos in positions:
                        asset = pos.get('symbol') or pos.get('coin')
                        if not asset:
                            continue
                        position_size = abs(float(pos.get('quantity', 0) or pos.get('szi', 0)))
                        if position_size <= 0:
                            continue
                        
                        # Find entry price and position info
                        entry_price = pos.get('entry_price') or pos.get('entryPx') or pos.get('entryPrice')
                        if not entry_price:
                            continue
                        
                        current_price = asset_prices.get(asset, 0)
                        if not current_price:
                            try:
                                current_price = await _ex_for(asset).get_current_price(asset)
                                asset_prices[asset] = current_price
                            except Exception:
                                continue
                        
                        raw_size = float(pos.get('szi', 0) or pos.get('quantity', 0))
                        is_long = raw_size > 0
                        
                        # Determine if switching TO scalping (need 5% TP) or TO trend (indicator-based)
                        is_scalping_now = "scalping" in current_strategy_name.lower()
                        was_scalping_before = "scalping" in previous_strategy_name.lower()
                        
                        # If switching TO scalping: Adjust TP to 5% if position is profitable
                        if is_scalping_now and not was_scalping_before:
                            pnl_pct = ((current_price - entry_price) / entry_price * 100) if is_long else ((entry_price - current_price) / entry_price * 100)
                            if pnl_pct > 0:  # Position is profitable
                                # Calculate new 5% TP
                                if is_long:
                                    new_tp = entry_price * 1.05
                                else:
                                    new_tp = entry_price * 0.95
                                
                                add_event(f"📊 Strategy switch to SCALPING: Adjusting TP for {asset} to 5% (${new_tp:.4f})")
                                try:
                                    await _ex_for(asset).cancel_all_orders(asset)
                                    await _ex_for(asset).place_take_profit(asset, is_long, position_size, new_tp)
                                    add_event(f"✅ Updated TP for {asset} to 5% (scalping strategy)")
                                except Exception as e:
                                    add_event(f"⚠️  Could not update TP for {asset}: {e}")
                        
                        # If switching TO trend: Let new strategy manage with indicator-based exits
                        elif not is_scalping_now and was_scalping_before:
                            add_event(f"📊 Strategy switch to TREND: {asset} will be managed with indicator-based exits")
                            # New strategy will handle exits based on indicators
                            # Cancel existing TP orders and let trend strategy set new ones based on indicators
                            try:
                                await _ex_for(asset).cancel_all_orders(asset)
                                add_event(f"🧹 Cancelled old TP/SL orders for {asset} - trend strategy will set new ones based on indicators")
                            except Exception as e:
                                add_event(f"⚠️  Could not cancel orders for {asset}: {e}")

            # PROTECT GAINS: Check TP/SL for ALL open positions and close immediately if reached
            # This protects against missed TP/SL orders and ensures gains are locked in
            for pos in positions:
                asset = pos.get('symbol') or pos.get('coin')
                if not asset:
                    continue
                    
                position_size = abs(float(pos.get('quantity', 0) or pos.get('szi', 0)))
                if position_size <= 0:
                    continue
                
                # Determine if long or short
                raw_size = float(pos.get('szi', 0) or pos.get('quantity', 0))
                is_long = raw_size > 0
                
                current_price = asset_prices.get(asset, 0)
                if not current_price or current_price <= 0:
                    try:
                        current_price = await _ex_for(asset).get_current_price(asset)
                        asset_prices[asset] = current_price
                    except Exception:
                        continue
                
                # Get TP/SL from diary entries (most reliable source)
                tp_price = None
                sl_price = None
                entry_price = pos.get('entry_price') or pos.get('entryPx') or pos.get('entryPrice')
                opened_at_str = None
                peak_profit_pct = None  # Track peak profit for drawdown protection
                
                # Find active trade record for additional tracking
                active_trade_record = None
                for tr in active_trades:
                    if tr.get('asset') == asset:
                        active_trade_record = tr
                        opened_at_str = tr.get('opened_at')
                        peak_profit_pct = tr.get('peak_profit_pct')  # Get stored peak profit
                        break
                
                try:
                    with open(diary_path, "r") as f:
                        lines = f.readlines()
                    for line in reversed(lines[-50:]):  # Check recent entries
                        try:
                            entry = json.loads(line)
                            if entry.get('asset') == asset and entry.get('action') in ['buy', 'sell']:
                                if not tp_price:
                                    tp_price = entry.get('tp_price')
                                if not sl_price:
                                    sl_price = entry.get('sl_price')
                                if not entry_price:
                                    entry_price = entry.get('entry_price')
                                if not opened_at_str:
                                    opened_at_str = entry.get('timestamp') or entry.get('opened_at')
                                if tp_price or sl_price:
                                    is_long = entry.get('action') == 'buy'
                                    break
                        except Exception:
                            continue
                except Exception:
                    pass
                
                # Convert to float immediately (values from JSON may be strings)
                try:
                    if entry_price:
                        entry_price = float(entry_price)
                    if tp_price:
                        tp_price = float(tp_price)
                    if sl_price:
                        sl_price = float(sl_price)
                except (ValueError, TypeError) as e:
                    logging.warning(f"Could not convert prices to float for {asset}: entry_price={entry_price}, tp_price={tp_price}, sl_price={sl_price}, error={e}")
                    if not entry_price or entry_price <= 0:
                        continue

                # TP/SL-only close mode: exchange protective orders + mechanical UI TP/SL%.
                # LLM / soft exits are skipped; UI take_profit_percent / stop_loss_percent are law.
                if not agent_manage_exits:
                    if not tp_price or not sl_price:
                        calc_tp, calc_sl = calculate_tp_sl_prices(entry_price, is_long, tp_percent, sl_percent)
                        if not tp_price:
                            tp_price = calc_tp
                            add_event(f"📊 TP/SL-only mode: calculated TP for {asset} at {tp_price:.4f}")
                        if not sl_price:
                            sl_price = calc_sl
                            add_event(f"🛡️  TP/SL-only mode: calculated SL for {asset} at {sl_price:.4f}")

                    tp_sl_only_settings = dict(trading_settings or {})
                    tp_sl_only_settings["enable_stop_loss_orders"] = True
                    protection = await _ensure_protective_orders(
                        asset=asset,
                        is_long=is_long,
                        position_size=position_size,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        trading_settings=tp_sl_only_settings,
                    )
                    tp_oid = protection.get("tp_oid")
                    sl_oid = protection.get("sl_oid")
                    if active_trade_record is not None:
                        if protection.get("tp_oid"):
                            active_trade_record["tp_oid"] = protection.get("tp_oid")
                        if protection.get("sl_oid"):
                            active_trade_record["sl_oid"] = protection.get("sl_oid")
                        if protection.get("tp_price"):
                            active_trade_record["tp_price"] = protection.get("tp_price")
                            tp_price = protection.get("tp_price") or tp_price
                        if protection.get("sl_price"):
                            active_trade_record["sl_price"] = protection.get("sl_price")
                            sl_price = protection.get("sl_price") or sl_price
                    if not tp_oid or not sl_oid:
                        add_event(
                            f"⚠️  TP/SL-only mode {asset}: protective orders incomplete "
                            f"(TP oid: {'yes' if tp_oid else 'no'}, SL oid: {'yes' if sl_oid else 'no'}). "
                            f"Bot will still enforce UI TP/SL% mechanically."
                        )

                    pnl_percent = _compute_position_roi_percent(
                        pos,
                        is_long=is_long,
                        entry_price=entry_price,
                        current_price=current_price,
                    )
                    current_strategy_name = agent.get_name()
                    is_scalping = "scalping" in current_strategy_name.lower()
                    effective_tp_percent = float(
                        scalping_tp_percent if is_scalping else tp_percent
                    )
                    effective_sl_percent = float(
                        scalping_sl_percent if is_scalping else sl_percent
                    )
                    tp_plan = _EXIT_PLANS.get(asset.upper())
                    # Prefer LIVE DB/UI tp_mode over stale per-position plan (so Coolify changes apply mid-trade).
                    tp_mode = normalize_tp_mode(
                        trading_settings.get("tp_mode")
                        or (tp_plan.tp_mode if tp_plan else None)
                        or "roi_percent"
                    )
                    take_profit_usd = None
                    if tp_plan and tp_plan.take_profit_usd is not None:
                        take_profit_usd = float(tp_plan.take_profit_usd)
                    else:
                        try:
                            raw_tp_usd = trading_settings.get("take_profit_usd")
                            take_profit_usd = float(raw_tp_usd) if raw_tp_usd not in (None, "") else None
                        except (TypeError, ValueError):
                            take_profit_usd = None

                    tp_hit = False
                    sl_hit = False
                    unrealized_pnl = float(pos.get("unrealized_pnl") or pos.get("pnl") or 0 or 0)
                    price_move_pct = None
                    if entry_price and current_price:
                        price_move_pct = (
                            ((current_price - entry_price) / entry_price) * 100.0 if is_long
                            else ((entry_price - current_price) / entry_price) * 100.0
                        )

                    if tp_mode == "usd" and take_profit_usd and take_profit_usd > 0:
                        if unrealized_pnl >= take_profit_usd:
                            tp_hit = True
                    elif tp_mode == "price_percent" and price_move_pct is not None:
                        price_tp = float(
                            (tp_plan.target_price_pct if tp_plan and tp_plan.target_price_pct else 0)
                            or effective_tp_percent
                            or 0
                        )
                        if price_tp > 0 and price_move_pct >= price_tp:
                            tp_hit = True
                    elif tp_mode == "atr_rr" and tp_plan is not None and price_move_pct is not None:
                        if price_move_pct >= float(tp_plan.target_price_pct or 0):
                            tp_hit = True
                    elif tp_mode == "roi_percent" and pnl_percent >= effective_tp_percent:
                        tp_hit = True
                    elif tp_mode not in ("usd", "price_percent", "atr_rr") and pnl_percent >= effective_tp_percent:
                        tp_hit = True

                    if pnl_percent <= -effective_sl_percent:
                        sl_hit = True

                    if tp_price:
                        try:
                            tp_price_f = float(tp_price)
                            if is_long and current_price >= tp_price_f:
                                tp_hit = True
                            elif not is_long and current_price <= tp_price_f:
                                tp_hit = True
                        except (TypeError, ValueError):
                            pass
                    if sl_price:
                        try:
                            sl_price_f = float(sl_price)
                            if is_long and current_price <= sl_price_f:
                                sl_hit = True
                            elif not is_long and current_price >= sl_price_f:
                                sl_hit = True
                        except (TypeError, ValueError):
                            pass

                    if tp_hit or sl_hit:
                        if tp_hit and tp_mode == "usd" and take_profit_usd:
                            reason = f"Take Profit (UI ${take_profit_usd:g})"
                        elif tp_hit:
                            reason = f"Take Profit (UI {effective_tp_percent:g}%)"
                        else:
                            reason = f"Stop Loss (UI {effective_sl_percent:g}%)"
                        add_event(
                            f"💰 TP/SL-only close {asset}: {pnl_percent:.2f}% "
                            f"(${unrealized_pnl:.2f}). Reason: {reason}"
                        )
                        try:
                            if is_long:
                                await _ex_for(asset).place_sell_order(
                                    asset, position_size, reduce_only=True
                                )
                            else:
                                await _ex_for(asset).place_buy_order(
                                    asset, position_size, reduce_only=True
                                )
                            await _safe_cancel_all_orders(asset, f"post-close {reason} cleanup")
                            for tr in active_trades[:]:
                                if tr.get("asset") == asset:
                                    active_trades.remove(tr)
                            with open(diary_path, "a") as f:
                                f.write(
                                    json.dumps(
                                        {
                                            "timestamp": datetime.now(timezone.utc).isoformat(),
                                            "asset": asset,
                                            "action": "close_tp" if tp_hit else "close_sl",
                                            "entry_price": entry_price,
                                            "exit_price": current_price,
                                            "tp_price": tp_price,
                                            "sl_price": sl_price,
                                            "amount": position_size,
                                            "reason": reason,
                                            "pnl": unrealized_pnl,
                                            "pnl_percent": pnl_percent,
                                        }
                                    )
                                    + "\n"
                                )
                            await _record_pair_hunter_outcome(
                                asset,
                                unrealized_pnl,
                                pnl_percent,
                                "close_tp" if tp_hit else "close_sl",
                                was_long=is_long,
                            )
                        except Exception as e:
                            add_event(f"❌ Failed TP/SL-only close for {asset}: {e}")
                    continue

                # Agent-managed mode: still keep exchange TP/SL alive every cycle
                if (not tp_price or not sl_price) and entry_price:
                    calc_tp, calc_sl = calculate_tp_sl_prices(entry_price, is_long, tp_percent, sl_percent)
                    tp_price = tp_price or calc_tp
                    sl_price = sl_price or calc_sl
                existing_tp_oid = (active_trade_record or {}).get("tp_oid")
                existing_sl_oid = (active_trade_record or {}).get("sl_oid")
                if tp_price and sl_price and (not existing_tp_oid or not existing_sl_oid):
                    try:
                        protection = await _ensure_protective_orders(
                            asset=asset,
                            is_long=is_long,
                            position_size=position_size,
                            tp_price=tp_price,
                            sl_price=sl_price,
                            trading_settings=dict(trading_settings or {}),
                        )
                        if active_trade_record is not None:
                            if protection.get("tp_oid"):
                                active_trade_record["tp_oid"] = protection.get("tp_oid")
                            if protection.get("sl_oid"):
                                active_trade_record["sl_oid"] = protection.get("sl_oid")
                            if protection.get("tp_price"):
                                active_trade_record["tp_price"] = protection.get("tp_price")
                            if protection.get("sl_price"):
                                active_trade_record["sl_price"] = protection.get("sl_price")
                    except Exception as e:
                        add_event(f"⚠️  Could not refresh protective orders for {asset}: {e}")
                
                # Calculate current PNL percentage using margin ROI (same basis as stop-loss checks)
                unrealized_pnl = float(pos.get('unrealized_pnl') or pos.get('pnl') or 0 or 0)
                if not entry_price or entry_price <= 0:
                    continue
                
                pnl_percent = _compute_position_roi_percent(
                    pos,
                    is_long=is_long,
                    entry_price=entry_price,
                    current_price=current_price,
                )
                
                # Update peak profit tracking for drawdown protection
                if peak_profit_pct is None or pnl_percent > peak_profit_pct:
                    peak_profit_pct = pnl_percent
                    # Store updated peak in active_trades
                    if active_trade_record:
                        active_trade_record['peak_profit_pct'] = peak_profit_pct
                    else:
                        # Create or update active trade record
                        for tr in active_trades:
                            if tr.get('asset') == asset:
                                tr['peak_profit_pct'] = peak_profit_pct
                                break
                        else:
                            # No record found, create one
                            active_trades.append({
                                'asset': asset,
                                'is_long': is_long,
                                'entry_price': entry_price,
                                'opened_at': opened_at_str or datetime.now(timezone.utc).isoformat(),
                                'peak_profit_pct': peak_profit_pct
                            })
                
                # ========================================================================
                # ADVANCED POSITION MANAGEMENT: Trailing Stop, Max Hold Time, Drawdown Protection
                # ========================================================================
                
                # 1. MAXIMUM HOLD TIME CHECK
                max_hold_hours = CONFIG.get('max_position_hold_hours', 24.0) if agent_manage_exits else 0
                if opened_at_str and max_hold_hours > 0:
                    try:
                        if 'T' in opened_at_str:
                            opened_at = datetime.fromisoformat(opened_at_str.replace('Z', '+00:00'))
                        else:
                            opened_at = datetime.fromisoformat(opened_at_str)
                        # Ensure opened_at is timezone-aware (UTC)
                        if opened_at.tzinfo is None:
                            opened_at = opened_at.replace(tzinfo=timezone.utc)
                        hours_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600
                        
                        if hours_open >= max_hold_hours:
                            add_event(f"⏰ Maximum hold time reached for {asset}: {hours_open:.1f} hours (max: {max_hold_hours}h). Closing position.")
                            should_take_profit = True
                            profit_reason = f"Max hold time ({hours_open:.1f}h)"
                            # Skip other checks and close immediately
                            try:
                                close_action = "sell" if is_long else "buy"
                                if is_long:
                                    close_order = await _ex_for(asset).place_sell_order(asset, position_size, reduce_only=True)
                                else:
                                    close_order = await _ex_for(asset).place_buy_order(asset, position_size, reduce_only=True)
                                add_event(f"✅ Closed {asset} position (max hold time) via {close_action} order")
                                await _safe_cancel_all_orders(asset, "post-close max-hold cleanup")
                                
                                # Remove from active trades
                                for tr in active_trades[:]:
                                    if tr.get('asset') == asset:
                                        active_trades.remove(tr)
                                
                                # Log to diary
                                with open(diary_path, "a") as f:
                                    f.write(json.dumps({
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "asset": asset,
                                        "action": "close_max_hold_time",
                                        "entry_price": entry_price,
                                        "exit_price": current_price,
                                        "amount": position_size,
                                        "reason": f"Maximum hold time reached ({hours_open:.1f}h)",
                                        "pnl": unrealized_pnl
                                    }) + "\n")
                                await _record_pair_hunter_outcome(asset, unrealized_pnl, pnl_percent, "close_max_hold_time", was_long=is_long)
                            except Exception as e:
                                add_event(f"❌ Failed to close {asset} position (max hold time): {e}")
                            continue  # Skip to next position
                    except Exception as e:
                        logging.warning(f"Could not parse opened_at for {asset} max hold check: {e}")
                
                # 2. DRAWDOWN PROTECTION: Close if profit drops significantly from peak
                enable_drawdown = CONFIG.get('enable_drawdown_protection', True) if agent_manage_exits else False
                max_drawdown_pct = CONFIG.get('max_drawdown_from_peak_pct', 5.0)
                drawdown_min_peak_profit_pct = CONFIG.get('drawdown_min_peak_profit_pct', 3.0)
                drawdown_confirm_cycles = max(1, int(CONFIG.get('drawdown_confirm_cycles', 2) or 2))
                drawdown_should_close = False
                drawdown_reason = ""
                if enable_drawdown and peak_profit_pct is not None and peak_profit_pct >= drawdown_min_peak_profit_pct:
                    drawdown_from_peak = peak_profit_pct - pnl_percent
                    if drawdown_from_peak >= max_drawdown_pct:
                        drawdown_trigger_count = 1
                        if active_trade_record is not None:
                            drawdown_trigger_count = int(active_trade_record.get("drawdown_trigger_count", 0) or 0) + 1
                            active_trade_record["drawdown_trigger_count"] = drawdown_trigger_count
                        if drawdown_trigger_count >= drawdown_confirm_cycles:
                            add_event(f"📉 Drawdown protection triggered for {asset}: Peak was {peak_profit_pct:.1f}%, now {pnl_percent:.1f}% (drawdown: {drawdown_from_peak:.1f}%). Closing to protect gains.")
                            drawdown_should_close = True
                            drawdown_reason = f"Drawdown protection ({drawdown_from_peak:.1f}% from peak)"
                        else:
                            add_event(f"📉 Drawdown warning for {asset}: {drawdown_from_peak:.1f}% from peak (confirm {drawdown_trigger_count}/{drawdown_confirm_cycles})")
                    elif active_trade_record is not None:
                        active_trade_record["drawdown_trigger_count"] = 0
                elif active_trade_record is not None:
                    active_trade_record["drawdown_trigger_count"] = 0
                
                # 2b. LOSS PROTECTION: Close if position is down significantly (backup if AI doesn't close)
                loss_protection_pct = CONFIG.get('loss_protection_pct', 5.0) if agent_manage_exits else 0.0  # Close if down 5%+
                loss_protection_min_hours = CONFIG.get('loss_protection_min_hours', 1.0)
                loss_protection_confirm_cycles = max(1, int(CONFIG.get('loss_protection_confirm_cycles', 2) or 2))
                if pnl_percent <= -loss_protection_pct:
                    # Check how long it's been losing
                    if opened_at_str:
                        try:
                            if 'T' in opened_at_str:
                                opened_at = datetime.fromisoformat(opened_at_str.replace('Z', '+00:00'))
                            else:
                                opened_at = datetime.fromisoformat(opened_at_str)
                            # Ensure opened_at is timezone-aware (UTC)
                            if opened_at.tzinfo is None:
                                opened_at = opened_at.replace(tzinfo=timezone.utc)
                            hours_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600
                            
                            # Only auto-close if loss persists for configured time and confirmation cycles
                            if hours_open >= loss_protection_min_hours:
                                loss_trigger_count = 1
                                if active_trade_record is not None:
                                    loss_trigger_count = int(active_trade_record.get("loss_trigger_count", 0) or 0) + 1
                                    active_trade_record["loss_trigger_count"] = loss_trigger_count
                                if loss_trigger_count >= loss_protection_confirm_cycles:
                                    add_event(f"⚠️  Loss protection triggered for {asset}: Down {abs(pnl_percent):.1f}% after {hours_open:.1f} hours. System closing as backup.")
                                    drawdown_should_close = True
                                    drawdown_reason = f"Loss protection (down {abs(pnl_percent):.1f}% after {hours_open:.1f}h)"
                                else:
                                    add_event(f"⚠️  Loss warning for {asset}: Down {abs(pnl_percent):.1f}% (confirm {loss_trigger_count}/{loss_protection_confirm_cycles})")
                        except Exception as e:
                            logging.warning(f"Could not parse opened_at for {asset} loss protection: {e}")
                elif active_trade_record is not None:
                    active_trade_record["loss_trigger_count"] = 0
                
                # 3. TRAILING STOP LOSS: Move SL up as profit increases
                enable_trailing = CONFIG.get('enable_trailing_stop', True)
                trailing_activation_pct = CONFIG.get('trailing_stop_activation_pct', 5.0)
                trailing_distance_pct = CONFIG.get('trailing_stop_distance_pct', 3.0)

                # `pnl_percent` is margin ROI while `trailing_distance_pct` is a price distance.
                # Mixing them places the stop further from entry as profit grows. In ATR mode both
                # sides are expressed in R (one R = the stop distance) so the units agree.
                trail_plan = _EXIT_PLANS.get(asset.upper())
                trailing_profit_metric = pnl_percent
                if trail_plan is not None and trail_plan.source == "atr" and entry_price and current_price:
                    price_move_pct = (
                        ((current_price - entry_price) / entry_price) * 100.0 if is_long
                        else ((entry_price - current_price) / entry_price) * 100.0
                    )
                    r_multiple = price_move_pct / trail_plan.stop_price_pct
                    trailing_profit_metric = r_multiple
                    trailing_activation_pct = float(trading_settings.get("trailing_stop_activation_r", 1.0) or 1.0)
                    trailing_distance_pct = (
                        float(trading_settings.get("trailing_stop_distance_r", 1.0) or 1.0)
                        * trail_plan.stop_price_pct
                    )

                    # Scale out at the configured R multiples. Banking part of the move is what
                    # turns a trade that stalls after 1R into a small win instead of a full loss.
                    if trading_settings.get("enable_profit_ladder", True):
                        fill = profit_ladder.next_fill(
                            asset, r_multiple, position_size, trading_settings
                        )
                        if fill:
                            close_size, rung, remaining_pct = fill
                            try:
                                if is_long:
                                    await _ex_for(asset).place_sell_order(asset, close_size, reduce_only=True)
                                else:
                                    await _ex_for(asset).place_buy_order(asset, close_size, reduce_only=True)
                                add_event(
                                    f"💵 Scaled out of {asset}: closed {rung.size_pct:.0f}% "
                                    f"({close_size:.6f}) at {r_multiple:.2f}R, {remaining_pct:.0f}% still open"
                                )
                                position_size = max(position_size - close_size, 0.0)
                                if position_size > 0:
                                    # Protective orders were sized for the full position.
                                    await _ensure_protective_orders(
                                        asset, is_long, position_size, tp_price, sl_price, trading_settings
                                    )
                            except Exception as e:
                                add_event(f"⚠️  Scale-out failed for {asset} at {r_multiple:.2f}R: {e}")

                    # Breakeven: once the trade has paid for itself, stop letting it round-trip.
                    # After a rung fills this is free — the banked profit already covers the risk.
                    be_after_fill = (
                        trading_settings.get("breakeven_after_first_fill", True)
                        and profit_ladder.has_taken_profit(asset)
                    )
                    if trading_settings.get("enable_breakeven_stop", True) or be_after_fill:
                        be_trigger = float(trading_settings.get("breakeven_trigger_r", 1.0) or 1.0)
                        if r_multiple >= be_trigger or be_after_fill:
                            be_price = float(entry_price)
                            improves = (be_price > sl_price) if is_long else (be_price < sl_price)
                            if not sl_price or improves:
                                sl_price = be_price
                                add_event(
                                    f"🛡️  Breakeven stop for {asset}: SL moved to entry ${be_price:.4f} "
                                    f"({r_multiple:.2f}R in profit)"
                                )

                if enable_trailing and trailing_profit_metric >= trailing_activation_pct:
                    # Calculate new trailing stop price
                    if is_long:
                        new_sl_price = current_price * (1 - trailing_distance_pct / 100)
                        # Only update if new SL is higher than current SL (or no SL set)
                        if not sl_price or new_sl_price > sl_price:
                            old_sl_price = sl_price
                            sl_price = new_sl_price
                            add_event(f"📈 Trailing stop updated for {asset}: SL moved to ${sl_price:.4f} (profit: {pnl_percent:.1f}%)")
                            
                            # Update SL order on exchange (if enabled)
                            if enable_stop_loss_orders:
                                try:
                                    # Cancel old SL order if exists
                                    if active_trade_record and active_trade_record.get('sl_oid'):
                                        try:
                                            await _ex_for(asset).cancel_order(asset, active_trade_record.get('sl_oid'))
                                        except Exception:
                                            pass
                                    
                                    # Place new trailing SL order
                                    sl_order = await _ex_for(asset).place_stop_loss(asset, is_long, position_size, sl_price)
                                    sl_oids = _ex_for(asset).extract_oids(sl_order) if hasattr(_ex_for(asset), 'extract_oids') else []
                                    sl_oid = sl_oids[0] if sl_oids else None
                                    
                                    # Update active trade record
                                    if active_trade_record:
                                        active_trade_record['sl_oid'] = sl_oid
                                        active_trade_record['sl_price'] = sl_price
                                    
                                    add_event(f"✅ Trailing SL order placed for {asset} at ${sl_price:.4f}")
                                except Exception as e:
                                    add_event(f"⚠️  Could not update trailing SL for {asset}: {e}")
                            else:
                                add_event(f"📍 Trailing SL updated for {asset} (exchange SL orders disabled)")
                    else:  # short position
                        new_sl_price = current_price * (1 + trailing_distance_pct / 100)
                        # Only update if new SL is lower than current SL (or no SL set)
                        if not sl_price or new_sl_price < sl_price:
                            old_sl_price = sl_price
                            sl_price = new_sl_price
                            add_event(f"📈 Trailing stop updated for {asset}: SL moved to ${sl_price:.4f} (profit: {pnl_percent:.1f}%)")
                            
                            # Update SL order on exchange (if enabled)
                            if enable_stop_loss_orders:
                                try:
                                    # Cancel old SL order if exists
                                    if active_trade_record and active_trade_record.get('sl_oid'):
                                        try:
                                            await _ex_for(asset).cancel_order(asset, active_trade_record.get('sl_oid'))
                                        except Exception:
                                            pass
                                    
                                    # Place new trailing SL order
                                    sl_order = await _ex_for(asset).place_stop_loss(asset, is_long, position_size, sl_price)
                                    sl_oids = _ex_for(asset).extract_oids(sl_order) if hasattr(_ex_for(asset), 'extract_oids') else []
                                    sl_oid = sl_oids[0] if sl_oids else None
                                    
                                    # Update active trade record
                                    if active_trade_record:
                                        active_trade_record['sl_oid'] = sl_oid
                                        active_trade_record['sl_price'] = sl_price
                                    
                                    add_event(f"✅ Trailing SL order placed for {asset} at ${sl_price:.4f}")
                                except Exception as e:
                                    add_event(f"⚠️  Could not update trailing SL for {asset}: {e}")
                            else:
                                add_event(f"📍 Trailing SL updated for {asset} (exchange SL orders disabled)")
                
                # SMART PROFIT-TAKING: Adjust TP dynamically based on current profit
                # Consider fees (~0.04% per entry/exit = ~0.08% round trip)
                # Take profits at reasonable levels, don't be too greedy
                should_take_profit = drawdown_should_close if agent_manage_exits else False  # Start with drawdown flag only when exits enabled
                profit_reason = drawdown_reason if drawdown_should_close else ""
                
                # Check if this is a scalping trade (TP around 5%)
                is_scalping_trade = False
                if tp_price and entry_price:
                    # Ensure tp_price and entry_price are floats (may come from JSON as strings)
                    try:
                        tp_price_float = float(tp_price) if tp_price else None
                        entry_price_float = float(entry_price) if entry_price else None
                        if tp_price_float and entry_price_float:
                            if is_long:
                                tp_percent_from_entry = ((tp_price_float - entry_price_float) / entry_price_float) * 100
                            else:
                                tp_percent_from_entry = ((entry_price_float - tp_price_float) / entry_price_float) * 100
                            # Scalping trades: TP around 7% (6-8% range matches SMART_PROFIT_TIER1_PCT)
                            is_scalping_trade = 6.0 <= tp_percent_from_entry <= 8.0
                    except (ValueError, TypeError) as e:
                        logging.debug(f"Could not calculate TP percent for {asset}: tp_price={tp_price}, entry_price={entry_price}, error={e}")
                        is_scalping_trade = False
                
                # PROFIT MILESTONE ALERTS: Notify as we approach targets
                tier1_profit_pct = CONFIG.get('smart_profit_tier1_pct', 7)
                if is_scalping_trade and pnl_percent >= 5.0 and pnl_percent < tier1_profit_pct:
                    # Check if we already notified for this milestone
                    if active_trade_record and not active_trade_record.get('milestone_5pct_notified'):
                        try:
                            unrealized_pnl = float(pos.get('unrealized_pnl', 0) or pos.get('unrealizedPnl', 0) or 0)
                            await webhook_notifier.notify_profit_milestone(
                                asset=asset,
                                pnl_percent=pnl_percent,
                                pnl_usd=unrealized_pnl,
                                milestone="Approaching 7% target"
                            )
                            active_trade_record['milestone_5pct_notified'] = True
                            add_event(f"📈 {asset} at +{pnl_percent:.1f}% - Approaching 7% profit target!")
                        except Exception as e:
                            logging.debug(f"Milestone notification failed: {e}")
                
                if agent_manage_exits:
                    # SCALPING STRATEGY: Close immediately at tier1 profit % (default 7%)
                    if is_scalping_trade and pnl_percent >= tier1_profit_pct:
                        should_take_profit = True
                        profit_reason = f"Scalping target reached ({pnl_percent:.1f}%) - closing immediately"
                        add_event(f"🎯 Scalping trade {asset}: Reached {tier1_profit_pct}% target ({pnl_percent:.1f}%), closing immediately")
                    
                    # TREND STRATEGY: Use higher thresholds, let indicators guide exits
                    elif not is_scalping_trade:
                        if pnl_percent >= 15.0:  # Up 15%+ - take profits immediately
                            should_take_profit = True
                            profit_reason = "Strong profit (15%+) - locking in gains"
                        elif pnl_percent >= 10.0:  # Up 10-15% - take profits if TP too high
                            # Check if TP is more than 20% away - if so, take profit now
                            if tp_price:
                                tp_percent = 0.0
                                try:
                                    tp_price_float = float(tp_price) if tp_price else None
                                    entry_price_float = float(entry_price) if entry_price else None
                                    if tp_price_float and entry_price_float:
                                        if is_long:
                                            tp_percent = ((tp_price_float - entry_price_float) / entry_price_float) * 100
                                        else:
                                            tp_percent = ((entry_price_float - tp_price_float) / entry_price_float) * 100
                                except (ValueError, TypeError) as e:
                                    logging.debug(f"Could not calculate TP percent for {asset} (trend check): tp_price={tp_price}, entry_price={entry_price}, error={e}")
                                    tp_percent = 0.0
                                
                                if tp_percent > 20.0:  # TP is more than 20% away
                                    should_take_profit = True
                                    profit_reason = f"Profit at {pnl_percent:.1f}% - TP too high ({tp_percent:.1f}%), taking profit"

                    # Indicator / exit-plan check (previously defined but never called)
                    if not should_take_profit and active_trade_record:
                        try:
                            if await check_exit_condition(active_trade_record, taapi, hyperliquid):
                                should_take_profit = True
                                profit_reason = f"Exit plan triggered: {active_trade_record.get('exit_plan', '')}"
                                add_event(f"📉 Exit plan triggered for {asset}: {active_trade_record.get('exit_plan', '')}")
                        except Exception as e:
                            logging.debug(f"check_exit_condition failed for {asset}: {e}")
                
                # Check TP/SL conditions
                tp_hit = False
                sl_hit = False
                
                # Determine which strategy is active for this position
                current_strategy_name = agent.get_name()
                is_scalping = "scalping" in current_strategy_name.lower()
                
                # Get TP/SL thresholds (scalping or regular)
                effective_tp_percent = scalping_tp_percent if is_scalping else tp_percent
                effective_sl_percent = scalping_sl_percent if is_scalping else sl_percent

                # Keep margin ROI for take-profit checks; ATR only rewrites the STOP comparison.
                margin_roi_percent = float(pnl_percent) if pnl_percent is not None else 0.0
                tp_plan = _EXIT_PLANS.get(asset.upper())
                # Prefer LIVE DB/UI tp_mode over stale per-position plan (so Coolify changes apply mid-trade).
                tp_mode = normalize_tp_mode(
                    trading_settings.get("tp_mode")
                    or (tp_plan.tp_mode if tp_plan else None)
                    or "roi_percent"
                )
                take_profit_usd = None
                if tp_plan and tp_plan.take_profit_usd is not None:
                    take_profit_usd = float(tp_plan.take_profit_usd)
                else:
                    try:
                        raw_tp_usd = trading_settings.get("take_profit_usd")
                        take_profit_usd = float(raw_tp_usd) if raw_tp_usd not in (None, "") else None
                    except (TypeError, ValueError):
                        take_profit_usd = None

                # ATR mode for STOP only: compare raw price move against stop distance.
                if tp_plan is not None and tp_plan.source == "atr" and entry_price and current_price:
                    effective_sl_percent = tp_plan.stop_price_pct

                # Always respect configured take-profit mechanically (agent and TP/SL-only modes).
                # tp_mode=price_percent → price move vs TAKE_PROFIT_PERCENT (5 = +5% of entry)
                # tp_mode=roi_percent   → margin ROI vs TAKE_PROFIT_PERCENT
                # tp_mode=usd           → unrealized $ vs TAKE_PROFIT_USD
                # tp_mode=atr_rr        → price move vs ATR R-multiple target (often far)
                price_move_pct = None
                if entry_price and current_price:
                    price_move_pct = (
                        ((current_price - entry_price) / entry_price) * 100.0 if is_long
                        else ((entry_price - current_price) / entry_price) * 100.0
                    )

                if tp_mode == "usd" and take_profit_usd and take_profit_usd > 0 and unrealized_pnl >= take_profit_usd:
                    tp_hit = True
                    should_take_profit = True
                    profit_reason = f"Take Profit (USD): ${unrealized_pnl:.2f} >= ${take_profit_usd:g}"
                    add_event(
                        f"🎯 TAKE PROFIT triggered for {asset}: ${unrealized_pnl:.2f} >= ${take_profit_usd:g}"
                    )
                elif tp_mode == "price_percent" and price_move_pct is not None:
                    # Prefer live plan target; fall back to UI take_profit_percent.
                    price_tp = float(
                        (tp_plan.target_price_pct if tp_plan and tp_plan.target_price_pct else 0)
                        or effective_tp_percent
                        or 0
                    )
                    if price_tp > 0 and price_move_pct >= price_tp:
                        tp_hit = True
                        should_take_profit = True
                        profit_reason = f"Take Profit (price %): {price_move_pct:.2f}% >= {price_tp:g}%"
                        add_event(
                            f"🎯 TAKE PROFIT triggered for {asset}: price +{price_move_pct:.2f}% >= {price_tp:g}%"
                        )
                elif tp_mode == "atr_rr" and tp_plan is not None and price_move_pct is not None:
                    atr_tp = float(tp_plan.target_price_pct or 0)
                    if atr_tp > 0 and price_move_pct >= atr_tp:
                        tp_hit = True
                        should_take_profit = True
                        profit_reason = f"Take Profit (ATR R:R): {price_move_pct:.2f}% >= {atr_tp:.2f}% price"
                        add_event(
                            f"🎯 TAKE PROFIT triggered for {asset}: price move {price_move_pct:.2f}% >= ATR target {atr_tp:.2f}%"
                        )
                elif tp_mode == "roi_percent" and margin_roi_percent >= effective_tp_percent:
                    tp_hit = True
                    should_take_profit = True
                    profit_reason = f"Take Profit (ROI%): {margin_roi_percent:.2f}% >= {effective_tp_percent}%"
                    add_event(
                        f"🎯 TAKE PROFIT triggered for {asset}: {margin_roi_percent:.2f}% ROI >= {effective_tp_percent}%"
                    )
                elif tp_mode not in ("usd", "price_percent", "atr_rr") and margin_roi_percent >= effective_tp_percent:
                    # Legacy fallback
                    tp_hit = True
                    should_take_profit = True
                    profit_reason = f"Take Profit (ROI%): {margin_roi_percent:.2f}% >= {effective_tp_percent}%"
                elif agent_manage_exits and take_profit_strict_enforcement and pnl_percent is not None:
                    # Kept for logging clarity when strict flag is on but TP not yet reached
                    pass
                
                # Check TP price hit
                if tp_price:
                    tp_price = float(tp_price)
                    if is_long:
                        tp_hit = current_price >= tp_price or tp_hit
                    else:  # short
                        tp_hit = current_price <= tp_price or tp_hit
                
                # Check SL price hit (already handled above, but check here too for redundancy)
                if sl_price:
                    sl_price = float(sl_price)
                    if is_long:
                        sl_hit = current_price <= sl_price
                    else:  # short
                        sl_hit = current_price >= sl_price
                
                if tp_hit or sl_hit:
                    should_take_profit = True
                    profit_reason = profit_reason or ("TP" if tp_hit else "SL")
                elif agent_manage_exits and not tp_price and not sl_price and not take_profit_strict_enforcement:
                    # No TP/SL found, but if up significantly, take profit (only if strict TP is not enabled)
                    if margin_roi_percent >= 10.0:
                        should_take_profit = True
                        profit_reason = f"Profit at {margin_roi_percent:.1f}% - no TP set, taking profit"
                
                if should_take_profit:
                    reason = profit_reason if profit_reason else ("TP" if tp_hit else ("SL" if sl_hit else "Smart Profit"))
                    target_price = tp_price if (tp_price and tp_hit) else (sl_price if (sl_price and sl_hit) else current_price)
                    add_event(f"💰 Taking profit for {asset}: {pnl_percent:.1f}% up (${unrealized_pnl:.2f} profit). Reason: {reason}. Locking in gains!")
                    
                    try:
                        # Close position immediately
                        close_action = "sell" if is_long else "buy"
                        if is_long:
                            close_order = await _ex_for(asset).place_sell_order(asset, position_size, reduce_only=True)
                        else:
                            close_order = await _ex_for(asset).place_buy_order(asset, position_size, reduce_only=True)
                        add_event(f"✅ Closed {asset} position ({reason}) via {close_action} order - Gains Protected!")
                        await _safe_cancel_all_orders(asset, f"post-close {reason} cleanup")
                        
                        # Send webhook notification for exit
                        try:
                            pnl_usd = (current_price - float(entry_price or current_price)) * position_size if is_long else (float(entry_price or current_price) - current_price) * position_size
                            await webhook_notifier.notify_exit(
                                asset=asset,
                                side="LONG" if is_long else "SHORT",
                                entry_price=float(entry_price or current_price),
                                exit_price=current_price,
                                pnl_percent=pnl_percent,
                                pnl_usd=pnl_usd,
                                reason=reason,
                                size=position_size
                            )
                        except Exception as e:
                            logging.debug(f"Webhook exit notification failed: {e}")
                        
                        # Remove from active trades if present
                        for tr in active_trades[:]:
                            if tr.get('asset') == asset:
                                active_trades.remove(tr)
                        
                        # Log to diary
                        with open(diary_path, "a") as f:
                            f.write(json.dumps({
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "asset": asset,
                                "action": "close_" + reason.lower(),
                                "entry_price": entry_price,
                                "exit_price": current_price,
                                "tp_price": tp_price,
                                "sl_price": sl_price,
                                "amount": position_size,
                                "reason": f"{reason} target reached - automatic close",
                                "pnl": (current_price - float(entry_price or current_price)) * position_size if is_long else (float(entry_price or current_price) - current_price) * position_size
                            }) + "\n")
                        realized_pnl = (current_price - float(entry_price or current_price)) * position_size if is_long else (float(entry_price or current_price) - current_price) * position_size
                        await _record_pair_hunter_outcome(asset, realized_pnl, pnl_percent, f"close_{reason.lower()}", was_long=is_long)
                    except Exception as e:
                        add_event(f"❌ Failed to close {asset} position: {e}")
                        import traceback
                        logging.error(f"Error closing position for {asset}: {e}\n{traceback.format_exc()}")

            # Check if trading is enabled (allows pausing new entries while still monitoring positions)
            trading_enabled = CONFIG.get("trading_enabled", True)
            if not trading_enabled:
                add_event("⏸️  TRADING PAUSED: Skipping new trade entries. Still monitoring existing positions for TP/SL.")
                logging.info("⏸️  Trading is disabled (TRADING_ENABLED=false). Monitoring positions only.")
            
            # Execute trades for each asset
            cycle_outcomes = []  # Final system results for Telegram (after filters)

            def _note_outcome(asset_name: str, final_status: str, proposed_action: str, detail: str = ""):
                cycle_outcomes.append({
                    "asset": asset_name,
                    "proposed_action": proposed_action,
                    "final_status": final_status,
                    "detail": detail,
                })

            for output in outputs.get("trade_decisions", []) if isinstance(outputs, dict) else []:
                try:
                    asset = output.get("asset")
                    if not asset or asset not in decision_assets:
                        continue
                    action = output.get("action")
                    current_price = _safe_float(asset_prices.get(asset), 0)
                    action = output["action"]
                    rationale = output.get("rationale", "")
                    if rationale:
                        add_event(f"Decision rationale for {asset}: {rationale}")
                    if action in ("buy", "sell") and current_price <= 0:
                        add_event(f"⏸️  SKIPPED {asset} {action.upper()}: no valid market price")
                        _note_outcome(asset, "skipped", action, "No valid market price")
                        continue
                    # If trading is disabled, skip new entries but allow closing existing positions
                    if action in ("buy", "sell") and not trading_enabled:
                        # Check if this is closing an existing position or opening a new one
                        existing_position = None
                        for pos in positions:
                            if pos.get('symbol') == asset and abs(float(pos.get('quantity', 0))) > 0:
                                existing_position = pos
                                break
                        
                        # Allow closing existing positions (safety), but block new entries
                        if not existing_position:
                            add_event(f"⏸️  SKIPPED {asset} {action.upper()}: Trading is paused. Only closing existing positions allowed.")
                            _note_outcome(asset, "blocked", action, "Trading paused — new entries disabled")
                            continue
                        else:
                            # This is closing an existing position - allow it for safety
                            add_event(f"✅ ALLOWED {asset} {action.upper()}: Closing existing position (trading paused but position management active)")
                    
                    if action in ("buy", "sell"):
                        is_buy = action == "buy"
                        llm_alloc_usd = _safe_float(output.get("allocation_usd"), 0.0)
                        
                        # CRITICAL: Check for existing positions and prevent unnecessary trades
                        # Find existing position for this asset
                        existing_position = None
                        existing_trade = None
                        existing_is_long = None
                        entry_price = None
                        opened_at_str = None
                        
                        for tr in active_trades:
                            if tr.get('asset') == asset:
                                existing_trade = tr
                                existing_is_long = tr.get('is_long', True)
                                entry_price = _safe_float(tr.get('entry_price'), current_price)
                                opened_at_str = tr.get('opened_at')
                                break
                        
                        for pos in positions:
                            if pos.get('symbol') == asset and abs(_safe_float(pos.get('quantity'), 0)) > 0:
                                existing_position = pos
                                # If we don't have trade record, determine direction from exchange
                                if existing_is_long is None:
                                    pos_quantity = _safe_float(pos.get('quantity'), 0)
                                    existing_is_long = pos_quantity > 0
                                
                                # Try to get opened_at from position's openTime if not in active_trades
                                if not opened_at_str:
                                    open_time = pos.get('openTime') or pos.get('openedAt')
                                    if open_time:
                                        try:
                                            if isinstance(open_time, (int, float)):
                                                # Timestamp in milliseconds
                                                opened_at_dt = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc)
                                                opened_at_str = opened_at_dt.isoformat()
                                            elif isinstance(open_time, str):
                                                # Try to parse ISO string
                                                opened_at_dt = datetime.fromisoformat(open_time.replace('Z', '+00:00'))
                                                opened_at_str = opened_at_dt.isoformat()
                                        except Exception as e:
                                            logging.debug(f"Could not parse openTime for {asset}: {e}")
                                
                                # Get entry price from position if not in trade record
                                if not entry_price or entry_price == current_price:
                                    entry_price = _safe_float(
                                        pos.get('entry_price') or pos.get('entryPrice'),
                                        current_price,
                                    )
                                
                                break
                        
                        # If we have an existing position, check what the LLM wants to do
                        if existing_position:
                            # Determine if this is adding to position (same direction) or closing/flipping (opposite direction)
                            is_same_direction = (is_buy and existing_is_long) or (not is_buy and not existing_is_long)
                            is_closing = (is_buy and not existing_is_long) or (not is_buy and existing_is_long)
                            
                            # CRITICAL: BLOCK adding to existing positions in the same direction
                            if is_same_direction:
                                add_event(f"⏸️  BLOCKED ADDING TO POSITION for {asset}: Already have {'long' if existing_is_long else 'short'} position. Only close/flip when exit conditions are met. Holding existing position.")
                                _note_outcome(
                                    asset, "blocked", action,
                                    f"Already in {'long' if existing_is_long else 'short'} — no add-on",
                                )
                                continue  # Skip this trade - hold the existing position
                            
                            # AI close: always flatten with reduce-only full size (never re-size via MARGIN_PER_POSITION)
                            if is_closing:
                                if not agent_manage_exits:
                                    add_event(f"⏸️  EXIT BLOCKED for {asset}: AGENT_MANAGE_EXITS=false (TP/SL-only close mode)")
                                    _note_outcome(asset, "blocked", action, "Exit blocked — AGENT_MANAGE_EXITS=false (TP/SL-only)")
                                    continue
                                close_size = abs(float(existing_position.get("quantity") or existing_position.get("szi") or 0))
                                if close_size <= 0:
                                    add_event(f"⚠️  EXIT skipped for {asset}: existing position size is zero")
                                    continue
                                price_move_pct = abs((current_price - entry_price) / entry_price * 100) if entry_price and entry_price > 0 else 0
                                roi_pct = _compute_position_roi_percent(
                                    existing_position,
                                    is_long=bool(existing_is_long),
                                    entry_price=entry_price,
                                    current_price=current_price,
                                )
                                pnl_usd = float(existing_position.get("unrealized_pnl") or existing_position.get("pnl") or 0 or 0)
                                add_event(
                                    f"✅ EXIT for {asset}: AI close → reduce-only flatten "
                                    f"(size={close_size}, ROI={roi_pct:.2f}%, price move={price_move_pct:.2f}%)"
                                )
                                closed = await _close_position_reduce_only(
                                    asset,
                                    bool(existing_is_long),
                                    close_size,
                                    "AI decision to close",
                                    entry_price=entry_price,
                                    current_price=current_price,
                                    pnl_usd=pnl_usd,
                                    pnl_percent=roi_pct,
                                    diary_action="close_llm_signal",
                                )
                                if closed:
                                    try:
                                        await webhook_notifier.notify_exit(
                                            asset=asset,
                                            side="LONG" if existing_is_long else "SHORT",
                                            entry_price=float(entry_price or current_price),
                                            exit_price=current_price,
                                            pnl_percent=roi_pct,
                                            pnl_usd=pnl_usd,
                                            reason="AI decision to close",
                                            size=close_size,
                                        )
                                    except Exception as e:
                                        logging.debug(f"Webhook exit notification failed: {e}")
                                    # Drop from local positions list so later logic does not re-act
                                    positions = [p for p in positions if p.get("symbol") != asset]
                                    _note_outcome(asset, "closed", action, "Flattened existing position on AI close")
                                continue

                        # New entry — three gates, cheapest first.

                        # 1. Session breakers: a daily loss limit or a losing streak pauses
                        #    new entries while leaving open positions under stop/target control.
                        asset_trading_settings = _settings_for_asset(asset, trading_settings)
                        if _looks_like_forex(asset) and not is_forex_session_open():
                            add_event(f"💱 ENTRY BLOCKED: {asset} — forex session closed")
                            _note_outcome(asset, "blocked", action, "Forex session closed")
                            continue
                        governor_ok, governor_reason = risk_governor.check_can_enter(asset_trading_settings)
                        if not governor_ok:
                            add_event(f"🚦 ENTRY PAUSED: {governor_reason}")
                            _note_outcome(asset, "blocked", action, f"Risk governor: {governor_reason}")
                            continue

                        # 2. Re-entry cooldown. Repeatedly flipping the same pair in chop was the
                        #    single largest source of realised losses.
                        entry_allowed, cooldown_reason = reentry_guard.check_entry(asset, is_buy, asset_trading_settings)
                        if not entry_allowed:
                            add_event(f"⏸️  ENTRY BLOCKED: {cooldown_reason}")
                            _note_outcome(asset, "blocked", action, cooldown_reason)
                            continue

                        # 3. Higher-timeframe agreement, so we stop buying dips in downtrends.
                        trend_ok, trend_reason = check_trend_agreement(
                            asset, is_buy, taapi.fetch_value, asset_trading_settings
                        )
                        if not trend_ok:
                            add_event(f"🧭 ENTRY BLOCKED: {trend_reason}")
                            _note_outcome(asset, "blocked", action, trend_reason)
                            continue

                        # Get per-asset leverage (from .env override or forex/crypto default)
                        asset_leverage = per_asset_leverage.get(
                            asset, int(asset_trading_settings.get("leverage") or default_leverage)
                        )
                        
                        # Get exchange max leverage for this asset
                        # Size against the venue that will take the order (IG cash ≠ OKX equity).
                        ex = _ex_for(asset)
                        if ex is None:
                            add_event(f"⏭️  Skipping entry for {asset}: no mapped venue (hard-split)")
                            continue
                        try:
                            venue_state = await ex.get_user_state()
                            available_balance = float(venue_state.get("balance") or state.get("balance", 0.0) or 0)
                        except Exception:
                            available_balance = float(state.get("balance", 0.0) or 0)
                        max_leverage = await get_max_leverage_for_asset(ex, asset)
                        
                        # Position sizing: risk mode sizes from stop distance; margin mode uses fixed MARGIN_PER_POSITION
                        margin_per_position = asset_trading_settings.get("margin_per_position")
                        sizing_mode_pre = str(asset_trading_settings.get("position_sizing_mode", "risk") or "risk").lower()

                        # Risk mode derives margin from the stop; margin mode uses fixed MARGIN_PER_POSITION.
                        if margin_per_position is None and sizing_mode_pre == "risk":
                            margin_per_position = available_balance
                        if margin_per_position is None and sizing_mode_pre == "margin":
                            margin_per_position = float(CONFIG.get("margin_per_position") or 30.0)
                        if margin_per_position is None:
                            add_event(f"❌ ERROR: MARGIN_PER_POSITION is not set and sizing mode is not Risk. Cannot place trade for {asset}.")
                            _note_outcome(asset, "blocked", action, "MARGIN_PER_POSITION not configured")
                            continue
                        
                        # Set leverage BEFORE calculating notional (for margin mode, this determines actual leverage)
                        # CRITICAL: Always use user's leverage setting (per-asset or default) for calculations
                        # Only use exchange max if user's setting exceeds it AND exchange rejects it
                        leverage_to_use = asset_leverage  # Always start with user's setting (per-asset or default)
                        
                        if hasattr(ex, 'set_leverage'):
                            try:
                                # Try to set user's requested leverage (per-asset or default)
                                await ex.set_leverage(asset, asset_leverage)
                                # Successfully set - use user's leverage
                                leverage_to_use = asset_leverage
                                if asset_leverage != default_leverage:
                                    add_event(f"✅ Set per-asset leverage for {asset} to {asset_leverage}x (from .env setting)")
                                else:
                                    add_event(f"✅ Set leverage for {asset} to {asset_leverage}x (default from settings)")
                            except Exception as e:
                                error_msg = str(e)
                                # Check if leverage was rejected (not valid)
                                if "-4028" in error_msg or "not valid" in error_msg.lower():
                                    # Exchange rejected user's setting - must use exchange max for actual trade
                                    # But warn user that their setting couldn't be applied
                                    if asset_leverage != default_leverage:
                                        add_event(f"⚠️  Per-asset leverage {asset_leverage}x not supported for {asset} (exchange max: {max_leverage}x). Exchange will use {max_leverage}x, but calculations use your setting {asset_leverage}x")
                                    else:
                                        add_event(f"⚠️  Default leverage {asset_leverage}x not supported for {asset} (exchange max: {max_leverage}x). Exchange will use {max_leverage}x, but calculations use your setting {asset_leverage}x")
                                    # Use exchange max for actual trade execution
                                    leverage_to_use = max_leverage
                                else:
                                    add_event(f"⚠️  Could not set leverage {asset_leverage}x for {asset}: {e}. Using exchange max {max_leverage}x.")
                                    leverage_to_use = max_leverage
                        else:
                            # No set_leverage method - use user's setting if within exchange limits, otherwise exchange max
                            if asset_leverage <= max_leverage:
                                leverage_to_use = asset_leverage
                            else:
                                leverage_to_use = max_leverage
                                add_event(f"⚠️  Per-asset leverage {asset_leverage}x exceeds exchange max {max_leverage}x for {asset}, using {max_leverage}x")
                        
                        # Resolve the exit plan first — in ATR mode the stop distance determines
                        # both where we exit and (in risk sizing mode) how large the position is.
                        active_exit_plan = _resolve_exit_plan(
                            asset,
                            asset_trading_settings,
                            leverage_to_use,
                            current_price,
                            asset_trading_settings.get("interval") or CONFIG.get("interval") or "15m",
                        )
                        if active_exit_plan.source == "atr":
                            add_event(
                                f"📐 {asset} STOP from ATR {active_exit_plan.atr_pct:.2f}%: "
                                f"{active_exit_plan.stop_price_pct:.2f}% of price "
                                f"(~{active_exit_plan.stop_roi_pct:.1f}% ROI at {leverage_to_use}x)"
                            )
                        if active_exit_plan.tp_mode == "usd" and active_exit_plan.take_profit_usd:
                            add_event(
                                f"🎯 {asset} TP mode=usd: lock at ${active_exit_plan.take_profit_usd:g} profit "
                                f"(stop unchanged)"
                            )
                        elif active_exit_plan.tp_mode == "price_percent":
                            add_event(
                                f"🎯 {asset} TP mode=price_percent: lock at {active_exit_plan.target_price_pct:g}% of price"
                            )
                        elif active_exit_plan.tp_mode == "roi_percent":
                            add_event(
                                f"🎯 {asset} TP mode=roi_percent: lock at {active_exit_plan.target_roi_pct:g}% margin ROI "
                                f"({active_exit_plan.target_price_pct:.3f}% of price)"
                            )
                        else:
                            add_event(
                                f"🎯 {asset} TP mode=atr_rr: target {active_exit_plan.target_price_pct:.2f}% of price "
                                f"(~{active_exit_plan.target_roi_pct:.1f}% ROI)"
                            )

                        sizing_mode = str(asset_trading_settings.get("position_sizing_mode", "risk") or "risk").lower()
                        if sizing_mode == "risk":
                            # Notional is solved from the stop distance so each trade risks the same USD.
                            alloc_usd = calculate_risk_based_allocation(
                                asset_trading_settings,
                                available_balance,
                                active_exit_plan.stop_price_pct,
                                leverage_to_use,
                            )
                            risk_usd = alloc_usd * leverage_to_use * (active_exit_plan.stop_price_pct / 100.0)
                            stop_pct = float(active_exit_plan.stop_price_pct or 0) or 1.0
                            target_pct = float(active_exit_plan.target_price_pct or 0)
                            reward_est = risk_usd * (target_pct / stop_pct) if target_pct > 0 else 0.0
                            add_event(
                                f"🎯 RISK MODE: ${alloc_usd:.2f} margin × {leverage_to_use}x = "
                                f"${alloc_usd * leverage_to_use:.2f} notional → risking ${risk_usd:.2f} "
                                f"to make ${reward_est:.2f}"
                            )
                            margin_per_position = max(float(margin_per_position or 0), alloc_usd)
                        else:
                            # MARGIN MODE: STRICTLY enforce MARGIN_PER_POSITION (never exceed user's setting)
                            alloc_usd = min(margin_per_position, available_balance)
                        
                        # STRICT VALIDATION: Margin must never exceed MARGIN_PER_POSITION
                        if alloc_usd > margin_per_position:
                            add_event(f"❌ CRITICAL ERROR: Calculated margin ${alloc_usd:.2f} exceeds MARGIN_PER_POSITION ${margin_per_position:.2f}. Skipping trade for {asset}.")
                            continue
                        
                        # Validate LLM didn't suggest exceeding margin (agent-level enforcement check)
                        if llm_alloc_usd > 0 and llm_alloc_usd > margin_per_position:
                            add_event(f"⚠️  WARNING: LLM suggested allocation ${llm_alloc_usd:.2f} exceeds MARGIN_PER_POSITION ${margin_per_position:.2f}. System enforcing ${alloc_usd:.2f} instead.")
                        
                        notional_preview = alloc_usd * leverage_to_use
                        
                        # Calculate expected profit for informational purposes only
                        from src.utils.position_sizing import calculate_profit
                        expected_profit_1_5pct = calculate_profit(alloc_usd, 1, leverage_to_use)
                        expected_profit_2pct = calculate_profit(alloc_usd, 2.0, leverage_to_use)
                        
                        mode_label = "RISK MODE" if sizing_mode == "risk" else "MARGIN MODE"
                        add_event(f"💰 {mode_label}: Using ${alloc_usd:.2f} margin with {leverage_to_use}x leverage = ${notional_preview:.2f} notional")
                        add_event(f"   📊 Expected profit: ${expected_profit_1_5pct:.2f} on 1% move, ${expected_profit_2pct:.2f} on 2% move")
                        add_event(f"   ✅ Leverage: {leverage_to_use}x (per-asset: {asset_leverage}x, default: {default_leverage}x)")
                        
                        if is_buy and alloc_usd <= 0:
                            add_event(
                                f"⏭️  Skipping buy for {asset}: risk sizing returned $0 "
                                f"(would exceed max loss or below exchange minimum)."
                            )
                            _note_outcome(asset, "blocked", action, "risk sizing returned 0")
                            continue

                        if alloc_usd <= 0:
                            # If LLM signals 'sell' with zero allocation but we have a position, close it reduce-only
                            if not is_buy and agent_manage_exits:
                                # Check if position exists
                                state_check = await _ex_for(asset).get_user_state()
                                close_size = 0.0
                                is_long_pos = True
                                close_pos = None
                                for p in state_check.get('positions', []):
                                    sym = p.get('coin') or p.get('symbol')
                                    if sym == asset:
                                        raw = float(p.get('szi') or p.get('quantity') or 0)
                                        if abs(raw) > 0:
                                            close_size = abs(raw)
                                            is_long_pos = raw > 0
                                            close_pos = p
                                            break
                                if close_size > 0:
                                    roi_pct = _compute_position_roi_percent(
                                        close_pos or {},
                                        is_long=is_long_pos,
                                        entry_price=entry_price,
                                        current_price=current_price,
                                    )
                                    pnl_usd = float((close_pos or {}).get("unrealized_pnl") or (close_pos or {}).get("pnl") or 0 or 0)
                                    await _close_position_reduce_only(
                                        asset,
                                        is_long_pos,
                                        close_size,
                                        "LLM sell with zero allocation",
                                        entry_price=entry_price,
                                        current_price=current_price,
                                        pnl_usd=pnl_usd,
                                        pnl_percent=roi_pct,
                                        diary_action="close_llm_signal",
                                    )
                                    continue
                            elif not is_buy and not agent_manage_exits:
                                add_event(f"⏸️  EXIT BLOCKED for {asset}: zero-allocation close ignored (AGENT_MANAGE_EXITS=false)")
                            add_event(f"Holding {asset}: zero/negative allocation")
                            continue
                        
                        # CRITICAL FIX: Apply leverage to convert margin to notional value before calculating contract quantity
                        # alloc_usd is the margin/capital allocated, but we need notional = margin × leverage
                        # Example: $10 margin with 10x leverage = $100 notional, then contracts = $100 / price
                        # Use leverage_to_use (the leverage that was actually set or max allowed)
                        notional_value = alloc_usd * leverage_to_use
                        amount = notional_value / current_price
                        
                        # Log position sizing details
                        if leverage_to_use != default_leverage:
                            add_event(f"📊 Position sizing: ${alloc_usd:.2f} margin × {leverage_to_use}x leverage (capped from {default_leverage}x) = ${notional_value:.2f} notional → {amount:.6f} contracts @ ${current_price:.2f}")
                        else:
                            add_event(f"📊 Position sizing: ${alloc_usd:.2f} margin × {leverage_to_use}x leverage = ${notional_value:.2f} notional → {amount:.6f} contracts @ ${current_price:.2f}")

                        ex = _ex_for(asset)
                        order = await ex.place_buy_order(asset, amount) if is_buy else await ex.place_sell_order(asset, amount)
                        
                        # Wait for order to fill and position to be established
                        await asyncio.sleep(2)  # Give exchange time to process
                        
                        # Verify position exists before placing TP/SL (on the venue that took the order)
                        state_check = await ex.get_user_state()
                        position_exists = False
                        actual_position_size = amount
                        
                        for pos in state_check.get('positions', []):
                            if pos.get('coin') == asset or pos.get('symbol') == asset:
                                pos_size = abs(float(pos.get('szi') or pos.get('quantity', 0)))
                                if pos_size > 0:
                                    position_exists = True
                                    actual_position_size = pos_size
                                    add_event(f"Position confirmed: {asset} size {actual_position_size}")
                                    # Anchor the scale-out ladder to the size actually filled.
                                    profit_ladder.register_position(asset, actual_position_size)
                                    break
                        
                        if not position_exists:
                            add_event(f"WARNING: Position for {asset} not found after order. Skipping TP/SL placement.")
                        
                        fills_check = await ex.get_recent_fills(limit=10) if hasattr(ex, 'get_recent_fills') else []
                        filled = False
                        for fc in reversed(fills_check):
                            try:
                                if (fc.get('coin') == asset or fc.get('asset') == asset):
                                    filled = True
                                    break
                            except Exception:
                                continue
                        trade_log.append({"type": action, "price": current_price, "amount": amount, "exit_plan": output["exit_plan"], "filled": filled})
                        
                        # Send webhook notification for entry
                        if position_exists and filled:
                            try:
                                await webhook_notifier.notify_entry(
                                    asset=asset,
                                    side="LONG" if is_buy else "SHORT",
                                    price=current_price,
                                    size=actual_position_size,
                                    leverage=leverage_to_use,
                                    reason=output.get("rationale", "")
                                )
                            except Exception as e:
                                logging.debug(f"Webhook entry notification failed: {e}")
                        
                        tp_oid = None
                        sl_oid = None
                        
                        # Calculate TP/SL prices if not provided by agent, or use agent's values
                        tp_price = output.get("tp_price")
                        sl_price = output.get("sl_price")
                        
                        if position_exists:
                            # Prefer volatility / tp_mode exit plan so ATR stop + ROI/$ TP stay consistent.
                            try:
                                plan_sl = active_exit_plan.stop_price(current_price, is_buy)
                                plan_tp = active_exit_plan.target_price(
                                    current_price, is_buy, quantity=actual_position_size
                                )
                            except Exception:
                                plan_sl = None
                                plan_tp = None

                            if not tp_price or not sl_price:
                                calculated_tp, calculated_sl = calculate_tp_sl_prices(
                                    current_price, is_buy, tp_percent, sl_percent
                                )
                                if not sl_price:
                                    sl_price = plan_sl if plan_sl else calculated_sl
                                    add_event(
                                        f"🛡️  Calculated SL for {asset}: {float(sl_price):.4f} "
                                        f"({active_exit_plan.source} stop {active_exit_plan.stop_price_pct:.2f}% price)"
                                    )
                                if not tp_price:
                                    if plan_tp:
                                        tp_price = plan_tp
                                        if active_exit_plan.tp_mode == "usd":
                                            add_event(
                                                f"📊 Calculated TP for {asset}: {float(tp_price):.4f} "
                                                f"(${active_exit_plan.take_profit_usd:g} USD lock)"
                                            )
                                        elif active_exit_plan.tp_mode == "price_percent":
                                            add_event(
                                                f"📊 Calculated TP for {asset}: {float(tp_price):.4f} "
                                                f"({active_exit_plan.target_price_pct:g}% of price)"
                                            )
                                        elif active_exit_plan.tp_mode == "roi_percent":
                                            add_event(
                                                f"📊 Calculated TP for {asset}: {float(tp_price):.4f} "
                                                f"({active_exit_plan.target_roi_pct:g}% margin ROI)"
                                            )
                                        else:
                                            add_event(
                                                f"📊 Calculated TP for {asset}: {float(tp_price):.4f} "
                                                f"(ATR R:R {active_exit_plan.target_price_pct:.2f}% price)"
                                            )
                                    else:
                                        tp_price = calculated_tp
                                        add_event(f"📊 Calculated TP for {asset}: {tp_price:.4f} ({tp_percent}% from entry)")
                            elif plan_sl and active_exit_plan.source == "atr":
                                # Agent may still propose a too-tight SL — keep ATR room.
                                try:
                                    if is_buy and float(sl_price) > float(plan_sl):
                                        sl_price = plan_sl
                                        add_event(f"🛡️  Widened SL to ATR stop for {asset}: {float(sl_price):.4f}")
                                    elif (not is_buy) and float(sl_price) < float(plan_sl):
                                        sl_price = plan_sl
                                        add_event(f"🛡️  Widened SL to ATR stop for {asset}: {float(sl_price):.4f}")
                                except (TypeError, ValueError):
                                    pass
                                # Always enforce configured TP (price%/ROI%/$); atr_rr also uses plan_tp.
                                if plan_tp and active_exit_plan.tp_mode in ("price_percent", "roi_percent", "usd", "atr_rr"):
                                    tp_price = plan_tp
                                    add_event(
                                        f"🎯 Enforcing configured TP ({active_exit_plan.tp_mode}) for {asset}: {float(tp_price):.4f}"
                                    )

                            # Always pull exchange SL so $ loss at stop ≤ risk_per_trade_usd.
                            try:
                                max_loss = resolve_max_loss_usd(asset_trading_settings, default=6.0)
                                capped_sl = tighten_sl_to_max_loss(
                                    current_price,
                                    is_buy,
                                    actual_position_size,
                                    sl_price,
                                    max_loss,
                                    allow_tighter_than_atr=True,
                                )
                                if capped_sl is not None and (
                                    sl_price is None or abs(float(capped_sl) - float(sl_price or 0)) > 1e-12
                                ):
                                    add_event(
                                        f"🛡️  SL for {asset} set to ${float(capped_sl):.4f} "
                                        f"(max loss ${max_loss:.2f})"
                                    )
                                    sl_price = capped_sl
                            except Exception as e:
                                logging.debug(f"USD SL tighten skipped for {asset}: {e}")

                            # Cancel existing TP/SL orders first to avoid "max stop order limit" error
                            try:
                                await _ex_for(asset).cancel_all_orders(asset)
                                add_event(f"🧹 Cancelled existing orders for {asset} before placing TP/SL")
                            except Exception as e:
                                add_event(f"⚠️  Could not cancel existing orders for {asset}: {e}")
                            
                            protection_settings = dict(trading_settings or {})
                            if not agent_manage_exits:
                                protection_settings["enable_stop_loss_orders"] = True
                            protection = await _ensure_protective_orders(
                                asset=asset,
                                is_long=is_buy,
                                position_size=actual_position_size,
                                tp_price=tp_price,
                                sl_price=sl_price,
                                trading_settings=protection_settings,
                            )
                            # Second attempt if incomplete
                            if not protection.get("protection_complete", False):
                                add_event(f"🔁 Retrying protective orders for {asset} after incomplete placement...")
                                try:
                                    await _ex_for(asset).cancel_all_orders(asset)
                                except Exception:
                                    pass
                                protection = await _ensure_protective_orders(
                                    asset=asset,
                                    is_long=is_buy,
                                    position_size=actual_position_size,
                                    tp_price=tp_price,
                                    sl_price=sl_price,
                                    trading_settings=protection_settings,
                                )

                            tp_price = protection.get("tp_price")
                            sl_price = protection.get("sl_price")
                            tp_oid = protection.get("tp_oid")
                            sl_oid = protection.get("sl_oid")

                            if tp_price and not tp_oid:
                                add_event(f"⚠️  TP for {asset} price is set but order ID is missing. Verify exchange open orders.")
                            if sl_price and trading_settings.get('enable_stop_loss_orders', True) and not sl_oid:
                                add_event(f"⚠️  SL for {asset} price is set but order ID is missing. Verify exchange open orders.")

                            # Fail closed: never leave a newly opened position without exchange protection
                            if not protection.get("protection_complete", False):
                                add_event(
                                    f"🛑 FAIL-CLOSED: Aborting unprotected {asset} position "
                                    f"(TP ok={protection.get('tp_ok')}, SL ok={protection.get('sl_ok')}). Flattening now."
                                )
                                await _close_position_reduce_only(
                                    asset,
                                    is_buy,
                                    actual_position_size,
                                    "protection_failed_abort",
                                    entry_price=current_price,
                                    current_price=current_price,
                                    pnl_usd=0,
                                    pnl_percent=0,
                                    diary_action="close_protection_failed",
                                )
                                continue
                        # Reconcile: if opposite-side position exists or TP/SL just filled, clear stale active_trades for this asset
                        for existing in active_trades[:]:
                            if existing.get('asset') == asset:
                                try:
                                    active_trades.remove(existing)
                                except ValueError:
                                    pass
                        active_trades.append({
                            "asset": asset,
                            "is_long": is_buy,
                            "amount": amount,
                            "entry_price": current_price,
                            "tp_oid": tp_oid,
                            "sl_oid": sl_oid,
                            "tp_price": tp_price,
                            "sl_price": sl_price,
                            "leverage": leverage_to_use,
                            "exit_plan": output["exit_plan"],
                            "opened_at": datetime.now(timezone.utc).isoformat(),
                            "peak_profit_pct": 0.0,
                        })
                        add_event(f"{action.upper()} {asset} amount {amount:.4f} at ~{current_price}")
                        if rationale:
                            add_event(f"Post-trade rationale for {asset}: {rationale}")
                        _note_outcome(
                            asset,
                            "filled",
                            action,
                            f"{'LONG' if is_buy else 'SHORT'} opened · size {amount:.4f} @ ~{current_price}",
                        )
                        # Write to diary after confirming fills status
                        with open(diary_path, "a") as f:
                            diary_entry = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "asset": asset,
                                "action": action,
                                "allocation_usd": alloc_usd,
                                "amount": amount,
                                "entry_price": current_price,
                                "tp_price": tp_price,
                                "tp_oid": tp_oid,
                                "sl_price": sl_price,
                                "sl_oid": sl_oid,
                                "exit_plan": output.get("exit_plan", ""),
                                "rationale": output.get("rationale", ""),
                                "reasoning": reasoning_text,  # Include full LLM reasoning from this cycle
                                "order_result": str(order),
                                "opened_at": datetime.now(timezone.utc).isoformat(),
                                "filled": filled
                            }
                            f.write(json.dumps(diary_entry) + "\n")
                    else:
                        # Hold decision - log with all fields for consistency
                        add_event(f"Hold {asset}: {output.get('rationale', '')}")
                        _note_outcome(
                            asset,
                            "held",
                            "hold",
                            (output.get("rationale") or "Stay flat")[:140],
                        )
                        # Write hold to diary with complete fields
                        with open(diary_path, "a") as f:
                            diary_entry = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "asset": asset,
                                "action": "hold",
                                "allocation_usd": output.get("allocation_usd", 0.0),  # May be 0 for holds
                                "amount": 0.0,  # No position size for holds
                                "entry_price": None,
                                "tp_price": output.get("tp_price"),  # May have TP/SL even on hold
                                "tp_oid": None,
                                "sl_price": output.get("sl_price"),
                                "sl_oid": None,
                                "exit_plan": output.get("exit_plan", ""),
                                "rationale": output.get("rationale", ""),
                                "reasoning": reasoning_text,  # Include full LLM reasoning from this cycle
                                "order_result": None,
                                "filled": False
                            }
                            f.write(json.dumps(diary_entry) + "\n")
                except Exception as e:
                    import traceback
                    add_event(f"Execution error {asset}: {e}")
                    try:
                        _note_outcome(asset, "skipped", output.get("action") if isinstance(output, dict) else "unknown", str(e)[:140])
                    except Exception:
                        pass

            # Telegram: final system outcomes for this cycle (agent proposal already sent above)
            try:
                if cycle_outcomes:
                    await webhook_notifier.notify_system_decision(cycle_outcomes)
            except Exception as e:
                logging.debug(f"System decision notification failed: {e}")

            # LLM / Pair Hunter cadence = INTERVAL (e.g. 15m).
            # Between cycles, poll TP/SL every EXIT_CHECK_SECONDS so ROI take-profit stays tight.
            decision_s = get_interval_seconds(
                str(trading_settings.get("interval") or args.interval or "15m")
            )
            exit_check_s = float(
                CONFIG.get("exit_check_seconds")
                or trading_settings.get("exit_check_seconds")
                or 20
            )
            exit_check_s = max(5.0, min(exit_check_s, float(decision_s)))
            add_event(
                f"⏱️  Next LLM cycle in {decision_s / 60.0:.1f}m — "
                f"TP/SL fast-check every {exit_check_s:.0f}s"
            )
            waited = 0.0
            while waited < decision_s:
                chunk = min(exit_check_s, decision_s - waited)
                await asyncio.sleep(chunk)
                waited += chunk
                if waited >= decision_s:
                    break
                try:
                    n_closed = await _mechanical_exit_sweep(trading_settings)
                    if n_closed:
                        add_event(f"⚡ Fast exit sweep closed {n_closed} position(s)")
                except Exception as e:
                    logging.warning(f"Fast exit sweep error: {e}")

    async def handle_diary(request):
        """Return diary entries as JSON or newline-delimited text."""
        try:
            raw = request.query.get('raw')
            download = request.query.get('download')
            if raw or download:
                if not os.path.exists(diary_path):
                    return web.Response(text="", content_type="text/plain")
                with open(diary_path, "r") as f:
                    data = f.read()
                headers = {}
                if download:
                    headers["Content-Disposition"] = f"attachment; filename=diary.jsonl"
                return web.Response(text=data, content_type="text/plain", headers=headers)
            limit = int(request.query.get('limit', '200'))
            with open(diary_path, "r") as f:
                lines = f.readlines()
            start = max(0, len(lines) - limit)
            entries = [json.loads(l) for l in lines[start:]]
            return web.json_response({"entries": entries})
        except FileNotFoundError:
            return web.json_response({"entries": []})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_logs(request):
        """Return recent log entries as JSON (structured) or plain text."""
        try:
            path = request.query.get('path', 'trading_agent.log')  # Default to main log file
            download = request.query.get('download')
            limit_param = request.query.get('limit')
            format_type = request.query.get('format', 'json')  # 'json' or 'text'
            
            if not os.path.exists(path):
                # Return empty result if log file doesn't exist yet
                if format_type == 'json':
                    return web.json_response([])
                return web.Response(text="", content_type="text/plain")
            
            with open(path, "r", encoding='utf-8') as f:
                lines = f.readlines()
            
            # Apply limit
            if limit_param:
                if limit_param.lower() == 'all' or limit_param == '-1':
                    filtered_lines = lines
                else:
                    limit = int(limit_param)
                    filtered_lines = lines[-limit:]
            else:
                filtered_lines = lines[-2000:]  # Default 2000 lines
            
            # If download requested, return as plain text file
            if download:
                headers = {"Content-Disposition": f"attachment; filename={os.path.basename(path)}"}
                return web.Response(text=''.join(filtered_lines), content_type="text/plain", headers=headers)
            
            # Return as JSON (structured) for dashboard
            if format_type == 'json':
                import re
                log_entries = []
                for line in filtered_lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Try to parse structured log format: "timestamp - LEVEL - message"
                    # Example: "2025-11-03 10:00:00,123 - INFO - Starting agent"
                    match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\s*-\s*(\w+)\s*-\s*(.+)', line)
                    if match:
                        timestamp_str, level_str, message = match.groups()
                        # Convert level string to our level enum
                        level_map = {
                            'DEBUG': 'info',
                            'INFO': 'info',
                            'WARNING': 'warning',
                            'ERROR': 'error',
                            'CRITICAL': 'error'
                        }
                        level = level_map.get(level_str.upper(), 'info')
                        
                        # Parse timestamp
                        try:
                            if ',' in timestamp_str:
                                ts_parts = timestamp_str.split(',')
                                dt = datetime.strptime(ts_parts[0], '%Y-%m-%d %H:%M:%S')
                                timestamp = dt.replace(tzinfo=timezone.utc).isoformat()
                            else:
                                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                                timestamp = dt.replace(tzinfo=timezone.utc).isoformat()
                        except:
                            timestamp = datetime.now(timezone.utc).isoformat()
                        
                        log_entries.append({
                            'id': f"log-{len(log_entries)}",
                            'level': level,
                            'message': message,
                            'timestamp': timestamp
                        })
                    else:
                        # Plain text line without structured format
                        log_entries.append({
                            'id': f"log-{len(log_entries)}",
                            'level': 'info',
                            'message': line,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                
                return web.json_response(log_entries)
            else:
                # Return as plain text
                limit = int(limit_param) if limit_param else 2000
                return web.Response(text=''.join(filtered_lines[-limit:]), content_type="text/plain")
        except Exception as e:
            logger.error(f"Error reading logs: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    def _normalize_order_type(order: dict) -> str:
        """Normalize exchange order type across Binance/Aster payload shapes."""
        if not isinstance(order, dict):
            return ""
        raw = (
            order.get("orderType")
            or order.get("type")
            or order.get("origType")
            or order.get("order_type")
            or ""
        )
        return str(raw).upper().replace("-", "_")

    def _extract_tp_sl_from_orders(open_orders, asset: str):
        """Extract best-effort TP/SL trigger prices for an asset from open reduce-only orders."""
        tp_price = None
        sl_price = None
        tp_oid = None
        sl_oid = None
        target_asset = str(asset or "").upper().replace("/USDT", "").replace("USDT", "")

        for order in open_orders or []:
            order_asset = str(order.get("coin") or order.get("asset") or order.get("symbol") or "").upper()
            order_asset = order_asset.replace("/USDT", "").replace("USDT", "")
            if order_asset != target_asset:
                continue

            order_type = _normalize_order_type(order)
            trigger = order.get("triggerPx")
            if trigger is None:
                trigger = order.get("stopPrice")
            if trigger is None:
                trigger = order.get("price")
            try:
                trigger = float(trigger) if trigger is not None else None
            except (ValueError, TypeError):
                trigger = None

            oid = order.get("oid") or order.get("orderId") or order.get("id")
            if oid is not None:
                oid = str(oid)

            if order_type.startswith("TAKE_PROFIT"):
                if trigger is not None:
                    tp_price = trigger
                if oid:
                    tp_oid = oid
            elif order_type.startswith("STOP"):
                if trigger is not None:
                    sl_price = trigger
                if oid:
                    sl_oid = oid

        return {
            "tp_price": tp_price,
            "sl_price": sl_price,
            "tp_oid": tp_oid,
            "sl_oid": sl_oid,
        }

    async def _safe_cancel_all_orders(asset: str, reason: str = ""):
        """Best-effort cancel of all open orders for an asset."""
        try:
            await _ex_for(asset).cancel_all_orders(asset)
            suffix = f" ({reason})" if reason else ""
            add_event(f"🧹 Cancelled all open orders for {asset}{suffix}")
        except Exception as e:
            suffix = f" ({reason})" if reason else ""
            add_event(f"⚠️  Could not cancel open orders for {asset}{suffix}: {e}")

    async def _ensure_protective_orders(asset: str, is_long: bool, position_size: float, tp_price, sl_price, trading_settings: dict):
        """Place and verify TP/SL orders with retries, returning confirmed values from open orders."""
        ex = _ex_for(asset)
        confirmed_tp = None
        confirmed_sl = None
        confirmed_tp_oid = None
        confirmed_sl_oid = None
        if not is_long:
            add_event(f"ℹ️  {asset} short position: both TP and SL close-orders appear as BUY reduce-only on exchange.")

        # Attempt placement first (best effort)
        if tp_price:
            try:
                tp_order = await ex.place_take_profit(asset, is_long, position_size, tp_price)
                tp_oids = ex.extract_oids(tp_order) if hasattr(ex, "extract_oids") else []
                if tp_oids:
                    confirmed_tp_oid = tp_oids[0]
                add_event(f"✅ TP placed {asset} at {float(tp_price):.4f}")
            except Exception as e:
                add_event(f"❌ Failed to place TP for {asset}: {e}")

        enable_sl_orders = bool(trading_settings.get("enable_stop_loss_orders", True))
        if sl_price and enable_sl_orders:
            try:
                sl_order = await ex.place_stop_loss(asset, is_long, position_size, sl_price)
                sl_oids = ex.extract_oids(sl_order) if hasattr(ex, "extract_oids") else []
                if sl_oids:
                    confirmed_sl_oid = sl_oids[0]
                add_event(f"✅ SL placed {asset} at {float(sl_price):.4f}")
            except Exception as e:
                add_event(f"❌ Failed to place SL for {asset}: {e}")

        # Verify via open orders and retry missing side (more attempts than before).
        for attempt in range(1, 5):
            try:
                open_orders = await ex.get_open_orders()
                extracted = _extract_tp_sl_from_orders(open_orders, asset)
                confirmed_tp = extracted.get("tp_price")
                confirmed_sl = extracted.get("sl_price")
                confirmed_tp_oid = extracted.get("tp_oid") or confirmed_tp_oid
                confirmed_sl_oid = extracted.get("sl_oid") or confirmed_sl_oid
            except Exception as e:
                add_event(f"⚠️  Could not verify TP/SL orders for {asset} (attempt {attempt}/4): {e}")
                await asyncio.sleep(1)
                continue

            missing_tp = bool(tp_price) and not (bool(confirmed_tp) or bool(confirmed_tp_oid))
            missing_sl = bool(sl_price) and enable_sl_orders and not (bool(confirmed_sl) or bool(confirmed_sl_oid))
            if not missing_tp and not missing_sl:
                break

            add_event(
                f"⚠️  Protective order check {asset} (attempt {attempt}/4): "
                f"TP {'missing' if missing_tp else 'ok'}, SL {'missing' if missing_sl else 'ok'}"
            )

            if missing_tp:
                try:
                    # Cancel leftovers then re-place to avoid max-stop / duplicate issues
                    try:
                        await ex.cancel_all_orders(asset)
                    except Exception:
                        pass
                    await ex.place_take_profit(asset, is_long, position_size, tp_price)
                    if sl_price and enable_sl_orders:
                        await ex.place_stop_loss(asset, is_long, position_size, sl_price)
                except Exception as e:
                    add_event(f"⚠️  TP retry failed for {asset}: {e}")
            elif missing_sl:
                try:
                    await ex.place_stop_loss(asset, is_long, position_size, sl_price)
                except Exception as e:
                    add_event(f"⚠️  SL retry failed for {asset}: {e}")
            await asyncio.sleep(1)

        tp_ok = (not bool(tp_price)) or bool(confirmed_tp_oid) or bool(confirmed_tp)
        sl_ok = (not bool(sl_price)) or (not enable_sl_orders) or bool(confirmed_sl_oid) or bool(confirmed_sl)
        protection_complete = tp_ok and sl_ok
        if not protection_complete:
            add_event(
                f"❌ Protective orders incomplete for {asset}: "
                f"TP={'ok' if tp_ok else 'MISSING'}, SL={'ok' if sl_ok else 'MISSING'}"
            )

        return {
            "tp_price": confirmed_tp if confirmed_tp is not None else (float(tp_price) if tp_price else None),
            "sl_price": confirmed_sl if confirmed_sl is not None else (float(sl_price) if sl_price else None),
            "tp_oid": confirmed_tp_oid,
            "sl_oid": confirmed_sl_oid,
            "protection_complete": protection_complete,
            "tp_ok": tp_ok,
            "sl_ok": sl_ok,
        }

    async def handle_positions(request):
        """Return current positions as JSON."""
        try:
            logging.debug("Fetching positions from exchange...")
            state = await hyperliquid.get_user_state()
            try:
                open_orders_for_positions = await hyperliquid.get_open_orders()
            except Exception as e:
                logging.warning(f"Could not fetch open orders for TP/SL enrichment: {e}")
                open_orders_for_positions = []
            positions_list = []
            
            logging.debug(f"Exchange returned {len(state.get('positions', []))} position(s)")
            
            for pos in state.get('positions', []):
                # Extract position data (works for both Aster and Binance formats)
                coin = pos.get('coin') or pos.get('symbol', '')
                szi = pos.get('szi') or pos.get('positionAmt') or pos.get('quantity', 0)
                entry_px = pos.get('entryPx') or pos.get('entryPrice') or pos.get('entry_price', 0)
                
                # Always fetch fresh current price for real-time updates
                current_px = None
                if coin:
                    try:
                        current_px = await _ex_for(coin).get_current_price(coin)
                    except Exception as e:
                        logging.warning(f"Could not fetch current price for {coin}: {e}")
                        # Fallback to markPrice from position data if fetch fails
                        current_px = pos.get('current_price') or pos.get('markPrice') or pos.get('markPrice') or pos.get('currentPrice')
                        if not current_px:
                            current_px = entry_px  # Last resort: use entry price
                
                pnl = pos.get('pnl') or pos.get('unRealizedProfit') or pos.get('unrealized_pnl', 0)
                
                # Determine side (positive = long, negative = short)
                size = float(szi) if szi else 0
                side = "long" if size > 0 else "short" if size < 0 else None
                
                if side and abs(size) > 0:  # Only include non-zero positions
                    protective_orders = _extract_tp_sl_from_orders(open_orders_for_positions, coin)
                    # ONLY use actual leverage from Binance position data (no fallback to settings/config)
                    leverage = pos.get('leverage')
                    if leverage:
                        try:
                            leverage = float(leverage)
                            logging.debug(f"Using actual leverage {leverage}x from Binance for position {coin}")
                        except (ValueError, TypeError):
                            leverage = None
                            logging.warning(f"⚠️  Could not parse leverage from Binance for {coin}, setting to None")
                    else:
                        leverage = None
                        logging.warning(f"⚠️  No leverage found in Binance position data for {coin}, setting to None")
                    
                    # Get liquidation price if available
                    liquidation_price = pos.get('liquidationPrice') or pos.get('liquidation_price') or pos.get('liqPx') or pos.get('liquidationPx')
                    
                    # Get actual initial margin from position data (more accurate than calculating)
                    # Binance API returns this as 'positionInitialMargin' or 'initialMargin'
                    initial_margin = pos.get('positionInitialMargin') or pos.get('initialMargin') or pos.get('position_initial_margin') or pos.get('initial_margin')
                    if initial_margin is None or initial_margin == 0:
                        # Fallback: calculate from notional and leverage
                        if entry_px and size and leverage:
                            notional = abs(size) * float(entry_px)
                            initial_margin = notional / leverage if leverage > 0 else None
                        else:
                            initial_margin = None
                    else:
                        initial_margin = float(initial_margin)
                    
                    # Get ROI percentage directly from Binance API
                    roi_percent = pos.get('roiPercent') or pos.get('roi')
                    if roi_percent is not None:
                        try:
                            roi_percent = float(roi_percent)
                        except (ValueError, TypeError):
                            roi_percent = None
                    
                    # Get notional value directly from Binance API
                    notional = pos.get('notional')
                    if notional is not None:
                        try:
                            notional = float(notional)
                        except (ValueError, TypeError):
                            notional = None
                    
                    # Get openTime from Binance API (timestamp in ms) - prioritize this over active_trades
                    open_time = pos.get('openTime') or pos.get('openedAt')
                    if open_time:
                        if isinstance(open_time, (int, float)):
                            # Already a timestamp in ms
                            open_time = int(open_time)
                        elif isinstance(open_time, str):
                            # Try to parse ISO string to timestamp
                            try:
                                dt = datetime.fromisoformat(open_time.replace('Z', '+00:00'))
                                open_time = int(dt.timestamp() * 1000)
                            except:
                                open_time = None
                        else:
                            open_time = None
                    else:
                        open_time = None
                    
                    # Get opened_at from active_trades as fallback (for ISO string format)
                    opened_at = None
                    if not open_time:
                        for tr in active_trades:
                            if tr.get('asset') == coin:
                                opened_at = tr.get('opened_at')
                                break
                    
                    # Use openTime to create ISO string for opened_at, or fallback to active_trades
                    if open_time:
                        opened_at = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc).isoformat()
                    elif not opened_at:
                        opened_at = datetime.now(timezone.utc).isoformat()
                    
                    positions_list.append({
                        'symbol': coin,
                        'side': side,
                        'size': abs(size),  # Always positive, side indicates direction
                        'entry_price': float(entry_px) if entry_px else 0.0,
                        'current_price': float(current_px) if current_px else 0.0,
                        'liquidation_price': float(liquidation_price) if liquidation_price else None,
                        'tp_price': protective_orders.get('tp_price'),
                        'sl_price': protective_orders.get('sl_price'),
                        'tp_oid': protective_orders.get('tp_oid'),
                        'sl_oid': protective_orders.get('sl_oid'),
                        'unrealized_pnl': float(pnl) if pnl else 0.0,
                        'realized_pnl': 0.0,  # Not available from exchange directly
                        'leverage': leverage,  # ONLY from Binance exchange, never from settings
                        'initial_margin': float(initial_margin) if initial_margin else None,  # Actual margin used
                        'roiPercent': roi_percent,  # ROI percentage from Binance API
                        'notional': notional,  # Notional value from Binance API
                        'opened_at': opened_at,  # ISO string format
                        'openTime': open_time,  # Timestamp in ms from Binance API
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    })
                    logging.debug(f"Added position: {coin} {side} {abs(size)}")
            
            logging.info(f"Returning {len(positions_list)} position(s) to client")
            return web.json_response(positions_list)
        except Exception as e:
            logging.error(f"Error getting positions: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_balances(request):
        """Return all asset balances (USDT, USDC, BTC, etc.) from the exchange."""
        try:
            logging.info("Fetching balances from exchange...")
            state = await hyperliquid.get_user_state()
            
            # Get asset balances if available (Binance returns this, Aster might not)
            asset_balances = state.get('asset_balances', [])
            
            logging.info(f"Retrieved {len(asset_balances)} asset balances from get_user_state")
            if asset_balances:
                logging.info(f"Asset balance keys: {[b.get('asset', 'N/A') for b in asset_balances[:5]]}")
            
            # If no asset_balances in state, try to construct from positions and account info
            if not asset_balances and exchange_name == "binance":
                # For Binance, we should have asset_balances from get_user_state
                # But if not, we can still return what we have
                logging.warning("No asset_balances found in state for Binance exchange")
                asset_balances = []
            
            # Format balances for frontend compatibility
            formatted_balances = []
            for balance in asset_balances:
                asset = balance.get('asset', '')
                if not asset:
                    continue
                    
                formatted_balance = {
                    'asset': asset,
                    'walletBalance': float(balance.get('walletBalance', 0) or 0),
                    'availableBalance': float(balance.get('availableBalance', 0) or 0),
                    'crossWalletBalance': float(balance.get('crossWalletBalance', 0) or 0),
                    'crossUnPnl': float(balance.get('crossUnPnl', 0) or 0),
                    'positionValue': float(balance.get('positionValue', balance.get('crossWalletBalance', 0)) or 0),
                }
                formatted_balances.append(formatted_balance)
                logging.debug(f"Formatted balance for {asset}: wallet={formatted_balance['walletBalance']}, cross={formatted_balance['crossWalletBalance']}")
            
            logging.info(f"Returning {len(formatted_balances)} formatted balances: {[b['asset'] for b in formatted_balances]}")
            
            # Calculate account value
            account_value = state.get('total_value', state.get('balance', 0))
            
            response_data = {
                "balances": formatted_balances,
                "accountValue": account_value,
                "balance": state.get('balance', 0),
                "exchange": exchange_name,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logging.info(f"Balances response: {len(formatted_balances)} assets, accountValue={account_value}")
            return web.json_response(response_data)
        except Exception as e:
            logging.error(f"Error getting balances: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({
                "error": str(e),
                "balances": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, status=500)

    async def handle_mechanical(request):
        """Mechanical scalp book: mode, demo/live network, resting order, skip reasons, scorecard."""
        try:
            return web.json_response(_mechanical_snapshot(), dumps=lambda o: json.dumps(o, default=json_default))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_hive_race(request):
        """Five mechanical demo runners plus the AYO row. No AYO keys are used."""
        try:
            from src.runners.hive_race import snapshot
            return web.json_response(
                snapshot(),
                dumps=lambda o: json.dumps(o, default=json_default),
                headers={"Cache-Control": "no-store, max-age=0"},
            )
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def handle_status(request):
        """Return API status and basic account info."""
        try:
            # Add timeout to prevent blocking
            try:
                state = await asyncio.wait_for(hyperliquid.get_user_state(), timeout=3.0)
            except asyncio.TimeoutError:
                # Return cached/fallback status if API is slow
                network, network_label = _venue_network()
                return web.json_response({
                    "connected": True,
                    "status": "online",
                    "network": network,
                    "network_label": network_label,
                    "exchange": exchange_name,
                    "decision_mode": "mechanical" if mech.is_mechanical_mode(_LIVE_SETTINGS) else "ai",
                    "balance": 0,
                    "account_value": 0,
                    "positions_count": 0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "warning": "Status check timed out, using cached data"
                })
            
            network, network_label = _venue_network()
            return web.json_response({
                "connected": True,
                "status": "online",
                "network": network,
                "network_label": network_label,
                "exchange": exchange_name,
                "decision_mode": "mechanical" if mech.is_mechanical_mode(_LIVE_SETTINGS) else "ai",
                "balance": round(state.get('balance', 0), 2),
                "account_value": round(state.get('total_value', 0), 2),
                "positions_count": len(state.get('positions', [])),
                # Surfaced so the dashboard can explain why no new trades are opening.
                "risk_governor": risk_governor.status(),
                "reentry_cooldowns": reentry_guard.active_cooldowns(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logging.error(f"Error getting status: {e}")
            return web.json_response({
                "connected": False,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, status=500)

    async def handle_pnl(request):
        """Return daily PNL data calculated from account value changes."""
        try:
            # Get current account state
            state = await hyperliquid.get_user_state()
            current_value = state.get('total_value') or state['balance'] + sum(p.get('pnl', 0) for p in state.get('positions', []))
            base_value = initial_account_value or current_value
            
            # Read diary entries to build timeline
            if not os.path.exists(diary_path):
                # Return current state only
                current_pnl = current_value - base_value
                return web.json_response([{
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "daily_pnl": current_pnl,
                    "cumulative_pnl": current_pnl
                }])
            
            with open(diary_path, "r") as f:
                lines = f.readlines()
            
            # Group entries by day
            entries_by_day = {}
            for line in lines:
                try:
                    entry = json.loads(line)
                    ts = entry.get('timestamp')
                    if ts:
                        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        day_key = dt.date().isoformat()
                        if day_key not in entries_by_day:
                            entries_by_day[day_key] = []
                        entries_by_day[day_key].append(entry)
                except Exception:
                    continue
            
            # Calculate cumulative PNL from account value progression
            # Use positions' unrealized PNL as proxy for daily changes
            result = []
            sorted_days = sorted(entries_by_day.keys())
            prev_cumulative = 0.0
            
            for day in sorted_days:
                # Get positions at that day (estimate from diary entries)
                # For simplicity, calculate from current positions and extrapolate
                day_entries = entries_by_day[day]
                
                # Calculate daily PNL estimate from entries
                daily_pnl = 0.0
                for entry in day_entries:
                    # If position was opened, estimate contribution
                    if entry.get('action') in ['buy', 'sell']:
                        # Use a simple estimate: if we have current positions, use their PNL
                        pass
                
                # Use current positions' PNL divided by number of days for rough estimate
                if not result and state.get('positions'):
                    # Estimate initial daily PNL from current positions
                    total_unrealized = sum(p.get('pnl', 0) for p in state.get('positions', []))
                    if len(sorted_days) > 0:
                        daily_pnl = total_unrealized / max(len(sorted_days), 1)
                
                cumulative = prev_cumulative + daily_pnl
                result.append({
                    "timestamp": f"{day}T00:00:00Z",
                    "daily_pnl": daily_pnl,
                    "cumulative_pnl": cumulative
                })
                prev_cumulative = cumulative
            
            # Add current state as latest entry
            current_total_pnl = current_value - base_value
            if result:
                latest = result[-1]
                # Update latest entry with actual current PNL
                result[-1] = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "daily_pnl": current_total_pnl - latest.get('cumulative_pnl', 0),
                    "cumulative_pnl": current_total_pnl
                }
            else:
                # If no history, return current state
                result.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "daily_pnl": current_total_pnl,
                    "cumulative_pnl": current_total_pnl
                })
            
            return web.json_response(result)
        except Exception as e:
            logging.error(f"Error getting PNL: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_performance(request):
        """Return performance data with closed trade statistics."""
        try:
            # Read diary entries to extract closed trades
            if not os.path.exists(diary_path):
                return web.json_response([])
            
            with open(diary_path, "r") as f:
                lines = f.readlines()
            
            # Extract closed trades from diary entries
            # Track position lifecycle: open -> close
            closed_trades = []
            position_stack = []  # Stack of opened positions (FIFO)
            
            for line_idx, line in enumerate(lines):
                try:
                    entry = json.loads(line)
                    action = entry.get('action', '').lower()
                    asset = entry.get('asset')
                    timestamp = entry.get('timestamp')
                    
                    if not asset or not timestamp:
                        continue
                    
                    # Track opened positions (entries with buy/sell action and filled=True)
                    if action in ['buy', 'sell']:
                        filled = entry.get('filled', False)
                        if filled or entry.get('entry_price'):  # Position was opened
                            position_stack.append({
                                'asset': asset,
                                'entry_price': entry.get('entry_price'),
                                'amount': entry.get('amount', 0),
                                'opened_at': timestamp,
                                'is_long': action == 'buy',
                                'entry_action': action
                            })
                    
                    # Identify closed trades
                    if (action.startswith('close_') or 
                        (entry.get('exit_price') is not None) or
                        (entry.get('reason') and 'close' in entry.get('reason', '').lower())):
                        # Find matching open position for this asset
                        matching_pos = None
                        for i, pos in enumerate(position_stack):
                            if pos['asset'] == asset:
                                matching_pos = position_stack.pop(i)
                                break
                        
                        # If no matching position found, try to use entry data
                        if not matching_pos:
                            # Look for recent entry for this asset (search backwards from current line)
                            for recent_line in reversed(lines[max(0, line_idx - 50):line_idx]):
                                try:
                                    recent_entry = json.loads(recent_line)
                                    if recent_entry.get('asset') == asset and recent_entry.get('action') in ['buy', 'sell']:
                                        matching_pos = {
                                            'entry_price': recent_entry.get('entry_price'),
                                            'amount': recent_entry.get('amount', 0),
                                            'is_long': recent_entry.get('action') == 'buy',
                                            'opened_at': recent_entry.get('timestamp')
                                        }
                                        break
                                except Exception:
                                    continue
                        
                        if matching_pos:
                            entry_price = matching_pos.get('entry_price')
                            exit_price = entry.get('exit_price') or entry.get('price', 0)
                            amount = matching_pos.get('amount', 0) or entry.get('amount', 0)
                            pnl = entry.get('pnl')
                            
                            # Calculate PNL if not provided
                            if pnl is None and entry_price and exit_price and amount:
                                is_long = matching_pos.get('is_long', True)
                                if is_long:
                                    pnl = (exit_price - entry_price) * amount
                                else:
                                    pnl = (entry_price - exit_price) * amount
                            
                            if pnl is not None:
                                closed_trades.append({
                                    "date": timestamp,
                                    "value": exit_price * amount if amount else 0,
                                    "pnl": float(pnl),
                                    "won": float(pnl) > 0,
                                    "asset": asset,
                                    "entry_price": entry_price,
                                    "exit_price": exit_price,
                                    "amount": amount
                                })
                                
                except Exception:
                    continue
            
            # Sort trades by date
            closed_trades.sort(key=lambda x: x.get('date', ''))
            
            # Calculate cumulative value over time for chart
            base_value = initial_account_value or 10000.0
            cumulative_pnl = 0.0
            result = []
            
            for trade in closed_trades:
                cumulative_pnl += trade['pnl']
                current_value = base_value + cumulative_pnl
                result.append({
                    "date": trade['date'],
                    "value": current_value,
                    "pnl": trade['pnl'],
                    "won": trade['won'],
                    "asset": trade['asset']
                })
            
            # If no closed trades, return empty array
            if not result:
                return web.json_response([])
            
            return web.json_response(result)
        except Exception as e:
            logging.error(f"Error getting performance: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_prices(request):
        """Return current prices for all trading assets."""
        try:
            prices_list = []
            
            # Get prices for all trading assets
            for asset in args.assets:
                try:
                    # Add timeout to prevent blocking
                    current_price = await asyncio.wait_for(_ex_for(asset).get_current_price(asset), timeout=2.0)
                    if current_price and current_price > 0:
                        # Calculate 24h change from price history if available
                        change_24h = 0.0
                        change_24h_percent = 0.0
                        
                        # Try to get 24h ago price from price_history if available
                        if asset in price_history and len(price_history[asset]) > 0:
                            # Get price from 24h ago (approximate)
                            now = datetime.now(timezone.utc)
                            prices_24h_ago = [
                                p for p in list(price_history[asset])
                                if (now - datetime.fromisoformat(p['t'].replace('Z', '+00:00'))).total_seconds() >= 86400 - 3600
                            ]
                            if prices_24h_ago:
                                price_24h_ago = prices_24h_ago[0].get('mid', current_price)
                                change_24h = current_price - price_24h_ago
                                if price_24h_ago > 0:
                                    change_24h_percent = (change_24h / price_24h_ago) * 100
                        
                        prices_list.append({
                            "symbol": asset,
                            "price": round(current_price, 2),
                            "change24h": round(change_24h, 2),
                            "change24hPercent": round(change_24h_percent, 2),
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                except Exception as e:
                    logging.warning(f"Failed to get price for {asset}: {e}")
                    continue
            
            return web.json_response(prices_list)
        except Exception as e:
            logging.error(f"Error getting prices: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def sync_trades_to_supabase():
        """Background task that continuously syncs trades to Supabase database."""
        if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
            logging.info("Supabase not configured - skipping trade sync")
            return
        
        # Prefer exchange.get_recent_fills (OKX/Binance/Aster) when available
        if not hasattr(exchange, "get_recent_fills"):
            logging.warning("Current exchange has no get_recent_fills — cannot sync trades")
            return
        
        try:
            from supabase import create_client, Client
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            # Prefer service_role key for server-side operations (bypasses RLS)
            # If SUPABASE_SERVICE_KEY is set, use it; otherwise use SUPABASE_KEY
            service_key = os.getenv('SUPABASE_SERVICE_KEY') or supabase_key
            
            if not service_key:
                logging.error("❌ SUPABASE_KEY or SUPABASE_SERVICE_KEY must be set")
                return
            
            supabase: Client = create_client(supabase_url, service_key)
            
            # Log which key type is being used
            if os.getenv('SUPABASE_SERVICE_KEY'):
                logging.info("🔑 Using SUPABASE_SERVICE_KEY (bypasses RLS)")
            else:
                logging.warning("⚠️ Using SUPABASE_KEY (anon key) - may fail if RLS is enabled")
                logging.warning("   Consider using SUPABASE_SERVICE_KEY for server-side operations")
        except Exception as e:
            logging.error(f"❌ Failed to initialize Supabase client: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return
        
        logging.info("🔄 Starting initial trade sync to Supabase...")
        
        while True:
            try:
                # Fetch recent fills from active exchange (OKX / Binance / etc.)
                exchange_name = getattr(exchange, "name", type(exchange).__name__)
                logging.info(f"📥 Fetching trades from {exchange_name}...")
                trades = await exchange.get_recent_fills(limit=1000)
                logging.info(f"📊 Fetched {len(trades)} trades from {exchange_name}")
                
                if not trades:
                    logging.info("No trades found, waiting 1 minute before next check...")
                    await asyncio.sleep(60)  # Wait 1 minute if no trades
                    continue
                
                # Get existing trades from Supabase to avoid duplicates
                existing_trades_set = set()
                try:
                    logging.info("🔍 Checking existing trades in Supabase...")
                    # Add retry logic for connection issues
                    for retry in range(3):
                        try:
                            result = supabase.table('trades').select('symbol, executed_at, price, size').limit(10000).execute()
                            if result.data:
                                logging.info(f"📋 Found {len(result.data)} existing trades in database")
                                for t in result.data:
                                    # Normalize timestamp for comparison
                                    exec_at = t.get('executed_at', '')
                                    if exec_at:
                                        # Remove timezone info for comparison
                                        exec_at_normalized = exec_at.split('+')[0].split('.')[0] if '+' in exec_at or '.' in exec_at else exec_at
                                        key = f"{t['symbol']}_{exec_at_normalized}_{t['price']}_{t['size']}"
                                        existing_trades_set.add(key)
                            else:
                                logging.info("📋 No existing trades in database (first sync)")
                            break  # Success, exit retry loop
                        except Exception as conn_err:
                            # Check if it's a connection error (httpx may not be available)
                            is_conn_error = (
                                isinstance(conn_err, (ConnectionError, OSError)) or
                                (httpx and isinstance(conn_err, httpx.ConnectError)) or
                                '10054' in str(conn_err) or  # Windows connection reset
                                'connection' in str(conn_err).lower() or
                                'forcibly closed' in str(conn_err).lower()
                            )
                            if is_conn_error:
                                if retry < 2:  # Retry up to 3 times
                                    wait_time = (retry + 1) * 2  # 2s, 4s, 6s
                                    logging.warning(f"Supabase connection error (attempt {retry + 1}/3): {conn_err}. Retrying in {wait_time}s...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    logging.warning(f"Could not fetch existing trades after 3 attempts: {conn_err}. Continuing without duplicate check.")
                                    break
                            else:
                                # Not a connection error, re-raise
                                raise
                except Exception as e:
                    logging.warning(f"Could not fetch existing trades: {e}. Continuing without duplicate check.")
                    logging.warning(f"Could not fetch existing trades: {e}. Continuing without duplicate check.")
                
                # Format and filter new trades
                new_trades = []
                for trade in trades:
                    symbol = str(trade.get('symbol') or trade.get('coin') or '').replace('USDT', '').replace('-SWAP', '')
                    side = str(trade.get('side', '')).lower()
                    if side not in ['buy', 'sell']:
                        side = 'buy' if side == 'buy' else 'sell'
                    
                    price = float(trade.get('price') or trade.get('px') or trade.get('fillPx') or 0)
                    size = float(trade.get('size') or trade.get('sz') or trade.get('qty') or trade.get('fillSz') or 0)
                    fee = float(trade.get('fee') or trade.get('commission') or trade.get('fee') or 0)
                    realized_pnl = trade.get('realizedPnl') or trade.get('realized_pnl') or trade.get('pnl')
                    if realized_pnl is None and isinstance(trade.get('raw'), dict):
                        realized_pnl = trade['raw'].get('fillPnl') or trade['raw'].get('pnl')
                    pnl = float(realized_pnl) if realized_pnl is not None and str(realized_pnl) != '' else None
                    
                    # Convert timestamp to ISO string
                    timestamp_ms = trade.get('time') or trade.get('timestamp') or trade.get('ts') or 0
                    try:
                        timestamp_ms = int(timestamp_ms)
                    except (TypeError, ValueError):
                        timestamp_ms = 0
                    if timestamp_ms:
                        if timestamp_ms > 1e12:  # Already in milliseconds
                            timestamp_iso = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
                        else:  # In seconds
                            timestamp_iso = datetime.fromtimestamp(timestamp_ms, tz=timezone.utc).isoformat()
                    else:
                        timestamp_iso = datetime.now(timezone.utc).isoformat()
                    
                    # Create composite key for duplicate check
                    exec_at_normalized = timestamp_iso.split('+')[0].split('.')[0] if '+' in timestamp_iso or '.' in timestamp_iso else timestamp_iso
                    key = f"{symbol}_{exec_at_normalized}_{price}_{abs(size)}"
                    
                    # Only add if not already in database
                    if key not in existing_trades_set:
                        new_trades.append({
                            'symbol': symbol,
                            'side': side,
                            'size': abs(size),
                            'price': price,
                            'fee': abs(fee),
                            'pnl': pnl,
                            'executed_at': timestamp_iso,
                            'order_id': None,
                        })
                        existing_trades_set.add(key)  # Add to set to avoid duplicates in this batch
                
                # Insert new trades in batches
                if new_trades:
                    try:
                        logging.info(f"💾 Saving {len(new_trades)} new trades to Supabase...")
                        # Insert in batches of 100 to avoid overwhelming the database
                        batch_size = 100
                        for i in range(0, len(new_trades), batch_size):
                            batch = new_trades[i:i + batch_size]
                            result = supabase.table('trades').insert(batch).execute()
                            logging.info(f"✅ Synced {len(batch)} trades to Supabase (batch {i//batch_size + 1})")
                        logging.info(f"✅ Total: Synced {len(new_trades)} new trades to Supabase")
                    except Exception as e:
                        error_msg = str(e)
                        logging.error(f"❌ Error inserting trades to Supabase: {error_msg}")
                        
                        # Provide helpful error message for RLS issues
                        if 'row-level security' in error_msg.lower() or '42501' in error_msg:
                            logging.error("")
                            logging.error("🔒 ROW LEVEL SECURITY (RLS) ERROR:")
                            logging.error("   Your Supabase table has RLS enabled, but you're using the anon key.")
                            logging.error("   Solution: Use SUPABASE_SERVICE_KEY instead of SUPABASE_KEY")
                            logging.error("")
                            logging.error("   Steps to fix:")
                            logging.error("   1. Go to Supabase Dashboard > Settings > API")
                            logging.error("   2. Copy the 'service_role' key (NOT the 'anon' key)")
                            logging.error("   3. Add to your .env file: SUPABASE_SERVICE_KEY=your_service_role_key")
                            logging.error("   4. Restart the agent")
                            logging.error("")
                        
                        import traceback
                        logging.error(traceback.format_exc())
                else:
                    logging.info("ℹ️ No new trades to sync (all trades already in database)")
                
                # Wait 5 minutes before next sync
                logging.info("⏳ Waiting 5 minutes before next sync...")
                await asyncio.sleep(300)
                
            except Exception as e:
                logging.error(f"Error in trade sync loop: {e}")
                import traceback
                logging.error(traceback.format_exc())
                await asyncio.sleep(60)  # Wait 1 minute on error before retrying

    async def sync_portfolio_assets():
        """Background task that syncs portfolio assets to database every hour."""
        # Get dashboard URL from environment
        dashboard_url = os.getenv('DASHBOARD_URL') or os.getenv('NEXT_PUBLIC_BASE_URL') or 'http://localhost:3001'
        dashboard_url = dashboard_url.rstrip('/')
        
        logging.info("🔄 Starting portfolio assets sync to database...")
        logging.info(f"   Dashboard URL: {dashboard_url}")
        
        # Perform initial sync immediately
        first_sync = True
        
        while True:
            try:
                # Call the portfolio assets sync endpoint
                sync_url = f"{dashboard_url}/api/portfolio/assets/sync"
                if first_sync:
                    logging.info(f"📥 Performing initial portfolio assets sync from {sync_url}...")
                    first_sync = False
                else:
                    logging.info(f"📥 Syncing portfolio assets from {sync_url}...")
                
                async with ClientSession() as session:
                    try:
                        async with session.post(sync_url, timeout=ClientTimeout(total=30)) as response:
                            if response.status == 200:
                                data = await response.json()
                                synced_count = data.get('synced', 0)
                                logging.info(f"✅ Successfully synced {synced_count} portfolio assets to database")
                            else:
                                error_text = await response.text()
                                logging.warning(f"⚠️ Portfolio assets sync returned status {response.status}: {error_text}")
                    except asyncio.TimeoutError:
                        logging.warning("⚠️ Portfolio assets sync timed out")
                    except Exception as e:
                        logging.warning(f"⚠️ Failed to sync portfolio assets: {e}")
                
                # Wait 1 hour (3600 seconds) before next sync
                logging.info("⏳ Waiting 1 hour before next portfolio assets sync...")
                await asyncio.sleep(3600)
                
            except Exception as e:
                logging.error(f"❌ Error in portfolio assets sync loop: {e}")
                import traceback
                logging.error(traceback.format_exc())
                # Wait 10 minutes on error before retrying
                await asyncio.sleep(600)

    async def handle_trades(request):
        """Return trade history from the active exchange (OKX / Binance / etc.)."""
        try:
            limit = int(request.query.get('limit', 1000))
            
            if not hasattr(exchange, "get_recent_fills"):
                return web.json_response({
                    'error': 'Exchange does not support trade history',
                    'trades': []
                }, status=503)
            
            exchange_name = getattr(exchange, "name", type(exchange).__name__).lower()
            
            # Get all trades using get_recent_fills with timeout
            try:
                trades = await asyncio.wait_for(exchange.get_recent_fills(limit=max(limit, 1000)), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("get_recent_fills timed out after 12s, returning empty trades")
                return web.json_response({
                    'trades': [],
                    'count': 0,
                    'source': exchange_name,
                    'warning': 'Request timed out'
                })
            except Exception as e:
                logger.warning(f"get_recent_fills error: {e}, returning empty trades")
                return web.json_response({
                    'trades': [],
                    'count': 0,
                    'source': exchange_name,
                    'warning': f'Error: {str(e)[:100]}'
                })
            
            # Format trades for frontend
            formatted_trades = []
            venue_network, _ = _venue_network()
            for trade in trades:
                symbol = str(trade.get('symbol') or trade.get('coin') or '').replace('USDT', '').replace('-SWAP', '')
                side = str(trade.get('side', '')).lower()
                if side not in ['buy', 'sell']:
                    side = 'buy' if side == 'buy' else 'sell'
                
                price = float(trade.get('price') or trade.get('px') or trade.get('fillPx') or 0)
                size = float(trade.get('size') or trade.get('sz') or trade.get('qty') or trade.get('fillSz') or 0)
                fee = float(trade.get('fee') or trade.get('commission') or 0)
                realized_pnl = trade.get('realizedPnl') or trade.get('realized_pnl') or trade.get('pnl')
                if realized_pnl is None and isinstance(trade.get('raw'), dict):
                    realized_pnl = trade['raw'].get('fillPnl') or trade['raw'].get('pnl')
                    fee = fee or float(trade['raw'].get('fee') or 0)
                pnl = float(realized_pnl) if realized_pnl is not None and str(realized_pnl) != '' else None
                
                # Convert timestamp to ISO string
                timestamp_ms = trade.get('time') or trade.get('timestamp') or trade.get('ts') or 0
                try:
                    timestamp_ms = int(timestamp_ms)
                except (TypeError, ValueError):
                    timestamp_ms = 0
                if timestamp_ms:
                    if timestamp_ms > 1e12:  # Already in milliseconds
                        timestamp_iso = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
                    else:  # In seconds
                        timestamp_iso = datetime.fromtimestamp(timestamp_ms, tz=timezone.utc).isoformat()
                else:
                    timestamp_iso = datetime.now(timezone.utc).isoformat()
                
                # Create unique ID
                trade_id = trade.get('id') or trade.get('tradeId') or trade.get('fillId')
                if not trade_id and isinstance(trade.get('raw'), dict):
                    trade_id = trade['raw'].get('tradeId') or trade['raw'].get('billId')
                if not trade_id:
                    # Create hash from trade data
                    import hashlib
                    trade_str = f"{symbol}_{side}_{price}_{size}_{timestamp_ms}"
                    trade_id = hashlib.sha256(trade_str.encode()).hexdigest()[:16]
                
                formatted_trades.append({
                    'id': str(trade_id),
                    'symbol': symbol,
                    'side': side,
                    'size': abs(size),
                    'price': price,
                    'fee': abs(fee),
                    'pnl': pnl,
                    'timestamp': timestamp_iso,
                    'network': venue_network,
                })
            
            # Sort by timestamp descending (newest first)
            formatted_trades.sort(key=lambda t: t['timestamp'], reverse=True)
            
            return web.json_response({
                'trades': formatted_trades,
                'count': len(formatted_trades),
                'source': exchange_name,
                'network': venue_network,
            })
        except Exception as e:
            logger.error(f"Error in handle_trades: {e}")
            import traceback
            traceback.print_exc()
            return web.json_response({
                'error': str(e),
                'trades': []
            }, status=500)

    async def handle_orders(request):
        """Return all orders (open + recent fills) from Aster."""
        try:
            # Get open orders
            open_orders = await hyperliquid.get_open_orders()
            
            # Get recent fills
            recent_fills = await hyperliquid.get_recent_fills(limit=100)
            
            # Combine and normalize orders
            all_orders = []
            
            # Process open orders
            for o in open_orders:
                try:
                    symbol = o.get('symbol', '').replace('USDT', '') or o.get('coin', '')
                    order_id = o.get('orderId') or o.get('oid') or o.get('id')
                    side_raw = o.get('side', '').upper()
                    is_buy = side_raw == 'BUY' or o.get('isBuy', False)
                    
                    all_orders.append({
                        'oid': str(order_id),
                        'order_id': str(order_id),
                        'coin': symbol,
                        'symbol': symbol,
                        'isBuy': is_buy,
                        'side': 'buy' if is_buy else 'sell',
                        'sz': float(o.get('origQty') or o.get('quantity') or o.get('size', 0)),
                        'size': float(o.get('origQty') or o.get('quantity') or o.get('size', 0)),
                        'px': float(o.get('price', 0)) if o.get('price') else None,
                        'price': float(o.get('price', 0)) if o.get('price') else None,
                        'orderType': o.get('type', 'MARKET').lower(),
                        'type': o.get('type', 'MARKET').lower(),
                        'status': o.get('status', 'NEW').lower(),
                        'time': int(o.get('time', 0)) if o.get('time') else int(time.time() * 1000),
                        'timestamp': int(o.get('time', 0)) if o.get('time') else int(time.time() * 1000),
                        'created_at': datetime.fromtimestamp(int(o.get('time', time.time() * 1000)) / 1000, tz=timezone.utc).isoformat() if o.get('time') else datetime.now(timezone.utc).isoformat(),
                        'filled_size': float(o.get('executedQty') or o.get('filled_size', 0)),
                    })
                except Exception as e:
                    logging.warning(f"Error processing open order: {e}")
                    continue
            
            # Process recent fills
            for f in recent_fills:
                try:
                    symbol = f.get('symbol', '').replace('USDT', '') or f.get('coin', '')
                    order_id = f.get('orderId') or f.get('id')
                    side_raw = f.get('side', '').upper()
                    is_buy = side_raw == 'BUY' or f.get('isBuy', False)
                    
                    # Check if this order is already in all_orders (as open order)
                    existing = next((o for o in all_orders if str(o.get('oid')) == str(order_id)), None)
                    if existing:
                        # Update existing order with fill info
                        existing['filled_size'] = float(f.get('qty') or f.get('quantity') or f.get('size', 0))
                        existing['status'] = 'filled' if existing['filled_size'] >= existing.get('sz', 0) else 'partially_filled'
                    else:
                        # Add as filled order
                        all_orders.append({
                            'oid': str(order_id),
                            'order_id': str(order_id),
                            'coin': symbol,
                            'symbol': symbol,
                            'isBuy': is_buy,
                            'side': 'buy' if is_buy else 'sell',
                            'sz': float(f.get('qty') or f.get('quantity') or f.get('size', 0)),
                            'size': float(f.get('qty') or f.get('quantity') or f.get('size', 0)),
                            'px': float(f.get('price', 0)) if f.get('price') else None,
                            'price': float(f.get('price', 0)) if f.get('price') else None,
                            'orderType': 'market',
                            'type': 'market',
                            'status': 'filled',
                            'time': int(f.get('time', 0)) if f.get('time') else int(time.time() * 1000),
                            'timestamp': int(f.get('time', 0)) if f.get('time') else int(time.time() * 1000),
                            'created_at': datetime.fromtimestamp(int(f.get('time', time.time() * 1000)) / 1000, tz=timezone.utc).isoformat() if f.get('time') else datetime.now(timezone.utc).isoformat(),
                            'filled_size': float(f.get('qty') or f.get('quantity') or f.get('size', 0)),
                        })
                except Exception as e:
                    logging.warning(f"Error processing fill: {e}")
                    continue
            
            # Sort by timestamp (newest first)
            all_orders.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
            
            return web.json_response(all_orders[:200])  # Limit to 200 most recent
        except Exception as e:
            logging.error(f"Error getting orders: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_alert_signal(request):
        """Receive trading signal from alert service and execute immediately."""
        try:
            signal = await request.json()
            
            # Validate signal format
            required_fields = ["asset", "action", "price", "tp_price", "sl_price", "allocation_usd"]
            for field in required_fields:
                if field not in signal:
                    return web.json_response({"error": f"Missing required field: {field}"}, status=400)
            
            asset = signal["asset"]
            action = signal["action"]
            entry_price = float(signal["price"])
            tp_price = float(signal["tp_price"])
            sl_price = float(signal["sl_price"])
            allocation_usd = float(signal["allocation_usd"])
            
            if action not in ("buy", "sell"):
                return web.json_response({"error": f"Invalid action: {action}"}, status=400)
            
            is_buy = action == "buy"
            
            # Get current price to calculate position size
            current_price = await _ex_for(asset).get_current_price(asset)
            if not current_price or current_price <= 0:
                return web.json_response({"error": f"Could not get current price for {asset}"}, status=500)
            
            # Calculate position size from allocation
            # For futures: size = allocation_usd / price
            position_size = allocation_usd / current_price
            
            # Round position size (Aster requires async, Binance is sync - both work with await)
            ex = _ex_for(asset)
            if hasattr(ex, 'round_size') and asyncio.iscoroutinefunction(ex.round_size):
                position_size = await ex.round_size(asset, position_size)
            else:
                position_size = ex.round_size(asset, position_size) if hasattr(ex, 'round_size') else position_size
            
            logging.info(
                f"🚨 ALERT SIGNAL: {asset} {action.upper()} @ ${current_price:.2f} "
                f"(TP: ${tp_price:.2f}, SL: ${sl_price:.2f}, Size: {position_size}, Allocation: ${allocation_usd:.2f})"
            )
            
            # Execute trade immediately
            try:
                if is_buy:
                    order_result = await _ex_for(asset).place_buy_order(asset, position_size)
                else:
                    order_result = await _ex_for(asset).place_sell_order(asset, position_size)
                
                # Check if order was successful (Aster and Binance have different response formats)
                if isinstance(order_result, dict):
                    # Aster format: direct response with orderId
                    if "orderId" in order_result:
                        # Aster - order is successful if orderId exists
                        pass
                    # Binance format: nested status
                    elif order_result.get("status") != "ok":
                        error_msg = order_result.get("response", {}).get("error", "Unknown error")
                        logging.error(f"❌ Alert trade failed for {asset}: {error_msg}")
                        return web.json_response({"error": f"Trade execution failed: {error_msg}"}, status=500)
                
                # Place TP/SL orders
                tp_oid = None
                sl_oid = None
                
                try:
                    # Place take profit order
                    tp_result = await _ex_for(asset).place_take_profit(asset, is_buy, position_size, tp_price)
                    if isinstance(tp_result, dict):
                        # Aster format: direct orderId
                        if "orderId" in tp_result:
                            tp_oid = tp_result.get("orderId")
                        # Binance format: nested response
                        elif tp_result.get("status") == "ok":
                            response_data = tp_result.get("response", {}).get("data", {})
                            statuses = response_data.get("statuses", [])
                            if statuses and isinstance(statuses[0], dict):
                                tp_resting = statuses[0].get("resting", {})
                                if tp_resting:
                                    tp_oid = tp_resting.get("oid")
                        else:
                            logging.warning(f"⚠️  Could not place TP order for {asset}: {tp_result}")
                    else:
                        logging.warning(f"⚠️  Could not place TP order for {asset}: {tp_result}")
                except Exception as e:
                    logging.warning(f"⚠️  Error placing TP order for {asset}: {e}")
                
                try:
                    # Place stop loss order (if enabled)
                    if CONFIG.get('enable_stop_loss_orders', True):
                        sl_result = await _ex_for(asset).place_stop_loss(asset, is_buy, position_size, sl_price)
                    else:
                        sl_result = {"disabled": True}
                    if isinstance(sl_result, dict):
                        # Aster format: direct orderId
                        if "orderId" in sl_result:
                            sl_oid = sl_result.get("orderId")
                        # Binance format: nested response
                        elif sl_result.get("status") == "ok":
                            response_data = sl_result.get("response", {}).get("data", {})
                            statuses = response_data.get("statuses", [])
                            if statuses and isinstance(statuses[0], dict):
                                sl_resting = statuses[0].get("resting", {})
                                if sl_resting:
                                    sl_oid = sl_resting.get("oid")
                        else:
                            logging.warning(f"⚠️  Could not place SL order for {asset}: {sl_result}")
                    else:
                        logging.warning(f"⚠️  Could not place SL order for {asset}: {sl_result}")
                except Exception as e:
                    logging.warning(f"⚠️  Error placing SL order for {asset}: {e}")
                
                # Log to diary
                diary_path = "diary.jsonl"
                exit_plan = f"TP: ${tp_price:.2f}, SL: ${sl_price:.2f}"
                with open(diary_path, "a") as f:
                    f.write(json.dumps({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "asset": asset,
                        "action": f"alert_{action}",
                        "entry_price": current_price,
                        "tp_price": tp_price,
                        "sl_price": sl_price,
                        "amount": position_size,
                        "allocation_usd": allocation_usd,
                        "tp_oid": tp_oid,
                        "sl_oid": sl_oid,
                        "reason": f"Alert service signal: {signal.get('signal_type', 'unknown')}",
                        "source": "alert_service"
                    }) + "\n")
                
                logging.info(
                    f"✅ Alert trade executed: {asset} {action.upper()} @ ${current_price:.2f} "
                    f"(TP: ${tp_price:.2f}, SL: ${sl_price:.2f}, TP_OID: {tp_oid}, SL_OID: {sl_oid})"
                )
                
                return web.json_response({
                    "success": True,
                    "asset": asset,
                    "action": action,
                    "entry_price": current_price,
                    "position_size": position_size,
                    "tp_price": tp_price,
                    "sl_price": sl_price,
                    "tp_oid": tp_oid,
                    "sl_oid": sl_oid
                })
                
            except Exception as e:
                logging.error(f"❌ Error executing alert trade for {asset}: {e}")
                import traceback
                logging.error(traceback.format_exc())
                return web.json_response({"error": f"Trade execution error: {str(e)}"}, status=500)
                
        except Exception as e:
            logging.error(f"Error handling alert signal: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_trading_info(request):
        """Return trading information for a given asset and amount (allocation)."""
        try:
            asset = request.query.get('asset', '').upper()
            amount_str = request.query.get('amount', '0')
            
            if not asset:
                return web.json_response({"error": "asset parameter is required"}, status=400)
            
            try:
                amount = float(amount_str)
            except (ValueError, TypeError):
                return web.json_response({"error": "amount must be a valid number"}, status=400)
            
            if amount <= 0:
                return web.json_response({"error": "amount must be greater than 0"}, status=400)
            
            # Get current price — _ex_for returns None when this process has no venue for the asset
            ex = _ex_for(asset)
            if ex is None:
                return web.json_response(
                    {"error": f"No exchange mapped for {asset} in this agent"},
                    status=404,
                )
            try:
                current_price = await ex.get_current_price(asset)
                if not current_price or current_price <= 0:
                    return web.json_response({"error": f"Could not get price for {asset}"}, status=404)
            except Exception as e:
                logging.error(f"Error getting price for {asset}: {e}")
                return web.json_response({"error": f"Error getting price: {str(e)}"}, status=500)
            
            # Get trading settings
            from src.utils.trading_settings import get_trading_settings, get_max_leverage_for_asset, calculate_tp_sl_prices
            trading_settings = await get_trading_settings()
            leverage = get_leverage_for_asset(asset, trading_settings.get('leverage', 10))
            
            # Get max leverage for asset from exchange
            max_leverage = await get_max_leverage_for_asset(ex, asset)
            leverage = min(leverage, max_leverage)
            
            # Calculate position size
            from src.utils.position_sizing import calculate_profit, calculate_liquidation_price
            position_size_units = amount / current_price  # Units of asset
            controlled_value = amount * leverage  # Total notional value
            
            # Calculate profit/loss scenarios
            profit_1pct = calculate_profit(amount, 1.0, leverage)
            profit_5pct = calculate_profit(amount, 5.0, leverage)
            loss_1pct = calculate_profit(amount, -1.0, leverage)
            loss_3pct = calculate_profit(amount, -3.0, leverage)
            
            # Calculate TP/SL prices
            take_profit_percent = trading_settings.get('take_profit_percent', 5.0)
            stop_loss_percent = trading_settings.get('stop_loss_percent', 3.0)
            
            tp_price_long, sl_price_long = calculate_tp_sl_prices(
                current_price, True, take_profit_percent, stop_loss_percent
            )
            tp_price_short, sl_price_short = calculate_tp_sl_prices(
                current_price, False, take_profit_percent, stop_loss_percent
            )
            
            # Calculate liquidation prices
            liquidation_long = calculate_liquidation_price(current_price, True, leverage)
            liquidation_short = calculate_liquidation_price(current_price, False, leverage)
            
            return web.json_response({
                "asset": asset,
                "current_price": round(current_price, 2),
                "allocation_usd": round(amount, 2),
                "leverage": leverage,
                "max_leverage": max_leverage,
                "position_size_units": round(position_size_units, 8),
                "controlled_value_usd": round(controlled_value, 2),
                "profit_scenarios": {
                    "profit_1pct": round(profit_1pct, 2),
                    "profit_5pct": round(profit_5pct, 2),
                    "loss_1pct": round(loss_1pct, 2),
                    "loss_3pct": round(loss_3pct, 2),
                },
                "long_position": {
                    "entry_price": round(current_price, 2),
                    "tp_price": round(tp_price_long, 2),
                    "sl_price": round(sl_price_long, 2),
                    "liquidation_price": round(liquidation_long, 2),
                    "tp_profit": round(profit_5pct, 2),
                    "sl_loss": round(loss_3pct, 2),
                },
                "short_position": {
                    "entry_price": round(current_price, 2),
                    "tp_price": round(tp_price_short, 2),
                    "sl_price": round(sl_price_short, 2),
                    "liquidation_price": round(liquidation_short, 2),
                    "tp_profit": round(profit_5pct, 2),
                    "sl_loss": round(loss_3pct, 2),
                },
                "trading_settings": {
                    "take_profit_percent": take_profit_percent,
                    "stop_loss_percent": stop_loss_percent,
                }
            })
        except Exception as e:
            logging.error(f"Error in handle_trading_info: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_position_protection(request):
        """Set/replace TP/SL protection orders for an existing position."""
        try:
            payload = await request.json()
            asset_raw = payload.get("asset") or payload.get("symbol")
            if not asset_raw:
                return web.json_response({"error": "asset (or symbol) is required"}, status=400)

            asset = str(asset_raw).upper().replace("/USDT", "").replace("USDT", "")
            tp_price = payload.get("tp_price")
            sl_price = payload.get("sl_price")

            # Allow either TP or SL update; null/empty means "clear that side".
            tp_price = float(tp_price) if tp_price not in (None, "") else None
            sl_price = float(sl_price) if sl_price not in (None, "") else None

            # Confirm an open position exists and detect side/size.
            state = await hyperliquid.get_user_state()
            position_size = 0.0
            is_long = True
            for p in state.get("positions", []):
                sym = p.get("coin") or p.get("symbol")
                if str(sym).upper().replace("/USDT", "").replace("USDT", "") != asset:
                    continue
                raw = float(p.get("szi") or p.get("quantity") or p.get("positionAmt") or 0)
                if abs(raw) > 0:
                    position_size = abs(raw)
                    is_long = raw > 0
                    break

            if position_size <= 0:
                return web.json_response({"error": f"No open position found for {asset}"}, status=404)

            # Replace protection: clear old conditional orders first.
            await _safe_cancel_all_orders(asset, "manual TP/SL update")

            # Recreate requested protection orders.
            result = await _ensure_protective_orders(
                asset=asset,
                is_long=is_long,
                position_size=position_size,
                tp_price=tp_price,
                sl_price=sl_price,
                trading_settings=await get_trading_settings(),
            )

            return web.json_response({
                "success": True,
                "asset": asset,
                "position_size": position_size,
                "side": "long" if is_long else "short",
                "tp_price": result.get("tp_price"),
                "sl_price": result.get("sl_price"),
                "tp_oid": result.get("tp_oid"),
                "sl_oid": result.get("sl_oid"),
            })
        except Exception as e:
            logging.error(f"Error updating position protection: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return web.json_response({"error": str(e)}, status=500)

    async def handle_health(_request):
        """Coolify / Docker health probes hit GET / — must return 200, not 404."""
        return web.json_response({"ok": True, "service": "ayo-trading-agent"})

    async def start_api(app):
        """Register HTTP endpoints for observing diary entries, logs, positions, and status."""
        app.router.add_get('/', handle_health)
        app.router.add_get('/health', handle_health)
        app.router.add_get('/diary', handle_diary)
        app.router.add_get('/logs', handle_logs)
        app.router.add_get('/positions', handle_positions)
        app.router.add_get('/status', handle_status)
        app.router.add_get('/balances', handle_balances)  # Add balances endpoint
        app.router.add_get('/api/orders', handle_orders)
        app.router.add_get('/api/pnl', handle_pnl)
        app.router.add_get('/api/performance', handle_performance)
        app.router.add_get('/api/prices', handle_prices)
        app.router.add_get('/api/trading-info', handle_trading_info)  # Add trading info endpoint
        app.router.add_get('/api/trades', handle_trades)  # Add trades endpoint
        app.router.add_get('/api/mechanical', handle_mechanical)  # Mechanical scalp book status
        app.router.add_get('/api/hive-race', handle_hive_race)
        app.router.add_post('/api/test', handle_status)  # Alias for compatibility
        app.router.add_post('/api/alert/signal', handle_alert_signal)
        app.router.add_post('/api/position-protection', handle_position_protection)

    def suppress_connection_errors():
        """Suppress harmless Windows socket connection errors that occur during rapid connection closures."""
        def handle_exception(loop, context):
            """Handle asyncio exceptions, suppressing harmless Windows socket errors."""
            exception = context.get('exception')
            message = context.get('message', '')
            
            # Suppress known harmless Windows socket errors
            if isinstance(exception, (OSError, ConnectionResetError)):
                error_code = getattr(exception, 'winerror', None) or getattr(exception, 'errno', None)
                # Windows error codes: 64 = network name no longer available, 10054 = connection reset
                if error_code in (64, 10054) or 'network name' in str(exception).lower() or 'connection reset' in str(exception).lower():
                    # Suppress these harmless errors - they occur when connections close rapidly
                    return
            
            # Log other exceptions normally
            loop.default_exception_handler(context)
        
        # Set custom exception handler for the event loop
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_exception)

    async def main_async():
        """Start the aiohttp server and kick off the trading loop."""
        # Suppress harmless Windows socket errors
        suppress_connection_errors()
        
        # Configure aiohttp to handle connection errors gracefully
        app = web.Application()
        
        # Add CORS middleware to allow requests from Vercel and other origins
        @web.middleware
        async def cors_middleware(request, handler):
            """Handle CORS headers for cross-origin requests."""
            # Handle preflight OPTIONS requests
            if request.method == 'OPTIONS':
                response = web.Response()
            else:
                response = await handler(request)
            
            # Get allowed origins from environment or use defaults
            allowed_origins = os.getenv('CORS_ALLOWED_ORIGINS', '').split(',')
            # Add common localhost origins for development
            if not allowed_origins or allowed_origins == ['']:
                allowed_origins = [
                    'http://localhost:3000',
                    'http://localhost:3001',
                    'https://*.vercel.app',  # Vercel preview deployments
                ]
            else:
                # Clean up the list
                allowed_origins = [origin.strip() for origin in allowed_origins if origin.strip()]
            
            # Get the origin from the request
            origin = request.headers.get('Origin', '')
            
            # Check if origin is allowed (supports wildcard for Vercel)
            origin_allowed = False
            if origin:
                for allowed in allowed_origins:
                    if allowed == '*' or origin == allowed:
                        origin_allowed = True
                        break
                    # Support wildcard matching for Vercel (*.vercel.app)
                    if '*' in allowed:
                        if fnmatch.fnmatch(origin, allowed):
                            origin_allowed = True
                            break
            
            # Set CORS headers
            if origin_allowed or not origin:
                response.headers['Access-Control-Allow-Origin'] = origin if origin_allowed else '*'
                response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
                response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                response.headers['Access-Control-Allow-Credentials'] = 'true'
            
            return response
        
        # Add middleware to catch and suppress connection errors
        @web.middleware
        async def suppress_connection_error_middleware(request, handler):
            try:
                return await handler(request)
            except (OSError, ConnectionResetError) as e:
                error_code = getattr(e, 'winerror', None) or getattr(e, 'errno', None)
                # Suppress harmless Windows socket errors (64, 10054)
                if error_code in (64, 10054):
                    # Return empty response instead of crashing
                    return web.Response(status=200, text='')
                raise
        
        app.middlewares.append(cors_middleware)
        app.middlewares.append(suppress_connection_error_middleware)
        
        await start_api(app)
        from src.config_loader import CONFIG as CFG
        # Free the port held by the early health thread before aiohttp binds.
        await asyncio.to_thread(stop_early_health_server)
        await asyncio.sleep(0.15)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, CFG.get("api_host"), int(CFG.get("api_port")))
        await site.start()
        
        logging.info(f"🌐 HTTP API server started on {CFG.get('api_host')}:{CFG.get('api_port')}")
        from src.runners.hive_race import start_hive_race
        asyncio.create_task(start_hive_race())
        
        # Start background task to continuously sync trades to Supabase
        # Check if exchange is BinanceAPI instance
        from src.trading.binance_api import BinanceAPI
        is_binance = isinstance(exchange, BinanceAPI)
        
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if is_binance and supabase_url and supabase_key:
            # Pass exchange as binance_api to the sync function
            asyncio.create_task(sync_trades_to_supabase())
            logging.info("🔄 Background trade sync to Supabase started")
            logging.info(f"   Supabase URL: {supabase_url[:30]}...")
        elif is_binance:
            logging.info("ℹ️ Supabase not configured - trade sync disabled")
            if not supabase_url:
                logging.info("   Missing: SUPABASE_URL environment variable")
            if not supabase_key:
                logging.info("   Missing: SUPABASE_KEY environment variable")
            logging.info("   Add SUPABASE_URL and SUPABASE_KEY to your .env file to enable trade sync")
        
        # Start background task to sync portfolio assets every hour
        # This works for both Binance and Aster exchanges
        dashboard_url = os.getenv('DASHBOARD_URL') or os.getenv('NEXT_PUBLIC_BASE_URL')
        if dashboard_url:
            asyncio.create_task(sync_portfolio_assets())
            logging.info("🔄 Background portfolio assets sync started (every hour)")
        else:
            logging.info("ℹ️ Dashboard URL not configured - portfolio assets sync disabled")
            logging.info("   Add DASHBOARD_URL or NEXT_PUBLIC_BASE_URL to your .env file to enable portfolio sync")
        
        await run_loop()

    def calculate_total_return(state, trade_log):
        """Compute percent return relative to an assumed initial balance."""
        initial = 10000
        current = state['balance'] + sum(p.get('pnl', 0) for p in state.get('positions', []))
        return ((current - initial) / initial) * 100 if initial else 0

    def calculate_sharpe(returns):
        """Compute a naive Sharpe-like ratio from the trade log."""
        if not returns:
            return 0
        vals = [r.get('pnl', 0) if 'pnl' in r else 0 for r in returns]
        if not vals:
            return 0
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = math.sqrt(var) if var > 0 else 0
        return mean / std if std > 0 else 0

    async def check_exit_condition(trade, taapi_client, exchange_client):
        """Evaluate whether a given trade's exit plan triggers a close."""
        plan = (trade.get("exit_plan") or "").lower().strip()
        if not plan:
            return False
        asset = trade.get("asset")
        if not asset:
            return False
        try:
            # MACD crosses / below threshold, e.g. "close if macd below 0" or "macd below -0.5"
            if "macd" in plan and ("below" in plan or "cross" in plan):
                indicators = taapi_client.get_indicators(asset, "4h") if hasattr(taapi_client, "get_indicators") else {}
                macd = None
                if isinstance(indicators, dict):
                    macd = (indicators.get("macd") or {}).get("valueMACD")
                    if macd is None:
                        macd = (indicators.get("macd") or {}).get("value")
                if macd is None:
                    return False
                if "below" in plan:
                    try:
                        threshold = float(plan.split("below")[-1].strip().split()[0].replace(",", ""))
                    except Exception:
                        threshold = 0.0
                    return float(macd) < threshold
                # cross below / bearish cross without numeric threshold
                return float(macd) < 0

            # EMA50: "close above ema50" (shorts) / "close below ema50" (longs)
            if "ema50" in plan or "ema 50" in plan:
                historical = taapi_client.get_historical_indicator(
                    "ema", f"{asset}/USDT", "4h", results=1, params={"period": 50}
                )
                if not historical:
                    return False
                ema50 = historical[0].get("value")
                if ema50 is None:
                    return False
                current = await exchange_client.get_current_price(asset)
                if "below" in plan:
                    return current < float(ema50)
                if "above" in plan:
                    return current > float(ema50)

            # RSI overbought/oversold style plans
            if "rsi" in plan:
                try:
                    historical = taapi_client.get_historical_indicator(
                        "rsi", f"{asset}/USDT", "15m", results=1, params={"period": 14}
                    )
                    if not historical:
                        return False
                    rsi = float(historical[0].get("value"))
                    if "above" in plan or "overbought" in plan:
                        try:
                            thr = float(plan.split("above")[-1].strip().split()[0]) if "above" in plan else 70.0
                        except Exception:
                            thr = 70.0
                        return rsi >= thr
                    if "below" in plan or "oversold" in plan:
                        try:
                            thr = float(plan.split("below")[-1].strip().split()[0]) if "below" in plan else 30.0
                        except Exception:
                            thr = 30.0
                        return rsi <= thr
                except Exception:
                    return False
        except Exception as e:
            logging.debug(f"check_exit_condition error for {asset}: {e}")
            return False
        return False

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.info("Trading agent stopped by user")
    except Exception as e:
        logging.error(f"Fatal error in main_async: {e}", exc_info=True)
        print(f"Fatal error: {e}", file=original_stderr)
        import traceback
        traceback.print_exc(file=original_stderr)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error in main: {e}", file=original_stderr)
        import traceback
        traceback.print_exc(file=original_stderr)
        raise

