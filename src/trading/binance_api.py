"""Binance Perpetual Futures trading client with async support."""

import asyncio
import logging
import hmac
import hashlib
import urllib.parse
from typing import Optional, Dict, List, Any
from binance.client import Client
from binance.exceptions import BinanceAPIException
from src.config_loader import CONFIG
import time
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceAPI:
    """Client for Binance Perpetual Futures trading with leverage support."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        leverage: Optional[int] = None,
    ):
        """Initialize Binance Futures client.

        Optional api_key/api_secret/testnet override CONFIG so hive runners
        (e.g. Confetti) can use dedicated demo keys without sharing AYO's.
        """
        api_key = (api_key if api_key is not None else CONFIG.get("binance_api_key") or "").strip()
        api_secret = (api_secret if api_secret is not None else CONFIG.get("binance_api_secret") or "").strip()
        if testnet is None:
            testnet = bool(CONFIG.get("binance_testnet", False))
        else:
            testnet = bool(testnet)

        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in .env")

        # Store API credentials for v3 endpoint calls
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        # Initialize Binance Client with retries to tolerate transient 5xx errors from Binance
        # python-binance Client pings in constructor; wrap in retry/backoff
        def _create_client():
            return Client(api_key, api_secret, testnet=testnet)

        max_attempts = 5
        backoff = 1.0
        last_err = None
        for attempt in range(max_attempts):
            try:
                self.client = _create_client()
                break
            except BinanceAPIException as e:
                last_err = e
                # Code 0 often corresponds to invalid HTML/JSON on 5xx like 502/504
                wait_s = backoff * (2 ** attempt)
                # Clean up error message - extract just the key info if it's HTML
                err_msg = getattr(e, 'message', str(e))
                if '<html>' in err_msg or '502' in err_msg or 'Bad Gateway' in err_msg:
                    err_msg = "502 Bad Gateway (Binance server issue)"
                elif len(err_msg) > 200:
                    err_msg = err_msg[:200] + "..."
                logger.warning(f"Binance init failed (attempt {attempt + 1}/{max_attempts}): {err_msg}. Retrying in {wait_s:.1f}s...")
                time.sleep(wait_s)
            except Exception as e:
                last_err = e
                wait_s = backoff * (2 ** attempt)
                err_msg = str(e)
                if len(err_msg) > 200:
                    err_msg = err_msg[:200] + "..."
                logger.warning(f"Binance init error (attempt {attempt + 1}/{max_attempts}): {err_msg}. Retrying in {wait_s:.1f}s...")
                time.sleep(wait_s)
        else:
            # Clean up final error message too
            final_err = str(last_err) if last_err else "Unknown error"
            if '<html>' in final_err or '502' in final_err:
                final_err = "502 Bad Gateway - Binance servers temporarily unavailable"
            elif len(final_err) > 200:
                final_err = final_err[:200] + "..."
            raise RuntimeError(f"Failed to initialize Binance Client after {max_attempts} attempts: {final_err}")
        
        # Sync local timestamp offset with Binance server to avoid -1021 timestamp errors
        try:
            server_time = self.client.get_server_time()  # returns {'serverTime': <ms>}
            if isinstance(server_time, dict) and 'serverTime' in server_time:
                local_ms = int(time.time() * 1000)
                offset_ms = int(server_time['serverTime']) - local_ms
                # python-binance respects timestamp_offset when generating signatures
                setattr(self.client, 'timestamp_offset', offset_ms)
                logger.info(f"Synchronized timestamp offset with Binance: {offset_ms}ms")
        except Exception as e:
            logger.warning(f"Could not sync timestamp with Binance server time: {e}")

        # Set recvWindow on client to handle timestamp synchronization
        # recvWindow is the time window (in milliseconds) for the request to be valid
        # Default is 5000ms (5 seconds). Setting to 60000ms (60 seconds) gives more buffer
        # This helps when system time is slightly off from Binance server time
        if hasattr(self.client, 'recvWindow'):
            self.client.recvWindow = 60000
        
        # Default leverage (can be configured per asset / hive runner)
        if leverage is not None:
            self.default_leverage = int(leverage)
        else:
            self.default_leverage = int(CONFIG.get("binance_leverage", 10))

        logger.info(f"Binance Futures client initialized (testnet={testnet}, leverage={self.default_leverage}x, recvWindow=60000ms)")

    def _to_futures_symbol(self, asset: str) -> str:
        """Convert asset name (BTC) to Binance Futures symbol (BTCUSDT)."""
        return f"{asset}USDT"

    def _from_futures_symbol(self, symbol: str) -> str:
        """Convert Binance Futures symbol (BTCUSDT) to asset name (BTC)."""
        return symbol.replace("USDT", "")

    async def _get_active_position_side(self, symbol: str) -> Optional[str]:
        """Return active positionSide ('LONG'/'SHORT') when available."""
        try:
            positions = await self._retry(lambda: self.client.futures_position_information(symbol=symbol))
            for pos in positions:
                if pos.get("symbol") != symbol:
                    continue
                pos_amt = float(pos.get("positionAmt", 0) or 0)
                if abs(pos_amt) <= 0:
                    continue
                side = (pos.get("positionSide") or "").upper().strip()
                if side in {"LONG", "SHORT"}:
                    return side
                return "LONG" if pos_amt > 0 else "SHORT"
        except Exception:
            return None
        return None

    async def place_protective_stop(
        self,
        asset: str,
        *,
        is_long: bool,
        sl_price: float,
        quantity: float = 0.0,
    ) -> Any:
        """Rest a STOP_MARKET reduce-only without futures_exchange_info (that call hangs hive)."""
        symbol = self._to_futures_symbol(asset)
        sl = float(sl_price or 0)
        if sl <= 0:
            raise ValueError("sl_price required")
        # Round lightly — exchange rejects insane precision; avoid full exchangeInfo fetch.
        sl = float(f"{sl:.8g}")
        side = "SELL" if is_long else "BUY"
        position_side = await self._get_active_position_side(symbol)
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": sl,
            "workingType": "MARK_PRICE",
            "closePosition": "true",
        }
        if position_side:
            params["positionSide"] = position_side
        order = await self._retry(lambda: self.client.futures_create_order(**params))
        logger.info(
            "Pinned protective STOP_MARKET %s %s @ %s",
            asset,
            "LONG" if is_long else "SHORT",
            sl,
        )
        return order

    async def close_position_market(self, asset: str, quantity: float, *, is_long: bool) -> Any:
        """Flatten a live futures position.

        Always re-read positionAmt so a wrong book side cannot sell into a short.
        Binance rejects ``closePosition`` on plain MARKET — use STOP_MARKET
        closePosition triggered through the mark (immediate flatten), then qty MARKET.
        """
        symbol = self._to_futures_symbol(asset)
        qty = abs(float(quantity or 0))
        live_long = bool(is_long)
        try:
            positions = await self._retry(
                lambda: self.client.futures_position_information(symbol=symbol)
            )
            for pos in positions or []:
                if pos.get("symbol") != symbol:
                    continue
                amt = float(pos.get("positionAmt", 0) or 0)
                if abs(amt) <= 0:
                    continue
                qty = abs(amt)
                live_long = amt > 0
                break
        except Exception as exc:
            logger.warning("Binance close size lookup failed for %s: %s", asset, exc)
        if qty <= 0:
            return None

        side = "SELL" if live_long else "BUY"
        position_side = await self._get_active_position_side(symbol)
        errors: list[str] = []

        # 1) Immediate STOP_MARKET + closePosition (valid Binance flatten; MARKET+closePosition is not).
        try:
            ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
            px = float(ticker.get("price") or 0)
            if px > 0:
                # BUY stop triggers when price >= stop; SELL when price <= stop.
                # Nudge through the mark so the stop is already active.
                stop_px = px * (0.998 if not live_long else 1.002)
                stop_params: Dict[str, Any] = {
                    "symbol": symbol,
                    "side": side,
                    "type": "STOP_MARKET",
                    "stopPrice": stop_px,
                    "closePosition": "true",
                    "workingType": "MARK_PRICE",
                }
                if position_side:
                    stop_params["positionSide"] = position_side
                order = await self._retry(lambda: self.client.futures_create_order(**stop_params))
                logger.info(
                    "Closed %s %s via STOP_MARKET closePosition @ ~%s",
                    asset,
                    "LONG" if live_long else "SHORT",
                    stop_px,
                )
                return order
        except Exception as exc:
            errors.append(f"stop_close: {exc}")
            logger.warning("Binance STOP_MARKET close failed for %s: %s", asset, exc)

        # 2) MARKET reduceOnly with live qty (trim obvious float junk).
        try:
            qty_str = f"{qty:.8f}".rstrip("0").rstrip(".")
            order_params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": qty_str,
            }
            if position_side:
                order_params["positionSide"] = position_side
            else:
                order_params["reduceOnly"] = "true"
            order = await self._retry(lambda: self.client.futures_create_order(**order_params))
            logger.info(
                "Closed %s %s qty=%s reduceOnly market",
                asset,
                "LONG" if live_long else "SHORT",
                qty_str,
            )
            return order
        except Exception as exc:
            errors.append(f"qty_market: {exc}")
            logger.error("Failed Binance flatten for %s: %s", asset, " | ".join(errors))
            raise RuntimeError(" | ".join(errors)) from exc

    async def _retry(self, fn, *args, max_attempts: int = 3, backoff: float = 0.5, **kwargs):
        """Retry helper with exponential backoff."""
        last_err = None
        for attempt in range(max_attempts):
            try:
                return await asyncio.to_thread(fn, *args, **kwargs)
            except BinanceAPIException as e:
                if e.code == -1021:  # Timestamp error
                    # Binance requires accurate timestamps, retry once
                    if attempt == 0:
                        await asyncio.sleep(0.1)
                        continue
                if e.code in [-1003, -1022, -1021] and attempt < max_attempts - 1:
                    wait = backoff * (2 ** attempt)
                    logger.warning(f"Binance API error {e.code}, retrying in {wait}s: {e.message}")
                    await asyncio.sleep(wait)
                    continue
                raise
            except Exception as e:
                last_err = e
                if attempt < max_attempts - 1:
                    wait = backoff * (2 ** attempt)
                    logger.warning(f"Error in Binance call, retrying in {wait}s: {e}")
                    await asyncio.sleep(wait)
                else:
                    raise
        raise last_err if last_err else RuntimeError("Max retries exceeded")

    async def set_leverage(self, asset: str, leverage: int):
        """Set leverage for a specific asset."""
        symbol = self._to_futures_symbol(asset)
        try:
            await self._retry(
                lambda: self.client.futures_change_leverage(symbol=symbol, leverage=leverage)
            )
            logger.info(f"Set leverage for {asset} to {leverage}x")
        except Exception as e:
            logger.error(f"Failed to set leverage for {asset}: {e}")

    async def place_buy_order(self, asset: str, amount: float, leverage: Optional[int] = None, slippage: float = 0.01, reduce_only: bool = False):
        """Place a market buy order for perpetual futures (opens LONG or closes SHORT).
        
        Args:
            asset: Asset symbol (e.g., 'BTC')
            amount: Contract quantity (number of contracts)
            leverage: Leverage to use (defaults to configured leverage)
            slippage: Unused (kept for compatibility with Hyperliquid interface)
            reduce_only: If True, only reduces position (for closing short positions)
        """
        symbol = self._to_futures_symbol(asset)
        lev = leverage or self.default_leverage
        
        # Set leverage first
        await self.set_leverage(asset, lev)
        
        # Get current price for logging
        ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
        current_price = float(ticker['price'])
        
        # Amount is already quantity (contract size), not notional
        quantity = amount
        
        # Get symbol info for precision
        exchange_info = await self._retry(lambda: self.client.futures_exchange_info())
        symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
        
        if symbol_info:
            # Round quantity to step size
            step_size = float([f['stepSize'] for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
            quantity = round(quantity / step_size) * step_size
            quantity = float(f"{quantity:.{len(str(step_size).split('.')[-1])}f}")
        
        try:
            # Build order params
            order_params = {
                'symbol': symbol,
                'side': 'BUY',
                'type': 'MARKET',
                'quantity': quantity
            }
            # Add reduceOnly for closing positions (bypasses $5 minimum)
            if reduce_only:
                position_side = await self._get_active_position_side(symbol)
                if position_side:
                    order_params['positionSide'] = position_side
                else:
                    order_params['reduceOnly'] = 'true'
            
            order = await self._retry(
                lambda: self.client.futures_create_order(**order_params)
            )
            logger.info(f"Placed BUY order for {asset}: {quantity} @ ~${current_price:.2f} (reduceOnly={reduce_only})")
            return order
        except BinanceAPIException as e:
            logger.error(f"Failed to place BUY order for {asset}: {e.message} (code: {e.code})")
            raise

    async def place_sell_order(self, asset: str, amount: float, leverage: Optional[int] = None, slippage: float = 0.01, reduce_only: bool = False):
        """Place a market sell order for perpetual futures (opens SHORT or closes LONG).
        
        Args:
            asset: Asset symbol (e.g., 'BTC')
            amount: Contract quantity (number of contracts)
            leverage: Leverage to use (defaults to configured leverage)
            slippage: Unused (kept for compatibility with Hyperliquid interface)
            reduce_only: If True, only reduces position (for closing long positions)
        """
        symbol = self._to_futures_symbol(asset)
        lev = leverage or self.default_leverage
        
        # Set leverage first
        await self.set_leverage(asset, lev)
        
        # Get current price for logging
        ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
        current_price = float(ticker['price'])
        
        # Amount is already quantity (contract size), not notional
        quantity = amount
        
        # Get symbol info for precision
        exchange_info = await self._retry(lambda: self.client.futures_exchange_info())
        symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
        
        if symbol_info:
            # Round quantity to step size
            step_size = float([f['stepSize'] for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
            quantity = round(quantity / step_size) * step_size
            quantity = float(f"{quantity:.{len(str(step_size).split('.')[-1])}f}")
        
        try:
            # Build order params
            order_params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': quantity
            }
            # Add reduceOnly for closing positions (bypasses $5 minimum)
            if reduce_only:
                position_side = await self._get_active_position_side(symbol)
                if position_side:
                    order_params['positionSide'] = position_side
                else:
                    order_params['reduceOnly'] = 'true'
            
            order = await self._retry(
                lambda: self.client.futures_create_order(**order_params)
            )
            logger.info(f"Placed SELL order for {asset}: {quantity} @ ~${current_price:.2f} (reduceOnly={reduce_only})")
            return order
        except BinanceAPIException as e:
            logger.error(f"Failed to place SELL order for {asset}: {e.message} (code: {e.code})")
            raise

    async def place_take_profit(self, asset: str, is_long: bool, quantity: float, tp_price: float):
        """Place a take-profit order (closes position at target price).
        
        Args:
            asset: Asset symbol
            is_long: True if closing a long position
            quantity: Position size to close (will be validated against actual position)
            tp_price: Take-profit price
        """
        symbol = self._to_futures_symbol(asset)
        
        # Get current position size to ensure we're closing the right amount
        try:
            positions = await self._retry(lambda: self.client.futures_position_information(symbol=symbol))
            actual_position = None
            for pos in positions:
                if pos.get('symbol') == symbol:
                    pos_amt = float(pos.get('positionAmt', 0))
                    if abs(pos_amt) > 0:  # Has position
                        actual_position = abs(pos_amt)
                        break
        except Exception:
            actual_position = None
        
        # Use actual position size if available, otherwise use provided quantity
        if actual_position:
            quantity = actual_position
            logger.info(f"Using actual position size {quantity} for TP order")
        
        # Get symbol info for precision
        exchange_info = await self._retry(lambda: self.client.futures_exchange_info())
        symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
        
        if symbol_info:
            # Round price to tick size
            tick_size = float([f['tickSize'] for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'][0])
            tp_price = round(tp_price / tick_size) * tick_size
            tp_price = float(f"{tp_price:.{len(str(tick_size).split('.')[-1])}f}")
            
            # Round quantity
            step_size = float([f['stepSize'] for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
            quantity = round(quantity / step_size) * step_size
            quantity = float(f"{quantity:.{len(str(step_size).split('.')[-1])}f}")
        
        # Validate TP price is correct direction
        current_price = None
        try:
            ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
            current_price = float(ticker['price'])
            
            # For LONG position: TP should be ABOVE current price
            # For SHORT position: TP should be BELOW current price
            if is_long and tp_price <= current_price:
                logger.warning(f"TP price {tp_price} is not above current price {current_price} for long position. Adjusting...")
                tp_price = current_price * 1.01  # Set TP 1% above current as minimum
                tp_price = round(tp_price / tick_size) * tick_size
                tp_price = float(f"{tp_price:.{len(str(tick_size).split('.')[-1])}f}")
            elif not is_long and tp_price >= current_price:
                logger.warning(f"TP price {tp_price} is not below current price {current_price} for short position. Adjusting...")
                tp_price = current_price * 0.99  # Set TP 1% below current as minimum
                tp_price = round(tp_price / tick_size) * tick_size
                tp_price = float(f"{tp_price:.{len(str(tick_size).split('.')[-1])}f}")
        except Exception as e:
            logger.warning(f"Could not validate TP price against current price: {e}")
        
        try:
            # Close position: if long, sell to close; if short, buy to close
            side = 'SELL' if is_long else 'BUY'
            position_side = await self._get_active_position_side(symbol)
            
            # Use closePosition=true instead of quantity for more reliable execution
            # But only if we have an actual position
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'TAKE_PROFIT_MARKET',
                'stopPrice': tp_price,
                'workingType': 'CONTRACT_PRICE'  # Use contract price for trigger
            }
            if position_side:
                order_params['positionSide'] = position_side
            else:
                order_params['reduceOnly'] = True
            
            # Use quantity if specified and valid, otherwise let Binance determine
            if quantity and quantity > 0:
                order_params['quantity'] = quantity
            
            order = await self._retry(
                lambda: self.client.futures_create_order(**order_params)
            )
            price_info = f" (current: ${current_price:.2f})" if current_price else ""
            logger.info(f"Placed TP order for {asset}: {quantity} @ ${tp_price:.2f}{price_info}")
            return order
        except BinanceAPIException as e:
            if e.code == -4120:
                logger.warning(
                    f"Primary TP order type unsupported for {asset} (code -4120). "
                    "Trying compatible TP fallbacks."
                )
            else:
                logger.error(f"Failed to place TP order for {asset}: {e.message} (code: {e.code})")
            # Some symbols/endpoints reject TP_MARKET on this route (-4120).
            # Fallback 1: reduce-only TAKE_PROFIT limit algo order.
            if e.code == -4120:
                logger.warning(f"TP_MARKET not supported for {asset}, trying reduce-only TAKE_PROFIT limit order")
                try:
                    fallback_order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='TAKE_PROFIT',
                            stopPrice=tp_price,
                            price=tp_price,
                            timeInForce='GTC',
                            quantity=quantity,
                            **({'reduceOnly': 'true'} if not position_side else {}),
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed fallback TAKE_PROFIT TP for {asset}: {quantity} @ ${tp_price:.2f}")
                    return fallback_order
                except Exception as e2:
                    logger.warning(f"Fallback TAKE_PROFIT TP failed for {asset}: {e2}")

                # Fallback 2: closePosition TP_MARKET without quantity.
                try:
                    fallback_order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='TAKE_PROFIT_MARKET',
                            stopPrice=tp_price,
                            closePosition='true',
                            workingType='CONTRACT_PRICE',
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed fallback closePosition TP_MARKET for {asset} @ ${tp_price:.2f}")
                    return fallback_order
                except Exception as e2:
                    logger.warning(f"Fallback closePosition TP_MARKET failed for {asset}: {e2}")

                # Fallback 3: reduce-only LIMIT take-profit order.
                logger.warning(f"Trying final fallback reduce-only LIMIT TP for {asset}")
                try:
                    fallback_order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='LIMIT',
                            price=tp_price,
                            timeInForce='GTC',
                            quantity=quantity,
                            **({'reduceOnly': 'true'} if not position_side else {}),
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed fallback LIMIT TP for {asset}: {quantity} @ ${tp_price:.2f}")
                    return fallback_order
                except Exception as e2:
                    logger.error(f"Fallback LIMIT TP failed for {asset}: {e2}")
            if e.code == -2022 and position_side and quantity and quantity > 0:
                logger.warning(f"ReduceOnly rejected for {asset}, retrying TP with positionSide-only close semantics")
                try:
                    order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='TAKE_PROFIT',
                            stopPrice=tp_price,
                            price=tp_price,
                            timeInForce='GTC',
                            quantity=quantity,
                            positionSide=position_side
                        )
                    )
                    logger.info(f"Placed TP with positionSide fallback for {asset}: {quantity} @ ${tp_price:.2f}")
                    return order
                except Exception as e2:
                    logger.error(f"PositionSide TP fallback failed for {asset}: {e2}")
            # If quantity error, try with closePosition
            if e.code in [-2021, -4006]:  # Order would immediately trigger or insufficient position
                logger.info(f"Retrying TP with closePosition for {asset}")
                try:
                    order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='TAKE_PROFIT_MARKET',
                            stopPrice=tp_price,
                            closePosition='true',  # Close entire position
                            workingType='CONTRACT_PRICE'
                        )
                    )
                    logger.info(f"Placed TP order with closePosition for {asset} @ ${tp_price:.2f}")
                    return order
                except Exception as e2:
                    logger.error(f"Failed to place TP with closePosition: {e2}")
            raise

    async def place_stop_loss(self, asset: str, is_long: bool, quantity: float, sl_price: float):
        """Place a stop-loss order (closes position at stop price).
        
        Args:
            asset: Asset symbol
            is_long: True if closing a long position
            quantity: Position size to close (will be validated against actual position)
            sl_price: Stop-loss price
        """
        symbol = self._to_futures_symbol(asset)
        
        # Get current position size to ensure we're closing the right amount
        try:
            positions = await self._retry(lambda: self.client.futures_position_information(symbol=symbol))
            actual_position = None
            for pos in positions:
                if pos.get('symbol') == symbol:
                    pos_amt = float(pos.get('positionAmt', 0))
                    if abs(pos_amt) > 0:  # Has position
                        actual_position = abs(pos_amt)
                        break
        except Exception:
            actual_position = None
        
        # Use actual position size if available, otherwise use provided quantity
        if actual_position:
            quantity = actual_position
            logger.info(f"Using actual position size {quantity} for SL order")
        
        # Get symbol info for precision
        exchange_info = await self._retry(lambda: self.client.futures_exchange_info())
        symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
        
        if symbol_info:
            # Round price to tick size
            tick_size = float([f['tickSize'] for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'][0])
            sl_price = round(sl_price / tick_size) * tick_size
            sl_price = float(f"{sl_price:.{len(str(tick_size).split('.')[-1])}f}")
            
            # Round quantity
            step_size = float([f['stepSize'] for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
            quantity = round(quantity / step_size) * step_size
            quantity = float(f"{quantity:.{len(str(step_size).split('.')[-1])}f}")
        
        # Validate SL price is correct direction
        current_price = None
        try:
            ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
            current_price = float(ticker['price'])
            
            # For LONG position: SL should be BELOW current price
            # For SHORT position: SL should be ABOVE current price
            if is_long and sl_price >= current_price:
                logger.warning(f"SL price {sl_price} is not below current price {current_price} for long position. Adjusting...")
                sl_price = current_price * 0.99  # Set SL 1% below current as minimum
                sl_price = round(sl_price / tick_size) * tick_size
                sl_price = float(f"{sl_price:.{len(str(tick_size).split('.')[-1])}f}")
            elif not is_long and sl_price <= current_price:
                logger.warning(f"SL price {sl_price} is not above current price {current_price} for short position. Adjusting...")
                sl_price = current_price * 1.01  # Set SL 1% above current as minimum
                sl_price = round(sl_price / tick_size) * tick_size
                sl_price = float(f"{sl_price:.{len(str(tick_size).split('.')[-1])}f}")
        except Exception as e:
            logger.warning(f"Could not validate SL price against current price: {e}")
        
        try:
            # Close position: if long, sell to close; if short, buy to close
            side = 'SELL' if is_long else 'BUY'
            position_side = await self._get_active_position_side(symbol)
            
            # Use closePosition=true for more reliable execution
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'STOP_MARKET',
                'stopPrice': sl_price,
                'workingType': 'CONTRACT_PRICE'  # Use contract price for trigger
            }
            if position_side:
                order_params['positionSide'] = position_side
            else:
                order_params['reduceOnly'] = True
            
            # Use quantity if specified and valid
            if quantity and quantity > 0:
                order_params['quantity'] = quantity
            
            order = await self._retry(
                lambda: self.client.futures_create_order(**order_params)
            )
            price_info = f" (current: ${current_price:.2f})" if current_price else ""
            logger.info(f"Placed SL order for {asset}: {quantity} @ ${sl_price:.2f}{price_info}")
            return order
        except BinanceAPIException as e:
            logger.error(f"Failed to place SL order for {asset}: {e.message} (code: {e.code})")
            # Some symbols/endpoints reject STOP_MARKET on this route (-4120).
            # Fallback to a reduce-only STOP order with stop+limit.
            if e.code == -4120:
                logger.warning(f"STOP_MARKET not supported for {asset}, falling back to reduce-only STOP order")
                try:
                    # Use a tiny limit offset to improve trigger fill probability.
                    limit_price = sl_price * (0.999 if is_long else 1.001)
                    if symbol_info:
                        tick_size = float([f['tickSize'] for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'][0])
                        limit_price = round(limit_price / tick_size) * tick_size
                        limit_price = float(f"{limit_price:.{len(str(tick_size).split('.')[-1])}f}")
                    fallback_order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='STOP',
                            stopPrice=sl_price,
                            price=limit_price,
                            timeInForce='GTC',
                            quantity=quantity,
                            **({'reduceOnly': 'true'} if not position_side else {}),
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed fallback STOP SL for {asset}: {quantity} stop=${sl_price:.2f} limit=${limit_price:.2f}")
                    return fallback_order
                except Exception as e2:
                    logger.warning(f"Fallback STOP SL failed for {asset}: {e2}")

                # Fallback: closePosition stop-market path.
                try:
                    fallback_order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='STOP_MARKET',
                            stopPrice=sl_price,
                            closePosition='true',
                            workingType='CONTRACT_PRICE',
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed fallback closePosition STOP_MARKET for {asset} @ ${sl_price:.2f}")
                    return fallback_order
                except Exception as e2:
                    logger.error(f"Fallback closePosition STOP_MARKET failed for {asset}: {e2}")
            if e.code == -2022 and position_side and quantity and quantity > 0:
                logger.warning(f"ReduceOnly rejected for {asset}, retrying SL with positionSide-only close semantics")
                try:
                    limit_price = sl_price * (0.999 if is_long else 1.001)
                    if symbol_info:
                        tick_size = float([f['tickSize'] for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'][0])
                        limit_price = round(limit_price / tick_size) * tick_size
                        limit_price = float(f"{limit_price:.{len(str(tick_size).split('.')[-1])}f}")
                    order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='STOP',
                            stopPrice=sl_price,
                            price=limit_price,
                            timeInForce='GTC',
                            quantity=quantity,
                            positionSide=position_side
                        )
                    )
                    logger.info(f"Placed SL with positionSide fallback for {asset}: stop=${sl_price:.2f} limit=${limit_price:.2f}")
                    return order
                except Exception as e2:
                    logger.error(f"PositionSide SL fallback failed for {asset}: {e2}")
            # If quantity error, try with closePosition
            if e.code in [-2021, -4006]:  # Order would immediately trigger or insufficient position
                logger.info(f"Retrying SL with closePosition for {asset}")
                try:
                    order = await self._retry(
                        lambda: self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type='STOP_MARKET',
                            stopPrice=sl_price,
                            closePosition='true',  # Close entire position
                            workingType='CONTRACT_PRICE',
                            **({'positionSide': position_side} if position_side else {})
                        )
                    )
                    logger.info(f"Placed SL order with closePosition for {asset} @ ${sl_price:.2f}")
                    return order
                except Exception as e2:
                    logger.error(f"Failed to place SL with closePosition: {e2}")
            raise

    def extract_oids(self, order_result: Dict) -> List[str]:
        """Extract order IDs from Binance order response."""
        if isinstance(order_result, dict) and 'orderId' in order_result:
            return [str(order_result['orderId'])]
        return []

    async def _get_v3_position_risk(self, symbol: Optional[str] = None) -> List[Dict]:
        """Get positions using /fapi/v3/positionRisk endpoint (returns positionInitialMargin).
        
        This endpoint provides positionInitialMargin which is more accurate than v2.
        v3 returns both positions with open orders, while v2 only returns held positions.
        
        Args:
            symbol: Optional symbol filter (e.g., 'BTCUSDT'). If provided, only returns positions for that symbol.
        """
        try:
            def _make_request():
                # Build query string with timestamp
                timestamp = int(time.time() * 1000)
                params = {'timestamp': timestamp, 'recvWindow': 60000}
                
                # Add symbol parameter if provided
                if symbol:
                    params['symbol'] = symbol
                
                query_string = urllib.parse.urlencode(params)
                
                # Create signature
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                # Add signature to params
                params['signature'] = signature
                
                # Build URL
                base_url = 'https://testnet.binancefuture.com' if self.testnet else 'https://fapi.binance.com'
                url = f"{base_url}/fapi/v3/positionRisk?{urllib.parse.urlencode(params)}"
                
                # Make request
                response = requests.get(
                    url,
                    headers={'X-MBX-APIKEY': self.api_key},
                    timeout=10
                )
                response.raise_for_status()
                return response.json()
            
            # Run in thread pool to avoid blocking
            return await asyncio.to_thread(_make_request)
        except Exception as e:
            logger.warning(f"Error fetching v3 positionRisk, falling back to v2: {e}")
            # Fallback to v2 if v3 fails
            if symbol:
                return await self._retry(lambda: self.client.futures_position_information(symbol=symbol))
            return await self._retry(lambda: self.client.futures_position_information())

    async def get_position(self, symbol: str = 'BTCUSDT', asset: Optional[str] = None) -> Dict[str, Any]:
        """Get position information for a specific symbol with calculated fields.
        
        This function gets information about both positions held and open orders.
        Note: /fapi/v2/positionRisk only returns held positions, but /fapi/v3/positionRisk
        returns both positions with open orders.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Defaults to 'BTCUSDT'.
                   If 'asset' parameter is provided, this will be ignored.
            asset: Asset name (e.g., 'BTC', 'ETH'). If provided, will be converted to symbol format.
            
        Returns:
            Dictionary containing position information with calculated fields (leverage, ROI, size),
            or empty dict if no position found.
            Includes:
            - All fields from Binance API response
            - Calculated leverage (from notional/positionInitialMargin)
            - Calculated ROI percentage (unRealizedProfit/positionInitialMargin * 100)
            - Size (positionAmt, absolute size)
            - Open time (from trade history if available, otherwise updateTime)
        """
        try:
            # Convert asset name to symbol if provided
            if asset:
                symbol = self._to_futures_symbol(asset)
            
            # Get positions using v3 endpoint (supports symbol filter)
            positions = await self._get_v3_position_risk(symbol=symbol)
            
            # Filter by symbol (in case multiple positions returned)
            pos = None
            for p in positions:
                if p.get('symbol') == symbol:
                    pos = p
                    break
            
            if not pos:
                return {}
            
            # Extract and calculate fields
            position_amt = float(pos.get('positionAmt', 0))
            entry_price = float(pos.get('entryPrice', 0))
            current_price = float(pos.get('markPrice', 0))
            unrealized_pnl = float(pos.get('unRealizedProfit', 0))
            
            # Get notional value
            notional = pos.get('notional')
            if notional:
                try:
                    notional = float(notional)
                except (ValueError, TypeError):
                    notional = abs(position_amt) * current_price if current_price > 0 else 0.0
            else:
                notional = abs(position_amt) * current_price if current_price > 0 else 0.0
            
            # Get positionInitialMargin (most accurate)
            position_initial_margin = pos.get('positionInitialMargin') or pos.get('initialMargin')
            if position_initial_margin:
                try:
                    position_initial_margin = float(position_initial_margin)
                except (ValueError, TypeError):
                    position_initial_margin = 0.0
            else:
                position_initial_margin = 0.0
            
            # Calculate leverage: leverage = abs(notional) / positionInitialMargin
            calculated_leverage = None
            if position_initial_margin and position_initial_margin > 0:
                calculated_leverage = abs(notional) / position_initial_margin
            
            # Try to get leverage from API (v2 might have it)
            api_leverage = pos.get('leverage')
            if api_leverage:
                try:
                    api_leverage = float(api_leverage)
                except (ValueError, TypeError):
                    api_leverage = None
            
            # Use calculated leverage if API doesn't provide it
            leverage = api_leverage if api_leverage else calculated_leverage
            
            # Calculate ROI: ROI (%) = (unRealizedProfit / positionInitialMargin) * 100
            roi_percent = 0.0
            if position_initial_margin and position_initial_margin > 0:
                roi_percent = (unrealized_pnl / position_initial_margin) * 100
            
            # Get update time
            update_time = pos.get('updateTime')
            if update_time:
                try:
                    update_time = int(update_time)
                except (ValueError, TypeError):
                    update_time = None
            else:
                update_time = None
            
            # Try to get actual open time from trade history
            opened_at = await self._get_position_open_time(symbol, position_amt)
            if not opened_at:
                opened_at = update_time  # Fallback to updateTime
            
            # Enrich position with calculated fields
            enriched_pos = dict(pos)  # Copy original response
            enriched_pos.update({
                'leverage': leverage,  # Calculated or from API
                'calculatedLeverage': calculated_leverage,  # Calculated value
                'roi': roi_percent,  # ROI percentage
                'roiPercent': roi_percent,  # Frontend format
                'size': abs(position_amt),  # Absolute size
                'quantity': abs(position_amt),  # Alias
                'notional': notional,  # Notional value
                'positionInitialMargin': position_initial_margin,  # Initial margin
                'openedAt': opened_at,  # Actual open time (from trade history or updateTime)
                'openTime': opened_at,  # Alternative format
                'updateTime': update_time,  # Last update time
            })
            
            return enriched_pos
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return {}
    
    async def _get_position_open_time(self, symbol: str, position_amt: float) -> Optional[int]:
        """Get the actual open time for a position from trade history.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            position_amt: Current position amount (positive for long, negative for short)
            
        Returns:
            Timestamp in milliseconds of when position was opened, or None if not found.
        """
        try:
            # Get recent trades for this symbol
            trades = await self._retry(lambda: self.client.futures_account_trades(symbol=symbol, limit=100))
            
            if not trades or not isinstance(trades, list):
                return None
            
            # Determine position side
            is_long = position_amt > 0
            
            # Find the first trade that opened this position
            # For long positions, look for BUY trades
            # For short positions, look for SELL trades
            # Sort by time ascending to find the earliest trade
            relevant_trades = []
            for trade in trades:
                trade_side = trade.get('side', '').upper()
                if (is_long and trade_side == 'BUY') or (not is_long and trade_side == 'SELL'):
                    relevant_trades.append(trade)
            
            if not relevant_trades:
                return None
            
            # Sort by time (ascending) to get the earliest trade
            relevant_trades.sort(key=lambda t: int(t.get('time', 0)))
            
            # Return the timestamp of the first trade
            first_trade_time = relevant_trades[0].get('time')
            if first_trade_time:
                try:
                    return int(first_trade_time)
                except (ValueError, TypeError):
                    return None
            
            return None
        except Exception as e:
            logger.debug(f"Could not get open time from trade history for {symbol}: {e}")
            return None

    async def get_user_state(self) -> Dict[str, Any]:
        """Get account balance and positions."""
        try:
            account = await self._retry(lambda: self.client.futures_account())
            # Use v3 endpoint to get positionInitialMargin directly
            positions = await self._get_v3_position_risk()
            
            # Account-level totals can include multi-asset mode (BNB/BTC/USDC valued in USDT).
            # Spot is never in fapi — but we still pin hive/UI equity to USDT-M USDT only.
            all_assets_wallet = float(account.get('totalWalletBalance', 0) or 0)
            all_assets_margin = float(account.get('totalMarginBalance', 0) or 0)
            balance = all_assets_wallet
            total_value = all_assets_margin
            
            # Get all asset balances using the balance endpoint (more reliable for getting all assets)
            # This endpoint returns ALL assets (USDT, USDC, BTC, etc.) that have balances
            # According to Binance docs: GET /fapi/v2/balance or /fapi/v3/balance
            asset_balances = []
            try:
                # Use futures_account_balance() which calls /fapi/v2/balance or /fapi/v3/balance endpoint
                # This returns all asset balances including USDT, USDC, BTC, etc.
                # Response format: [{"asset": "USDT", "balance": "...", "crossWalletBalance": "...", ...}, ...]
                # Check if method exists (should exist in python-binance)
                if not hasattr(self.client, 'futures_account_balance'):
                    logger.warning("futures_account_balance() method not found on client, trying alternative approach")
                    raise AttributeError("futures_account_balance method not available")
                
                balances_response = await self._retry(lambda: self.client.futures_account_balance())
                
                logger.debug(f"Binance balances response type: {type(balances_response)}, length: {len(balances_response) if isinstance(balances_response, list) else 'N/A'}")
                
                if isinstance(balances_response, list):
                    for balance_data in balances_response:
                        asset_name = balance_data.get('asset', '')
                        if not asset_name:
                            continue
                            
                        # Get balance values - handle both string and numeric formats
                        # According to Binance docs, values are returned as strings
                        balance_str = balance_data.get('balance', '0')
                        available_balance_str = balance_data.get('availableBalance', '0')
                        cross_wallet_balance_str = balance_data.get('crossWalletBalance', '0')
                        cross_unrealized_pnl_str = balance_data.get('crossUnPnl', '0')
                        
                        wallet_balance = float(balance_str) if balance_str else 0.0
                        available_balance = float(available_balance_str) if available_balance_str else 0.0
                        cross_wallet_balance = float(cross_wallet_balance_str) if cross_wallet_balance_str else 0.0
                        cross_unrealized_pnl = float(cross_unrealized_pnl_str) if cross_unrealized_pnl_str else 0.0
                        
                        # Include all assets with non-zero balance (USDT, USDC, BTC, etc.)
                        # Use a small threshold (0.00000001) to avoid floating point precision issues
                        if abs(wallet_balance) > 1e-8 or abs(cross_wallet_balance) > 1e-8:
                            asset_balances.append({
                                'asset': asset_name,
                                'walletBalance': wallet_balance,
                                'availableBalance': available_balance,
                                'crossWalletBalance': cross_wallet_balance,
                                'crossUnPnl': cross_unrealized_pnl,
                                'positionValue': cross_wallet_balance,  # For compatibility with frontend
                            })
                            logger.debug(f"Added asset balance: {asset_name} - wallet: {wallet_balance}, cross: {cross_wallet_balance}")
                
                logger.info(f"Fetched {len(asset_balances)} asset balances from Binance: {[b['asset'] for b in asset_balances]}")
                
            except Exception as e:
                logger.error(f"Failed to fetch asset balances from balance endpoint: {e}")
                import traceback
                logger.error(traceback.format_exc())
                
                # Fallback to account endpoint if balance endpoint fails
                logger.info("Falling back to account endpoint for asset balances...")
                if 'assets' in account and isinstance(account['assets'], list):
                    for asset_data in account['assets']:
                        asset_name = asset_data.get('asset', '')
                        if not asset_name:
                            continue
                            
                        wallet_balance = float(asset_data.get('walletBalance', 0) or 0)
                        available_balance = float(asset_data.get('availableBalance', 0) or 0)
                        cross_wallet_balance = float(asset_data.get('crossWalletBalance', 0) or 0)
                        cross_unrealized_pnl = float(asset_data.get('crossUnPnl', 0) or 0)
                        
                        # Only include assets with non-zero balance
                        if abs(wallet_balance) > 1e-8 or abs(cross_wallet_balance) > 1e-8:
                            asset_balances.append({
                                'asset': asset_name,
                                'walletBalance': wallet_balance,
                                'availableBalance': available_balance,
                                'crossWalletBalance': cross_wallet_balance,
                                'crossUnPnl': cross_unrealized_pnl,
                                'positionValue': cross_wallet_balance,  # For compatibility with frontend
                            })
                    logger.info(f"Fetched {len(asset_balances)} asset balances from account endpoint: {[b['asset'] for b in asset_balances]}")

            # Prefer USDT-M USDT only for balance/total_value (Confetti hive equity, Made, etc.).
            usdt_row = next((b for b in asset_balances if str(b.get("asset") or "").upper() == "USDT"), None)
            if usdt_row:
                usdt_wallet = float(usdt_row.get("walletBalance") or usdt_row.get("crossWalletBalance") or 0)
                usdt_cross = float(usdt_row.get("crossWalletBalance") or usdt_wallet)
                usdt_upl = float(usdt_row.get("crossUnPnl") or 0)
                balance = usdt_wallet
                # Margin equity in USDT = cross wallet + USDT unrealized (not BNB/BTC/USDC).
                total_value = usdt_cross + usdt_upl
                if abs(all_assets_margin - total_value) > 1.0:
                    logger.info(
                        "Binance equity USDT-only $%.2f (account totalMarginBalance was $%.2f — other assets excluded)",
                        total_value,
                        all_assets_margin,
                    )
            
            # Format positions
            enriched_positions = []
            for pos in positions:
                position_amt = float(pos.get('positionAmt', 0))
                if abs(position_amt) > 0:  # Only include open positions
                    entry_price = float(pos.get('entryPrice', 0))
                    current_price = float(pos.get('markPrice', 0))
                    unrealized_pnl = float(pos.get('unRealizedProfit', 0))
                    liquidation_price = float(pos.get('liquidationPrice', 0)) if pos.get('liquidationPrice') else None
                    
                    # Get notional value from API (most accurate)
                    notional = pos.get('notional')
                    if notional:
                        try:
                            notional = float(notional)
                        except (ValueError, TypeError):
                            # Calculate notional: notional = abs(positionAmt) * markPrice
                            notional = abs(position_amt) * current_price if current_price > 0 else 0.0
                    else:
                        # Calculate notional: notional = abs(positionAmt) * markPrice
                        notional = abs(position_amt) * current_price if current_price > 0 else 0.0
                    
                    # Get actual initial margin from Binance v3 positionRisk endpoint
                    # v3 endpoint provides positionInitialMargin directly (most accurate)
                    # IMPORTANT: Do NOT use isolatedMargin as fallback - it's the CURRENT margin, not INITIAL margin
                    # isolatedMargin can change due to funding fees, making ROI calculations incorrect
                    position_initial_margin = pos.get('positionInitialMargin') or pos.get('initialMargin')
                    if position_initial_margin:
                        try:
                            position_initial_margin = float(position_initial_margin)
                        except (ValueError, TypeError):
                            position_initial_margin = 0.0
                    else:
                        # Fallback: calculate from notional value and leverage
                        # For Binance USDT-M futures: notional = abs(positionAmt) * markPrice
                        # initialMargin = abs(notional) / leverage
                        position_initial_margin = 0.0
                    
                    # Calculate leverage: leverage = abs(notional) / positionInitialMargin
                    # According to Binance docs: leverage is not directly in v3 response, calculate it
                    actual_leverage = None
                    if position_initial_margin and position_initial_margin > 0:
                        actual_leverage = abs(notional) / position_initial_margin
                    else:
                        # Try to get leverage from position data (v2 might have it)
                        leverage_from_pos = pos.get('leverage')
                        if leverage_from_pos:
                            try:
                                actual_leverage = float(leverage_from_pos)
                            except (ValueError, TypeError):
                                actual_leverage = None
                    
                    # Calculate ROI (Return on Investment): ROI (%) = (unRealizedProfit / positionInitialMargin) * 100
                    roi_percent = 0.0
                    if position_initial_margin and position_initial_margin > 0:
                        roi_percent = (unrealized_pnl / position_initial_margin) * 100
                    
                    # Get update time (last update timestamp)
                    update_time = pos.get('updateTime')
                    if update_time:
                        try:
                            update_time = int(update_time)
                        except (ValueError, TypeError):
                            update_time = None
                    else:
                        update_time = None
                    
                    # Try to get actual open time from trade history
                    symbol_for_trades = pos.get('symbol', '')
                    opened_at = await self._get_position_open_time(symbol_for_trades, position_amt)
                    if not opened_at:
                        opened_at = update_time  # Fallback to updateTime
                    
                    asset = self._from_futures_symbol(symbol_for_trades)
                    enriched_positions.append({
                        'coin': asset,
                        'symbol': asset,  # Add both for compatibility
                        'szi': position_amt,  # Positive = long, negative = short (SIZE)
                        'size': abs(position_amt),  # Absolute size for display
                        'quantity': abs(position_amt),  # Absolute size
                        'positionAmt': position_amt,  # Original signed value
                        'entryPx': entry_price,
                        'entryPrice': entry_price,  # Add both formats
                        'pnl': unrealized_pnl,
                        'unrealized_pnl': unrealized_pnl,  # Add both formats
                        'unrealizedPnl': unrealized_pnl,  # Frontend format
                        'roi': roi_percent,  # ROI percentage
                        'roiPercent': roi_percent,  # Frontend format
                        'current_price': current_price,
                        'currentPrice': current_price,  # Frontend format
                        'markPrice': current_price,  # Add both formats
                        'liquidation_price': liquidation_price,
                        'liquidationPrice': liquidation_price,  # Add both formats
                        'liquidationPx': liquidation_price,  # Hyperliquid format
                        'leverage': actual_leverage,  # Calculated or from API
                        'positionInitialMargin': position_initial_margin,  # Actual margin used
                        'initialMargin': position_initial_margin,  # Alias for compatibility
                        'notional': notional,  # Notional value
                        'updateTime': update_time,  # Last update timestamp (ms)
                        'openedAt': opened_at,  # Actual open time (from trade history or updateTime)
                        'openTime': opened_at,  # Alternative format
                        'update_time': update_time  # Alternative format
                    })
            
            return {
                'balance': balance,
                'total_value': total_value,
                'total_value_all_assets': all_assets_margin,
                'positions': enriched_positions,
                'asset_balances': asset_balances  # Include all asset balances
            }
        except Exception as e:
            logger.error(f"Error getting user state: {e}")
            return {
                'balance': 0,
                'total_value': 0,
                'total_value_all_assets': 0,
                'positions': [],
                'asset_balances': [],
            }

    async def get_current_price(self, asset: str) -> float:
        """Get current mark price for an asset."""
        symbol = self._to_futures_symbol(asset)
        try:
            ticker = await self._retry(lambda: self.client.futures_symbol_ticker(symbol=symbol))
            return float(ticker['price'])
        except Exception as e:
            logger.error(f"Error getting price for {asset}: {e}")
            return 0.0

    async def get_open_orders(self) -> List[Dict]:
        """Get all open orders."""
        try:
            orders = await self._retry(lambda: self.client.futures_get_open_orders())
            formatted_orders = []
            for o in orders:
                asset = self._from_futures_symbol(o.get('symbol', ''))
                formatted_orders.append({
                    'coin': asset,
                    'oid': str(o.get('orderId', '')),
                    'isBuy': o.get('side') == 'BUY',
                    'sz': float(o.get('origQty', 0)),
                    'px': float(o.get('price', 0)),
                    'triggerPx': float(o.get('stopPrice', 0)),
                    'orderType': o.get('type', '')
                })
            return formatted_orders
        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []

    async def get_recent_fills(self, limit: int = 50) -> List[Dict]:
        """Get recent trade history with all fields from Binance API."""
        try:
            trades = await self._retry(lambda: self.client.futures_account_trades())
            formatted_trades = []
            for t in trades[-limit:]:
                asset = self._from_futures_symbol(t.get('symbol', ''))
                # Preserve all fields from Binance API response
                formatted_trades.append({
                    # Original Binance fields
                    'id': t.get('id'),
                    'tradeId': t.get('id'),
                    'symbol': t.get('symbol', ''),
                    'coin': asset,
                    'asset': asset,
                    'side': t.get('side', ''),
                    'isBuy': t.get('side') == 'BUY',
                    'positionSide': t.get('positionSide', ''),
                    'price': float(t.get('price', 0)),
                    'px': float(t.get('price', 0)),
                    'qty': float(t.get('qty', 0)),
                    'sz': float(t.get('qty', 0)),
                    'size': float(t.get('qty', 0)),
                    'quoteQty': float(t.get('quoteQty', 0)),
                    'time': t.get('time', 0),
                    'timestamp': t.get('time', 0),
                    'orderId': t.get('orderId'),
                    'maker': t.get('maker', False),
                    'buyer': t.get('buyer', False),
                    'realizedPnl': float(t.get('realizedPnl', 0)) if t.get('realizedPnl') is not None else None,
                    'realized_pnl': float(t.get('realizedPnl', 0)) if t.get('realizedPnl') is not None else None,
                    'commission': float(t.get('commission', 0)) if t.get('commission') else 0,
                    'fee': float(t.get('commission', 0)) if t.get('commission') else 0,
                    'commissionAsset': t.get('commissionAsset', 'USDT'),
                })
            return formatted_trades
        except Exception as e:
            logger.error(f"Error getting recent fills: {e}")
            return []

    async def get_open_interest(self, asset: str) -> Optional[float]:
        """Get open interest for an asset."""
        symbol = self._to_futures_symbol(asset)
        try:
            oi_data = await self._retry(lambda: self.client.futures_open_interest(symbol=symbol))
            return float(oi_data.get('openInterest', 0))
        except Exception as e:
            logger.error(f"Error getting open interest for {asset}: {e}")
            return None

    async def get_funding_rate(self, asset: str) -> Optional[float]:
        """Get current funding rate for an asset."""
        symbol = self._to_futures_symbol(asset)
        try:
            funding = await self._retry(lambda: self.client.futures_funding_rate(symbol=symbol, limit=1))
            if funding and len(funding) > 0:
                return float(funding[0].get('fundingRate', 0))
            return None
        except Exception as e:
            logger.error(f"Error getting funding rate for {asset}: {e}")
            return None

    async def cancel_order(self, asset: str, oid: str):
        """Cancel a specific order."""
        symbol = self._to_futures_symbol(asset)
        try:
            return await self._retry(
                lambda: self.client.futures_cancel_order(symbol=symbol, orderId=int(oid))
            )
        except Exception as e:
            logger.error(f"Error cancelling order {oid} for {asset}: {e}")
            raise

    async def cancel_all_orders(self, asset: str):
        """Cancel all open orders for an asset."""
        symbol = self._to_futures_symbol(asset)
        try:
            return await self._retry(
                lambda: self.client.futures_cancel_all_open_orders(symbol=symbol)
            )
        except Exception as e:
            logger.error(f"Error cancelling all orders for {asset}: {e}")
            raise

    async def round_size(self, asset: str, amount: float) -> float:
        """Round order size to asset precision based on LOT_SIZE filter.
        
        Args:
            asset: Asset symbol (e.g., 'BTC')
            amount: Desired size before rounding
            
        Returns:
            Rounded size
        """
        symbol = self._to_futures_symbol(asset)
        try:
            exchange_info = await self._retry(lambda: self.client.futures_exchange_info())
            symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
            
            if symbol_info:
                # Get step size from LOT_SIZE filter
                step_size = None
                for filt in symbol_info.get('filters', []):
                    if filt.get('filterType') == 'LOT_SIZE':
                        step_size = float(filt.get('stepSize', '0.001'))
                        break
                
                if step_size and step_size > 0:
                    # Round down to step size multiple
                    rounded = (amount // step_size) * step_size
                    # Format to appropriate precision
                    precision = len(str(step_size).split('.')[-1].rstrip('0'))
                    return round(rounded, precision)
            
            # Fallback: round to 3 decimals
            logger.warning(f"⚠️  Precision not found for {symbol}, using default 3 decimals")
            return round(amount, 3)
        except Exception as e:
            logger.error(f"Error rounding size for {asset}: {e}")
            return round(amount, 3)

