"""Alpaca Trading API client (stocks + crypto) with the same interface as Binance/Aster."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from src.config_loader import CONFIG

logger = logging.getLogger(__name__)

# Common crypto tickers Alpaca supports as BTC/USD-style pairs.
DEFAULT_CRYPTO_ASSETS = {
    "BTC", "ETH", "SOL", "DOGE", "AVAX", "XRP", "LINK", "LTC", "BCH", "UNI",
    "AAVE", "DOT", "MATIC", "SHIB", "BNB", "ADA", "XLM", "ATOM", "NEAR", "APT",
}


class AlpacaAPI:
    """Client for Alpaca paper/live trading (US stocks + crypto)."""

    def __init__(self):
        api_key = CONFIG.get("alpaca_api_key")
        api_secret = CONFIG.get("alpaca_api_secret")
        paper = bool(CONFIG.get("alpaca_paper", True))

        if not api_key or not api_secret:
            raise ValueError("ALPACA_API_KEY and ALPACA_API_SECRET must be set in .env")

        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.trading_base = (
            CONFIG.get("alpaca_paper_url", "https://paper-api.alpaca.markets")
            if paper
            else CONFIG.get("alpaca_live_url", "https://api.alpaca.markets")
        )
        self.data_base = CONFIG.get("alpaca_data_url", "https://data.alpaca.markets")
        self.default_leverage = int(CONFIG.get("alpaca_leverage", CONFIG.get("default_leverage", 1)) or 1)
        self._headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._crypto_assets = self._load_crypto_asset_set()

        # Validate credentials early
        account = self._request("GET", "/v2/account")
        status = account.get("status", "unknown")
        cash = account.get("cash", "?")
        logger.info(
            f"Alpaca client initialized (paper={paper}, status={status}, cash=${cash}, "
            f"leverage_hint={self.default_leverage}x)"
        )

    def _load_crypto_asset_set(self) -> set:
        crypto = set(DEFAULT_CRYPTO_ASSETS)
        raw = CONFIG.get("crypto_assets") or ""
        for part in raw.replace(",", " ").split():
            sym = part.strip().upper().replace("/USD", "").replace("USD", "")
            if sym:
                crypto.add(sym)
        return crypto

    def is_crypto(self, asset: str) -> bool:
        base = self._base_asset(asset)
        # Explicit stock list wins
        stock_raw = CONFIG.get("stock_assets") or ""
        stocks = {p.strip().upper() for p in stock_raw.replace(",", " ").split() if p.strip()}
        if base in stocks:
            return False
        return base in self._crypto_assets

    def _base_asset(self, asset: str) -> str:
        a = (asset or "").strip().upper()
        a = a.replace("/USDT", "").replace("/USD", "").replace("USDT", "").replace("-", "/")
        if "/" in a:
            return a.split("/")[0]
        return a

    def _to_symbol(self, asset: str) -> str:
        """Map bot asset name to Alpaca symbol (AAPL or BTC/USD)."""
        base = self._base_asset(asset)
        if self.is_crypto(base):
            return f"{base}/USD"
        return base

    def _from_symbol(self, symbol: str) -> str:
        s = (symbol or "").upper()
        if "/" in s:
            return s.split("/")[0]
        return s.replace("USD", "").replace("USDT", "")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        base: Optional[str] = None,
        max_attempts: int = 3,
    ) -> Any:
        url = f"{(base or self.trading_base).rstrip('/')}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(max_attempts):
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._headers,
                    params=params,
                    json=json_body,
                    timeout=20,
                )
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                    wait = 0.5 * (2 ** attempt)
                    logger.warning(f"Alpaca {resp.status_code} on {path}, retry in {wait:.1f}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    detail = resp.text[:400]
                    raise RuntimeError(f"Alpaca {method} {path} failed ({resp.status_code}): {detail}")
                if resp.status_code == 204 or not resp.content:
                    return {}
                return resp.json()
            except Exception as e:
                last_err = e
                if attempt < max_attempts - 1 and not isinstance(e, RuntimeError):
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                raise
        raise last_err or RuntimeError("Alpaca request failed")

    async def _retry(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def set_leverage(self, asset: str, leverage: int):
        """Alpaca does not expose per-symbol leverage like crypto futures — no-op."""
        logger.info(
            f"Alpaca set_leverage({asset}, {leverage}x): ignored "
            "(stocks/crypto spot use account buying power, not futures leverage)"
        )

    def _round_qty(self, asset: str, qty: float) -> float:
        qty = abs(float(qty or 0))
        if qty <= 0:
            return 0.0
        if self.is_crypto(asset):
            return float(f"{qty:.6f}")
        # Fractional shares supported; keep 4 dp for most equities
        return float(f"{qty:.4f}")

    async def place_buy_order(
        self,
        asset: str,
        amount: float,
        leverage: Optional[int] = None,
        slippage: float = 0.01,
        reduce_only: bool = False,
    ):
        """Market buy (open/add long, or cover short)."""
        _ = leverage, slippage
        symbol = self._to_symbol(asset)
        qty = self._round_qty(asset, amount)
        if qty <= 0:
            raise ValueError(f"Invalid buy qty for {asset}: {amount}")

        if reduce_only:
            # Close short / reduce: prefer close endpoint for full flatten when sizes match
            pos = await self._get_raw_position(symbol)
            if pos and float(pos.get("qty", 0) or 0) < 0:
                close_qty = min(qty, abs(float(pos.get("qty", 0))))
                body = {
                    "symbol": symbol,
                    "qty": str(close_qty),
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "gtc" if self.is_crypto(asset) else "day",
                }
                order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
                logger.info(f"Alpaca reduce-only BUY {symbol} qty={close_qty}")
                return order

        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": "buy",
            "type": "market",
            "time_in_force": "gtc" if self.is_crypto(asset) else "day",
        }
        order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
        logger.info(f"Alpaca BUY {symbol} qty={qty} id={order.get('id')}")
        return order

    async def place_sell_order(
        self,
        asset: str,
        amount: float,
        leverage: Optional[int] = None,
        slippage: float = 0.01,
        reduce_only: bool = False,
    ):
        """Market sell (open/add short, or close long)."""
        _ = leverage, slippage
        symbol = self._to_symbol(asset)
        qty = self._round_qty(asset, amount)
        if qty <= 0:
            raise ValueError(f"Invalid sell qty for {asset}: {amount}")

        if reduce_only:
            pos = await self._get_raw_position(symbol)
            if pos and float(pos.get("qty", 0) or 0) > 0:
                held = abs(float(pos.get("qty", 0)))
                # Full flatten when requested size covers the position
                if qty >= held * 0.99:
                    closed = await self._retry(
                        lambda: self._request("DELETE", f"/v2/positions/{symbol}")
                    )
                    logger.info(f"Alpaca CLOSE POSITION {symbol} (reduce-only flatten)")
                    return closed
                body = {
                    "symbol": symbol,
                    "qty": str(min(qty, held)),
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "gtc" if self.is_crypto(asset) else "day",
                }
                order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
                logger.info(f"Alpaca reduce-only SELL {symbol} qty={min(qty, held)}")
                return order

        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": "sell",
            "type": "market",
            "time_in_force": "gtc" if self.is_crypto(asset) else "day",
        }
        order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
        logger.info(f"Alpaca SELL {symbol} qty={qty} id={order.get('id')}")
        return order

    async def place_take_profit(self, asset: str, is_long: bool, quantity: float, tp_price: float):
        """Place a limit order that takes profit (sell for long, buy for short).

        ``quantity`` is honored when smaller than the open position (scale-out).
        """
        symbol = self._to_symbol(asset)
        qty = self._round_qty(asset, quantity)
        pos = await self._get_raw_position(symbol)
        if pos:
            pos_qty = self._round_qty(asset, abs(float(pos.get("qty", 0) or 0)))
            if qty <= 0 or qty > pos_qty:
                qty = pos_qty
        if qty <= 0:
            raise ValueError(f"No qty for TP on {asset}")

        side = "sell" if is_long else "buy"
        tif = "gtc" if self.is_crypto(asset) else "day"
        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "limit",
            "limit_price": str(round(float(tp_price), 4)),
            "time_in_force": tif,
        }
        order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
        logger.info(f"Alpaca TP {side.upper()} {symbol} @ {tp_price} qty={qty}")
        return order

    async def place_stop_loss(self, asset: str, is_long: bool, quantity: float, sl_price: float):
        """Place a stop order that cuts losses."""
        symbol = self._to_symbol(asset)
        qty = self._round_qty(asset, quantity)
        pos = await self._get_raw_position(symbol)
        if pos:
            pos_qty = self._round_qty(asset, abs(float(pos.get("qty", 0) or 0)))
            if qty <= 0 or qty > pos_qty:
                qty = pos_qty
        if qty <= 0:
            raise ValueError(f"No qty for SL on {asset}")

        side = "sell" if is_long else "buy"
        tif = "gtc" if self.is_crypto(asset) else "day"
        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "stop",
            "stop_price": str(round(float(sl_price), 4)),
            "time_in_force": tif,
        }
        order = await self._retry(lambda: self._request("POST", "/v2/orders", json_body=body))
        logger.info(f"Alpaca SL {side.upper()} {symbol} @ {sl_price} qty={qty}")
        return order

    def extract_oids(self, order_result: Dict) -> List[str]:
        if not order_result:
            return []
        oid = order_result.get("id") or order_result.get("order_id") or order_result.get("client_order_id")
        return [str(oid)] if oid else []

    def _get_raw_position(self, symbol: str) -> Optional[Dict]:
        try:
            return self._request("GET", f"/v2/positions/{symbol}")
        except Exception:
            return None

    async def _get_raw_position_async(self, symbol: str) -> Optional[Dict]:
        return await self._retry(lambda: self._get_raw_position(symbol))

    @staticmethod
    def _bar_timeframe(interval: str) -> str:
        """Map bot intervals to Alpaca stock bar timeframes."""
        m = {
            "1m": "1Min",
            "5m": "5Min",
            "15m": "15Min",
            "30m": "30Min",
            "1h": "1Hour",
            "4h": "4Hour",
            "1d": "1Day",
        }
        return m.get((interval or "15m").lower(), "15Min")

    async def get_stock_bars(
        self,
        asset: str,
        interval: str = "15m",
        limit: int = 100,
    ) -> List[List[Any]]:
        """OHLCV bars as [ts_ms, open, high, low, close, volume] for equities.

        Always pass an explicit ``start`` window. Without it Alpaca often returns
        only the current RTH session (~20–26×15m bars), which is below the
        strategy's ~39-bar minimum and blocks every entry.
        """
        from datetime import datetime as _dt, timedelta, timezone as _tz

        symbol = self._to_symbol(asset)
        if self.is_crypto(asset):
            # Stock scanner/strategy should never request crypto pairs here
            logger.warning(f"get_stock_bars called for crypto-like {asset}; skipping")
            return []
        tf = self._bar_timeframe(interval)
        want = min(int(limit or 100), 1000)
        # ~26 fifteen-minute bars per RTH day; pad calendar days for weekends.
        days_back = 10 if tf in ("15Min", "5Min", "1Min", "30Min") else 30
        if tf in ("1Hour", "4Hour"):
            days_back = 40
        if tf == "1Day":
            days_back = 120
        start = (_dt.now(_tz.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        feed = str(CONFIG.get("alpaca_data_feed") or "iex").strip().lower() or "iex"
        try:
            data = await self._retry(
                lambda: self._request(
                    "GET",
                    "/v2/stocks/bars",
                    params={
                        "symbols": symbol,
                        "timeframe": tf,
                        "start": start,
                        "limit": str(want),
                        "adjustment": "split",
                        "feed": feed,
                        "sort": "asc",
                    },
                    base=self.data_base,
                )
            )
            bars_map = (data or {}).get("bars") or {}
            raw = bars_map.get(symbol) or []
            # Single-symbol responses sometimes return a bare list.
            if not raw and isinstance(data, dict) and isinstance(data.get("bars"), list):
                raw = data.get("bars") or []
            if not isinstance(raw, list):
                return []
            out: List[List[Any]] = []
            for b in raw:
                ts = b.get("t") or b.get("timestamp")
                try:
                    if isinstance(ts, str):
                        ts_ms = int(_dt.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
                    else:
                        ts_ms = int(ts or 0)
                except Exception:
                    ts_ms = 0
                out.append([
                    ts_ms,
                    float(b.get("o") or 0),
                    float(b.get("h") or 0),
                    float(b.get("l") or 0),
                    float(b.get("c") or 0),
                    float(b.get("v") or 0),
                ])
            if len(out) < want // 3:
                logger.warning(
                    "Alpaca bars thin for %s feed=%s: got %d (wanted %d, start=%s)",
                    symbol,
                    feed,
                    len(out),
                    want,
                    start,
                )
            return out
        except Exception as e:
            logger.error(f"Alpaca bars error for {asset}: {e}")
            return []

    async def get_clock(self) -> Dict[str, Any]:
        try:
            return await self._retry(lambda: self._request("GET", "/v2/clock")) or {}
        except Exception as e:
            logger.debug(f"Alpaca clock error: {e}")
            return {}

    async def get_current_price(self, asset: str) -> float:
        symbol = self._to_symbol(asset)
        try:
            if self.is_crypto(asset):
                # Latest crypto trade/quote
                data = await self._retry(
                    lambda: self._request(
                        "GET",
                        "/v1beta3/crypto/us/latest/trades",
                        params={"symbols": symbol},
                        base=self.data_base,
                    )
                )
                trades = (data or {}).get("trades") or {}
                trade = trades.get(symbol) or next(iter(trades.values()), None)
                if trade and trade.get("p") is not None:
                    return float(trade["p"])
            else:
                data = await self._retry(
                    lambda: self._request(
                        "GET",
                        f"/v2/stocks/{symbol}/trades/latest",
                        base=self.data_base,
                    )
                )
                trade = (data or {}).get("trade") or data
                if trade and trade.get("p") is not None:
                    return float(trade["p"])

            # Fallback: position current_price or snapshot
            pos = await self._get_raw_position_async(symbol)
            if pos and pos.get("current_price"):
                return float(pos["current_price"])
        except Exception as e:
            logger.error(f"Alpaca price error for {asset}: {e}")
        return 0.0

    async def get_user_state(self) -> Dict[str, Any]:
        try:
            account = await self._retry(lambda: self._request("GET", "/v2/account"))
            positions_raw = await self._retry(lambda: self._request("GET", "/v2/positions"))
            if not isinstance(positions_raw, list):
                positions_raw = []

            cash = float(account.get("cash") or 0)
            equity = float(account.get("equity") or account.get("portfolio_value") or cash)
            buying_power = float(account.get("buying_power") or cash)

            enriched = []
            for pos in positions_raw:
                qty = float(pos.get("qty") or 0)
                if abs(qty) <= 0:
                    continue
                symbol = pos.get("symbol") or ""
                asset = self._from_symbol(symbol)
                entry = float(pos.get("avg_entry_price") or 0)
                current = float(pos.get("current_price") or 0)
                pnl = float(pos.get("unrealized_pl") or 0)
                # Alpaca returns fraction (0.05 = 5%)
                plpc = pos.get("unrealized_plpc")
                try:
                    roi = float(plpc) * 100.0 if plpc is not None else None
                except (TypeError, ValueError):
                    roi = None
                market_value = abs(float(pos.get("market_value") or (abs(qty) * current)))
                cost_basis = abs(float(pos.get("cost_basis") or (abs(qty) * entry)))
                side = (pos.get("side") or ("long" if qty > 0 else "short")).lower()
                signed_qty = abs(qty) if side == "long" else -abs(qty)

                enriched.append({
                    "coin": asset,
                    "symbol": asset,
                    "szi": signed_qty,
                    "quantity": signed_qty,
                    "entryPx": entry,
                    "entry_price": entry,
                    "entryPrice": entry,
                    "markPrice": current,
                    "current_price": current,
                    "pnl": pnl,
                    "unrealized_pnl": pnl,
                    "unRealizedProfit": pnl,
                    "roiPercent": roi,
                    "roi": roi,
                    "notional": market_value,
                    "initialMargin": cost_basis,
                    "positionInitialMargin": cost_basis,
                    "initial_margin": cost_basis,
                    "leverage": 1.0,
                    "liquidationPx": None,
                    "alpaca_symbol": symbol,
                })

            return {
                "balance": buying_power if buying_power > 0 else cash,
                "total_value": equity,
                "positions": enriched,
                "asset_balances": [
                    {
                        "asset": "USD",
                        "walletBalance": cash,
                        "availableBalance": buying_power,
                        "crossWalletBalance": cash,
                        "crossUnPnl": float(account.get("unrealized_pl") or 0),
                        "positionValue": equity,
                    }
                ],
                "alpaca_account": {
                    "status": account.get("status"),
                    "pattern_day_trader": account.get("pattern_day_trader"),
                    "daytrade_count": account.get("daytrade_count"),
                    "paper": self.paper,
                },
            }
        except Exception as e:
            logger.error(f"Alpaca get_user_state error: {e}", exc_info=True)
            return {"balance": 0, "total_value": 0, "positions": [], "asset_balances": []}

    async def get_open_orders(self) -> List[Dict]:
        try:
            orders = await self._retry(
                lambda: self._request("GET", "/v2/orders", params={"status": "open", "limit": 100})
            )
            if not isinstance(orders, list):
                return []
            out = []
            for o in orders:
                asset = self._from_symbol(o.get("symbol", ""))
                otype = (o.get("type") or "").upper()
                # Map to names the bot's TP/SL extractor understands
                if otype == "LIMIT" and (o.get("side") or "").lower() in ("sell", "buy"):
                    # Heuristic: limit exit ≈ take profit
                    mapped_type = "TAKE_PROFIT"
                elif otype in ("STOP", "STOP_LIMIT"):
                    mapped_type = "STOP"
                else:
                    mapped_type = otype
                trigger = o.get("stop_price") or o.get("limit_price") or 0
                out.append({
                    "coin": asset,
                    "asset": asset,
                    "symbol": asset,
                    "oid": str(o.get("id", "")),
                    "isBuy": (o.get("side") or "").lower() == "buy",
                    "sz": float(o.get("qty") or 0),
                    "px": float(o.get("limit_price") or 0 or 0),
                    "triggerPx": float(trigger or 0),
                    "orderType": mapped_type,
                    "type": mapped_type,
                })
            return out
        except Exception as e:
            logger.error(f"Alpaca get_open_orders error: {e}")
            return []

    async def get_recent_fills(self, limit: int = 50) -> List[Dict]:
        try:
            orders = await self._retry(
                lambda: self._request(
                    "GET",
                    "/v2/orders",
                    params={"status": "closed", "limit": min(limit, 100), "direction": "desc"},
                )
            )
            if not isinstance(orders, list):
                return []
            fills = []
            for o in orders:
                if (o.get("status") or "").lower() not in ("filled", "partially_filled"):
                    continue
                asset = self._from_symbol(o.get("symbol", ""))
                fills.append({
                    "coin": asset,
                    "asset": asset,
                    "symbol": o.get("symbol"),
                    "side": o.get("side"),
                    "sz": float(o.get("filled_qty") or o.get("qty") or 0),
                    "size": float(o.get("filled_qty") or o.get("qty") or 0),
                    "px": float(o.get("filled_avg_price") or o.get("limit_price") or 0),
                    "price": float(o.get("filled_avg_price") or 0),
                    "id": o.get("id"),
                })
            return fills[:limit]
        except Exception as e:
            logger.error(f"Alpaca get_recent_fills error: {e}")
            return []

    async def cancel_order(self, asset: str, oid: str):
        await self._retry(lambda: self._request("DELETE", f"/v2/orders/{oid}"))
        logger.info(f"Alpaca cancelled order {oid} ({asset})")

    async def cancel_all_orders(self, asset: str):
        symbol = self._to_symbol(asset)
        try:
            await self._retry(
                lambda: self._request("DELETE", "/v2/orders", params={"symbols": symbol})
            )
            logger.info(f"Alpaca cancelled open orders for {symbol}")
        except Exception as e:
            # Fallback: cancel one-by-one
            logger.warning(f"Alpaca bulk cancel failed for {symbol}: {e}; falling back")
            orders = await self.get_open_orders()
            for o in orders:
                if o.get("coin") == self._from_symbol(symbol) or o.get("coin") == asset.upper():
                    try:
                        await self.cancel_order(asset, o.get("oid"))
                    except Exception:
                        pass

    async def round_size(self, asset: str, amount: float) -> float:
        return self._round_qty(asset, amount)

    async def get_open_interest(self, asset: str) -> Optional[float]:
        return None

    async def get_funding_rate(self, asset: str) -> Optional[float]:
        return None
