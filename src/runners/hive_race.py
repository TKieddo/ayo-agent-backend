"""One loop, five OKX demo keys. Public market data is fetched once per tick.

Each runner keeps its own $300 ledger. Orders are real demo orders at $30
margin (10x isolated). The AYO agent key is never used here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from src.config_loader import CONFIG
from src.runners.rules import (
    ADD_NOTIONAL,
    ENTRY_NOTIONAL,
    LEVERAGE,
    RETIRE_EQUITY,
    RUNNER_LABELS,
    RUNNERS,
    START_EQUITY,
    Action,
    Book,
    Coin,
    Pos,
    decide,
    fee_usd,
    stop_pct,
    stop_price,
    upl,
)

logger = logging.getLogger(__name__)

_STATE: Dict[str, Any] = {
    "started_at": None,
    "runners": {},
    "updated_at": None,
    "market_coins": 0,
    "last_error": None,
    "ayo_start_equity": None,
}
_LOCK = asyncio.Lock()
_CLIENTS: Dict[str, Any] = {}
_AYO_CLIENT: Any = None
_OI: Dict[str, float] = {}
_FUNDING_CACHE: Dict[str, Any] = {}
_PERSIST_LOGGED = False

ENV_PREFIX = {
    "mayhem": "MAYHEM",
    "windward": "WINDWARD",
    "orakelia": "ORAKELIA",
    "peaksting": "PEAKSTING",
    "peakstinger": "PEAKSTINGER",
}


def _path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "hive_race.json"


def _base_url() -> str:
    return (CONFIG.get("okx_base_url") or "https://www.okx.com").rstrip("/")


def _enabled() -> bool:
    return os.getenv("HIVE_RACE", "true").strip().lower() not in ("0", "false", "no", "off")


def _creds(runner: str) -> Optional[tuple]:
    prefix = ENV_PREFIX[runner]
    key = os.getenv(f"{prefix}_OKX_DEMO_API_KEY", "").strip()
    secret = os.getenv(f"{prefix}_OKX_DEMO_API_SECRET", "").strip()
    phrase = os.getenv(f"{prefix}_OKX_DEMO_API_PASSPHRASE", "").strip()
    if key and secret and phrase:
        return key, secret, phrase
    return None


def _public(path: str, params: Optional[dict] = None) -> Any:
    resp = requests.get(f"{_base_url()}{path}", params=params or {}, timeout=20)
    resp.raise_for_status()
    payload = resp.json() or {}
    if str(payload.get("code", "0")) != "0":
        raise RuntimeError(payload.get("msg") or "okx public error")
    return payload.get("data") or []


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses -= delta
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - (100 / (1 + rs))


def _atr_pct(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) <= period:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    atr = sum(trs[-period:]) / period
    if closes[-1] <= 0:
        return None
    return atr / closes[-1] * 100


def _vol_z(volumes: List[float]) -> float:
    if len(volumes) < 12:
        return 0.0
    recent = sum(volumes[-4:])
    buckets = []
    for i in range(0, len(volumes) - 4, 4):
        buckets.append(sum(volumes[i : i + 4]))
    if len(buckets) < 3:
        return 0.0
    mean = sum(buckets) / len(buckets)
    var = sum((b - mean) ** 2 for b in buckets) / len(buckets)
    sd = var ** 0.5 or 1.0
    return (recent - mean) / sd


def _candles(inst_id: str, bar: str, limit: int) -> List[list]:
    rows = _public("/api/v5/market/candles", {"instId": inst_id, "bar": bar, "limit": limit})
    rows = [r for r in rows if isinstance(r, list) and len(r) >= 6]
    rows.reverse()
    return rows


def _funding_z(inst_id: str, current: float) -> float:
    now = time.time()
    cached = _FUNDING_CACHE.get(inst_id)
    if cached and now - cached["ts"] < 3600:
        hist = cached["hist"]
    else:
        try:
            rows = _public("/api/v5/public/funding-rate-history", {"instId": inst_id, "limit": 30})
            hist = [_f(r.get("fundingRate")) for r in rows if isinstance(r, dict)]
        except Exception:
            hist = []
        _FUNDING_CACHE[inst_id] = {"ts": now, "hist": hist}
    if len(hist) < 5:
        return 0.0
    mean = sum(hist) / len(hist)
    var = sum((x - mean) ** 2 for x in hist) / len(hist)
    sd = var ** 0.5 or 1e-12
    return (current - mean) / sd


def _demo_swap_ids() -> set:
    """Instruments the demo venue will actually accept. Live tickers include names demo rejects."""
    resp = requests.get(
        f"{_base_url()}/api/v5/public/instruments",
        params={"instType": "SWAP"},
        headers={"x-simulated-trading": "1", "Accept": "application/json"},
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json() or {}
    ids = set()
    for row in payload.get("data") or []:
        inst = str(row.get("instId") or "")
        state = str(row.get("state") or "").lower()
        if inst.endswith("-USDT-SWAP") and state in ("", "live"):
            ids.add(inst)
    return ids


def build_coins() -> List[Coin]:
    tickers = _public("/api/v5/market/tickers", {"instType": "SWAP"})
    try:
        listed = _demo_swap_ids()
    except Exception as exc:
        logger.warning("hive demo instrument list failed: %s", exc)
        listed = set()
    ranked = []
    for row in tickers:
        inst = str(row.get("instId") or "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        if listed and inst not in listed:
            continue
        last = _f(row.get("last"))
        bid = _f(row.get("bidPx"))
        ask = _f(row.get("askPx"))
        if last <= 0 or bid <= 0 or ask <= 0:
            continue
        vol_usd = _f(row.get("volCcy24h")) * last
        spread_bp = (ask - bid) / ((ask + bid) / 2) * 10000
        if vol_usd < 15_000_000 or spread_bp > 8:
            continue
        open24 = _f(row.get("open24h"), last)
        ret24 = (last - open24) / open24 * 100 if open24 else 0.0
        ranked.append((vol_usd, inst, row, last, spread_bp, ret24))
    ranked.sort(key=lambda r: r[0], reverse=True)
    coins: List[Coin] = []
    for vol_usd, inst, row, last, spread_bp, ret24 in ranked[:12]:
        coin = inst.replace("-USDT-SWAP", "")
        try:
            m15 = _candles(inst, "15m", 100)
            daily = _candles(inst, "1Dutc", 8)
        except Exception as exc:
            logger.debug("hive candles %s: %s", inst, exc)
            continue
        if len(m15) < 20:
            continue
        closes = [_f(c[4]) for c in m15]
        highs = [_f(c[2]) for c in m15]
        lows = [_f(c[3]) for c in m15]
        vols = [_f(c[5]) for c in m15]
        ret1h = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 and closes[-5] else 0.0
        ret7 = 0.0
        if len(daily) >= 2 and _f(daily[0][4]):
            ret7 = (last - _f(daily[0][4])) / _f(daily[0][4]) * 100
        atr15 = _atr_pct(highs, lows, closes)
        atr1h = atr15 * 2 if atr15 is not None else None
        oi_chg = 0.0
        try:
            oi_rows = _public("/api/v5/public/open-interest", {"instType": "SWAP", "instId": inst})
            oi_now = _f(oi_rows[0].get("oi")) if oi_rows else 0.0
            prev = _OI.get(inst)
            if prev and oi_now:
                oi_chg = (oi_now - prev) / prev * 100
            if oi_now:
                _OI[inst] = oi_now
        except Exception:
            oi_chg = 0.0
        funding_z = 0.0
        try:
            fr = _public("/api/v5/public/funding-rate", {"instId": inst})
            current = _f(fr[0].get("fundingRate")) if fr else 0.0
            funding_z = _funding_z(inst, current)
        except Exception:
            funding_z = 0.0
        coins.append(
            Coin(
                coin=coin,
                mid=last,
                spread_bp=spread_bp,
                vol24h_usd=vol_usd,
                ret1h=ret1h,
                ret24h=ret24,
                ret7d=ret7,
                rsi14=_rsi(closes),
                atr1h_pct=atr1h,
                oi_chg_1h=oi_chg,
                funding_z=funding_z,
                vol_z=_vol_z(vols),
                high24h=_f(row.get("high24h"), last),
            )
        )
    return coins


def _book_from(raw: Dict[str, Any]) -> Book:
    pos = None
    if raw.get("position"):
        p = raw["position"]
        pos = Pos(
            coin=p["coin"],
            side=p["side"],
            entry_px=float(p["entry_px"]),
            notional=float(p["notional"]),
            coin_qty=float(p.get("coin_qty") or 0),
            opened_at=float(p["opened_at"]),
            stop_px=float(p["stop_px"]),
            peak_px=float(p.get("peak_px") or p["entry_px"]),
            added=bool(p.get("added")),
            risk_usd=float(p.get("risk_usd") or 0),
        )
    return Book(
        equity=float(raw.get("equity") or START_EQUITY),
        day_start_equity=float(raw.get("day_start_equity") or START_EQUITY),
        trades_today=int(raw.get("trades_today") or 0),
        fees_today=float(raw.get("fees_today") or 0),
        last_order_at=float(raw.get("last_order_at") or 0),
        cap=raw.get("cap"),
        position=pos,
        blacklist={k: True for k in (raw.get("blacklist") or [])},
        peak_equity=float(raw.get("peak_equity") or START_EQUITY),
        max_dd_pct=float(raw.get("max_dd_pct") or 0),
    )


def _book_to(book: Book, trades: List[dict], configured: bool, error: Optional[str]) -> dict:
    pos = None
    if book.position:
        p = book.position
        pos = {
            "coin": p.coin,
            "side": p.side,
            "entry_px": p.entry_px,
            "notional": p.notional,
            "coin_qty": p.coin_qty,
            "opened_at": p.opened_at,
            "stop_px": p.stop_px,
            "peak_px": p.peak_px,
            "added": p.added,
            "risk_usd": p.risk_usd,
        }
    return {
        "equity": round(book.equity, 4),
        "day_start_equity": book.day_start_equity,
        "trades_today": book.trades_today,
        "fees_today": round(book.fees_today, 4),
        "last_order_at": book.last_order_at,
        "cap": book.cap,
        "position": pos,
        "blacklist": list(book.blacklist),
        "peak_equity": book.peak_equity,
        "max_dd_pct": round(book.max_dd_pct, 4),
        "trades": trades[-80:],
        "configured": configured,
        "error": error,
    }


def _load() -> None:
    path = _path()
    if not path.exists():
        return
    try:
        saved = json.loads(path.read_text())
        _STATE["started_at"] = saved.get("started_at")
        _STATE["runners"] = saved.get("runners") or {}
    except Exception as exc:
        logger.warning("hive race state unreadable: %s", exc)


def _save() -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({k: _STATE[k] for k in ("started_at", "runners")}, default=str))
    tmp.replace(path)


def _roll_day(book: Book, now: float) -> None:
    key = time.strftime("%Y-%m-%d", time.gmtime(now))
    prev = getattr(book, "day_key", None)
    if prev == key:
        return
    book.day_key = key  # type: ignore[attr-defined]
    book.day_start_equity = book.equity
    book.trades_today = 0
    book.fees_today = 0.0
    if book.cap in ("trade_cap", "fee_budget", "loss_stop"):
        book.cap = None


def _note_dd(book: Book, mtm: float) -> None:
    if mtm > book.peak_equity:
        book.peak_equity = mtm
    if book.peak_equity > 0:
        dd = (book.peak_equity - mtm) / book.peak_equity * 100
        if dd > book.max_dd_pct:
            book.max_dd_pct = dd


def _persist_trade(runner: str, row: dict) -> None:
    global _PERSIST_LOGGED
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        return
    try:
        from supabase import create_client

        client = create_client(url, key)
        payload = {
            "symbol": row["coin"],
            "side": "sell" if row["side"] == "short" else "buy",
            "size": row.get("coin_qty") or 0.001,
            "price": row["price"],
            "fee": row["fee"],
            "pnl": row["pnl"],
            "executed_at": row["at"],
            "order_id": f"hive-{runner}-{int(time.time())}",
            "book": "crypto",
            "venue": "okx",
            "runner": runner,
        }
        try:
            client.table("trades").insert(payload).execute()
        except Exception:
            payload.pop("runner", None)
            client.table("trades").insert(payload).execute()
    except Exception as exc:
        if not _PERSIST_LOGGED:
            logger.info("hive trade persist skipped: %s", exc)
            _PERSIST_LOGGED = True


async def _order(client: Any, side: str, coin: str, notional: float, price: float, reduce_only: bool) -> float:
    qty = notional / price if price else 0
    if side == "long":
        fn = client.place_sell_order if reduce_only else client.place_buy_order
    else:
        fn = client.place_buy_order if reduce_only else client.place_sell_order
    await fn(coin, qty, leverage=LEVERAGE, reduce_only=reduce_only)
    return qty


def _open_pos(coin: Coin, side: str, notional: float, qty: float, now: float) -> Pos:
    pct = stop_pct(coin) or 8.0
    stop = stop_price(side, coin.mid, pct)
    risk = abs(coin.mid - stop) / coin.mid * notional
    return Pos(
        coin=coin.coin,
        side=side,
        entry_px=coin.mid,
        notional=notional,
        coin_qty=qty,
        opened_at=now,
        stop_px=stop,
        peak_px=coin.mid,
        added=False,
        risk_usd=risk,
    )


async def _act(runner: str, client: Any, book: Book, coins: List[Coin], action: Action, trades: List[dict], now: float) -> None:
    by_coin = {c.coin: c for c in coins}
    if action.kind == "hold":
        return
    if action.kind == "close" and book.position:
        pos = book.position
        coin = by_coin.get(pos.coin)
        mid = coin.mid if coin else pos.entry_px
        qty = await _order(client, pos.side, pos.coin, pos.notional, mid, True)
        pnl = upl(pos, mid)
        fee = fee_usd(pos.notional)
        book.equity += pnl - fee
        book.fees_today += fee
        book.last_order_at = now
        book.position = None
        row = {
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            "coin": pos.coin,
            "side": pos.side,
            "action": action.reason or "close",
            "price": mid,
            "notional": pos.notional,
            "pnl": round(pnl - fee, 4),
            "fee": round(fee, 4),
            "coin_qty": qty,
        }
        trades.append(row)
        _persist_trade(runner, row)
        return
    if action.kind == "open":
        coin = by_coin.get(action.coin or "")
        if not coin or not action.side:
            return
        qty = await _order(client, action.side, coin.coin, action.notional or ENTRY_NOTIONAL, coin.mid, False)
        fee = fee_usd(action.notional or ENTRY_NOTIONAL)
        book.equity -= fee
        book.fees_today += fee
        book.trades_today += 1
        book.last_order_at = now
        book.position = _open_pos(coin, action.side, action.notional or ENTRY_NOTIONAL, qty, now)
        return
    if action.kind == "add" and book.position:
        pos = book.position
        coin = by_coin.get(pos.coin)
        if not coin:
            return
        qty = await _order(client, pos.side, pos.coin, ADD_NOTIONAL, coin.mid, False)
        fee = fee_usd(ADD_NOTIONAL)
        book.equity -= fee
        book.fees_today += fee
        book.last_order_at = now
        filled = pos.notional + ADD_NOTIONAL
        pos.entry_px = (pos.entry_px * pos.notional + coin.mid * ADD_NOTIONAL) / filled
        pos.notional = filled
        pos.coin_qty += qty
        pos.added = True
        pct = stop_pct(coin)
        if pct:
            # Keep the stop at least at the new average so the add is not left unprotected.
            fresh = stop_price(pos.side, pos.entry_px, pct)
            if pos.side == "long":
                pos.stop_px = max(pos.stop_px, min(fresh, pos.entry_px))
            else:
                pos.stop_px = min(pos.stop_px, max(fresh, pos.entry_px))
        return
    if action.kind == "switch" and book.position:
        closing = Action("close", book.position.coin, book.position.side, reason="rotate")
        await _act(runner, client, book, coins, closing, trades, now)
        opening = Action("open", action.coin, action.side, ENTRY_NOTIONAL, "rotate")
        await _act(runner, client, book, coins, opening, trades, now)


async def _refresh_ayo() -> None:
    """Pull the main AYO OKX book so the leaderboard can compare it with runners."""
    global _AYO_CLIENT
    if _AYO_CLIENT is None:
        return
    try:
        state = await _AYO_CLIENT.get_user_state()
        equity = float(state.get("total_value") or state.get("balance") or 0)
        if _STATE.get("ayo_start_equity") is None and equity > 0:
            _STATE["ayo_start_equity"] = equity
        start = float(_STATE.get("ayo_start_equity") or equity or START_EQUITY)
        positions = state.get("positions") or []
        pos = None
        best_notional = -1.0
        upl_total = 0.0
        open_count = 0
        for p in positions:
            try:
                sz = float(p.get("size") or p.get("szi") or p.get("quantity") or 0)
            except (TypeError, ValueError):
                sz = 0.0
            if abs(sz) <= 0:
                continue
            open_count += 1
            side = "long" if sz > 0 else "short"
            coin = str(p.get("coin") or p.get("symbol") or p.get("asset") or "").upper()
            coin = coin.replace("-USDT-SWAP", "").replace("-USDT", "")
            entry = float(p.get("entryPx") or p.get("entry_price") or p.get("avgPx") or 0)
            mark = float(p.get("markPrice") or p.get("current_price") or entry)
            notional = abs(float(p.get("notionalUsd") or p.get("notional") or (abs(sz) * (mark or entry or 0))))
            upl = float(p.get("pnl") or p.get("unrealized_pnl") or p.get("upl") or p.get("unrealizedPnl") or 0)
            upl_total += upl
            if notional >= best_notional:
                best_notional = notional
                pos = {
                    "coin": coin or "?",
                    "side": side,
                    "notional": round(notional, 2),
                    "entry_px": entry,
                    "stop_px": float(p.get("stop_px") or p.get("liquidation_price") or p.get("liquidationPx") or 0),
                    "added": False,
                }
        # Show the real OKX book on the race board (not the $300 race ledger).
        pct = ((equity - start) / start * 100) if start else 0.0
        _STATE["runners"]["ayo"] = {
            "configured": True,
            "equity": round(equity, 4),
            "mtm": round(equity, 4),
            "account_equity": round(equity, 2),
            "ayo_start_equity": round(start, 2),
            "pct": round(pct, 2),
            "upl": round(upl_total, 4),
            "position": pos,
            "open_count": open_count,
            "trades_today": 0,
            "fees_today": 0,
            "max_dd_pct": 0,
            "cap": None,
            "error": None,
            "trades": [],
            "note": f"Live OKX book · {open_count} open · race start ${start:,.0f}",
        }
    except Exception as exc:
        raw = _STATE["runners"].get("ayo") or {}
        raw["configured"] = True
        raw["error"] = str(exc)[:240]
        _STATE["runners"]["ayo"] = raw
        logger.warning("hive ayo refresh failed: %s", exc)


async def _tick() -> None:
    coins = await asyncio.to_thread(build_coins)
    now = time.time()
    await _refresh_ayo()
    async with _LOCK:
        _STATE["market_coins"] = len(coins)
        _STATE["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
        _STATE["last_error"] = None
        for runner, client in list(_CLIENTS.items()):
            if runner not in RUNNERS:
                continue
            raw = _STATE["runners"].get(runner) or {}
            book = _book_from(raw)
            book.day_key = raw.get("day_key")  # type: ignore[attr-defined]
            trades = list(raw.get("trades") or [])
            error = None
            try:
                _roll_day(book, now)
                raw_day = time.strftime("%Y-%m-%d", time.gmtime(now))
                book.day_key = raw_day  # type: ignore[attr-defined]
                action = decide(runner, book, coins, now)
                await _act(runner, client, book, coins, action, trades, now)
                if book.equity <= RETIRE_EQUITY:
                    book.cap = "retired"
            except Exception as exc:
                error = str(exc)
                logger.warning("hive %s tick failed: %s", runner, exc)
            mid = None
            if book.position:
                match = next((c for c in coins if c.coin == book.position.coin), None)
                mid = match.mid if match else book.position.entry_px
            mtm = book.equity + (upl(book.position, mid) if book.position and mid else 0)
            _note_dd(book, mtm)
            stored = _book_to(book, trades, True, error)
            stored["day_key"] = time.strftime("%Y-%m-%d", time.gmtime(now))
            stored["mtm"] = round(mtm, 4)
            stored["upl"] = round(mtm - book.equity, 4)
            _STATE["runners"][runner] = stored
        _save()


def snapshot() -> dict:
    rows = []
    for runner in ("ayo",) + RUNNERS:
        raw = _STATE["runners"].get(runner) or {}
        configured = runner == "ayo" or bool(raw.get("configured")) or runner in _CLIENTS
        mtm = raw.get("mtm", START_EQUITY if runner != "ayo" else None)
        pct = raw.get("pct")
        if pct is None and isinstance(mtm, (int, float)):
            pct = (float(mtm) - START_EQUITY) / START_EQUITY * 100
        pos = raw.get("position")
        rows.append(
            {
                "id": runner,
                "name": RUNNER_LABELS[runner],
                "configured": bool(raw.get("configured")) if runner == "ayo" else configured,
                "equity": mtm,
                "pct": None if pct is None else round(float(pct), 2),
                "upl": raw.get("upl"),
                "position": pos,
                "open_count": raw.get("open_count"),
                "account_equity": raw.get("account_equity"),
                "ayo_start_equity": raw.get("ayo_start_equity"),
                "trades_today": raw.get("trades_today", 0),
                "fees_today": raw.get("fees_today", 0),
                "max_dd_pct": raw.get("max_dd_pct", 0),
                "cap": raw.get("cap"),
                "error": raw.get("error"),
                "trades": raw.get("trades") or [],
                "note": raw.get("note"),
            }
        )
    return {
        "ok": True,
        "enabled": _enabled(),
        "started_at": _STATE.get("started_at"),
        "updated_at": _STATE.get("updated_at"),
        "market_coins": _STATE.get("market_coins") or 0,
        "last_error": _STATE.get("last_error"),
        "start_equity": START_EQUITY,
        "margin_entry": 30,
        "margin_added": 45,
        "leverage": LEVERAGE,
        "runners": rows,
    }


async def start_hive_race() -> None:
    global _AYO_CLIENT
    if not _enabled():
        logger.info("hive race off (HIVE_RACE=false)")
        return
    _load()
    if not _STATE.get("started_at"):
        _STATE["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    from src.trading.okx_api import OKXAPI

    for runner in RUNNERS:
        creds = _creds(runner)
        raw = _STATE["runners"].get(runner) or {}
        if not creds:
            _STATE["runners"][runner] = {
                **_book_to(Book(), raw.get("trades") or [], False, None),
                "mtm": raw.get("mtm", START_EQUITY),
                "error": "API key not set",
                "configured": False,
            }
            continue
        try:
            client = OKXAPI(api_key=creds[0], api_secret=creds[1], passphrase=creds[2], demo=True, td_mode="isolated")
            client.default_leverage = LEVERAGE
            _CLIENTS[runner] = client
            if runner not in _STATE["runners"]:
                _STATE["runners"][runner] = _book_to(Book(), [], True, None)
            else:
                _STATE["runners"][runner]["configured"] = True
                _STATE["runners"][runner]["error"] = None
            logger.info("hive runner %s ready", runner)
        except Exception as exc:
            _STATE["runners"][runner] = {
                **_book_to(_book_from(raw) if raw else Book(), raw.get("trades") or [], True, str(exc)),
                "configured": True,
                "error": str(exc),
            }
            logger.warning("hive runner %s key failed: %s", runner, exc)
    key = os.getenv("OKX_API_KEY", "").strip()
    secret = os.getenv("OKX_API_SECRET", "").strip()
    phrase = os.getenv("OKX_PASSPHRASE", "").strip()
    if key and secret and phrase:
        demo = os.getenv("OKX_DEMO", "true").strip().lower() not in ("0", "false", "no", "off")
        try:
            _AYO_CLIENT = OKXAPI(api_key=key, api_secret=secret, passphrase=phrase, demo=demo, td_mode="cross")
            await _refresh_ayo()
            logger.info("hive ayo book linked")
        except Exception as exc:
            logger.warning("hive ayo key failed: %s", exc)
            _STATE["runners"]["ayo"] = {"configured": False, "error": str(exc), "trades": [], "mtm": None}
    else:
        _STATE["runners"]["ayo"] = {"configured": False, "error": "OKX key not set", "trades": [], "mtm": None}
    _save()
    if not _CLIENTS:
        logger.info("hive race idle: no demo keys yet")
        return
    while True:
        try:
            await _tick()
        except Exception as exc:
            _STATE["last_error"] = str(exc)
            logger.warning("hive race tick failed: %s", exc)
        await asyncio.sleep(60)
