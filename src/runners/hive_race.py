"""One loop, five OKX demo keys. Public market data is fetched once per tick.

Each runner keeps a $300 race ledger with up to 3 concurrent opens at $40
margin (10x isolated), +$15 margin add when winning. Live OKX positions are
reconciled every tick so the book cannot desync. AYO's key is never used here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.config_loader import CONFIG
from src.runners.rules import (
    ADD_MARGIN,
    ADD_NOTIONAL,
    ENTRY_MARGIN,
    ENTRY_NOTIONAL,
    LEVERAGE,
    MAX_OPENS,
    RETIRE_EQUITY,
    RUNNER_LABELS,
    RUNNERS,
    START_EQUITY,
    Action,
    Book,
    Coin,
    Pos,
    decide_actions,
    fee_usd,
    flip_side,
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
    # Oppo Mayhem 1:1 pair — reuses Peak Stinger demo keys (PEAKSTINGER_*).
    "oppo_mayhem": "PEAKSTINGER",
}

# Mayhem open/close → Oppo Mayhem opposite open/close (drained on Oppo's tick).
_OPPO_PAIR_Q: List[Action] = []


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


def _pos_from_dict(p: Dict[str, Any]) -> Pos:
    return Pos(
        coin=str(p["coin"]),
        side=str(p["side"]),
        entry_px=float(p["entry_px"]),
        notional=float(p["notional"]),
        coin_qty=float(p.get("coin_qty") or 0),
        opened_at=float(p.get("opened_at") or time.time()),
        stop_px=float(p.get("stop_px") or p["entry_px"]),
        peak_px=float(p.get("peak_px") or p["entry_px"]),
        added=bool(p.get("added")),
        risk_usd=float(p.get("risk_usd") or 0),
    )


def _pos_to_dict(p: Pos, live_upl: Optional[float] = None) -> dict:
    row = {
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
    if live_upl is not None:
        row["upl"] = round(float(live_upl), 4)
    return row


def _book_from(raw: Dict[str, Any]) -> Book:
    positions: List[Pos] = []
    raw_positions = raw.get("positions")
    if isinstance(raw_positions, list) and raw_positions:
        for p in raw_positions:
            if isinstance(p, dict) and p.get("coin"):
                positions.append(_pos_from_dict(p))
    elif raw.get("position"):
        positions.append(_pos_from_dict(raw["position"]))
    book = Book(
        equity=float(raw.get("equity") or START_EQUITY),
        day_start_equity=float(raw.get("day_start_equity") or START_EQUITY),
        trades_today=int(raw.get("trades_today") or 0),
        fees_today=float(raw.get("fees_today") or 0),
        last_order_at=float(raw.get("last_order_at") or 0),
        cap=raw.get("cap"),
        positions=positions,
        blacklist={k: True for k in (raw.get("blacklist") or [])},
        peak_equity=float(raw.get("peak_equity") or START_EQUITY),
        max_dd_pct=float(raw.get("max_dd_pct") or 0),
    )
    book.sync_primary()
    return book


def _book_to(
    book: Book,
    trades: List[dict],
    configured: bool,
    error: Optional[str],
    upl_by_coin: Optional[Dict[str, float]] = None,
) -> dict:
    book.sync_primary()
    upl_by_coin = upl_by_coin or {}
    positions = [_pos_to_dict(p, upl_by_coin.get(p.coin)) for p in book.positions]
    pos = positions[0] if positions else None
    if book.position and not pos:
        pos = _pos_to_dict(book.position, upl_by_coin.get(book.position.coin))
    return {
        "equity": round(book.equity, 4),
        "day_start_equity": book.day_start_equity,
        "trades_today": book.trades_today,
        "fees_today": round(book.fees_today, 4),
        "last_order_at": book.last_order_at,
        "cap": book.cap,
        "position": pos,
        "positions": positions,
        "open_count": len(positions),
        "blacklist": list(book.blacklist),
        "peak_equity": book.peak_equity,
        "max_dd_pct": round(book.max_dd_pct, 4),
        "trades": trades[-80:],
        "configured": configured,
        "error": error,
    }


def _migrate_runner_ids(runners: Dict[str, Any]) -> Dict[str, Any]:
    """Rename legacy mayhem_inv → oppo_mayhem so state/UI stay readable."""
    if not isinstance(runners, dict):
        return {}
    out = dict(runners)
    legacy = out.pop("mayhem_inv", None)
    if legacy is not None and "oppo_mayhem" not in out:
        out["oppo_mayhem"] = legacy
    return out


def _queue_oppo_pair(action: Action) -> None:
    """Queue Oppo entries/adds only — never clone Mayhem closes (Oppo exits alone)."""
    if action.kind == "open" and action.coin and action.side:
        _OPPO_PAIR_Q.append(
            Action(
                "open",
                action.coin,
                flip_side(action.side),
                action.notional or ENTRY_NOTIONAL,
                "oppo_pair_open",
            )
        )
    elif action.kind == "add" and action.coin and action.side:
        _OPPO_PAIR_Q.append(
            Action(
                "add",
                action.coin,
                flip_side(action.side),
                action.notional or ADD_NOTIONAL,
                "oppo_pair_add",
            )
        )


def _oppo_sync_actions(mayhem_book: Book, oppo_book: Book) -> List[Action]:
    """Open missing flipped Mayhem legs. Do not close when Mayhem flats (own exits)."""
    m_by = {p.coin: p for p in mayhem_book.positions}
    o_by = {p.coin: p for p in oppo_book.positions}
    out: List[Action] = []
    for coin, mpos in m_by.items():
        want = flip_side(mpos.side)
        opos = o_by.get(coin)
        if opos is not None and opos.side != want:
            # Wrong way vs Mayhem — flip once, then Oppo manages the fade.
            out.append(Action("close", coin, opos.side, reason="oppo_pair_flip"))
            out.append(
                Action(
                    "open",
                    coin,
                    want,
                    float(mpos.notional or ENTRY_NOTIONAL),
                    "oppo_pair_open",
                )
            )
        elif opos is None:
            out.append(
                Action(
                    "open",
                    coin,
                    want,
                    float(mpos.notional or ENTRY_NOTIONAL),
                    "oppo_pair_open",
                )
            )
    return out


def _dedupe_actions(actions: List[Action]) -> List[Action]:
    seen_close: set = set()
    seen_open: set = set()
    out: List[Action] = []
    for a in actions:
        if a.kind == "close":
            if a.coin in seen_close:
                continue
            seen_close.add(a.coin)
        elif a.kind == "open":
            if a.coin in seen_open:
                continue
            seen_open.add(a.coin)
        out.append(a)
    return out


def _load() -> None:
    path = _path()
    if not path.exists():
        return
    try:
        saved = json.loads(path.read_text())
        _STATE["started_at"] = saved.get("started_at")
        _STATE["runners"] = _migrate_runner_ids(saved.get("runners") or {})
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


def _supabase_rest() -> Optional[tuple]:
    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        return None
    return url, key


def _persist_trade(runner: str, row: dict) -> None:
    """Durable hive close → Supabase. Do not set order_id (FK to orders); tag runner."""
    global _PERSIST_LOGGED
    creds = _supabase_rest()
    if not creds:
        if not _PERSIST_LOGGED:
            logger.warning("hive trade persist skipped: SUPABASE_URL/KEY missing")
            _PERSIST_LOGGED = True
        return
    url, key = creds
    action = str(row.get("action") or "close").replace("|", "-")[:48]
    payload = {
        "symbol": row["coin"],
        # DB side is buy/sell; map position side so hydrate can reverse it.
        "side": "sell" if row["side"] == "short" else "buy",
        "size": float(row.get("coin_qty") or row.get("notional") or 0.001) or 0.001,
        "price": float(row["price"]),
        "fee": float(row.get("fee") or 0),
        "pnl": float(row.get("pnl") or 0),
        "executed_at": row["at"],
        "book": "crypto",
        "venue": f"okx|{action}",
        "runner": runner,
    }
    try:
        req = urllib.request.Request(
            f"{url}/rest/v1/trades",
            data=json.dumps(payload).encode(),
            method="POST",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status not in (200, 201, 204):
                raise RuntimeError(f"supabase status {resp.status}")
    except Exception as exc:
        body = ""
        if isinstance(exc, urllib.error.HTTPError):
            try:
                body = exc.read().decode()[:300]
            except Exception:
                body = str(exc)
        logger.warning("hive trade persist failed for %s %s: %s %s", runner, row.get("coin"), exc, body)
        _PERSIST_LOGGED = True


def _db_row_to_hive_trade(row: dict) -> dict:
    side_raw = str(row.get("side") or "buy").lower()
    side = "short" if side_raw == "sell" else "long"
    venue = str(row.get("venue") or "okx")
    action = "close"
    if "|" in venue:
        action = venue.split("|", 1)[1] or "close"
    at = str(row.get("executed_at") or "")
    if at.endswith("+00:00"):
        at = at[:-6] + "Z"
    elif at and not at.endswith("Z") and "T" in at:
        at = at.split(".")[0] + "Z"
    return {
        "at": at,
        "coin": str(row.get("symbol") or "").upper(),
        "side": side,
        "action": action,
        "price": float(row.get("price") or 0),
        "notional": abs(float(row.get("size") or 0) * float(row.get("price") or 0)),
        "pnl": float(row.get("pnl") or 0),
        "fee": float(row.get("fee") or 0),
        "coin_qty": float(row.get("size") or 0),
    }


def _fetch_runner_trades(runner: str, limit: int = 200) -> List[dict]:
    creds = _supabase_rest()
    if not creds:
        return []
    url, key = creds
    qs = urllib.parse.urlencode(
        {
            "select": "symbol,side,size,price,fee,pnl,executed_at,venue,runner",
            "runner": f"eq.{runner}",
            "order": "executed_at.asc",
            "limit": str(limit),
        }
    )
    try:
        req = urllib.request.Request(
            f"{url}/rest/v1/trades?{qs}",
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            rows = json.loads(resp.read().decode() or "[]")
        return [_db_row_to_hive_trade(r) for r in rows if isinstance(r, dict)]
    except Exception as exc:
        logger.warning("hive trade hydrate fetch failed for %s: %s", runner, exc)
        return []


def _trade_key(t: dict) -> tuple:
    return (str(t.get("at") or ""), str(t.get("coin") or ""), round(float(t.get("pnl") or 0), 4))


def _hydrate_from_supabase() -> None:
    """Merge durable Supabase hive closes into in-memory/file state after Coolify wipes."""
    earliest: Optional[str] = None
    for runner in RUNNERS:
        db_trades = _fetch_runner_trades(runner)
        # Legacy id from the short-lived Mayhem Inv A/B slot.
        if runner == "oppo_mayhem":
            db_trades = db_trades + _fetch_runner_trades("mayhem_inv")
        if not db_trades:
            continue
        raw = dict(_STATE["runners"].get(runner) or {})
        file_trades = list(raw.get("trades") or [])
        by_key = {_trade_key(t): t for t in file_trades}
        for t in db_trades:
            by_key.setdefault(_trade_key(t), t)
        merged = sorted(by_key.values(), key=lambda t: str(t.get("at") or ""))
        raw["trades"] = merged[-80:]
        realized = sum(float(t.get("pnl") or 0) for t in merged)
        # If container state lost the closes, rebuild race equity from START + closed PnL.
        if not file_trades and merged:
            raw["equity"] = round(START_EQUITY + realized, 4)
            if raw.get("mtm") is None or abs(float(raw.get("mtm") or 0) - START_EQUITY) < 1e-6:
                raw["mtm"] = raw["equity"]
            raw["configured"] = True
            logger.info(
                "hive hydrate %s: restored %d closes from Supabase, equity≈%.2f",
                runner,
                len(merged),
                float(raw["equity"]),
            )
        elif len(merged) > len(file_trades):
            raw["trades"] = merged[-80:]
            logger.info("hive hydrate %s: merged %d Supabase closes into file state", runner, len(merged) - len(file_trades))
        _STATE["runners"][runner] = raw
        for t in merged:
            at = str(t.get("at") or "")
            if at and (earliest is None or at < earliest):
                earliest = at
    if earliest and _STATE.get("started_at"):
        # Keep the older race start if we rebuilt history that predates this container boot.
        if earliest < str(_STATE["started_at"]):
            _STATE["started_at"] = earliest
    elif earliest and not _STATE.get("started_at"):
        _STATE["started_at"] = earliest


async def _order(
    client: Any,
    side: str,
    coin: str,
    notional: float,
    price: float,
    reduce_only: bool,
    coin_qty: Optional[float] = None,
) -> float:
    qty = abs(float(coin_qty)) if coin_qty and coin_qty > 0 else (notional / price if price else 0)
    if qty <= 0:
        return 0.0
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


def _find_pos(book: Book, coin: Optional[str]) -> Optional[Pos]:
    if not coin:
        return book.position
    for p in book.positions:
        if p.coin == coin:
            return p
    if book.position and book.position.coin == coin:
        return book.position
    return None


async def _reconcile_okx(
    book: Book, client: Any, coins: List[Coin], now: float
) -> Tuple[Dict[str, float], float]:
    """Overwrite book.positions from live OKX opens; return per-coin UPL and total."""
    from src.runners.rules import mark as ratchet_stop

    state = await client.get_user_state()
    by_coin = {c.coin: c for c in coins}
    prev = {p.coin: p for p in book.positions}
    if book.position and book.position.coin not in prev:
        prev[book.position.coin] = book.position

    upl_by_coin: Dict[str, float] = {}
    positions: List[Pos] = []
    for p in state.get("positions") or []:
        try:
            sz = float(p.get("quantity") or p.get("szi") or 0)
        except (TypeError, ValueError):
            sz = 0.0
        if abs(sz) <= 0:
            continue
        coin = str(p.get("coin") or p.get("symbol") or "").upper()
        coin = coin.replace("-USDT-SWAP", "").replace("-USDT", "")
        if not coin:
            continue
        side = "long" if sz > 0 else "short"
        entry = float(p.get("entryPrice") or p.get("entry_price") or p.get("entryPx") or 0)
        mark_px = float(p.get("markPrice") or p.get("current_price") or entry)
        notional = abs(float(p.get("notional") or (abs(sz) * (mark_px or entry or 0))))
        live_upl = float(p.get("pnl") or p.get("unrealized_pnl") or 0)
        upl_by_coin[coin] = live_upl
        old = prev.get(coin)
        quote = by_coin.get(coin)
        if old:
            stop = old.stop_px
            peak = old.peak_px
            opened_at = old.opened_at
            added = old.added or notional > ENTRY_NOTIONAL * 1.15
            risk = old.risk_usd
        else:
            pct = stop_pct(quote) if quote else None
            stop = stop_price(side, entry, pct or 8.0) if entry else entry
            peak = mark_px or entry
            opened_at = now
            added = notional > ENTRY_NOTIONAL * 1.15
            risk = abs(entry - stop) / entry * notional if entry else 0.0
        row = Pos(
            coin=coin,
            side=side,
            entry_px=entry or (quote.mid if quote else 0),
            notional=notional or ENTRY_NOTIONAL,
            coin_qty=abs(sz),
            opened_at=opened_at,
            stop_px=stop,
            peak_px=peak,
            added=added,
            risk_usd=risk,
        )
        if quote:
            ratchet_stop(row, quote)
        positions.append(row)

    positions.sort(key=lambda r: r.notional, reverse=True)
    book.positions = positions
    book.sync_primary()
    return upl_by_coin, float(sum(upl_by_coin.values()))


async def _act(runner: str, client: Any, book: Book, coins: List[Coin], action: Action, trades: List[dict], now: float) -> None:
    by_coin = {c.coin: c for c in coins}
    if action.kind == "hold":
        return
    if action.kind == "close":
        pos = _find_pos(book, action.coin)
        if not pos:
            return
        coin = by_coin.get(pos.coin)
        mid = coin.mid if coin else pos.entry_px
        qty = await _order(client, pos.side, pos.coin, pos.notional, mid, True, coin_qty=pos.coin_qty)
        pnl = upl(pos, mid)
        fee = fee_usd(pos.notional)
        book.equity += pnl - fee
        book.fees_today += fee
        book.last_order_at = now
        book.positions = [p for p in book.positions if p.coin != pos.coin]
        book.sync_primary()
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
        # Mayhem closes are NOT mirrored — Oppo keeps the fade until its own exit.
        return
    if action.kind == "open":
        # Oppo Mayhem must mirror Mayhem even when already full — free the worst slot.
        if len(book.positions) >= MAX_OPENS:
            if runner == "oppo_mayhem" and action.reason == "oppo_pair_open" and book.positions:
                worst = min(
                    book.positions,
                    key=lambda p: upl(p, by_coin[p.coin].mid if p.coin in by_coin else p.entry_px),
                )
                await _act(
                    runner,
                    client,
                    book,
                    coins,
                    Action("close", worst.coin, worst.side, reason="oppo_slot"),
                    trades,
                    now,
                )
            else:
                return
        if len(book.positions) >= MAX_OPENS:
            return
        coin = by_coin.get(action.coin or "")
        if not coin or not action.side:
            return
        if any(p.coin == coin.coin for p in book.positions):
            return
        notional = action.notional or ENTRY_NOTIONAL
        qty = await _order(client, action.side, coin.coin, notional, coin.mid, False)
        fee = fee_usd(notional)
        book.equity -= fee
        book.fees_today += fee
        book.trades_today += 1
        book.last_order_at = now
        book.positions.append(_open_pos(coin, action.side, notional, qty, now))
        book.sync_primary()
        if runner == "mayhem":
            _queue_oppo_pair(Action("open", coin.coin, action.side, notional, action.reason or "entry"))
        return
    if action.kind == "add":
        pos = _find_pos(book, action.coin)
        if not pos:
            return
        coin = by_coin.get(pos.coin)
        if not coin:
            return
        # Oppo pair add must stay on the flipped side already held.
        if runner == "oppo_mayhem" and action.side and action.side != pos.side:
            return
        add_n = float(action.notional or ADD_NOTIONAL)
        qty = await _order(client, pos.side, pos.coin, add_n, coin.mid, False)
        fee = fee_usd(add_n)
        book.equity -= fee
        book.fees_today += fee
        book.last_order_at = now
        filled = pos.notional + add_n
        pos.entry_px = (pos.entry_px * pos.notional + coin.mid * add_n) / filled
        pos.notional = filled
        pos.coin_qty += qty
        pos.added = True
        pct = stop_pct(coin)
        if pct:
            fresh = stop_price(pos.side, pos.entry_px, pct)
            if pos.side == "long":
                pos.stop_px = max(pos.stop_px, min(fresh, pos.entry_px))
            else:
                pos.stop_px = min(pos.stop_px, max(fresh, pos.entry_px))
        book.sync_primary()
        if runner == "mayhem":
            _queue_oppo_pair(Action("add", pos.coin, pos.side, add_n, action.reason or "pyramid"))
        return
    if action.kind == "switch":
        # Handled as close+open action lists from decide_actions.
        return


def _ayo_closed_trades(limit: int = 80) -> List[dict]:
    """AYO closes logged by the mechanical book. The live OKX snapshot has opens only."""
    # Fetch extra then drop dust/$0 spam so the race board stays readable.
    rows = _fetch_runner_trades("ayo", limit=min(max(limit * 5, 200), 500))
    started = str(_STATE.get("started_at") or "")
    if started:
        rows = [t for t in rows if str(t.get("at") or "") >= started]
    cleaned: List[dict] = []
    for t in rows:
        action = str(t.get("action") or "").lower()
        pnl = abs(float(t.get("pnl") or 0))
        if "dust" in action:
            continue
        # Dust sweep used to log empty mech closes as action=mechanical pnl=$0.
        if pnl < 0.01 and action in ("mechanical", "close", ""):
            continue
        cleaned.append(t)
    return cleaned[-limit:]


async def _refresh_ayo() -> None:
    """AYO on the race board = $300 practice ledger (same start as hive runners).

    Opens/UPL still come from the live OKX book; wallet size is ignored for equity/%.
    """
    global _AYO_CLIENT
    if _AYO_CLIENT is None:
        return
    try:
        from src.utils.practice_bankroll import practice_bankroll_usd

        state = await _AYO_CLIENT.get_user_state()
        wallet = float(state.get("total_value") or state.get("balance") or 0)
        start = float(practice_bankroll_usd() or START_EQUITY)
        # Force practice start — never lock onto a fat demo wallet snapshot.
        _STATE["ayo_start_equity"] = start
        closed = _ayo_closed_trades()
        realized = sum(float(t.get("pnl") or 0) for t in closed)
        equity = start + realized
        positions = state.get("positions") or []
        pos = None
        open_positions: List[Dict[str, Any]] = []
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
            row = {
                "coin": coin or "?",
                "side": side,
                "notional": round(notional, 2),
                "entry_px": entry,
                "stop_px": float(p.get("stop_px") or p.get("liquidation_price") or p.get("liquidationPx") or 0),
                "upl": round(upl, 4),
                "added": False,
            }
            open_positions.append(row)
            if notional >= best_notional:
                best_notional = notional
                pos = row
        open_positions.sort(key=lambda r: float(r.get("notional") or 0), reverse=True)
        mtm = equity + upl_total
        pct = ((mtm - start) / start * 100) if start else 0.0
        _STATE["runners"]["ayo"] = {
            "configured": True,
            "equity": round(mtm, 4),
            "mtm": round(mtm, 4),
            "account_equity": round(wallet, 2),  # real OKX wallet — display only, not race math
            "ayo_start_equity": round(start, 2),
            "pct": round(pct, 2),
            "upl": round(upl_total, 4),
            "position": pos,
            "positions": open_positions,
            "open_count": open_count,
            "trades_today": 0,
            "fees_today": 0,
            "max_dd_pct": 0,
            "cap": None,
            "error": None,
            "trades": closed,
            "note": (
                f"Practice ${start:,.0f} book · {open_count} open · "
                f"OKX wallet ${wallet:,.0f} ignored for race %"
            ),
        }
    except Exception as exc:
        raw = _STATE["runners"].get("ayo") or {}
        raw["configured"] = True
        raw["error"] = str(exc)[:240]
        _STATE["runners"]["ayo"] = raw
        logger.warning("hive ayo refresh failed: %s", exc)


async def _tick(decide: bool = True) -> None:
    coins = await asyncio.to_thread(build_coins) if decide else []
    now = time.time()
    await _refresh_ayo()
    async with _LOCK:
        if decide:
            _STATE["market_coins"] = len(coins)
        _STATE["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
        _STATE["last_error"] = None
        # RUNNERS order: Mayhem before Oppo Mayhem so the pair queue is filled first.
        for runner in RUNNERS:
            client = _CLIENTS.get(runner)
            if not client:
                continue
            raw = _STATE["runners"].get(runner) or {}
            book = _book_from(raw)
            book.day_key = raw.get("day_key")  # type: ignore[attr-defined]
            trades = list(raw.get("trades") or [])
            error = None
            upl_by_coin: Dict[str, float] = {}
            live_upl = 0.0
            try:
                if not coins:
                    coins = await asyncio.to_thread(build_coins)
                    _STATE["market_coins"] = len(coins)
                upl_by_coin, live_upl = await _reconcile_okx(book, client, coins, now)
                if decide:
                    _roll_day(book, now)
                    book.day_key = time.strftime("%Y-%m-%d", time.gmtime(now))  # type: ignore[attr-defined]
                    actions: List[Action] = []
                    if runner == "oppo_mayhem":
                        mayhem_raw = _STATE["runners"].get("mayhem") or {}
                        mayhem_book = _book_from(mayhem_raw)
                        # Pair opens/adds with Mayhem; exits = Oppo 1.5R / stop / time (no bail).
                        actions.extend(_oppo_sync_actions(mayhem_book, book))
                        actions.extend(_OPPO_PAIR_Q)
                        _OPPO_PAIR_Q.clear()
                        actions.extend(decide_actions(runner, book, coins, now))
                    else:
                        actions.extend(decide_actions(runner, book, coins, now))
                    for action in _dedupe_actions(actions):
                        await _act(runner, client, book, coins, action, trades, now)
                    # Re-read exchange after orders so cards show fresh UPL/size.
                    upl_by_coin, live_upl = await _reconcile_okx(book, client, coins, now)
                # Mayhem stays live for Oppo 1:1 even when the race ledger is deep red.
                if runner != "mayhem" and book.equity <= RETIRE_EQUITY:
                    book.cap = "retired"
                elif runner == "mayhem" and book.cap in ("retired", "loss_stop"):
                    book.cap = None
            except Exception as exc:
                error = str(exc)
                logger.warning("hive %s tick failed: %s", runner, exc)
            # Prefer live OKX UPL; fall back to mid mark if reconcile failed.
            if not upl_by_coin and book.positions:
                for pos in book.positions:
                    match = next((c for c in coins if c.coin == pos.coin), None)
                    mid = match.mid if match else pos.entry_px
                    upl_by_coin[pos.coin] = upl(pos, mid)
                live_upl = float(sum(upl_by_coin.values()))
            mtm = book.equity + live_upl
            _note_dd(book, mtm)
            stored = _book_to(book, trades, True, error, upl_by_coin)
            stored["day_key"] = time.strftime("%Y-%m-%d", time.gmtime(now))
            stored["mtm"] = round(mtm, 4)
            stored["upl"] = round(live_upl, 4)
            stored["note"] = f"{len(book.positions)}/{MAX_OPENS} open · ${ENTRY_MARGIN:.0f}+${ADD_MARGIN:.0f} margin"
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
        upl_val = raw.get("upl")
        positions = list(raw.get("positions") or [])
        if not positions and isinstance(pos, dict):
            positions = [pos]
        # Ensure each open carries live UPL for cards when only book-level UPL exists.
        if (
            positions
            and len(positions) == 1
            and isinstance(positions[0], dict)
            and positions[0].get("upl") is None
            and isinstance(upl_val, (int, float))
        ):
            positions = [{**positions[0], "upl": round(float(upl_val), 4)}]
        if isinstance(pos, dict) and pos.get("upl") is None and positions:
            pos = positions[0]
        rows.append(
            {
                "id": runner,
                "name": RUNNER_LABELS[runner],
                "configured": bool(raw.get("configured")) if runner == "ayo" else configured,
                "equity": mtm,
                "pct": None if pct is None else round(float(pct), 2),
                "upl": upl_val,
                "position": pos,
                "positions": positions,
                "open_count": raw.get("open_count") if raw.get("open_count") is not None else len(positions or []),
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
        "margin_entry": ENTRY_MARGIN,
        "margin_added": ADD_MARGIN,
        "max_opens": MAX_OPENS,
        "leverage": LEVERAGE,
        "runners": rows,
    }


async def start_hive_race() -> None:
    global _AYO_CLIENT
    if not _enabled():
        logger.info("hive race off (HIVE_RACE=false)")
        return
    _load()
    _hydrate_from_supabase()
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
    # Fast mark loop keeps Open PnL live; decide every 4th cycle (~60s).
    mark_n = 0
    while True:
        try:
            mark_n += 1
            await _tick(decide=(mark_n % 4 == 1))
        except Exception as exc:
            _STATE["last_error"] = str(exc)
            logger.warning("hive race tick failed: %s", exc)
        await asyncio.sleep(15)
