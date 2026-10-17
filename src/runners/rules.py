"""Fixed entry and exit rules for the five hive runners.

Shared risk matches the public BeeBots momentum engine, resized for a $300
book at 10x isolated: $30 margin ($300 notional) to enter, one +$15 add
($450 notional) only after price has already run 1.5 x 1h ATR in favor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

START_EQUITY = 300.0
LEVERAGE = 10
ENTRY_NOTIONAL = 300.0
ADD_NOTIONAL = 150.0
MAX_NOTIONAL = 450.0
MIN_VOL_USD = 15_000_000.0
MAX_SPREAD_BP = 8.0
STOP_ATR = 2.5
TRAIL_ATR = 2.0
MAX_STOP_PCT = 8.0
MIN_HOLD_S = 30 * 60
TIME_STOP_S = 12 * 3600
COOLDOWN_S = 60 * 60
MAX_TRADES = 3
FEE_BUDGET = 1.50
DAILY_LOSS_PCT = 0.08
RETIRE_EQUITY = 120.0
TAKER_FEE = 0.0005
LOCKS = ((1.5, 0.4), (3.0, 0.6), (5.0, 0.75))

RUNNERS = ("mayhem", "windward", "orakelia", "peaksting", "peakstinger")
RUNNER_LABELS = {
    "mayhem": "Mayhem",
    "windward": "Windward",
    "orakelia": "Orakelia",
    "peaksting": "PeakSting",
    "peakstinger": "Peak Stinger",
    "ayo": "AYO",
}


@dataclass
class Coin:
    coin: str
    mid: float
    spread_bp: float
    vol24h_usd: float
    ret1h: float
    ret24h: float
    ret7d: float
    rsi14: Optional[float]
    atr1h_pct: Optional[float]
    oi_chg_1h: float
    funding_z: float
    vol_z: float
    high24h: float


@dataclass
class Pos:
    coin: str
    side: str
    entry_px: float
    notional: float
    coin_qty: float
    opened_at: float
    stop_px: float
    peak_px: float
    added: bool
    risk_usd: float


@dataclass
class Book:
    equity: float = START_EQUITY
    day_start_equity: float = START_EQUITY
    trades_today: int = 0
    fees_today: float = 0.0
    last_order_at: float = 0.0
    cap: Optional[str] = None
    position: Optional[Pos] = None
    blacklist: Dict[str, bool] = field(default_factory=dict)
    peak_equity: float = START_EQUITY
    max_dd_pct: float = 0.0


@dataclass
class Action:
    kind: str
    coin: Optional[str] = None
    side: Optional[str] = None
    notional: float = 0.0
    reason: str = ""


def stop_pct(coin: Coin) -> Optional[float]:
    if coin.atr1h_pct is None or coin.atr1h_pct <= 0:
        return None
    pct = STOP_ATR * coin.atr1h_pct
    if pct > MAX_STOP_PCT or pct <= 0:
        return None
    return pct


def stop_price(side: str, entry: float, pct: float) -> float:
    if side == "long":
        return entry * (1 - pct / 100)
    return entry * (1 + pct / 100)


def liquid(coin: Coin) -> bool:
    return coin.mid > 0 and coin.vol24h_usd >= MIN_VOL_USD and coin.spread_bp <= MAX_SPREAD_BP


def impulse(coin: Coin) -> Optional[str]:
    rsi = 50.0 if coin.rsi14 is None else coin.rsi14
    if coin.ret24h >= 1.5 and coin.ret1h >= 0 and coin.oi_chg_1h >= 0 and rsi <= 78 and coin.funding_z <= 1.5:
        return "long"
    if coin.ret24h <= -1.5 and coin.ret1h < 0 and rsi >= 22:
        return "short"
    return None


def _tradable(coin: Coin) -> bool:
    return liquid(coin) and stop_pct(coin) is not None


def upl(pos: Pos, mid: float) -> float:
    direction = 1 if pos.side == "long" else -1
    if pos.entry_px <= 0:
        return 0.0
    return direction * (mid - pos.entry_px) / pos.entry_px * pos.notional


def _against(pos: Pos, coin: Coin) -> bool:
    return (pos.side == "long" and coin.ret1h < 0) or (pos.side == "short" and coin.ret1h > 0)


def _run_atr(pos: Pos, coin: Coin) -> float:
    if coin.atr1h_pct is None or coin.atr1h_pct <= 0 or coin.mid <= 0:
        return 0.0
    atr = coin.mid * coin.atr1h_pct / 100
    if atr <= 0:
        return 0.0
    direction = 1 if pos.side == "long" else -1
    return direction * (coin.mid - pos.entry_px) / atr


def mark(pos: Pos, coin: Coin) -> None:
    """Ratchet the stop with the trail and the profit lock. Never loosens it."""
    if pos.side == "long":
        if coin.mid > pos.peak_px:
            pos.peak_px = coin.mid
    elif coin.mid < pos.peak_px:
        pos.peak_px = coin.mid

    if coin.atr1h_pct and coin.atr1h_pct > 0:
        dist = coin.mid * TRAIL_ATR * coin.atr1h_pct / 100
        trail = coin.mid - dist if pos.side == "long" else coin.mid + dist
        _ratchet(pos, trail)

    direction = 1 if pos.side == "long" else -1
    move = direction * (pos.peak_px - pos.entry_px)
    if move > 0 and pos.entry_px > 0:
        gain = move / pos.entry_px * 100
        keep = 0.0
        for at, k in LOCKS:
            if gain >= at:
                keep = max(keep, k)
        if keep > 0:
            _ratchet(pos, pos.entry_px + direction * keep * move)


def _ratchet(pos: Pos, cand: float) -> None:
    if pos.side == "long" and cand > pos.stop_px:
        pos.stop_px = cand
    elif pos.side == "short" and cand < pos.stop_px:
        pos.stop_px = cand


def stop_hit(pos: Pos, mid: float) -> bool:
    if pos.side == "long":
        return mid <= pos.stop_px
    return mid >= pos.stop_px


def _mayhem(coins: List[Coin]) -> List[Coin]:
    rows = [c for c in coins if _tradable(c) and impulse(c)]
    return sorted(rows, key=lambda c: (abs(c.ret1h), abs(c.ret24h)), reverse=True)


def _windward(coins: List[Coin]) -> List[Coin]:
    rows = []
    for c in coins:
        side = impulse(c)
        if not side or not _tradable(c) or c.vol_z <= 0:
            continue
        if c.ret24h == 0 or c.ret7d == 0 or (c.ret24h > 0) != (c.ret7d > 0):
            continue
        if (c.ret1h > 0) != (c.ret24h > 0) and not (c.ret1h == 0 and c.ret24h > 0):
            continue
        rows.append(c)
    return sorted(rows, key=lambda c: (abs(c.ret24h), abs(c.ret7d)), reverse=True)


def _orakelia(coins: List[Coin], blacklist: Dict[str, bool]) -> List[Coin]:
    ranked = sorted((c for c in coins if liquid(c)), key=lambda c: c.ret7d, reverse=True)
    if ranked and ranked[0].vol_z > 0:
        blacklist.pop(ranked[0].coin, None)
    rows = []
    for c in ranked:
        if blacklist.get(c.coin):
            continue
        if c.ret24h > 0 and c.vol_z > 0 and _tradable(c):
            rows.append(c)
    return rows


def _off_high_pct(coin: Coin) -> float:
    if coin.high24h <= 0:
        return 0.0
    return (coin.high24h - coin.mid) / coin.high24h * 100


def _peaksting(coins: List[Coin]) -> List[Coin]:
    rows = [
        c
        for c in coins
        if _tradable(c) and c.ret24h >= 6 and c.vol_z >= 1.5 and c.ret1h < 0 and _off_high_pct(c) >= 1.5
    ]
    return sorted(rows, key=lambda c: c.ret24h, reverse=True)


def _peakstinger(coins: List[Coin]) -> List[Coin]:
    rows = []
    for c in coins:
        rsi = c.rsi14 if c.rsi14 is not None else 0
        if not _tradable(c) or c.ret24h < 6 or rsi < 70 or c.ret1h >= 0 or c.vol_z >= 0:
            continue
        if c.oi_chg_1h > 0:
            continue
        rows.append(c)
    return sorted(rows, key=lambda c: (c.rsi14 or 0, c.ret24h), reverse=True)


def candidates(runner: str, coins: List[Coin], book: Book) -> List[Coin]:
    if runner == "mayhem":
        return _mayhem(coins)
    if runner == "windward":
        return _windward(coins)
    if runner == "orakelia":
        return _orakelia(coins, book.blacklist)
    if runner == "peaksting":
        return _peaksting(coins)
    if runner == "peakstinger":
        return _peakstinger(coins)
    return []


def _side_for(runner: str, coin: Coin) -> str:
    if runner in ("peaksting", "peakstinger"):
        return "short"
    if runner == "orakelia":
        return "long"
    return impulse(coin) or "long"


def _blocked(book: Book, now: float) -> Optional[str]:
    if book.cap == "retired":
        return "retired"
    if book.equity <= RETIRE_EQUITY:
        return "retired"
    if book.cap == "loss_stop":
        return "loss_stop"
    if book.equity <= book.day_start_equity * (1 - DAILY_LOSS_PCT):
        return "loss_stop"
    if book.trades_today >= MAX_TRADES:
        return "trade_cap"
    if book.fees_today >= FEE_BUDGET:
        return "fee_budget"
    if book.last_order_at and now - book.last_order_at < COOLDOWN_S:
        return "cooldown"
    return None


def decide(runner: str, book: Book, coins: List[Coin], now: float) -> Action:
    by_coin = {c.coin: c for c in coins}
    pos = book.position
    if pos:
        coin = by_coin.get(pos.coin)
        mid = coin.mid if coin else pos.peak_px
        if coin:
            mark(pos, coin)
            mid = coin.mid
        if stop_hit(pos, mid):
            return Action("close", pos.coin, pos.side, reason="stop")
        if now - pos.opened_at >= TIME_STOP_S:
            return Action("close", pos.coin, pos.side, reason="time_stop")
        block = _blocked(book, now)
        if block in ("retired", "loss_stop", "trade_cap", "fee_budget"):
            book.cap = block if block != "cooldown" else book.cap
            return Action("hold", reason=block or "capped")
        if now - pos.opened_at < MIN_HOLD_S:
            return Action("hold", reason="min_hold")
        if coin is None:
            return Action("hold", reason="no_quote")

        if runner == "mayhem":
            if _against(pos, coin) or upl(pos, mid) < 0:
                return Action("close", pos.coin, pos.side, reason="bail")
            ranked = _mayhem(coins)
            if ranked and ranked[0].coin != pos.coin:
                nxt = ranked[0]
                return Action("switch", nxt.coin, _side_for(runner, nxt), ENTRY_NOTIONAL, "rotate")
        elif runner == "windward":
            if (coin.ret1h > 0) != (coin.ret24h > 0) and coin.ret1h != 0:
                return Action("close", pos.coin, pos.side, reason="trend_break")
        elif runner == "orakelia":
            if coin.ret1h < 0 or coin.vol_z < 0:
                book.blacklist[pos.coin] = True
                return Action("close", pos.coin, pos.side, reason="momentum_fade")
        elif runner in ("peaksting", "peakstinger"):
            if coin.ret1h > 0:
                return Action("close", pos.coin, pos.side, reason="fade_weakened")

        if (
            not pos.added
            and upl(pos, mid) > 0
            and _run_atr(pos, coin) >= 1.5
            and pos.notional + ADD_NOTIONAL <= MAX_NOTIONAL + 1e-6
            and (runner != "orakelia" or coin.ret1h >= 0)
        ):
            return Action("add", pos.coin, pos.side, ADD_NOTIONAL, "pyramid")
        return Action("hold", reason="ride")

    block = _blocked(book, now)
    if block:
        if block in ("retired", "loss_stop", "trade_cap", "fee_budget"):
            book.cap = block
        return Action("hold", reason=block)
    ranked = candidates(runner, coins, book)
    if not ranked:
        return Action("hold", reason="no_setup")
    pick = ranked[0]
    return Action("open", pick.coin, _side_for(runner, pick), ENTRY_NOTIONAL, "entry")


def fee_usd(notional: float) -> float:
    return abs(notional) * TAKER_FEE


def apply_equity_mark(book: Book, mid: Optional[float]) -> None:
    realized = book.equity
    if book.position and mid:
        # equity field is cash; mark-to-market is reported separately by the loop.
        pass
    if realized > book.peak_equity:
        book.peak_equity = realized
    if book.peak_equity > 0:
        dd = (book.peak_equity - realized) / book.peak_equity * 100
        if dd > book.max_dd_pct:
            book.max_dd_pct = dd
