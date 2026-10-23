"""Fixed entry and exit rules for the five hive runners.

$300 race start equity, up to 3 concurrent opens at $40 margin ($400 notional)
10x isolated, one +$15 margin add ($150 notional) after price has run 1.5x 1h ATR.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

START_EQUITY = 300.0
LEVERAGE = 10
ENTRY_MARGIN = 40.0
ADD_MARGIN = 15.0
ENTRY_NOTIONAL = ENTRY_MARGIN * LEVERAGE  # 400
ADD_NOTIONAL = ADD_MARGIN * LEVERAGE  # 150
MAX_SLOT_NOTIONAL = ENTRY_NOTIONAL + ADD_NOTIONAL  # 550
MAX_NOTIONAL = MAX_SLOT_NOTIONAL  # backward-compatible alias
MAX_OPENS = 3
MIN_VOL_USD = 15_000_000.0
MAX_SPREAD_BP = 8.0
STOP_ATR = 2.5
TRAIL_ATR = 2.0
MAX_STOP_PCT = 8.0
MIN_HOLD_S = 30 * 60
TIME_STOP_S = 0  # disabled — crypto 24/7, no position time stops
COOLDOWN_S = 10 * 60
# Entry count and fee spend do not gate new trades. Crypto books stay live 24/7 —
# no daily-loss / retire freeze and no position time stops (policy).
DAILY_LOSS_PCT = 0.08  # retained for metrics only; not used to block
RETIRE_EQUITY = 120.0  # retained for metrics only; not used to block
TAKER_FEE = 0.0005
LOCKS = ((1.5, 0.4), (3.0, 0.6), (5.0, 0.75))

# Oppo Mayhem: 1:1 opposite *entries*; bank ~1R / giveback — no clock exit.
# PeakSting: idle (no Mayhem sync) — slot reserved for a future solo strategy.
OPPO_FADE_R = 1.0          # full take-profit once UPL >= 1R
OPPO_GIVEBACK_ARM_R = 0.5  # arm giveback after peak UPL hits 0.5R
OPPO_GIVEBACK_KEEP = 0.4   # close if open UPL falls below 40% of peak
OPPO_TIME_STOP_S = 0       # disabled — crypto 24/7, no position time stops
RUNNERS = ("mayhem", "windward", "orakelia", "peaksting", "oppo_mayhem")
RUNNER_LABELS = {
    "mayhem": "Mayhem",
    "windward": "Windward",
    "orakelia": "Orakelia",
    "peaksting": "Peak · idle",
    "oppo_mayhem": "Oppo Mayhem",
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
    positions: List[Pos] = field(default_factory=list)
    blacklist: Dict[str, bool] = field(default_factory=dict)
    peak_equity: float = START_EQUITY
    max_dd_pct: float = 0.0

    def sync_primary(self) -> None:
        if self.positions:
            self.position = max(self.positions, key=lambda p: p.notional)
        else:
            self.position = None


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
    if runner == "oppo_mayhem":
        # Entries only arrive via 1:1 Mayhem pair queue in hive_race.
        return []
    if runner == "windward":
        return _windward(coins)
    if runner == "orakelia":
        return _orakelia(coins, book.blacklist)
    if runner == "peaksting":
        # Entries only via hard 1:1 Mayhem pair sync in hive_race.
        return []
    return []


def flip_side(side: str) -> str:
    """Opposite of a long/short position side (Mayhem mirrors)."""
    return "short" if side == "long" else "long"


def _side_for(runner: str, coin: Coin) -> str:
    if runner == "orakelia":
        return "long"
    if runner in ("oppo_mayhem", "peaksting"):
        return flip_side(impulse(coin) or "long")
    return impulse(coin) or "long"


def _blocked(book: Book, now: float, runner: str = "") -> Optional[str]:
    # All crypto hive runners stay live 24/7 — clear retire / daily-loss freezes.
    del runner  # all runners share the same always-live policy
    if book.cap in ("retired", "loss_stop", "trade_cap", "fee_budget"):
        book.cap = None
    if book.last_order_at and now - book.last_order_at < COOLDOWN_S:
        return "cooldown"
    return None


def _open_slots(book: Book) -> List[Pos]:
    if book.positions:
        return list(book.positions)
    if book.position:
        return [book.position]
    return []


def _exit_reason(runner: str, pos: Pos, coin: Optional[Coin], mid: float, book: Book, now: float) -> Optional[str]:
    # Peak idle — no solo exits here; tick flattens leftovers via peak_idle closes.
    if runner == "peaksting":
        return None
    if stop_hit(pos, mid):
        return "stop"
    # No position time stops (crypto 24/7 policy). TIME_STOP_S / OPPO_TIME_STOP_S are 0.
    hold_s = OPPO_TIME_STOP_S if runner == "oppo_mayhem" else TIME_STOP_S
    if hold_s > 0 and now - pos.opened_at >= hold_s:
        return "time_stop"
    if coin is None:
        return None
    if runner == "mayhem":
        if _against(pos, coin) or upl(pos, mid) < 0:
            return "bail"
    elif runner == "oppo_mayhem":
        # Bank ~1R; if it ran 0.5R+ and gave most of it back, scratch — don't wait 12h.
        risk = float(pos.risk_usd or 0)
        u = upl(pos, mid)
        peak_u = upl(pos, pos.peak_px)
        if risk > 0 and u >= OPPO_FADE_R * risk:
            return "fade_target"
        if risk > 0 and peak_u >= OPPO_GIVEBACK_ARM_R * risk:
            floor = max(OPPO_GIVEBACK_KEEP * peak_u, 0.15 * risk)
            if u < floor:
                return "fade_giveback"
    elif runner == "windward":
        if (coin.ret1h > 0) != (coin.ret24h > 0) and coin.ret1h != 0:
            return "trend_break"
    elif runner == "orakelia":
        if coin.ret1h < 0 or coin.vol_z < 0:
            book.blacklist[pos.coin] = True
            return "momentum_fade"
    return None


def decide_actions(runner: str, book: Book, coins: List[Coin], now: float) -> List[Action]:
    """Manage every open, then fill free slots. At most one open/add/rotate per call list."""
    by_coin = {c.coin: c for c in coins}
    opens = _open_slots(book)
    book.positions = opens
    book.sync_primary()

    # Cooldown only — retire / daily-loss freezes disabled for crypto 24/7.
    block = _blocked(book, now, runner=runner)
    if block == "cooldown" and not opens:
        return [Action("hold", reason=block)]

    # Trim OKX orphans above the race slot cap (smallest first).
    if len(opens) > MAX_OPENS:
        extras = sorted(opens, key=lambda p: p.notional)[: len(opens) - MAX_OPENS]
        return [Action("close", p.coin, p.side, reason="slot_cap") for p in extras]

    closes: List[Action] = []
    for pos in opens:
        coin = by_coin.get(pos.coin)
        mid = coin.mid if coin else pos.peak_px
        if coin:
            mark(pos, coin)
            mid = coin.mid
        reason = _exit_reason(runner, pos, coin, mid, book, now)
        if reason:
            closes.append(Action("close", pos.coin, pos.side, reason=reason))
    if closes:
        return closes

    # Min-hold / missing quote: ride for now.
    for pos in opens:
        if now - pos.opened_at < MIN_HOLD_S:
            continue
        coin = by_coin.get(pos.coin)
        if coin is None:
            continue
        mid = coin.mid
        if (
            runner not in ("oppo_mayhem", "peaksting")  # mirrors: adds only via Mayhem pair
            and not pos.added
            and upl(pos, mid) > 0
            and _run_atr(pos, coin) >= 1.5
            and pos.notional + ADD_NOTIONAL <= MAX_SLOT_NOTIONAL + 1e-6
            and (runner != "orakelia" or coin.ret1h >= 0)
        ):
            return [Action("add", pos.coin, pos.side, ADD_NOTIONAL, "pyramid")]

    # Mayhem: free a slot for a stronger name when full (Oppo mirrors via pair queue).
    if runner == "mayhem" and len(opens) >= MAX_OPENS:
        ranked = _mayhem(coins)
        held = {p.coin for p in opens}
        if ranked and ranked[0].coin not in held:
            worst = min(opens, key=lambda p: upl(p, by_coin[p.coin].mid if p.coin in by_coin else p.entry_px))
            nxt = ranked[0]
            return [
                Action("close", worst.coin, worst.side, reason="rotate"),
                Action("open", nxt.coin, _side_for(runner, nxt), ENTRY_NOTIONAL, "rotate"),
            ]

    if block:
        return [Action("hold", reason=block)]

    if len(opens) >= MAX_OPENS:
        return [Action("hold", reason="slots_full")]

    ranked = candidates(runner, coins, book)
    held = {p.coin for p in opens}
    for pick in ranked:
        if pick.coin in held:
            continue
        return [Action("open", pick.coin, _side_for(runner, pick), ENTRY_NOTIONAL, "entry")]
    return [Action("hold", reason="no_setup" if not ranked else "all_held")]


def decide(runner: str, book: Book, coins: List[Coin], now: float) -> Action:
    """Backward-compatible single-action wrapper."""
    actions = decide_actions(runner, book, coins, now)
    return actions[0] if actions else Action("hold", reason="empty")


def fee_usd(notional: float) -> float:
    return abs(notional) * TAKER_FEE


def apply_equity_mark(book: Book, mid: Optional[float]) -> None:
    realized = book.equity
    if realized > book.peak_equity:
        book.peak_equity = realized
    if book.peak_equity > 0:
        dd = (book.peak_equity - realized) / book.peak_equity * 100
        if dd > book.max_dd_pct:
            book.max_dd_pct = dd
