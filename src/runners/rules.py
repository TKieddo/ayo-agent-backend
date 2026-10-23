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

# Mayhem: Super Scalper. Peak: Ichimoku+Hull. Oppo: UT Bot. Windward: Momentum ZZ (QQE).
OPPO_FADE_R = 1.0
OPPO_GIVEBACK_ARM_R = 0.5
OPPO_GIVEBACK_KEEP = 0.4
OPPO_TIME_STOP_S = 0
# Super Scalper — Mayhem only.
SS_RUNNERS = frozenset({"mayhem"})
SS_BAND_MULT = 1.0
SS_ATR_LEN = 14
SS_RSI_FAST = 25
SS_RSI_SLOW = 100
SS_STOP_PCT = 8.0
# Peak Ichimoku blend.
ICHI_RUNNERS = frozenset({"peaksting"})
ICHI_STOP_PCT = 8.0
# Oppo UT Bot (ATR trail flip; Heikin Ashi off).
UT_RUNNERS = frozenset({"oppo_mayhem"})
UT_KEY = 1.0
UT_ATR_LEN = 10
UT_STOP_PCT = 8.0
# Windward — Peter_O Momentum ZigZag (QQE default); flip + pivot SL (TP=200 skipped).
MZ_RUNNERS = frozenset({"windward"})
MZ_RSI_LEN = 14
MZ_QQE_FACTOR = 4.238
MZ_RSI_SMOOTH = 5
MZ_RSI5_OB = 80
MZ_RSI5_OS = 20
MZ_STOP_PCT = 8.0
# Pine-faithful flip books share disaster-stop + no trail.
FLIP_RUNNERS = SS_RUNNERS | ICHI_RUNNERS | UT_RUNNERS | MZ_RUNNERS
RUNNERS = ("mayhem", "windward", "orakelia", "peaksting", "oppo_mayhem", "confetti")
RUNNER_LABELS = {
    "mayhem": "Mayhem · Super Scalper",
    "windward": "Windward · Momentum ZZ",
    "orakelia": "Orakelia",
    "peaksting": "Peak · Ichimoku Hull",
    "oppo_mayhem": "Oppo · UT Bot",
    "confetti": "Confetti · Momentum",
    "ayo": "AYO",
}
# BeeBots Momentum (boozy) on Binance — see beebots_boozy.py
CONFETTI_RUNNERS = frozenset({"confetti"})


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
    # Super Scalper (15m completed bar)
    atr15_pct: Optional[float] = None
    ss_side: Optional[str] = None  # long | short
    ss_score: float = 0.0
    # Peak Ichimoku+Hull+MACD — full opposite signal only (not "condition cleared")
    ichi_side: Optional[str] = None  # long | short
    # Oppo UT Bot — buy/sell flip only
    ut_side: Optional[str] = None  # long | short
    # Windward Momentum ZigZag (QQE) — signal bar only
    mz_side: Optional[str] = None  # long | short
    mz_sl_px: Optional[float] = None  # pivot stop from Pine


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


def stop_pct(coin: Coin, runner: str = "") -> Optional[float]:
    if runner in FLIP_RUNNERS:
        # Pine flip books: no strategy.exit SL/TP — wide disaster stop only.
        if runner in SS_RUNNERS:
            return min(SS_STOP_PCT, MAX_STOP_PCT)
        if runner in ICHI_RUNNERS:
            return min(ICHI_STOP_PCT, MAX_STOP_PCT)
        if runner in MZ_RUNNERS:
            return min(MZ_STOP_PCT, MAX_STOP_PCT)
        return min(UT_STOP_PCT, MAX_STOP_PCT)
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


def _tradable(coin: Coin, runner: str = "") -> bool:
    return liquid(coin) and stop_pct(coin, runner=runner) is not None


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


def _super_scalper(coins: List[Coin]) -> List[Coin]:
    """ATR-band fade + dual RSI on completed 15m (Pine Super Scalper, fixed exits)."""
    rows = [c for c in coins if _tradable(c, "mayhem") and c.ss_side in ("long", "short")]
    return sorted(rows, key=lambda c: float(c.ss_score or 0), reverse=True)


def _ichi_hull(coins: List[Coin]) -> List[Coin]:
    """Pine Ichimoku + daily open + Hull MA + Hull-MACD — full signal only."""
    rows = [c for c in coins if _tradable(c, "peaksting") and c.ichi_side in ("long", "short")]
    return sorted(rows, key=lambda c: abs(c.ret24h), reverse=True)


def _ut_bot(coins: List[Coin]) -> List[Coin]:
    """Pine UT Bot — ATR trail cross buy/sell."""
    rows = [c for c in coins if _tradable(c, "oppo_mayhem") and c.ut_side in ("long", "short")]
    return sorted(rows, key=lambda c: abs(c.ret24h), reverse=True)


def _momentum_zz(coins: List[Coin]) -> List[Coin]:
    """Pine Momentum ZigZag (QQE) — GoLong/GoShort on completed 15m."""
    rows = [c for c in coins if _tradable(c, "windward") and c.mz_side in ("long", "short")]
    return sorted(rows, key=lambda c: abs(c.ret24h), reverse=True)


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


def candidates(runner: str, coins: List[Coin], book: Book) -> List[Coin]:
    if runner in SS_RUNNERS:
        return _super_scalper(coins)
    if runner in ICHI_RUNNERS:
        return _ichi_hull(coins)
    if runner in UT_RUNNERS:
        return _ut_bot(coins)
    if runner in MZ_RUNNERS:
        return _momentum_zz(coins)
    if runner == "orakelia":
        return _orakelia(coins, book.blacklist)
    return []


def flip_side(side: str) -> str:
    """Opposite of a long/short position side."""
    return "short" if side == "long" else "long"


def _side_for(runner: str, coin: Coin) -> str:
    if runner in SS_RUNNERS:
        return coin.ss_side or "long"
    if runner in ICHI_RUNNERS:
        return coin.ichi_side or "long"
    if runner in UT_RUNNERS:
        return coin.ut_side or "long"
    if runner in MZ_RUNNERS:
        return coin.mz_side or "long"
    if runner == "orakelia":
        return "long"
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
    if stop_hit(pos, mid):
        return "stop"
    hold_s = TIME_STOP_S
    if hold_s > 0 and now - pos.opened_at >= hold_s:
        return "time_stop"
    if coin is None:
        return None
    if runner in ICHI_RUNNERS:
        if coin.ichi_side and coin.ichi_side != pos.side:
            return "ichi_flip"
        return None
    if runner in SS_RUNNERS:
        if coin.ss_side and coin.ss_side != pos.side:
            return "ss_flip"
        return None
    if runner in UT_RUNNERS:
        if coin.ut_side and coin.ut_side != pos.side:
            return "ut_flip"
        return None
    if runner in MZ_RUNNERS:
        if coin.mz_side and coin.mz_side != pos.side:
            return "mz_flip"
        return None
    if runner == "orakelia":
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

    # Confetti: BeeBots Momentum (boozy) mechanical engine — hive notional, no LLM.
    if runner in CONFETTI_RUNNERS:
        if block == "cooldown" and opens:
            # Still trail/stop while cooling down after an order.
            from src.runners.beebots_boozy import decide_confetti

            acts = decide_confetti(book, coins, now)
            return [a for a in acts if a.kind == "close"] or [Action("hold", reason=block)]
        from src.runners.beebots_boozy import decide_confetti

        return decide_confetti(book, coins, now)

    # Trim OKX orphans above the race slot cap (smallest first).
    if len(opens) > MAX_OPENS:
        extras = sorted(opens, key=lambda p: p.notional)[: len(opens) - MAX_OPENS]
        return [Action("close", p.coin, p.side, reason="slot_cap") for p in extras]

    closes: List[Action] = []
    for pos in opens:
        coin = by_coin.get(pos.coin)
        mid = coin.mid if coin else pos.peak_px
        if coin:
            # Pine-faithful flip books: no hive ATR trail / locks — peak for UPL only.
            if runner in FLIP_RUNNERS:
                if pos.side == "long" and coin.mid > pos.peak_px:
                    pos.peak_px = coin.mid
                elif pos.side == "short" and coin.mid < pos.peak_px:
                    pos.peak_px = coin.mid
            else:
                mark(pos, coin)
            mid = coin.mid
        reason = _exit_reason(runner, pos, coin, mid, book, now)
        if reason:
            closes.append(Action("close", pos.coin, pos.side, reason=reason))
    if closes:
        return closes

    # Pyramid adds: Windward / Orakelia only.
    for pos in opens:
        if now - pos.opened_at < MIN_HOLD_S:
            continue
        coin = by_coin.get(pos.coin)
        if coin is None:
            continue
        mid = coin.mid
        if (
            runner not in FLIP_RUNNERS
            and not pos.added
            and upl(pos, mid) > 0
            and _run_atr(pos, coin) >= 1.5
            and pos.notional + ADD_NOTIONAL <= MAX_SLOT_NOTIONAL + 1e-6
            and (runner != "orakelia" or coin.ret1h >= 0)
        ):
            return [Action("add", pos.coin, pos.side, ADD_NOTIONAL, "pyramid")]

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
