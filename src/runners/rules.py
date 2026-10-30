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
# Cap adverse move ≈ $20 on $400 notional (was 8% ≈ $32, near full $40 margin).
MAX_STOP_PCT = 5.0
# Hard dollar floor vs notional — never lose more than ~15% of the $400 slot (~$60).
# ≈ 15% margin ROI at 10x when price moves ~1.5%; pairs with tighter Peak/Orakelia stops.
MAX_LOSS_FRAC_NOTIONAL = 0.15
MIN_HOLD_S = 30 * 60
TIME_STOP_S = 0  # disabled — crypto 24/7, no position time stops
COOLDOWN_S = 10 * 60
# Entry count and fee spend do not gate new trades. Crypto books stay live 24/7 —
# no daily-loss / retire freeze and no position time stops (policy).
DAILY_LOSS_PCT = 0.08  # retained for metrics only; not used to block
RETIRE_EQUITY = 120.0  # retained for metrics only; not used to block
TAKER_FEE = 0.0005
# Bank open winners: after +X% from entry, ratchet stop to keep K of the run.
# Extra rungs on big runners so a +$400 open cannot bleed back near flat
# before the stop fires (keep most of a 20%+ peak run).
LOCKS = ((1.5, 0.4), (3.0, 0.6), (5.0, 0.75), (15.0, 0.85), (30.0, 0.92))
# Pump hunter locks — intentionally LOOSER than flip books.
# IRYS-class: +1700% → retract to +1000% → resume. Keeping 90% of peak would
# eject on that retract. Keep a rising floor, leave room for violent pullbacks.
# (gain_from_entry%, fraction_of_peak_run_to_keep)
PUMP_LOCKS = ((3.0, 0.25), (8.0, 0.40), (20.0, 0.50), (50.0, 0.55), (100.0, 0.60))
# Hard floor once a pump has really run: never give back below +X% from entry.
PUMP_FLOOR_AFTER_PCT = 25.0   # arm after +25% peak gain
PUMP_FLOOR_KEEP_PCT = 12.0    # then stop never below entry+12%
# Allow deep peak giveback before forced exit (locks still set the real floor).
PUMP_GIVEBACK_KEEP = 0.35     # exit only if open run < 35% of peak run

# Mayhem: Super Scalper. Peak: Ichimoku+Hull. Oppo: UT Bot. Windward: Pump hunter.
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
SS_STOP_PCT = 5.0
# Peak Ichimoku blend — tighter disaster stop (~2% price ≈ 20% of $40 margin @ 10x).
ICHI_RUNNERS = frozenset({"peaksting"})
ICHI_STOP_PCT = 2.0
# Oppo UT Bot (ATR trail flip; Heikin Ashi off).
UT_RUNNERS = frozenset({"oppo_mayhem"})
UT_KEY = 1.0
UT_ATR_LEN = 10
UT_STOP_PCT = 5.0
# Legacy MZ constants kept for signal helpers; Windward no longer uses MZ entries.
MZ_RUNNERS = frozenset()  # empty — Windward is pump hunter now
MZ_RSI_LEN = 14
MZ_QQE_FACTOR = 4.238
MZ_RSI_SMOOTH = 5
MZ_RSI5_OB = 80
MZ_RSI5_OS = 20
MZ_STOP_PCT = 5.0
# Windward — hunt explosive 1h/24h pumps (IRYS-class), long-only.
# Keep pair breadth (low $ vol floor); raise *quality* bars so weak pumps don't enter.
PUMP_RUNNERS = frozenset({"windward"})
PUMP_MIN_RET1H = 4.0          # was 2.5 — need a real impulse hour
PUMP_MIN_RET24H = 12.0        # was 8 — trend already in force
PUMP_MIN_VOL_Z = 0.85         # was 0.25 — volume must actually expand
PUMP_MIN_VOL_USD = 5_000_000.0
PUMP_MAX_SPREAD_BP = 12.0
PUMP_MAX_RSI = 82.0           # skip blow-off tops
PUMP_STOP_PCT = 3.5           # slightly tighter cut when wrong
# Orakelia — simple trend + pullback (multi-horizon return proxy for EMA trend).
# No fade spam: enter dips in an uptrend, exit on stop / trend break only.
ORA_RUNNERS = frozenset({"orakelia"})
ORA_STOP_PCT = 2.0
ORA_MIN_RET24H = 1.5
ORA_MIN_RET7D = 0.0
ORA_PULLBACK_RET1H_MIN = -2.0
ORA_PULLBACK_RET1H_MAX = 0.6
ORA_RSI_MIN = 35.0
ORA_RSI_MAX = 62.0
# Pine flip books: still exit on opposite signal, but NOW trail + profit-lock (no naked ride).
FLIP_RUNNERS = SS_RUNNERS | ICHI_RUNNERS | UT_RUNNERS
RUNNERS = ("mayhem", "windward", "orakelia", "peaksting", "oppo_mayhem", "confetti")
RUNNER_LABELS = {
    "mayhem": "Mayhem · Super Scalper",
    "windward": "Windward · Pump Hunter",
    "orakelia": "Orakelia · Trend Pullback",
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
    # Peak live exchange UPL ($) — dollar locks trail this, not just % price move.
    peak_upl: float = 0.0
    # Last exchange UPL ($) — Confetti stop checks use this when ticker mid lags.
    last_upl: float = 0.0


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
    if runner in PUMP_RUNNERS:
        return min(PUMP_STOP_PCT, MAX_STOP_PCT)
    if runner in ORA_RUNNERS:
        return min(ORA_STOP_PCT, MAX_STOP_PCT)
    if runner in FLIP_RUNNERS:
        # Disaster stop (tightened) — trail/locks ratchet from here via mark().
        if runner in SS_RUNNERS:
            return min(SS_STOP_PCT, MAX_STOP_PCT)
        if runner in ICHI_RUNNERS:
            return min(ICHI_STOP_PCT, MAX_STOP_PCT)
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


def _dollar_keep_frac(peak_u: float) -> float:
    """Fraction of peak UPL to bank. ≥$300 peak → 95% (≈$440 on a $463 peak)."""
    if peak_u >= 300:
        return 0.95
    if peak_u >= 150:
        return 0.92
    return 0.88


def mark(pos: Pos, coin: Coin, locks: tuple = LOCKS, live_upl: Optional[float] = None) -> None:
    """Ratchet the stop with the trail and the profit lock. Never loosens it.

    Flip books also trail a *dollar* floor off peak exchange UPL so a +$460
    winner locks ~$400+, not a weak price-% stop that only banks ~$200.
    """
    mid = float(coin.mid or 0)
    # Reject absurd peak prints (bad mark tick) that would slam stop through the market.
    if live_upl is not None and pos.entry_px > 0 and abs(float(pos.coin_qty or 0)) > 0:
        qty_g = abs(float(pos.coin_qty))
        implied = (
            pos.entry_px + float(live_upl) / qty_g
            if pos.side == "long"
            else pos.entry_px - float(live_upl) / qty_g
        )
        if implied > 0:
            if pos.side == "long" and pos.peak_px > max(pos.entry_px, implied) * 1.15:
                pos.peak_px = max(pos.entry_px, implied)
            elif pos.side == "short" and 0 < pos.peak_px < min(pos.entry_px, implied) * 0.85:
                pos.peak_px = min(pos.entry_px, implied)

    if pos.side == "long":
        if mid > pos.peak_px:
            pos.peak_px = mid
    elif mid > 0 and (pos.peak_px <= 0 or mid < pos.peak_px):
        pos.peak_px = mid

    # ATR trail — skip on pump books, and skip once dollar locks are active
    # (ATR hugs price and would block a proper ~90%+ USD floor).
    use_atr_trail = locks is not PUMP_LOCKS and (
        live_upl is None or float(live_upl) < 40
    )
    if use_atr_trail and coin.atr1h_pct and coin.atr1h_pct > 0 and mid > 0:
        dist = mid * TRAIL_ATR * coin.atr1h_pct / 100
        trail = mid - dist if pos.side == "long" else mid + dist
        _ratchet(pos, trail)

    direction = 1 if pos.side == "long" else -1
    move = direction * (pos.peak_px - pos.entry_px)
    if move > 0 and pos.entry_px > 0:
        gain = move / pos.entry_px * 100
        keep = 0.0
        for at, k in locks:
            if gain >= at:
                keep = max(keep, k)
        if keep > 0:
            _ratchet(pos, pos.entry_px + direction * keep * move)
        # Pump hard floor: once +25% has printed, never surrender back under +12%.
        if locks is PUMP_LOCKS and gain >= PUMP_FLOOR_AFTER_PCT:
            _ratchet(pos, pos.entry_px * (1 + direction * PUMP_FLOOR_KEEP_PCT / 100))

    # Dollar lock (flip / momentum books only — pumps keep wider % room).
    # Floor is PEAK-based only — never slide down with current UPL (that let
    # Peak BZ bleed 463 → 401 instead of banking ~95% ≈ $440).
    if locks is PUMP_LOCKS or live_upl is None or pos.entry_px <= 0:
        return
    upl_now = float(live_upl)
    if upl_now > float(pos.peak_upl or 0):
        pos.peak_upl = upl_now
    peak_u = float(pos.peak_upl or 0)
    if peak_u < 40 or upl_now <= 0:
        return
    floor_u = peak_u * _dollar_keep_frac(peak_u)
    qty = 0.0
    if pos.side == "long" and mid > pos.entry_px:
        qty = upl_now / (mid - pos.entry_px)
    elif pos.side == "short" and mid < pos.entry_px:
        qty = upl_now / (pos.entry_px - mid)
    if qty <= 0:
        qty = abs(float(pos.coin_qty or 0))
    if qty <= 0 and pos.notional > 0:
        qty = abs(pos.notional / pos.entry_px)
    if qty <= 0:
        return
    if pos.side == "long":
        stop_cand = pos.entry_px + floor_u / qty
        # Rest just under mid at the tip so a 1-tick pause does not eject.
        if mid > 0:
            stop_cand = min(stop_cand, mid * 0.999)
        if stop_cand > 0:
            _ratchet(pos, stop_cand)
    else:
        stop_cand = pos.entry_px - floor_u / qty
        if mid > 0:
            stop_cand = max(stop_cand, mid * 1.001)
        if stop_cand <= 0:
            return
        # Corrupt % locks can leave short stop << mid (already pierced). Repair
        # up to the dollar floor — normal ratchet only tightens downward.
        if pos.stop_px <= 0 or pos.stop_px < mid:
            pos.stop_px = stop_cand
        else:
            _ratchet(pos, stop_cand)


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
    """Pine Ichimoku + Hull — require HTF return to agree with signal side."""
    rows = []
    for c in coins:
        if not _tradable(c, "peaksting") or c.ichi_side not in ("long", "short"):
            continue
        # Filter: don't short a still-rising 24h tape / don't long a dumping tape.
        if c.ichi_side == "long" and c.ret24h < 0:
            continue
        if c.ichi_side == "short" and c.ret24h > 0:
            continue
        rows.append(c)
    return sorted(rows, key=lambda c: abs(c.ret24h), reverse=True)


def _ut_bot(coins: List[Coin]) -> List[Coin]:
    """Pine UT Bot — ATR trail cross buy/sell."""
    rows = [c for c in coins if _tradable(c, "oppo_mayhem") and c.ut_side in ("long", "short")]
    return sorted(rows, key=lambda c: abs(c.ret24h), reverse=True)


def _pump_liquid(coin: Coin) -> bool:
    return (
        coin.mid > 0
        and coin.vol24h_usd >= PUMP_MIN_VOL_USD
        and coin.spread_bp <= PUMP_MAX_SPREAD_BP
    )


def pump_score(coin: Coin) -> float:
    """Rank explosive longs — used by Windward's dedicated scan + candidate filter."""
    return (
        float(coin.ret1h)
        + 0.35 * float(coin.ret24h)
        + 0.5 * max(float(coin.vol_z or 0), 0.0)
    )


def _pump_hunter(coins: List[Coin]) -> List[Coin]:
    """Windward: long explosive pumps — breadth kept, quality bars raised.

    Still scans the full mid-cap universe (rare names can appear). Rejects weak
    1h/24h pops, quiet volume, and RSI blow-offs that were the −$14 epoch bleed.
    """
    rows = []
    for c in coins:
        if not _pump_liquid(c):
            continue
        rsi = 50.0 if c.rsi14 is None else float(c.rsi14)
        if (
            c.ret1h >= PUMP_MIN_RET1H
            and c.ret24h >= PUMP_MIN_RET24H
            and float(c.vol_z or 0) >= PUMP_MIN_VOL_Z
            and rsi <= PUMP_MAX_RSI
            and float(c.oi_chg_1h or 0) >= -1.0  # avoid pure short-covering dumps
            and stop_pct(c, runner="windward") is not None
        ):
            rows.append(c)
    return sorted(rows, key=pump_score, reverse=True)


def _orakelia(coins: List[Coin], blacklist: Dict[str, bool]) -> List[Coin]:
    """Simple trend + pullback (EMA-style via multi-horizon returns).

    Classic playbook: only long liquid names in a 24h/7d uptrend on a mild 1h
    dip with RSI mid-range. No fade, no chase. Blacklist cleared when trend heals.
    """
    del blacklist  # kept in signature for call-site compat; no longer used to block
    rows = []
    for c in coins:
        if not liquid(c) or not _tradable(c, "orakelia"):
            continue
        rsi = 50.0 if c.rsi14 is None else float(c.rsi14)
        if c.ret24h < ORA_MIN_RET24H or c.ret7d < ORA_MIN_RET7D:
            continue
        if not (ORA_PULLBACK_RET1H_MIN <= c.ret1h <= ORA_PULLBACK_RET1H_MAX):
            continue
        if not (ORA_RSI_MIN <= rsi <= ORA_RSI_MAX):
            continue
        if float(c.vol_z or 0) < 0:
            continue
        rows.append(c)
    # Prefer stronger 24h trend, then deeper (but valid) pullback.
    return sorted(rows, key=lambda c: (c.ret24h, -abs(c.ret1h)), reverse=True)


def candidates(runner: str, coins: List[Coin], book: Book) -> List[Coin]:
    if runner in SS_RUNNERS:
        return _super_scalper(coins)
    if runner in ICHI_RUNNERS:
        return _ichi_hull(coins)
    if runner in UT_RUNNERS:
        return _ut_bot(coins)
    if runner in PUMP_RUNNERS:
        return _pump_hunter(coins)
    if runner in ORA_RUNNERS:
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
    if runner in PUMP_RUNNERS:
        return "long"
    if runner in ORA_RUNNERS:
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


def _live_upl(pos: Pos, mid: float) -> float:
    live = getattr(pos, "last_upl", None)
    if live is not None:
        try:
            return float(live)
        except (TypeError, ValueError):
            pass
    return float(upl(pos, mid))


def _exit_reason(runner: str, pos: Pos, coin: Optional[Coin], mid: float, book: Book, now: float) -> Optional[str]:
    if stop_hit(pos, mid):
        return "stop"
    # Hard backstop: never bleed more than ~15% of slot notional (~$60 on $400).
    notion = float(pos.notional or 0) or ENTRY_NOTIONAL
    if _live_upl(pos, mid) <= -MAX_LOSS_FRAC_NOTIONAL * notion:
        return "max_loss"
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
    if runner in PUMP_RUNNERS:
        # Don't kill on a single red 1h while still clearly winning — pumps chop.
        # Only fade-exit when 1h is red AND open run has collapsed near entry.
        if pos.entry_px > 0 and pos.peak_px > pos.entry_px:
            peak_run = (pos.peak_px - pos.entry_px) / pos.entry_px
            cur_run = (mid - pos.entry_px) / pos.entry_px
            if coin.ret1h < 0 and cur_run < max(0.03, peak_run * 0.15):
                return "pump_fade"
            # Deep giveback vs peak (locks already banked a floor via stop).
            if peak_run >= 0.08 and cur_run < peak_run * PUMP_GIVEBACK_KEEP:
                return "pump_giveback"
        elif coin.ret1h < 0:
            return "pump_fade"
        return None
    if runner in ORA_RUNNERS:
        # Trend break only — mild red 1h is noise on a pullback book.
        rsi = 50.0 if coin.rsi14 is None else float(coin.rsi14)
        if coin.ret24h < 0 or coin.ret7d < -2.0 or rsi < 30.0:
            return "trend_break"
        return None
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
            # Trail + profit locks for everyone (flip books included — still exit on signal flip).
            # Pass live exchange UPL so dollar floors (Peak BZ ~$400 lock) arm on decide ticks.
            locks = PUMP_LOCKS if runner in PUMP_RUNNERS else LOCKS
            live = getattr(pos, "last_upl", None)
            mark(
                pos,
                coin,
                locks=locks,
                live_upl=float(live) if live is not None else None,
            )
            mid = coin.mid
        reason = _exit_reason(runner, pos, coin, mid, book, now)
        if reason:
            closes.append(Action("close", pos.coin, pos.side, reason=reason))
    if closes:
        return closes

    # Pyramid adds: Orakelia + pump hunter (not Pine flip books).
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
            and (runner not in ORA_RUNNERS or coin.ret1h >= 0)
            and (runner not in PUMP_RUNNERS or coin.ret1h >= 0)
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
