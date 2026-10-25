"""BeeBots Momentum engine (boozy) — mechanical port for AYO hive Confetti.

Source of truth: beebots/src/bees/boozy.ts + jev fallbackDecide.
Sizing / 12h time-stop / daily fee caps are NOT ported — hive race uses
ENTRY_NOTIONAL + shared cooldown. Strategy gates, rank, menu, ATR stop/trail,
and profit-lock rungs match BeeBots.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.runners.rules import (
    ADD_NOTIONAL,
    Action,
    Book,
    Coin,
    ENTRY_NOTIONAL,
    MAX_OPENS,
    MAX_SLOT_NOTIONAL,
    Pos,
    stop_hit,
    stop_price,
    upl,
)

# --- boozy.ts constants (strategy) ---
BOOZY_MIN_VOL_USD = 15_000_000.0
BOOZY_MIN_HOLD_S = 30 * 60
BOOZY_SPREAD_BP = 8.0
BOOZY_CANDIDATES = 5
STOP_ATR1H = 2.5
TRAIL_ATR1H = 2.0
BOOZY_PROFIT_LOCK = ((1.5, 0.4), (3.0, 0.6), (5.0, 0.75))

CONFETTI_RULES = (
    "Each tick, scan liquid coins for the strongest 7-day gainer, meme coins included, "
    "and go long it. Add to the position while it keeps making new highs and momentum "
    "stays positive. Rotate out when a stronger mover appears or momentum flips negative. "
    "Cut fast on a sharp reversal against the trend, and stay flat when nothing is clearly pumping."
)

# Hourly #1 streak for SWITCH (bee.top1 in BeeBots engine).
_TOP1: Dict[str, object] = {"coin": None, "streak": 0, "hour_key": None}


@dataclass
class Candidate:
    coin: Coin
    score: float
    impulse: Optional[str]


def _zs(xs: List[float]):
    n = len(xs) or 1
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / n) or 1.0
    return lambda x: (x - m) / sd


def atr1h_px(coin: Coin) -> Optional[float]:
    """1h ATR ≈ 2 × 15m ATR% (boozy atr1hPx). Prefer atr1h_pct when set."""
    if coin.atr1h_pct and coin.atr1h_pct > 0 and coin.mid > 0:
        return coin.mid * coin.atr1h_pct / 100.0
    if coin.atr15_pct and coin.atr15_pct > 0 and coin.mid > 0:
        return coin.mid * coin.atr15_pct * 2.0 / 100.0
    return None


def impulse_side(coin: Coin) -> Optional[str]:
    """boozy impulseSide — long and short."""
    r24 = float(coin.ret24h or 0)
    r1 = float(coin.ret1h or 0)
    oi1 = float(coin.oi_chg_1h or 0)
    rsi = 50.0 if coin.rsi14 is None else float(coin.rsi14)
    fz = float(coin.funding_z or 0)
    if r24 >= 1.5 and r1 >= 0 and oi1 >= 0 and rsi <= 78 and fz <= 1.5:
        return "long"
    if r24 <= -1.5 and r1 < 0 and rsi >= 22:
        return "short"
    return None


def rank_candidates(coins: List[Coin]) -> List[Candidate]:
    """boozy rankCandidates — $15M vol + spread gate + multi-horizon z-score."""
    pool = [
        c
        for c in coins
        if c.mid > 0
        and c.vol24h_usd >= BOOZY_MIN_VOL_USD
        and c.spread_bp <= BOOZY_SPREAD_BP
    ]
    if not pool:
        return []
    z24 = _zs([abs(c.ret24h) for c in pool])
    z1 = _zs([abs(c.ret1h) for c in pool])
    z7 = _zs([abs(c.ret7d if c.ret7d else c.ret24h) for c in pool])
    out: List[Candidate] = []
    for c in pool:
        attention = float(c.vol_z or 0)
        impulse = impulse_side(c)
        score = (
            0.45 * z24(abs(c.ret24h))
            + 0.35 * z1(abs(c.ret1h))
            + 0.2 * z7(abs(c.ret7d if c.ret7d else c.ret24h))
            + 0.25 * max(0.0, attention)
            + (0.5 if impulse else 0.0)
        )
        out.append(Candidate(coin=c, score=score, impulse=impulse))
    out.sort(key=lambda x: x.score, reverse=True)
    return out


def _update_top1(ranked: List[Candidate], now: float) -> None:
    hour_key = time.strftime("%Y-%m-%dT%H", time.gmtime(now))
    leader = ranked[0].coin.coin if ranked else None
    if _TOP1.get("hour_key") != hour_key:
        prev = _TOP1.get("coin")
        if leader and leader == prev:
            _TOP1["streak"] = int(_TOP1.get("streak") or 0) + 1
        else:
            _TOP1["streak"] = 1 if leader else 0
        _TOP1["coin"] = leader
        _TOP1["hour_key"] = hour_key


def switch_target(pos: Pos, top: List[Candidate], ranked: List[Candidate]) -> Optional[Candidate]:
    """boozy switchTarget — new #1 with impulse and hourly streak >= 2."""
    if not pos or not top:
        return None
    leader = top[0]
    held_at = next((i for i, c in enumerate(ranked) if c.coin.coin == pos.coin), -1)
    if held_at < 0 or leader.coin.coin == pos.coin or not leader.impulse:
        return None
    if _TOP1.get("coin") == leader.coin.coin and int(_TOP1.get("streak") or 0) >= 2:
        return leader
    return None


def _run_atr1h(pos: Pos, coin: Coin) -> float:
    atr = atr1h_px(coin)
    if not atr or atr <= 0:
        return 0.0
    direction = 1 if pos.side == "long" else -1
    return direction * (coin.mid - pos.entry_px) / atr


def _upl_r(pos: Pos, coin: Coin) -> float:
    """Rough R-multiple vs initial ATR stop distance."""
    atr = atr1h_px(coin)
    if not atr or atr <= 0:
        return 0.0
    risk = STOP_ATR1H * atr
    if risk <= 0:
        return 0.0
    direction = 1 if pos.side == "long" else -1
    return direction * (coin.mid - pos.entry_px) / risk


def boozy_stop_px(side: str, entry: float, coin: Coin) -> float:
    atr = atr1h_px(coin)
    if atr is None or atr <= 0:
        # Fallback: 2.5 × atr1h_pct as % of price if we only have pct
        pct = STOP_ATR1H * float(coin.atr1h_pct or coin.atr15_pct or 1.0)
        return stop_price(side, entry, min(pct, 8.0))
    return entry - STOP_ATR1H * atr if side == "long" else entry + STOP_ATR1H * atr


def mark_boozy(pos: Pos, coin: Coin) -> None:
    """Trail 2.0× ATR(1h) + BeeBots profit-lock rungs (same as boozy)."""
    if pos.side == "long":
        if coin.mid > pos.peak_px:
            pos.peak_px = coin.mid
    elif coin.mid < pos.peak_px:
        pos.peak_px = coin.mid

    atr = atr1h_px(coin)
    if atr and atr > 0:
        trail = coin.mid - TRAIL_ATR1H * atr if pos.side == "long" else coin.mid + TRAIL_ATR1H * atr
        if pos.side == "long" and trail > pos.stop_px:
            pos.stop_px = trail
        elif pos.side == "short" and trail < pos.stop_px:
            pos.stop_px = trail

    direction = 1 if pos.side == "long" else -1
    move = direction * (pos.peak_px - pos.entry_px)
    if move > 0 and pos.entry_px > 0:
        gain = move / pos.entry_px * 100
        keep = 0.0
        for at, k in BOOZY_PROFIT_LOCK:
            if gain >= at:
                keep = max(keep, k)
        if keep > 0:
            cand = pos.entry_px + direction * keep * move
            if pos.side == "long" and cand > pos.stop_px:
                pos.stop_px = cand
            elif pos.side == "short" and cand < pos.stop_px:
                pos.stop_px = cand


def _menu_flat(top: List[Candidate]) -> Dict[str, Tuple[str, Optional[str], Optional[str], str]]:
    """label -> (kind, coin, side, reason) for flat book."""
    m: Dict[str, Tuple[str, Optional[str], Optional[str], str]] = {}
    for i, c in enumerate(top):
        if c.impulse == "long":
            m[f"APE_{c.coin.coin}"] = ("open", c.coin.coin, "long", "entry")
        elif c.impulse == "short":
            m[f"SHORT_{c.coin.coin}"] = ("open", c.coin.coin, "short", "entry")
    if m:
        m["WAIT"] = ("hold", None, None, "wait")
    return m


def _menu_held(
    pos: Pos,
    coin: Optional[Coin],
    top: List[Candidate],
    ranked: List[Candidate],
    now: float,
) -> Dict[str, Tuple[str, Optional[str], Optional[str], str]]:
    m: Dict[str, Tuple[str, Optional[str], Optional[str], str]] = {
        "RIDE": ("hold", None, None, "ride"),
    }
    committed = (now - pos.opened_at) < BOOZY_MIN_HOLD_S
    if coin is None:
        return m
    direction = 1 if pos.side == "long" else -1
    r1h = coin.ret1h
    upl_r = _upl_r(pos, coin)
    u = upl(pos, coin.mid)

    if not committed:
        if upl_r >= 1.5:
            m["TAKE_PROFIT"] = ("close", pos.coin, pos.side, "take_profit")
        if direction * r1h < 0 or u < 0:
            m["BAIL"] = ("close", pos.coin, pos.side, "bail")
        best = switch_target(pos, top, ranked)
        if best and best.impulse:
            m["SWITCH_COIN"] = ("switch", best.coin.coin, best.impulse, "switch")
        if pos.side == "long" and coin.ret1h < 0 and coin.oi_chg_1h < 0:
            m["FLIP_SHORT"] = ("switch", pos.coin, "short", "flip_short")

    if (
        not pos.added
        and pos.notional + ADD_NOTIONAL <= MAX_SLOT_NOTIONAL + 1e-6
        and _run_atr1h(pos, coin) >= 1.5
    ):
        m["DOUBLE_DOWN"] = ("add", pos.coin, pos.side, "pyramid")
    return m


def fallback_decide(
    menu: Dict[str, Tuple[str, Optional[str], Optional[str], str]],
    upl_r: Optional[float],
) -> Optional[str]:
    """jev.ts fallbackDecide priorities — ignores owner rules text."""
    labels = list(menu.keys())
    if not labels:
        return None
    if "TAKE_PROFIT" in menu:
        return "TAKE_PROFIT"
    if "BAIL" in menu and upl_r is not None and upl_r < 0:
        return "BAIL"
    if "DOUBLE_DOWN" in menu and (upl_r is None or upl_r >= 1.0):
        return "DOUBLE_DOWN"
    for lab in labels:
        kind = menu[lab][0]
        if kind == "open":
            return lab
    for hold in ("RIDE", "WAIT"):
        if hold in menu:
            return hold
    for lab in labels:
        if menu[lab][0] in ("close", "switch"):
            return lab
    return labels[0]


def decide_confetti(book: Book, coins: List[Coin], now: float) -> List[Action]:
    """Mechanical Momentum for Confetti — up to MAX_OPENS concurrent (hive sizing).

    Still BeeBots menus per slot, but free slots can fill with other top movers
    instead of riding a single MAGMA forever while 2/3 sit empty.
    """
    ranked = rank_candidates(coins)
    _update_top1(ranked, now)
    top = ranked[:BOOZY_CANDIDATES]
    opens = list(book.positions) if book.positions else ([book.position] if book.position else [])
    book.positions = opens
    book.sync_primary()
    by_coin = {c.coin: c for c in coins}

    if len(opens) > MAX_OPENS:
        extras = sorted(opens, key=lambda p: p.notional)[: len(opens) - MAX_OPENS]
        return [Action("close", p.coin, p.side, reason="slot_cap") for p in extras]

    # Hard stop / trail / locks on every open first.
    for pos in opens:
        coin = by_coin.get(pos.coin)
        if coin:
            mark_boozy(pos, coin)
            if stop_hit(pos, coin.mid):
                return [Action("close", pos.coin, pos.side, reason="stop")]

    # Per-slot exits / pyramid (one action per tick — same as other hive books).
    for pos in opens:
        coin = by_coin.get(pos.coin)
        menu = _menu_held(pos, coin, top, ranked, now)
        upl_r = _upl_r(pos, coin) if coin else None
        pick = fallback_decide(menu, upl_r)
        if not pick:
            continue
        kind, c_coin, side, reason = menu[pick]
        if kind == "close":
            return [Action("close", pos.coin, pos.side, reason=reason)]
        if kind == "add":
            return [Action("add", pos.coin, pos.side, ADD_NOTIONAL, reason)]
        if kind == "switch" and c_coin and side:
            # Only rotate when flat slots are full — otherwise just open the new name.
            if len(opens) >= MAX_OPENS:
                actions = [Action("close", pos.coin, pos.side, reason=reason)]
                if c_coin != pos.coin or side != pos.side:
                    actions.append(Action("open", c_coin, side, ENTRY_NOTIONAL, reason))
                return actions
            # Free slot: open the stronger name beside the current ride.
            if c_coin != pos.coin and side:
                held = {p.coin for p in opens}
                if c_coin not in held:
                    return [Action("open", c_coin, side, ENTRY_NOTIONAL, reason)]

    # Fill free slots with next ranked impulses not already held.
    if len(opens) < MAX_OPENS:
        held = {p.coin for p in opens}
        for c in ranked:
            if c.coin.coin in held or not c.impulse:
                continue
            return [Action("open", c.coin.coin, c.impulse, ENTRY_NOTIONAL, "entry")]
        if not opens:
            return [Action("hold", reason="no_setup" if not ranked else "wait")]
        return [Action("hold", reason="ride")]

    return [Action("hold", reason="slots_full" if opens else "wait")]
