"""Hive race: OKX demo runners + Confetti on Binance Futures demo.

Each runner keeps a $300 race ledger with up to 3 concurrent opens at $40
margin (10x isolated), +$15 margin add when winning. Live exchange positions
are reconciled every tick. AYO's key is never used for hive demo books.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
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
    "race_epoch": None,  # ISO — board/equity only count closes at/after this
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
    # Confetti · BeeBots Momentum on Binance (not OKX).
    "confetti": "CONFETTI",
}
BINANCE_RUNNERS = frozenset({"confetti"})

# Mayhem → Oppo (entries/adds only). PeakSting is idle (no Mayhem sync).
_OPPO_PAIR_Q: List[Action] = []
_PEAK_PAIR_Q: List[Action] = []


def _path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "hive_race.json"


def _base_url() -> str:
    return (CONFIG.get("okx_base_url") or "https://www.okx.com").rstrip("/")


def _enabled() -> bool:
    return os.getenv("HIVE_RACE", "true").strip().lower() not in ("0", "false", "no", "off")


def _creds(runner: str) -> Optional[tuple]:
    prefix = ENV_PREFIX[runner]
    if runner in BINANCE_RUNNERS:
        key = os.getenv(f"{prefix}_BINANCE_API_KEY", "").strip()
        secret = os.getenv(f"{prefix}_BINANCE_API_SECRET", "").strip()
        if key and secret:
            testnet_raw = os.getenv(f"{prefix}_BINANCE_TESTNET", "true").strip().lower()
            testnet = testnet_raw not in ("0", "false", "no", "off")
            return key, secret, testnet
        return None
    key = os.getenv(f"{prefix}_OKX_DEMO_API_KEY", "").strip()
    secret = os.getenv(f"{prefix}_OKX_DEMO_API_SECRET", "").strip()
    phrase = os.getenv(f"{prefix}_OKX_DEMO_API_PASSPHRASE", "").strip()
    if key and secret and phrase:
        return key, secret, phrase
    return None


def _binance_fapi() -> str:
    # Public market data — mainnet fapi (same marks Confetti trades against on demo when keys are testnet).
    return (os.getenv("BINANCE_FAPI_URL") or "https://fapi.binance.com").rstrip("/")


def _binance_get(path: str, params: Optional[dict] = None) -> Any:
    url = f"{_binance_fapi()}{path}"
    resp = requests.get(url, params=params or {}, timeout=20)
    resp.raise_for_status()
    return resp.json()


def build_binance_coins() -> List[Coin]:
    """USDT-M liquid perps for Confetti (BeeBots Momentum gates applied in beebots_boozy)."""
    from src.runners.beebots_boozy import BOOZY_MIN_VOL_USD, BOOZY_SPREAD_BP

    try:
        tickers = _binance_get("/fapi/v1/ticker/24hr")
    except Exception as exc:
        logger.warning("binance tickers failed: %s", exc)
        return []
    if not isinstance(tickers, list):
        return []
    ranked: List[Tuple[float, str, dict, float, float, float]] = []
    for row in tickers:
        sym = str(row.get("symbol") or "")
        if not sym.endswith("USDT"):
            continue
        # Skip quarterlies / weird symbols
        base = sym[:-4]
        if not base or not base.isalnum():
            continue
        if base.upper() in HIVE_BLOCKLIST:
            continue
        last = _f(row.get("lastPrice"))
        bid = _f(row.get("bidPrice"))
        ask = _f(row.get("askPrice"))
        if last <= 0:
            continue
        if bid <= 0 or ask <= 0:
            bid, ask = last, last
        vol_usd = _f(row.get("quoteVolume"))
        spread_bp = (ask - bid) / ((ask + bid) / 2) * 10000 if (ask + bid) > 0 else 99.0
        if vol_usd < BOOZY_MIN_VOL_USD or spread_bp > BOOZY_SPREAD_BP:
            continue
        open24 = _f(row.get("openPrice"), last)
        ret24 = (last - open24) / open24 * 100 if open24 else 0.0
        ranked.append((vol_usd, base.upper(), row, last, spread_bp, ret24))
    ranked.sort(key=lambda r: r[0], reverse=True)
    out: List[Coin] = []
    for vol_usd, coin, row, last, spread_bp, ret24 in ranked[:HIVE_SCAN_POOL]:
        sym = f"{coin}USDT"
        try:
            raw = _binance_get("/fapi/v1/klines", {"symbol": sym, "interval": "15m", "limit": "160"})
            daily = _binance_get("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": "8"})
        except Exception as exc:
            logger.debug("binance candles %s: %s", sym, exc)
            continue
        if not isinstance(raw, list) or len(raw) < 30:
            continue
        # Binance kline: [openTime, o, h, l, c, vol, ...]
        done = raw[:-1] if len(raw) >= 3 else raw
        closes = [_f(c[4]) for c in done]
        highs = [_f(c[2]) for c in done]
        lows = [_f(c[3]) for c in done]
        vols = [_f(c[5]) for c in done]
        ret1h = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 and closes[-5] else 0.0
        ret7 = 0.0
        if isinstance(daily, list) and len(daily) >= 2 and _f(daily[0][4]):
            # daily[0] is oldest when limit=8; use first open of oldest vs last
            old = _f(daily[0][1]) or _f(daily[0][4])
            if old:
                ret7 = (last - old) / old * 100
        atr15 = _atr_pct(highs, lows, closes)
        atr1h = atr15 * 2 if atr15 is not None else None
        # OI / funding optional — missing data treated as 0 so long impulse OI>=0 still passes.
        oi_chg = 0.0
        funding_z = 0.0
        try:
            oi = _binance_get("/futures/data/openInterestHist", {"symbol": sym, "period": "5m", "limit": "3"})
            if isinstance(oi, list) and len(oi) >= 2:
                a = _f(oi[-1].get("sumOpenInterest"))
                b = _f(oi[-2].get("sumOpenInterest"))
                if b:
                    oi_chg = (a - b) / b * 100
        except Exception:
            oi_chg = 0.0
        try:
            fr = _binance_get("/fapi/v1/premiumIndex", {"symbol": sym})
            if isinstance(fr, dict):
                funding_z = _funding_z(sym, _f(fr.get("lastFundingRate")))
        except Exception:
            funding_z = 0.0
        out.append(
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
                high24h=_f(row.get("highPrice"), last),
                atr15_pct=atr15,
            )
        )
    return out


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


def _wma_series(values: List[float], length: int) -> List[float]:
    n = len(values)
    out = [float("nan")] * n
    if length <= 0 or n < length:
        return out
    weights = list(range(1, length + 1))
    wsum = float(sum(weights))
    for i in range(length - 1, n):
        window = values[i - length + 1 : i + 1]
        out[i] = sum(a * b for a, b in zip(window, weights)) / wsum
    return out


def _hma_series(values: List[float], length: int) -> List[float]:
    """TradingView ta.hma / hma()."""
    half = max(1, length // 2)
    sqrt_l = max(1, int(round(math.sqrt(length))))
    wma_half = _wma_series(values, half)
    wma_full = _wma_series(values, length)
    raw: List[float] = []
    for i in range(len(values)):
        a, b = wma_half[i], wma_full[i]
        if a != a or b != b:  # NaN
            raw.append(float("nan"))
        else:
            raw.append(2.0 * a - b)
    return _wma_series(raw, sqrt_l)


def _donchian_mid(highs: List[float], lows: List[float], length: int) -> Optional[float]:
    if len(highs) < length or len(lows) < length:
        return None
    return (max(highs[-length:]) + min(lows[-length:])) / 2.0


def _ichi_hull_signal(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    daily_opens: List[float],
    *,
    hull_period: int = 14,
) -> Optional[str]:
    """Pine: HMA rise + daily open rise + price>hma2 + cloud + Hull-MACD.

    Returns 'long' / 'short' only when the *full* condition set is true (as-is).
    """
    need = max(hull_period + 5, 52 + 5, 26 + 9 + 5)
    if len(opens) < need or len(daily_opens) < 2:
        return None
    # Source = open (Pine defval=open)
    price = opens
    hma = _hma_series(price, hull_period)
    if len(hma) < 2:
        return None
    hma1, hma2 = hma[-1], hma[-2]
    if hma1 != hma1 or hma2 != hma2:
        return None
    px = price[-1]
    d1, d2 = daily_opens[-1], daily_opens[-2]

    conversion = _donchian_mid(highs, lows, 9)
    base = _donchian_mid(highs, lows, 26)
    lead2 = _donchian_mid(highs, lows, 52)
    if conversion is None or base is None or lead2 is None:
        return None
    lead1 = (conversion + base) / 2.0

    # Hull-MACD: hma(12)-hma(26), signal = hma(macd, 9)
    hma_fast = _hma_series(price, 12)
    hma_slow = _hma_series(price, 26)
    macd_line: List[float] = []
    for i in range(len(price)):
        f, s = hma_fast[i], hma_slow[i]
        if f != f or s != s:
            macd_line.append(float("nan"))
        else:
            macd_line.append(f - s)
    # Replace leading NaNs with 0 so WMA can warm up on valid tail
    clean = [0.0 if x != x else x for x in macd_line]
    amacd = _hma_series(clean, 9)
    macd_v, amacd_v = macd_line[-1], amacd[-1]
    if macd_v != macd_v or amacd_v != amacd_v:
        return None

    long_ok = hma1 > hma2 and d1 > d2 and px > hma2 and lead1 > lead2 and macd_v > amacd_v
    short_ok = hma1 < hma2 and d1 < d2 and px < hma2 and lead1 < lead2 and macd_v < amacd_v
    if long_ok:
        return "long"
    if short_ok:
        return "short"
    return None


def _atr_rma_series(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[float]:
    """Wilder ATR (TradingView atr())."""
    n = len(closes)
    out = [float("nan")] * n
    if n < period + 1:
        return out
    trs: List[float] = [highs[0] - lows[0]]
    for i in range(1, n):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    # First ATR = SMA of first `period` TRs
    seed = sum(trs[1 : period + 1]) / period
    out[period] = seed
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def _ut_bot_signal(
    closes: List[float],
    highs: List[float],
    lows: List[float],
) -> Optional[str]:
    """Pine UT Bot (Heikin Ashi off): ATR trail cross buy/sell on completed bars."""
    from src.runners.rules import UT_ATR_LEN, UT_KEY

    if len(closes) < UT_ATR_LEN + 5:
        return None
    atr = _atr_rma_series(highs, lows, closes, UT_ATR_LEN)
    trail = [0.0] * len(closes)
    for i in range(len(closes)):
        if atr[i] != atr[i] or atr[i] <= 0:
            trail[i] = trail[i - 1] if i else closes[i]
            continue
        n_loss = UT_KEY * atr[i]
        src = closes[i]
        prev = trail[i - 1] if i else 0.0
        src_prev = closes[i - 1] if i else src
        if src > prev and src_prev > prev:
            trail[i] = max(prev, src - n_loss)
        elif src < prev and src_prev < prev:
            trail[i] = min(prev, src + n_loss)
        elif src > prev:
            trail[i] = src - n_loss
        else:
            trail[i] = src + n_loss
    # ema(src, 1) == src; crossover(src, trail) / crossover(trail, src)
    i = len(closes) - 1
    if i < 1:
        return None
    src, src1 = closes[i], closes[i - 1]
    t, t1 = trail[i], trail[i - 1]
    above = src1 <= t1 and src > t
    below = src1 >= t1 and src < t
    buy = src > t and above
    sell = src < t and below
    if buy:
        return "long"
    if sell:
        return "short"
    return None


def _ema_series(values: List[float], length: int) -> List[float]:
    """TradingView ta.ema — SMA seed then EMA."""
    n = len(values)
    out = [float("nan")] * n
    if length <= 0 or n < length:
        return out
    alpha = 2.0 / (length + 1)
    seed = sum(values[:length]) / length
    out[length - 1] = seed
    for i in range(length, n):
        prev = out[i - 1]
        out[i] = alpha * values[i] + (1.0 - alpha) * prev
    return out


def _rsi_wilder_series(closes: List[float], period: int) -> List[float]:
    """TradingView ta.rsi (Wilder)."""
    n = len(closes)
    out = [float("nan")] * n
    if period <= 0 or n <= period:
        return out
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        out[period] = 100.0
    else:
        out[period] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            out[i] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return out


def _momentum_zz_signal(
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[Optional[str], Optional[float]]:
    """Peter_O Momentum ZigZag — QQE default, RSI5 force filter, pivot SL.

    Returns (side, stop_px) only on a GoLong/GoShort bar. Absolute TP=200 omitted
    (unusable across coins); exit is opposite signal + disaster stop in rules.
    """
    from src.runners.rules import (
        MZ_QQE_FACTOR,
        MZ_RSI5_OB,
        MZ_RSI5_OS,
        MZ_RSI_LEN,
        MZ_RSI_SMOOTH,
    )

    n = len(closes)
    need = MZ_RSI_LEN * 4 + MZ_RSI_SMOOTH + 20
    if n < need:
        return None, None

    rsi5 = _rsi_wilder_series(closes, 5)
    wilders = MZ_RSI_LEN * 2 - 1
    rsi_ma = _ema_series(_rsi_wilder_series(closes, MZ_RSI_LEN), MZ_RSI_SMOOTH)

    atr_rsi = [float("nan")] * n
    for i in range(1, n):
        a, b = rsi_ma[i], rsi_ma[i - 1]
        if a == a and b == b:
            atr_rsi[i] = abs(b - a)
    dar = _ema_series(_ema_series(atr_rsi, wilders), wilders)
    for i in range(n):
        if dar[i] == dar[i]:
            dar[i] *= MZ_QQE_FACTOR

    longband = [0.0] * n
    shortband = [0.0] * n
    trend = [1] * n
    last_qqe_high = [highs[0]] * n
    last_qqe_low = [lows[0]] * n
    last_xlong_i = -10_000
    last_xshort_i = -10_000
    qqe_flip = [0] * n  # 1 = long flip, -1 = short flip

    for i in range(n):
        rs, d = rsi_ma[i], dar[i]
        if rs != rs or d != d:
            if i:
                longband[i] = longband[i - 1]
                shortband[i] = shortband[i - 1]
                trend[i] = trend[i - 1]
                last_qqe_high[i] = last_qqe_high[i - 1]
                last_qqe_low[i] = last_qqe_low[i - 1]
            continue

        new_short, new_long = rs + d, rs - d
        if i == 0:
            longband[i], shortband[i] = new_long, new_short
        else:
            prev_long, prev_short = longband[i - 1], shortband[i - 1]
            prev_rs = rsi_ma[i - 1]
            if prev_rs == prev_rs and prev_rs > prev_long and rs > prev_long:
                longband[i] = max(prev_long, new_long)
            else:
                longband[i] = new_long
            if prev_rs == prev_rs and prev_rs < prev_short and rs < prev_short:
                shortband[i] = min(prev_short, new_short)
            else:
                shortband[i] = new_short

        going_up = (i - last_xlong_i) < (i - last_xshort_i)
        going_down = (i - last_xlong_i) > (i - last_xshort_i)
        prev_going_up = ((i - 1 - last_xlong_i) < (i - 1 - last_xshort_i)) if i else going_up
        prev_going_down = ((i - 1 - last_xlong_i) > (i - 1 - last_xshort_i)) if i else going_down
        prev_hi = last_qqe_high[i - 1] if i else highs[i]
        prev_lo = last_qqe_low[i - 1] if i else lows[i]
        if (highs[i] > prev_hi and going_up) or (prev_going_down and going_up):
            last_qqe_high[i] = highs[i]
        else:
            last_qqe_high[i] = prev_hi
        if (lows[i] < prev_lo and going_down) or (prev_going_up and going_down):
            last_qqe_low[i] = lows[i]
        else:
            last_qqe_low[i] = prev_lo

        prev_trend = trend[i - 1] if i else 1
        prev_rs = rsi_ma[i - 1] if i else rs
        prev_short_b = shortband[i - 1] if i else shortband[i]
        prev_long_b = longband[i - 1] if i else longband[i]
        cross_up = prev_rs <= prev_short_b and rs > prev_short_b
        cross_dn = prev_rs >= prev_long_b and rs < prev_long_b
        cross_hi = i > 0 and highs[i - 1] <= prev_hi and highs[i] > prev_hi
        cross_lo = i > 0 and lows[i - 1] >= prev_lo and lows[i] < prev_lo
        if cross_up or cross_hi:
            trend[i] = 1
        elif cross_dn or cross_lo:
            trend[i] = -1
        else:
            trend[i] = prev_trend

        if trend[i] == 1 and prev_trend == -1:
            last_xlong_i = i
            qqe_flip[i] = 1
        elif trend[i] == -1 and prev_trend == 1:
            last_xshort_i = i
            qqe_flip[i] = -1

    mom_dir = [0] * n
    mom_up = [False] * n
    mom_dn = [False] * n
    for i in range(n):
        if qqe_flip[i] == 1:
            mom_up[i] = True
            mom_dir[i] = 1
        elif qqe_flip[i] == -1:
            mom_dn[i] = True
            mom_dir[i] = -1
        else:
            mom_dir[i] = mom_dir[i - 1] if i else 0

    last_up = last_dn = last_ob = last_os = -10_000
    ages_up = [10_000] * n
    ages_dn = [10_000] * n
    ages_ob = [10_000] * n
    ages_os = [10_000] * n
    for i in range(n):
        if rsi5[i] == rsi5[i]:
            if rsi5[i] > MZ_RSI5_OB:
                last_ob = i
            if rsi5[i] < MZ_RSI5_OS:
                last_os = i
        if mom_up[i]:
            last_up = i
        if mom_dn[i]:
            last_dn = i
        ages_up[i] = i - last_up if last_up >= 0 else 10_000
        ages_dn[i] = i - last_dn if last_dn >= 0 else 10_000
        ages_ob[i] = i - last_ob if last_ob >= 0 else 10_000
        ages_os[i] = i - last_os if last_os >= 0 else 10_000

    go_long = [False] * n
    go_short = [False] * n
    for i in range(n):
        if mom_up[i]:
            force_dn = i >= 1 and ages_dn[i - 1] >= ages_os[i - 1]
            go_long[i] = not force_dn
        if mom_dn[i]:
            force_up = i >= 1 and ages_up[i - 1] >= ages_ob[i - 1]
            go_short[i] = not force_up

    zz_peak = [highs[0]] * n
    zz_bottom = [lows[0]] * n
    zigzag = [float("nan")] * n
    for i in range(n):
        up, dn = mom_dir[i] == 1, mom_dir[i] == -1
        prev_up = mom_dir[i - 1] == 1 if i else False
        prev_dn = mom_dir[i - 1] == -1 if i else False
        prev_peak = zz_peak[i - 1] if i else highs[i]
        prev_bot = zz_bottom[i - 1] if i else lows[i]
        zz_peak[i] = highs[i] if ((highs[i] > prev_peak and up) or (prev_dn and up)) else prev_peak
        zz_bottom[i] = lows[i] if ((lows[i] < prev_bot and dn) or (prev_up and dn)) else prev_bot
        if up and prev_dn:
            zigzag[i] = prev_bot
        elif prev_up and dn:
            zigzag[i] = prev_peak

    pl = ph = float("nan")
    for i in range(n):
        if mom_up[i] and zigzag[i] == zigzag[i]:
            pl = zigzag[i]
        if mom_dn[i] and zigzag[i] == zigzag[i]:
            ph = zigzag[i]

    i = n - 1
    if go_long[i]:
        sl = lows[i] if pl != pl or lows[i] < pl else pl
        return "long", float(sl)
    if go_short[i]:
        sl = highs[i] if ph != ph or highs[i] > ph else ph
        return "short", float(sl)
    return None, None


def _super_scalper_signal(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
    atr15_pct: Optional[float] = None,
) -> Tuple[Optional[str], float]:
    """Pine Super Scalper as-is: WMA(TR) bands + RSI25/100; no SL/TP in TV."""
    from src.runners.rules import SS_ATR_LEN, SS_BAND_MULT, SS_RSI_FAST, SS_RSI_SLOW

    del atr15_pct  # bands use WMA(TR), not SMA ATR
    if len(closes) < max(SS_RSI_SLOW + 5, SS_ATR_LEN + 5):
        return None, 0.0
    trs: List[float] = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    wma_tr = _wma_series(trs, SS_ATR_LEN)
    atr_abs = wma_tr[-1]
    if atr_abs != atr_abs or atr_abs <= 0:
        return None, 0.0
    o, c = opens[-1], closes[-1]
    if c <= 0:
        return None, 0.0
    upper = c + atr_abs * SS_BAND_MULT
    lower = c - atr_abs * SS_BAND_MULT
    rsi_f = _rsi(closes, SS_RSI_FAST)
    rsi_s = _rsi(closes, SS_RSI_SLOW)
    if rsi_f is None or rsi_s is None:
        return None, 0.0
    if o < lower and rsi_f > rsi_s:
        score = (lower - o) / atr_abs
        return "long", float(score)
    if o > upper and rsi_f < rsi_s:
        score = (o - upper) / atr_abs
        return "short", float(score)
    return None, 0.0


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


# Hive universe: liquid prefilter → candle metrics → score → keep top N.
# Strategy rules (SS / Ichimoku / UT / MZ) are unchanged; only which names we load.
HIVE_SCAN_POOL = 40  # richest by 24h volume after liquidity floor
HIVE_KEEP = 20  # best metric score kept for the race tick
# Equity / metal / FX-style swaps: SS trail math + ticker mid often diverge from
# position mark (seen as TSLA entry ~199 vs mid ~378 → phantom 8% stops).
HIVE_BLOCKLIST = frozenset(
    {
        "TSLA", "AAPL", "AMZN", "MSFT", "NVDA", "META", "GOOG", "GOOGL", "COIN",
        "MSTR", "HOOD", "AMD", "INTC", "BABA", "TSM", "PLTR", "MU", "SMCI",
        "XAU", "XAG", "XPT", "XPD", "WTI", "BTCDOM",
    }
)
# If hive mid vs position entry diverges beyond this, prefer exchange mark / skip stop.
HIVE_MID_SANITY_PCT = 20.0


def _hive_pair_score(
    *,
    vol_usd: float,
    atr15_pct: Optional[float],
    ret1h: float,
    ret24: float,
    vol_z: float,
    spread_bp: float,
    ss_side: Optional[str],
    ss_score: float,
    ichi_side: Optional[str],
    ut_side: Optional[str],
    mz_side: Optional[str],
) -> float:
    """Rank liquid names for hive: ATR band + momentum + any Pine signal ready."""
    # Liquidity (log so BTC doesn't always dominate)
    liq = math.log10(max(vol_usd, 1.0))
    # Prefer tradable ATR% (sweet spot ~0.8–2.5% on 15m); dead or meme-hot score low
    atr = float(atr15_pct or 0.0)
    if atr <= 0.35:
        atr_s = 0.0
    elif atr <= 0.8:
        atr_s = (atr - 0.35) / 0.45
    elif atr <= 2.5:
        atr_s = 1.0
    elif atr <= 4.0:
        atr_s = max(0.0, 1.0 - (atr - 2.5) / 1.5)
    else:
        atr_s = 0.0
    mom = min(abs(ret24) / 8.0, 1.5) + 0.35 * min(abs(ret1h) / 3.0, 1.0)
    vz = max(0.0, min(float(vol_z or 0.0), 2.0)) * 0.25
    tight = max(0.0, 1.0 - spread_bp / 8.0)
    signal = 0.0
    if ss_side in ("long", "short"):
        signal += 1.0 + min(abs(float(ss_score or 0.0)), 2.0) * 0.15
    if ichi_side in ("long", "short"):
        signal += 1.0
    if ut_side in ("long", "short"):
        signal += 1.0
    if mz_side in ("long", "short"):
        signal += 1.0
    return 0.30 * liq + 1.20 * atr_s + 0.90 * mom + vz + 0.40 * tight + 1.10 * signal


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
    scored: List[Tuple[float, Coin]] = []
    for vol_usd, inst, row, last, spread_bp, ret24 in ranked[:HIVE_SCAN_POOL]:
        coin = inst.replace("-USDT-SWAP", "")
        if coin.upper() in HIVE_BLOCKLIST:
            continue
        try:
            m15 = _candles(inst, "15m", 160)
            daily = _candles(inst, "1Dutc", 8)
        except Exception as exc:
            logger.debug("hive candles %s: %s", inst, exc)
            continue
        if len(m15) < 30:
            continue
        # Drop forming bar for structure / Super Scalper (completed 15m only).
        done = m15[:-1] if len(m15) >= 3 else m15
        closes = [_f(c[4]) for c in done]
        highs = [_f(c[2]) for c in done]
        lows = [_f(c[3]) for c in done]
        opens = [_f(c[1]) for c in done]
        vols = [_f(c[5]) for c in done]
        ret1h = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 and closes[-5] else 0.0
        ret7 = 0.0
        if len(daily) >= 2 and _f(daily[0][4]):
            ret7 = (last - _f(daily[0][4])) / _f(daily[0][4]) * 100
        atr15 = _atr_pct(highs, lows, closes)
        atr1h = atr15 * 2 if atr15 is not None else None
        ss_side, ss_score = _super_scalper_signal(opens, highs, lows, closes, atr15)
        daily_done = daily[:-1] if len(daily) >= 3 else daily
        daily_opens = [_f(c[1]) for c in daily_done] if daily_done else []
        ichi_side = _ichi_hull_signal(opens, highs, lows, daily_opens)
        ut_side = _ut_bot_signal(closes, highs, lows)
        mz_side, mz_sl_px = _momentum_zz_signal(highs, lows, closes)
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
        vz = _vol_z(vols)
        c = Coin(
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
            vol_z=vz,
            high24h=_f(row.get("high24h"), last),
            atr15_pct=atr15,
            ss_side=ss_side,
            ss_score=ss_score,
            ichi_side=ichi_side,
            ut_side=ut_side,
            mz_side=mz_side,
            mz_sl_px=mz_sl_px,
        )
        score = _hive_pair_score(
            vol_usd=vol_usd,
            atr15_pct=atr15,
            ret1h=ret1h,
            ret24=ret24,
            vol_z=vz,
            spread_bp=spread_bp,
            ss_side=ss_side,
            ss_score=ss_score,
            ichi_side=ichi_side,
            ut_side=ut_side,
            mz_side=mz_side,
        )
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:HIVE_KEEP]]


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
    """No-op: Oppo is idle (Mayhem runs Super Scalper solo)."""
    del action
    return


def _queue_peak_pair(action: Action) -> None:
    """No-op: PeakSting is idle and no longer mirrors Mayhem."""
    del action
    return


def _oppo_sync_actions(mayhem_book: Book, oppo_book: Book) -> List[Action]:
    """Open missing flipped Mayhem legs. Do not close when Mayhem flats (own exits)."""
    m_by = {p.coin: p for p in mayhem_book.positions}
    o_by = {p.coin: p for p in oppo_book.positions}
    out: List[Action] = []
    for coin, mpos in m_by.items():
        want = flip_side(mpos.side)
        opos = o_by.get(coin)
        if opos is not None and opos.side != want:
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


def _idle_flatten(book: Book, reason: str = "idle") -> List[Action]:
    """Flatten leftover positions for an idle runner."""
    return [Action("close", p.coin, p.side, reason=reason) for p in book.positions]


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
        _STATE["race_epoch"] = saved.get("race_epoch") or saved.get("started_at")
        _STATE["runners"] = _migrate_runner_ids(saved.get("runners") or {})
        for rid, raw in list(_STATE["runners"].items()):
            if isinstance(raw, dict) and raw.get("trades"):
                raw["trades"] = _trades_in_epoch(list(raw.get("trades") or []))
                _STATE["runners"][rid] = raw
    except Exception as exc:
        logger.warning("hive race state unreadable: %s", exc)


def _save() -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({k: _STATE[k] for k in ("started_at", "race_epoch", "runners")}, default=str)
    )
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


def _epoch_iso() -> Optional[str]:
    ep = _STATE.get("race_epoch") or _STATE.get("started_at")
    return str(ep) if ep else None


def _trades_in_epoch(trades: List[dict]) -> List[dict]:
    """Keep closes at/after race_epoch so a reset is not undone by Supabase hydrate."""
    ep = _epoch_iso()
    if not ep:
        return list(trades)
    out = []
    for t in trades:
        at = str(t.get("at") or "")
        if at and at >= ep:
            out.append(t)
    return out


def _hydrate_from_supabase() -> None:
    """Merge durable Supabase hive closes into in-memory/file state after Coolify wipes."""
    earliest: Optional[str] = None
    for runner in RUNNERS:
        db_trades = _fetch_runner_trades(runner)
        # Legacy id from the short-lived Mayhem Inv A/B slot.
        if runner == "oppo_mayhem":
            db_trades = db_trades + _fetch_runner_trades("mayhem_inv")
        db_trades = _trades_in_epoch(db_trades)
        if not db_trades:
            continue
        raw = dict(_STATE["runners"].get(runner) or {})
        file_trades = _trades_in_epoch(list(raw.get("trades") or []))
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
    # Do not pull started_at before race_epoch — that undoes a deliberate reset.
    if _STATE.get("race_epoch"):
        return
    if earliest and _STATE.get("started_at"):
        # Keep the older race start if we rebuilt history that predates this container boot.
        if earliest < str(_STATE["started_at"]):
            _STATE["started_at"] = earliest
    elif earliest and not _STATE.get("started_at"):
        _STATE["started_at"] = earliest


async def reset_race(*, flatten: bool = True) -> Dict[str, Any]:
    """Flatten hive demo books and restart the $300 race ledger (clean scoreboard).

    Does not wipe Supabase trade history — race_epoch filters the board to this race only.
    AYO practice row is left alone (separate OKX key).
    """
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    flat_report: Dict[str, Any] = {}
    async with _LOCK:
        if flatten:
            for runner, client in list(_CLIENTS.items()):
                closed = []
                errors = []
                try:
                    state = await client.get_user_state()
                    for p in state.get("positions") or []:
                        coin = str(p.get("coin") or p.get("symbol") or "").upper()
                        coin = coin.replace("-USDT-SWAP", "").replace("-USDT", "")
                        qty = abs(float(p.get("quantity") or p.get("szi") or 0))
                        if not coin or qty <= 0:
                            continue
                        try:
                            if hasattr(client, "close_position_market"):
                                await client.close_position_market(coin)
                            else:
                                side = "long" if float(p.get("quantity") or p.get("szi") or 0) > 0 else "short"
                                if side == "long":
                                    await client.place_sell_order(coin, qty, leverage=LEVERAGE, reduce_only=True)
                                else:
                                    await client.place_buy_order(coin, qty, leverage=LEVERAGE, reduce_only=True)
                            closed.append(coin)
                        except Exception as exc:
                            errors.append(f"{coin}: {exc}")
                except Exception as exc:
                    errors.append(str(exc))
                flat_report[runner] = {"closed": closed, "errors": errors}

        _STATE["race_epoch"] = now_iso
        _STATE["started_at"] = now_iso
        _STATE["last_error"] = None
        for runner in RUNNERS:
            prev = _STATE["runners"].get(runner) or {}
            configured = bool(prev.get("configured") or runner in _CLIENTS)
            err = None if configured else (prev.get("error") or "API key not set")
            book = Book()
            _STATE["runners"][runner] = {
                **_book_to(book, [], configured, err),
                "mtm": START_EQUITY,
                "day_key": time.strftime("%Y-%m-%d", time.gmtime()),
            }
        _save()
    logger.info("hive race reset epoch=%s flatten=%s", now_iso, flatten)
    return {
        "ok": True,
        "race_epoch": now_iso,
        "start_equity": START_EQUITY,
        "flatten": flat_report,
        "runners": list(RUNNERS),
    }


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


def _open_pos(coin: Coin, side: str, notional: float, qty: float, now: float, runner: str = "") -> Pos:
    from src.runners.rules import CONFETTI_RUNNERS, MZ_RUNNERS, MZ_STOP_PCT

    if runner in CONFETTI_RUNNERS:
        from src.runners.beebots_boozy import boozy_stop_px

        stop = boozy_stop_px(side, coin.mid, coin)
    else:
        pct = stop_pct(coin, runner=runner) or 8.0
        stop = stop_price(side, coin.mid, pct)
        # Momentum ZZ: Pine pivot SL, clamped to disaster max (never wider than MZ_STOP_PCT).
        if runner in MZ_RUNNERS and coin.mz_sl_px and coin.mid > 0:
            disaster = stop_price(side, coin.mid, min(MZ_STOP_PCT, pct))
            mz = float(coin.mz_sl_px)
            if side == "long" and 0 < mz < coin.mid:
                stop = max(mz, disaster)
            elif side == "short" and mz > coin.mid:
                stop = min(mz, disaster)
    risk = abs(coin.mid - stop) / coin.mid * notional if coin.mid else 0.0
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


def _sane_mark(entry: float, candidate: float, fallback: float = 0.0) -> float:
    """Reject hive ticker mids that diverged wildly from the live entry/mark."""
    if entry > 0 and candidate > 0:
        if abs(candidate - entry) / entry * 100.0 <= HIVE_MID_SANITY_PCT:
            return candidate
    return fallback if fallback > 0 else (entry if entry > 0 else candidate)


async def _reconcile_okx(
    book: Book,
    client: Any,
    coins: List[Coin],
    now: float,
    *,
    runner: str = "",
) -> Tuple[Dict[str, float], float]:
    """Overwrite book.positions from live OKX opens; return per-coin UPL and total."""
    from src.runners.rules import FLIP_RUNNERS, mark as ratchet_stop

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
            # Repair inverted stops left by accidental trail on flip books.
            if entry > 0:
                if side == "long" and stop >= entry:
                    stop = stop_price("long", entry, stop_pct(quote, runner=runner) or 8.0)
                elif side == "short" and stop <= entry:
                    stop = stop_price("short", entry, stop_pct(quote, runner=runner) or 8.0)
        else:
            pct = stop_pct(quote, runner=runner) if quote else None
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
        # Pine flip books: disaster stop only — never ATR trail / profit-lock ratchet.
        if quote and runner not in FLIP_RUNNERS:
            ratchet_stop(row, quote)
        elif mark_px > 0:
            if side == "long" and mark_px > row.peak_px:
                row.peak_px = mark_px
            elif side == "short" and mark_px < row.peak_px:
                row.peak_px = mark_px
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
        # Prefer exchange mark already on the pos peak / live UPL path — never
        # book PnL off a ticker mid that disagrees with entry (TSLA 199 vs 378).
        raw_mid = coin.mid if coin else pos.peak_px
        mid = _sane_mark(pos.entry_px, float(raw_mid or 0), fallback=float(pos.peak_px or pos.entry_px))
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
        return
    if action.kind == "open":
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
        book.positions.append(_open_pos(coin, action.side, notional, qty, now, runner=runner))
        book.sync_primary()
        return
    if action.kind == "add":
        # Pine flip books: no pyramids.
        if runner in ("oppo_mayhem", "mayhem", "peaksting"):
            return
        pos = _find_pos(book, action.coin)
        if not pos:
            return
        coin = by_coin.get(pos.coin)
        if not coin:
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
        pct = stop_pct(coin, runner=runner)
        if pct:
            fresh = stop_price(pos.side, pos.entry_px, pct)
            if pos.side == "long":
                pos.stop_px = max(pos.stop_px, min(fresh, pos.entry_px))
            else:
                pos.stop_px = min(pos.stop_px, max(fresh, pos.entry_px))
        book.sync_primary()
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
    bn_coins: List[Coin] = []
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
                if runner in BINANCE_RUNNERS:
                    if decide or not bn_coins:
                        bn_coins = await asyncio.to_thread(build_binance_coins)
                    runner_coins = bn_coins
                else:
                    if not coins:
                        coins = await asyncio.to_thread(build_coins)
                        _STATE["market_coins"] = len(coins)
                    runner_coins = coins
                upl_by_coin, live_upl = await _reconcile_okx(
                    book, client, runner_coins, now, runner=runner
                )
                if decide:
                    _roll_day(book, now)
                    book.day_key = time.strftime("%Y-%m-%d", time.gmtime(now))  # type: ignore[attr-defined]
                    actions: List[Action] = []
                    # Mayhem SS / Peak Ichimoku / Oppo UT Bot / Windward MZ / Orakelia / Confetti.
                    _OPPO_PAIR_Q.clear()
                    _PEAK_PAIR_Q.clear()
                    # Patch coin.mid when ticker mid is insane vs live entry (TSLA case).
                    by_coin = {c.coin: c for c in runner_coins}
                    for pos in book.positions:
                        c = by_coin.get(pos.coin)
                        if not c or pos.entry_px <= 0 or c.mid <= 0:
                            continue
                        if abs(c.mid - pos.entry_px) / pos.entry_px * 100.0 > HIVE_MID_SANITY_PCT:
                            c.mid = float(pos.peak_px or pos.entry_px)
                    actions.extend(decide_actions(runner, book, runner_coins, now))
                    for action in _dedupe_actions(actions):
                        await _act(runner, client, book, runner_coins, action, trades, now)
                    # Re-read exchange after orders so cards show fresh UPL/size.
                    upl_by_coin, live_upl = await _reconcile_okx(
                        book, client, runner_coins, now, runner=runner
                    )
                # All crypto runners stay live — clear any stale retire / loss_stop caps.
                if book.cap in ("retired", "loss_stop", "trade_cap", "fee_budget"):
                    book.cap = None
            except Exception as exc:
                error = str(exc)
                logger.warning("hive %s tick failed: %s", runner, exc)
            # Prefer live exchange UPL; fall back to mid mark if reconcile failed.
            if not upl_by_coin and book.positions:
                pool = bn_coins if runner in BINANCE_RUNNERS else coins
                for pos in book.positions:
                    match = next((c for c in pool if c.coin == pos.coin), None)
                    mid = match.mid if match else pos.entry_px
                    upl_by_coin[pos.coin] = upl(pos, mid)
                live_upl = float(sum(upl_by_coin.values()))
            mtm = book.equity + live_upl
            _note_dd(book, mtm)
            stored = _book_to(book, trades, True, error, upl_by_coin)
            stored["day_key"] = time.strftime("%Y-%m-%d", time.gmtime(now))
            stored["mtm"] = round(mtm, 4)
            stored["upl"] = round(live_upl, 4)
            venue = "Binance" if runner in BINANCE_RUNNERS else "OKX"
            stored["note"] = (
                f"{len(book.positions)}/{MAX_OPENS} open · ${ENTRY_MARGIN:.0f}+${ADD_MARGIN:.0f} margin · {venue}"
            )
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
        "race_epoch": _STATE.get("race_epoch"),
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
    from src.trading.binance_api import BinanceAPI
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
            if runner in BINANCE_RUNNERS:
                client = BinanceAPI(
                    api_key=creds[0],
                    api_secret=creds[1],
                    testnet=bool(creds[2]),
                    leverage=LEVERAGE,
                )
            else:
                client = OKXAPI(
                    api_key=creds[0],
                    api_secret=creds[1],
                    passphrase=creds[2],
                    demo=True,
                    td_mode="isolated",
                )
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
