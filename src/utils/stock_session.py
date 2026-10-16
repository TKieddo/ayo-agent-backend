"""US cash-equity session gate for the Alpaca stock book.

Source of truth order:
1. Alpaca `/v2/clock` (`is_open`) — includes weekends, NYSE holidays, early closes
2. Local fallback: America/New_York RTH Mon–Fri 09:30–16:00 + NYSE holiday calendar

Crypto is 24/7 and never uses this module. Forex uses `forex_session.py`.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# RTH in Eastern Time
RTH_OPEN_MIN = 9 * 60 + 30   # 09:30
RTH_CLOSE_MIN = 16 * 60      # 16:00
# Early-close days (typically 13:00 ET) — used only when Alpaca clock is unavailable
EARLY_CLOSE_MIN = 13 * 60


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """nth weekday in month (weekday: Mon=0 … Sun=6). n is 1-based; negative = from end."""
    if n > 0:
        d = date(year, month, 1)
        while d.weekday() != weekday:
            d += timedelta(days=1)
        d += timedelta(weeks=n - 1)
        return d
    # last / 2nd-last …
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    d -= timedelta(weeks=(-n) - 1)
    return d


def _observed(d: date) -> date:
    """NYSE observed holiday: Sat → Fri prior, Sun → Mon next."""
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


def nyse_holidays(year: int) -> set[date]:
    """Full-day NYSE holidays for a calendar year (observed dates)."""
    holidays = {
        _observed(date(year, 1, 1)),                          # New Year's Day
        _nth_weekday(year, 1, 0, 3),                          # MLK Day (3rd Mon Jan)
        _nth_weekday(year, 2, 0, 3),                          # Presidents' Day (3rd Mon Feb)
        _observed(date(year, 6, 19)),                         # Juneteenth
        _observed(date(year, 7, 4)),                          # Independence Day
        _nth_weekday(year, 9, 0, 1),                          # Labor Day (1st Mon Sep)
        _nth_weekday(year, 11, 3, 4),                         # Thanksgiving (4th Thu Nov)
        _observed(date(year, 12, 25)),                        # Christmas
    }
    # Good Friday: Friday before Easter
    holidays.add(_good_friday(year))
    return holidays


def _good_friday(year: int) -> date:
    """Anonymous Gregorian algorithm → Easter Sunday, then Good Friday."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    el = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * el) // 451
    month = (h + el - 7 * m + 114) // 31
    day = ((h + el - 7 * m + 114) % 31) + 1
    easter = date(year, month, day)
    return easter - timedelta(days=2)


def nyse_early_close_days(year: int) -> set[date]:
    """Common NYSE 1:00 PM ET early-close days (fallback only; Alpaca clock is preferred)."""
    days: set[date] = set()
    # Day after Thanksgiving
    days.add(_nth_weekday(year, 11, 3, 4) + timedelta(days=1))
    # Christmas Eve when it is a weekday
    xmas_eve = date(year, 12, 24)
    if xmas_eve.weekday() < 5:
        days.add(xmas_eve)
    # Trading day immediately before Independence Day (observed)
    jul4 = _observed(date(year, 7, 4))
    day_before = jul4 - timedelta(days=1)
    if day_before.weekday() < 5:
        days.add(day_before)
    return days


def local_rth_status(
    now: Optional[datetime] = None,
    *,
    entry_cutoff_minutes: int = 10,
) -> Tuple[bool, str, Optional[datetime]]:
    """Local ET session status without Alpaca.

    Returns (entries_allowed, reason, next_boundary_et_naive_or_aware).
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(ET)
    d = local.date()
    minutes = local.hour * 60 + local.minute

    if local.weekday() >= 5:
        return False, "weekend", None

    if d in nyse_holidays(d.year):
        return False, "holiday", None

    early = d in nyse_early_close_days(d.year)
    close_m = EARLY_CLOSE_MIN if early else RTH_CLOSE_MIN

    if minutes < RTH_OPEN_MIN:
        return False, "premarket", None
    if minutes >= close_m:
        return False, "afterhours" if not early else "early_close_done", None

    # Block new entries in the final N minutes (avoid opening into the close)
    cutoff = max(0, int(entry_cutoff_minutes or 0))
    if cutoff > 0 and minutes >= close_m - cutoff:
        return False, f"entry_cutoff_{cutoff}m", None

    label = "rth_early_close" if early else "rth"
    return True, label, None


def parse_alpaca_clock(clock: Any) -> Tuple[Optional[bool], Optional[datetime], Optional[datetime]]:
    """Extract (is_open, next_open, next_close) from Alpaca clock payload."""
    if not isinstance(clock, dict) or not clock:
        return None, None, None
    is_open = clock.get("is_open")
    if is_open is not None:
        is_open = bool(is_open)

    def _parse_ts(raw: Any) -> Optional[datetime]:
        if not raw:
            return None
        try:
            if isinstance(raw, (int, float)):
                return datetime.fromtimestamp(float(raw), tz=timezone.utc)
            s = str(raw).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    return is_open, _parse_ts(clock.get("next_open")), _parse_ts(clock.get("next_close"))


async def is_stock_market_open_for_entries(
    alpaca_client: Any = None,
    *,
    now: Optional[datetime] = None,
    allow_overnight: bool = False,
    entry_cutoff_minutes: int = 10,
) -> Tuple[bool, str]:
    """Whether NEW stock entries are allowed.

    - Weekends / full holidays: never (even if allow_overnight).
    - Primary: Alpaca clock ``is_open`` (holidays + early closes).
    - Fallback: local RTH + NYSE holiday calendar.
    - ``allow_overnight`` only relaxes the local premarket/afterhours block when
      Alpaca clock is unavailable; it never opens a closed NYSE day.
    - Last ``entry_cutoff_minutes`` of the session: no new entries.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(ET)

    # Hard block: weekends — cash equities do not trade
    if local.weekday() >= 5:
        return False, "weekend"

    # Hard block: known full holidays (even before consulting Alpaca)
    if local.date() in nyse_holidays(local.year):
        return False, "holiday"

    # Prefer Alpaca exchange clock
    if alpaca_client is not None and hasattr(alpaca_client, "get_clock"):
        try:
            clock = await alpaca_client.get_clock()
            is_open, next_open, next_close = parse_alpaca_clock(clock)
            if is_open is not None:
                if not is_open:
                    # Distinguish premarket vs afterhours vs holiday via next_open
                    reason = "market_closed"
                    if next_open:
                        no_local = next_open.astimezone(ET)
                        if no_local.date() == local.date() and local.hour * 60 + local.minute < RTH_OPEN_MIN:
                            reason = "premarket"
                        elif no_local.date() > local.date():
                            reason = "closed_until_next_session"
                    return False, reason

                # Market is open — still apply entry cutoff before next_close
                cutoff = max(0, int(entry_cutoff_minutes or 0))
                if cutoff > 0 and next_close is not None:
                    mins_left = (next_close - now).total_seconds() / 60.0
                    if mins_left <= cutoff:
                        return False, f"entry_cutoff_{cutoff}m"
                return True, "alpaca_rth"
        except Exception as e:
            logger.warning(f"Alpaca clock unavailable, using local session fallback: {e}")

    # Local fallback (includes early-close estimate + holidays)
    ok, reason, _ = local_rth_status(now, entry_cutoff_minutes=entry_cutoff_minutes)
    if ok:
        return True, reason

    # allow_overnight: only unlock premarket/afterhours on a trading day — never weekends/holidays
    if allow_overnight and reason in ("premarket", "afterhours", "early_close_done"):
        return True, f"extended_{reason}"
    return False, reason


def us_regular_session_open(
    now: Optional[datetime] = None,
    allow_overnight: bool = False,
    entry_cutoff_minutes: int = 10,
) -> Tuple[bool, str]:
    """Sync local-only check (no Alpaca). Used by pure evaluate() without a client.

    Prefer ``is_stock_market_open_for_entries`` in the live bot loop.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(ET)
    if local.weekday() >= 5:
        return False, "weekend"
    if local.date() in nyse_holidays(local.year):
        return False, "holiday"
    ok, reason, _ = local_rth_status(now, entry_cutoff_minutes=entry_cutoff_minutes)
    if ok:
        return True, reason
    if allow_overnight and reason in ("premarket", "afterhours", "early_close_done"):
        return True, f"extended_{reason}"
    return False, reason
