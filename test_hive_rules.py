"""Rules for the five hive runners. No network."""

from src.runners.rules import Book, Coin, Pos, decide, stop_pct


def coin(**kw):
    base = dict(
        coin="SOL",
        mid=100,
        spread_bp=2,
        vol24h_usd=20_000_000,
        ret1h=1,
        ret24h=3,
        ret7d=4,
        rsi14=55,
        atr1h_pct=2,
        oi_chg_1h=1,
        funding_z=0,
        vol_z=1,
        high24h=110,
    )
    base.update(kw)
    return Coin(**base)


def test_wide_stop_is_skipped():
    assert stop_pct(coin(atr1h_pct=4)) is None
    assert stop_pct(coin(atr1h_pct=2)) == 5


def test_mayhem_picks_the_hardest_move():
    book = Book()
    slow = coin(coin="ETH", ret1h=0.2, ret24h=2)
    fast = coin(coin="SOL", ret1h=4, ret24h=3)
    action = decide("mayhem", book, [slow, fast], now=1_000_000)
    assert action.kind == "open" and action.coin == "SOL" and action.side == "long"


def test_mayhem_does_not_add_to_a_loser():
    book = Book(
        position=Pos("SOL", "long", 100, 300, 3, opened_at=0, stop_px=95, peak_px=100, added=False, risk_usd=15),
        last_order_at=0,
    )
    action = decide("mayhem", book, [coin(mid=99, ret1h=-1)], now=40 * 60)
    assert action.kind == "close" and action.reason == "bail"


def test_add_only_after_the_run():
    book = Book(
        position=Pos("SOL", "long", 100, 300, 3, opened_at=0, stop_px=95, peak_px=100, added=False, risk_usd=15)
    )
    # 1h ATR is 2% of 100 = $2. 1.5 ATR = $3. Price 104 is a 2 ATR run.
    action = decide("mayhem", book, [coin(mid=104, ret1h=2, high24h=104)], now=40 * 60)
    assert action.kind == "add" and action.notional == 150


def test_orakelia_is_long_only():
    book = Book()
    down = coin(coin="DOGE", ret24h=-4, ret1h=-2, ret7d=-10, vol_z=2)
    up = coin(coin="SOL", ret24h=2, ret1h=1, ret7d=8, vol_z=1)
    action = decide("orakelia", book, [down, up], now=1_000_000)
    assert action.kind == "open" and action.coin == "SOL" and action.side == "long"


def test_peaksting_shorts_the_fade():
    book = Book()
    pumped = coin(coin="PEPE", ret24h=12, ret1h=-2, vol_z=2, rsi14=72, high24h=100, mid=97)
    action = decide("peaksting", book, [pumped], now=1_000_000)
    assert action.kind == "open" and action.side == "short" and action.coin == "PEPE"


def test_oppo_mayhem_does_not_open_solo():
    """Oppo Mayhem only enters via the 1:1 Mayhem pair queue in hive_race."""
    book = Book()
    pumped = coin(coin="SOL", ret1h=4, ret24h=3, oi_chg_1h=1, rsi14=55)
    action = decide("oppo_mayhem", book, [pumped], now=1_000_000)
    assert action.kind == "hold"


def test_flip_side_opposite():
    from src.runners.rules import flip_side

    assert flip_side("long") == "short"
    assert flip_side("short") == "long"


def test_entry_count_does_not_block_a_valid_setup():
    book = Book(trades_today=12, fees_today=9.0, cap="trade_cap")
    action = decide("mayhem", book, [coin()], now=1_000_000)
    assert action.kind == "open"
    assert book.cap is None
