"""Rules for the hive runners. No network."""

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


def test_mayhem_picks_ss_signal():
    book = Book()
    slow = coin(coin="ETH", ss_side="long", ss_score=1.0, ret1h=0.2, ret24h=2)
    fast = coin(coin="SOL", ss_side="long", ss_score=4.0, ret1h=4, ret24h=3)
    action = decide("mayhem", book, [slow, fast], now=1_000_000)
    assert action.kind == "open" and action.coin == "SOL" and action.side == "long"


def test_mayhem_closes_on_ss_flip():
    book = Book(
        position=Pos("SOL", "long", 100, 400, 4, opened_at=0, stop_px=95, peak_px=100, added=False, risk_usd=15),
        last_order_at=0,
    )
    action = decide("mayhem", book, [coin(mid=99, ret1h=-1, ss_side="short", ss_score=2)], now=40 * 60)
    assert action.kind == "close" and action.reason == "ss_flip"


def test_add_only_after_the_run_non_flip():
    """Pyramid is for Orakelia / Windward — flip books exit on signal, no add."""
    book = Book(
        position=Pos("SOL", "long", 100, 400, 4, opened_at=0, stop_px=95, peak_px=104, added=False, risk_usd=15)
    )
    # Pullback entry shape that already holds — mid 104 is a 2 ATR run on atr1h=2%.
    action = decide(
        "orakelia",
        book,
        [coin(mid=104, ret1h=0.2, ret24h=3, ret7d=5, rsi14=50, high24h=104)],
        now=40 * 60,
    )
    assert action.kind == "add" and action.notional == 150


def test_orakelia_trend_pullback_long_only():
    book = Book()
    down = coin(coin="DOGE", ret24h=-4, ret1h=-2, ret7d=-10, vol_z=2, rsi14=40)
    # Mild 1h dip inside an uptrend — classic pullback.
    up = coin(coin="SOL", ret24h=3, ret1h=-0.8, ret7d=8, vol_z=1, rsi14=48)
    chase = coin(coin="ETH", ret24h=4, ret1h=2.5, ret7d=9, vol_z=1, rsi14=70)
    action = decide("orakelia", book, [down, up, chase], now=1_000_000)
    assert action.kind == "open" and action.coin == "SOL" and action.side == "long"


def test_orakelia_exits_on_trend_break():
    book = Book(
        position=Pos("SOL", "long", 100, 400, 4, opened_at=0, stop_px=98, peak_px=102, added=False, risk_usd=15),
    )
    action = decide(
        "orakelia",
        book,
        [coin(mid=101, ret24h=-1.0, ret1h=-0.5, ret7d=2, rsi14=45)],
        now=40 * 60,
    )
    assert action.kind == "close" and action.reason == "trend_break"


def test_max_loss_backstop():
    book = Book(
        position=Pos(
            "SOL",
            "long",
            100,
            400,
            4,
            opened_at=0,
            stop_px=90,
            peak_px=100,
            added=False,
            risk_usd=15,
            last_upl=-70.0,
        ),
    )
    action = decide("peaksting", book, [coin(mid=95, ichi_side="long")], now=40 * 60)
    assert action.kind == "close" and action.reason == "max_loss"


def test_peaksting_requires_htf_agree():
    book = Book()
    # Short signal but still rising 24h — filtered out.
    rising = coin(coin="PEPE", ret24h=12, ret1h=-2, ichi_side="short", rsi14=72, high24h=100, mid=97)
    fading = coin(coin="WIF", ret24h=-5, ret1h=-2, ichi_side="short", rsi14=40, high24h=100, mid=90)
    action = decide("peaksting", book, [rising, fading], now=1_000_000)
    assert action.kind == "open" and action.side == "short" and action.coin == "WIF"


def test_oppo_mayhem_needs_ut_side():
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
    action = decide("mayhem", book, [coin(ss_side="long", ss_score=2)], now=1_000_000)
    assert action.kind == "open"
    assert book.cap is None
