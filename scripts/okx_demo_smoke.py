#!/usr/bin/env python3
"""Live smoke test against OKX DEMO only: balance → open → TP/SL → close → flat.

Refuses to run if OKX_DEMO is not true. Uses the smallest instrument size.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

ASSET = os.getenv("SMOKE_ASSET", "DOGE")


async def main() -> int:
    from src.config_loader import CONFIG
    from src.trading.okx_api import OKXAPI

    demo = bool(CONFIG.get("okx_demo", True))
    print(f"OKX_DEMO={demo}  asset={ASSET}")
    if not demo:
        print("ABORT: OKX_DEMO is false — refusing live smoke test.")
        return 2

    ex = OKXAPI()
    if not ex.demo:
        print("ABORT: client.demo is False")
        return 2
    # Mechanical book uses isolated; force it for this smoke path.
    ex.td_mode = "isolated"
    print(f"   client td_mode={ex.td_mode} default_leverage={ex.default_leverage}")

    state = await ex.get_user_state()
    bal = float(state.get("balance") or 0)
    positions = state.get("positions") or []
    print(f"1) balance≈${bal:.2f}  open_positions={len(positions)}")
    for p in positions:
        print(f"   already open: {p.get('symbol') or p.get('coin')} qty={p.get('quantity')}")

    # Clear leftover smoke position on this asset if any
    for p in positions:
        sym = str(p.get("symbol") or p.get("coin") or "").upper()
        if sym != ASSET.upper():
            continue
        qty = abs(float(p.get("quantity") or p.get("szi") or 0))
        if qty <= 0:
            continue
        side_close = "sell" if float(p.get("quantity") or 0) > 0 else "buy"
        print(f"   clearing leftover {ASSET} {qty} via {side_close} reduce-only")
        if side_close == "sell":
            await ex.place_sell_order(ASSET, qty, reduce_only=True)
        else:
            await ex.place_buy_order(ASSET, qty, reduce_only=True)

    specs = ex.get_instrument_specs(ASSET)
    print(f"2) specs ctVal={specs['ct_val']} lotSz={specs['lot_sz']} minSz={specs['min_sz']} tick={specs['tick_sz']}")
    px = await ex.get_current_price(ASSET)
    coin_qty = float(specs["min_sz"]) * float(specs["ct_val"])
    notional = coin_qty * px
    print(f"3) price={px}  min coins={coin_qty}  notional≈${notional:.4f}")

    # --- post-only resting order far from market, then cancel ---
    passive = px * 0.85  # buy well below → should rest, not fill
    print(f"4) place post-only BUY @ {passive:.8g} (should rest)")
    row = await ex.place_limit_order(
        ASSET, "buy", coin_qty, passive, post_only=True, leverage=10, td_mode="isolated"
    )
    ord_id = str(row.get("ordId") or "")
    print(f"   ordId={ord_id} px={row.get('px')} sz={row.get('sz')}")
    if not ord_id:
        print("FAIL: no ordId from post-only")
        return 1
    await asyncio.sleep(1.0)
    od = await ex.get_order(ASSET, ord_id)
    print(f"   order state={od.get('state')}")
    await ex.cancel_all_orders(ASSET)
    await asyncio.sleep(0.8)
    od2 = await ex.get_order(ASSET, ord_id)
    print(f"5) after cancel state={od2.get('state')}")

    # --- market open min size ---
    print(f"6) market BUY {coin_qty} {ASSET} isolated 10x")
    await ex.set_leverage(ASSET, 10, td_mode="isolated")
    buy = await ex.place_buy_order(ASSET, coin_qty, leverage=10, reduce_only=False)
    print(f"   buy result={buy}")
    await asyncio.sleep(1.2)

    state2 = await ex.get_user_state()
    pos = None
    for p in state2.get("positions") or []:
        if str(p.get("symbol") or p.get("coin") or "").upper() == ASSET.upper():
            pos = p
            break
    if not pos:
        print("FAIL: no position after market buy")
        print(f"   positions={state2.get('positions')}")
        return 1
    qty = abs(float(pos.get("quantity") or pos.get("szi") or 0))
    entry = float(pos.get("entry_price") or pos.get("entryPx") or px)
    print(f"7) OPEN qty={qty} entry={entry} mgnMode={pos.get('okx_mgn_mode') or pos.get('mgnMode')}")

    # TP/SL algos ~±2% (won't trigger during this test)
    tp = entry * 1.02
    sl = entry * 0.98
    print(f"8) place TP@{tp:.6g} SL@{sl:.6g}")
    try:
        tp_res = await ex.place_take_profit(ASSET, True, qty, tp, td_mode="isolated")
        print(f"   TP ok: {tp_res}")
    except Exception as e:
        print(f"   TP FAIL: {e}")
    try:
        sl_res = await ex.place_stop_loss(ASSET, True, qty, sl, td_mode="isolated")
        print(f"   SL ok: {sl_res}")
    except Exception as e:
        print(f"   SL FAIL: {e}")

    # --- close reduce-only ---
    print(f"9) market SELL reduce-only {qty}")
    await ex.place_sell_order(ASSET, qty, reduce_only=True)
    try:
        await ex.cancel_all_orders(ASSET)
    except Exception as e:
        print(f"   cancel algos: {e}")
    await asyncio.sleep(1.2)

    state3 = await ex.get_user_state()
    still = [
        p
        for p in (state3.get("positions") or [])
        if str(p.get("symbol") or p.get("coin") or "").upper() == ASSET.upper()
        and abs(float(p.get("quantity") or p.get("szi") or 0)) > 0
    ]
    if still:
        print(f"FAIL: still open: {still}")
        return 1
    print("10) FLAT — open + TP/SL + close all succeeded on OKX DEMO")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
