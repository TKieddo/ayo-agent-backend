# Alpaca Setup (Stocks only)

Wire the AYO bot to Alpaca **paper** trading for US **stocks**.

**Hard-split rule:** `STOCK_ASSETS` → Alpaca only. Crypto stays on OKX/Binance/Aster via `CRYPTO_ASSETS` — there is **no** crypto→Alpaca or stock→OKX fallback. Unmapped symbols are skipped.

Dashboard **Stocks** tab is UI scaffolding until you enable keys + `STOCK_ASSETS` below.

## 1. Get API keys

1. Log in at [app.alpaca.markets](https://app.alpaca.markets/)
2. Open **Paper Trading**
3. **API Keys** → Generate
4. Copy **Key ID** and **Secret** (shown once)

Use **Trading API** keys — not Broker API.

## 2. Alpaca-only process

```bash
EXCHANGE=alpaca
ALPACA_API_KEY=your_key_id
ALPACA_API_SECRET=your_secret
ALPACA_PAPER=true

STOCK_ASSETS=AAPL TSLA NVDA MSFT
INTERVAL=15m

DEFAULT_LEVERAGE=1
ALPACA_LEVERAGE=1
MARGIN_PER_POSITION=100
POSITION_SIZING_MODE=margin
MAX_POSITIONS=4

AGENT_MANAGE_EXITS=true
ENABLE_STOP_LOSS_ORDERS=true
TRADING_ENABLED=true
DEEPSEEK_API_KEY=your_deepseek_key
```

Do **not** put crypto tickers in `STOCK_ASSETS`. Do not rely on a mixed `ASSETS=` list for multi-venue routing.

## 3. Multi-exchange (OKX crypto + Alpaca stocks)

```bash
MULTI_EXCHANGE_MODE=true
EXCHANGE=okx

# OKX crypto
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_PASSPHRASE=...
CRYPTO_ASSETS=BTC ETH SOL

# Alpaca stocks
ALPACA_API_KEY=...
ALPACA_API_SECRET=...
ALPACA_PAPER=true
STOCK_ASSETS=AAPL TSLA
```

Boot log should show routing like `{'BTC': 'okx', 'AAPL': 'alpaca'}` — never `BTC → alpaca`.

## 4. Run

```bash
cd "AYO-Trading-Agent-Backend---AI--main"
poetry install   # or: pip install -e .
python -m src.main
```

## 5. Notes

| Topic | Detail |
|--------|--------|
| Paper vs live | Keep `ALPACA_PAPER=true` until exits look solid |
| Leverage | Spot/margin buying power — not 10x perps |
| Market hours | US stocks: regular session |
| PDT | Live accounts under $25k have day-trade limits |
| Mechanical scalp | Crypto/OKX only — does not trade Alpaca stocks |
