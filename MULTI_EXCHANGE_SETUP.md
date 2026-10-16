# Multi-Exchange Trading Setup Guide

## Overview

Trade multiple asset classes in one agent process with **hard-split routing** (no cross-venue fallback):

| List | Venue |
|------|--------|
| `CRYPTO_ASSETS` | OKX / Binance / Aster only |
| `STOCK_ASSETS` | Alpaca only |
| `FOREX_ASSETS` | IG only |

If the required venue is missing or a symbol is unmapped, that asset is **skipped** — crypto never falls through to Alpaca, and stocks never hit OKX.

Dashboard UI: Crypto \| Stocks \| Forex pill. Stocks/Forex tabs are scaffolding until Alpaca/IG keys + lists are enabled.

## How It Works

### Single Exchange Mode (Default)
- Trade on ONE exchange at a time
- Set `EXCHANGE=okx`, `binance`, `aster`, `alpaca`, or `ig` in `.env`
- Universe is filtered to that venue’s class only (e.g. OKX → `CRYPTO_ASSETS` only)

### Multi-Exchange Mode
- Trade on MULTIPLE exchanges simultaneously
- Explicit lists only — no “try the other broker” fallback

## Setup Instructions

### 1. Enable Multi-Exchange Mode

Add to your `.env` file:

```env
# Enable multi-exchange mode
MULTI_EXCHANGE_MODE=true

# Configure ALL exchanges you want to use
# Aster (crypto)
ASTER_USER_ADDRESS=0x...
ASTER_SIGNER_ADDRESS=0x...
ASTER_PRIVATE_KEY=0x...

# Binance (crypto)
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
BINANCE_TESTNET=false  # Set to true for testnet

# Trading assets
ASSETS=BTC ETH SOL BNB AVAX
INTERVAL=5m
```

### 2. Asset Mapping (hard-split)

```env
CRYPTO_ASSETS=BTC ETH SOL
STOCK_ASSETS=AAPL TSLA
FOREX_ASSETS=EURUSD GBPUSD
```

- **Crypto** → preferred `EXCHANGE`, then okx → binance → aster (never alpaca/ig)
- **Stocks** → alpaca only (skip if Alpaca not initialized)
- **Forex / metals** → ig only (skip if IG not initialized)

### 3. Run the Agent

```bash
poetry run python src/main.py --assets BTC ETH EURUSD GBPUSD --interval 5m
```

The system will:
- Trade BTC, ETH on Aster (crypto)
- Trade EURUSD, GBPUSD on Pepperstone (forex)
- All simultaneously!

## Features

### ✅ Simultaneous Trading
- Trade crypto and forex at the same time
- No need to switch between exchanges
- All positions tracked in one dashboard

### ✅ Hard-split routing
- Crypto → OKX / Binance / Aster
- Stocks → Alpaca
- Forex → IG
- No cross-venue fallback

### ✅ Aggregated View
- See all positions from all exchanges
- Combined balance and PnL
- Unified trading dashboard

### ✅ Independent Risk Management
- Each exchange has its own balance
- Positions tracked separately
- Risk managed per exchange

## Example Configuration

### Trading Both Crypto and Forex

```env
MULTI_EXCHANGE_MODE=true

# Crypto exchange (Aster)
ASTER_USER_ADDRESS=0x...
ASTER_SIGNER_ADDRESS=0x...
ASTER_PRIVATE_KEY=0x...

# Forex exchange (Pepperstone)
PEPPERSTONE_CLIENT_ID=your_id
PEPPERSTONE_CLIENT_SECRET=your_secret
PEPPERSTONE_ACCOUNT_ID=your_account
PEPPERSTONE_ENVIRONMENT=demo

# Mixed assets
ASSETS=BTC ETH SOL EURUSD GBPUSD USDJPY AUDUSD
INTERVAL=5m
```

### Trading Only Crypto (Multiple Exchanges)

```env
MULTI_EXCHANGE_MODE=true

# Both Aster and Hyperliquid
ASTER_USER_ADDRESS=0x...
ASTER_SIGNER_ADDRESS=0x...
ASTER_PRIVATE_KEY=0x...

HYPERLIQUID_PRIVATE_KEY=0x...

ASSETS=BTC ETH SOL
```

## How Assets Are Mapped

### Crypto Detection
- Assets like: BTC, ETH, SOL, BNB, DOGE, XRP
- Any asset containing: BTC, ETH, SOL
- Defaults to crypto exchange if unclear

### Forex Detection
- Pairs like: EURUSD, GBPUSD, USDJPY
- Contains currency codes: USD, EUR, GBP, JPY, AUD, CAD, CHF, NZD
- Must be 6+ characters (typical forex pair format)

## Troubleshooting

### "No exchange available for [asset]"
- Make sure you have the correct exchange configured
- Check that credentials are correct
- Verify asset format (EURUSD not EUR/USD)

### "Exchange initialization failed"
- Check API credentials
- Verify network connectivity
- Check exchange-specific requirements

### Positions Not Showing
- Wait a few seconds for positions to sync
- Check exchange-specific position formats
- Verify you have open positions on that exchange

## Benefits

1. **Diversification**: Trade multiple asset classes simultaneously
2. **Efficiency**: No need to switch between systems
3. **Unified View**: See all trades in one place
4. **Flexibility**: Mix and match exchanges as needed

## Next Steps

1. Set `MULTI_EXCHANGE_MODE=true` in `.env`
2. Configure all exchanges you want to use
3. Add mixed assets to `ASSETS` (crypto + forex)
4. Run the agent and watch it trade on multiple exchanges!
