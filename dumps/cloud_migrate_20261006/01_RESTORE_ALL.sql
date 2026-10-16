-- AI-Trading Agent dump (fresh-install safe)
-- Project: rwcamygafesbuwvwbqrz
-- Skips historical clear_* wipe migrations (they assumed existing cloud tables)
-- Supabase Schema for Alpha Arena Trading Dashboard
-- Run this in your Supabase SQL editor

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Prices table (real-time price data)
CREATE TABLE IF NOT EXISTS prices (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  symbol VARCHAR(10) NOT NULL,
  price DECIMAL(20, 8) NOT NULL,
  change_24h DECIMAL(20, 8) DEFAULT 0,
  change_24h_percent DECIMAL(10, 4) DEFAULT 0,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(symbol)
);

-- Positions table
CREATE TABLE IF NOT EXISTS positions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  symbol VARCHAR(10) NOT NULL,
  side VARCHAR(10) NOT NULL CHECK (side IN ('long', 'short')),
  size DECIMAL(20, 8) NOT NULL,
  entry_price DECIMAL(20, 8) NOT NULL,
  current_price DECIMAL(20, 8) NOT NULL,
  liquidation_price DECIMAL(20, 8),
  unrealized_pnl DECIMAL(20, 8) DEFAULT 0,
  realized_pnl DECIMAL(20, 8) DEFAULT 0,
  leverage INTEGER,
  opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  closed_at TIMESTAMPTZ
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id BIGINT NOT NULL,
  symbol VARCHAR(10) NOT NULL,
  side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
  type VARCHAR(20) NOT NULL CHECK (type IN ('market', 'limit', 'stop', 'take_profit')),
  size DECIMAL(20, 8) NOT NULL,
  price DECIMAL(20, 8),
  status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'filled', 'canceled', 'rejected', 'triggered')),
  filled_size DECIMAL(20, 8) DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(order_id, symbol)
);

-- Trades table (executed trades)
CREATE TABLE IF NOT EXISTS trades (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  symbol VARCHAR(10) NOT NULL,
  side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
  size DECIMAL(20, 8) NOT NULL,
  price DECIMAL(20, 8) NOT NULL,
  fee DECIMAL(20, 8) DEFAULT 0,
  pnl DECIMAL(20, 8),
  executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  order_id UUID REFERENCES orders(id)
);

-- Account metrics table
CREATE TABLE IF NOT EXISTS account_metrics (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  total_value DECIMAL(20, 8) NOT NULL,
  balance DECIMAL(20, 8) NOT NULL,
  open_positions INTEGER DEFAULT 0,
  total_pnl DECIMAL(20, 8) DEFAULT 0,
  daily_pnl DECIMAL(20, 8) DEFAULT 0,
  win_rate DECIMAL(5, 2) DEFAULT 0,
  total_trades INTEGER DEFAULT 0,
  leverage DECIMAL(5, 2) DEFAULT 1,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trading logs table
CREATE TABLE IF NOT EXISTS trading_logs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  level VARCHAR(20) NOT NULL CHECK (level IN ('info', 'warning', 'error', 'success')),
  message TEXT NOT NULL,
  data JSONB,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Decisions table (AI agent decisions)
CREATE TABLE IF NOT EXISTS decisions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  asset VARCHAR(10) NOT NULL,
  action VARCHAR(10) NOT NULL CHECK (action IN ('buy', 'sell', 'hold')),
  allocation_usd DECIMAL(20, 8),
  tp_price DECIMAL(20, 8),
  sl_price DECIMAL(20, 8),
  rationale TEXT,
  reasoning TEXT,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pair Hunter stats table (per-asset performance memory)
CREATE TABLE IF NOT EXISTS pair_hunter_stats (
  asset VARCHAR(16) PRIMARY KEY,
  total_trades INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  win_rate DECIMAL(7, 3) NOT NULL DEFAULT 0,
  total_pnl_usd DECIMAL(20, 8) NOT NULL DEFAULT 0,
  total_pnl_percent DECIMAL(20, 8) NOT NULL DEFAULT 0,
  expectancy_usd DECIMAL(20, 8) NOT NULL DEFAULT 0,
  expectancy_percent DECIMAL(20, 8) NOT NULL DEFAULT 0,
  data_fail_count INTEGER NOT NULL DEFAULT 0,
  excluded_until TIMESTAMPTZ,
  exclusion_reason TEXT,
  last_data_error TEXT,
  last_close_reason TEXT,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at ON trades(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON trading_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON trading_logs(level);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_asset ON decisions(asset);
CREATE INDEX IF NOT EXISTS idx_pair_hunter_stats_updated_at ON pair_hunter_stats(updated_at DESC);

-- Enable Row Level Security (optional, adjust as needed)
ALTER TABLE prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE account_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE trading_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE pair_hunter_stats ENABLE ROW LEVEL SECURITY;

-- Policy: Allow all reads (adjust for security as needed)
CREATE POLICY "Allow public read access" ON prices FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON positions FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON orders FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON trades FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON account_metrics FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON trading_logs FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON decisions FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON pair_hunter_stats FOR SELECT USING (true);

-- Real-time subscriptions (enabled by default in Supabase)
-- Prices table will broadcast changes automatically


-- === Migrations (create/alter only) ===

-- >>> 20250104_order_metrics_fees.sql
-- Migration: Add order metrics and fees/profit summary table
-- Stores aggregated metrics for orders and fees/profit calculations

-- Create order_metrics_summary table for historical tracking
DO $$ BEGIN
  CREATE TABLE IF NOT EXISTS order_metrics_summary (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Order metrics
    total_orders INTEGER DEFAULT 0,
    open_orders INTEGER DEFAULT 0,
    filled_orders INTEGER DEFAULT 0,
    canceled_orders INTEGER DEFAULT 0,
    rejected_orders INTEGER DEFAULT 0,
    open_pct DECIMAL(5, 2) DEFAULT 0,
    filled_pct DECIMAL(5, 2) DEFAULT 0,
    canceled_pct DECIMAL(5, 2) DEFAULT 0,
    rejected_pct DECIMAL(5, 2) DEFAULT 0,
    -- Fees and profit
    total_fees DECIMAL(20, 8) DEFAULT 0,
    total_pnl DECIMAL(20, 8) DEFAULT 0,
    net_profit DECIMAL(20, 8) DEFAULT 0,
    avg_fee_per_trade DECIMAL(20, 8) DEFAULT 0,
    fee_to_pnl_ratio DECIMAL(10, 4) DEFAULT 0,
    profit_margin DECIMAL(10, 4) DEFAULT 0,
    -- Metadata
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(timestamp)
  );
EXCEPTION WHEN others THEN NULL;
END $$;

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_order_metrics_summary_timestamp ON order_metrics_summary(timestamp DESC);

-- Create a function to upsert order metrics summary
CREATE OR REPLACE FUNCTION upsert_order_metrics_summary(
  p_timestamp TIMESTAMPTZ,
  p_total_orders INTEGER,
  p_open_orders INTEGER,
  p_filled_orders INTEGER,
  p_canceled_orders INTEGER,
  p_rejected_orders INTEGER,
  p_total_fees DECIMAL,
  p_total_pnl DECIMAL,
  p_net_profit DECIMAL,
  p_avg_fee_per_trade DECIMAL,
  p_fee_to_pnl_ratio DECIMAL,
  p_profit_margin DECIMAL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  p_open_pct DECIMAL(5, 2);
  p_filled_pct DECIMAL(5, 2);
  p_canceled_pct DECIMAL(5, 2);
  p_rejected_pct DECIMAL(5, 2);
BEGIN
  -- Calculate percentages
  p_open_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_open_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_filled_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_filled_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_canceled_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_canceled_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_rejected_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_rejected_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;

  -- Upsert
  INSERT INTO order_metrics_summary (
    timestamp,
    total_orders, open_orders, filled_orders, canceled_orders, rejected_orders,
    open_pct, filled_pct, canceled_pct, rejected_pct,
    total_fees, total_pnl, net_profit, avg_fee_per_trade, fee_to_pnl_ratio, profit_margin,
    calculated_at
  ) VALUES (
    p_timestamp,
    p_total_orders, p_open_orders, p_filled_orders, p_canceled_orders, p_rejected_orders,
    p_open_pct, p_filled_pct, p_canceled_pct, p_rejected_pct,
    p_total_fees, p_total_pnl, p_net_profit, p_avg_fee_per_trade, p_fee_to_pnl_ratio, p_profit_margin,
    NOW()
  )
  ON CONFLICT (timestamp) DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    open_orders = EXCLUDED.open_orders,
    filled_orders = EXCLUDED.filled_orders,
    canceled_orders = EXCLUDED.canceled_orders,
    rejected_orders = EXCLUDED.rejected_orders,
    open_pct = EXCLUDED.open_pct,
    filled_pct = EXCLUDED.filled_pct,
    canceled_pct = EXCLUDED.canceled_pct,
    rejected_pct = EXCLUDED.rejected_pct,
    total_fees = EXCLUDED.total_fees,
    total_pnl = EXCLUDED.total_pnl,
    net_profit = EXCLUDED.net_profit,
    avg_fee_per_trade = EXCLUDED.avg_fee_per_trade,
    fee_to_pnl_ratio = EXCLUDED.fee_to_pnl_ratio,
    profit_margin = EXCLUDED.profit_margin,
    calculated_at = NOW();
END;
$$;

-- Add comment
COMMENT ON TABLE order_metrics_summary IS 'Aggregated order metrics and fees/profit summary for historical tracking';
COMMENT ON FUNCTION upsert_order_metrics_summary IS 'Upsert order metrics and fees summary with automatic percentage calculations';



-- >>> 20250104_wins_losses_stats.sql
-- Migration: Add wins/losses statistics support
-- Wins/Losses are calculated dynamically from trades table, but this migration
-- adds indexes and views for performance optimization

-- Add index on pnl for faster filtering of wins/losses
CREATE INDEX IF NOT EXISTS idx_trades_pnl ON trades(pnl) WHERE pnl IS NOT NULL;

-- Add composite index for filtered queries (symbol + pnl + executed_at)
CREATE INDEX IF NOT EXISTS idx_trades_symbol_pnl_executed ON trades(symbol, pnl, executed_at DESC) WHERE pnl IS NOT NULL;

-- Create a materialized view for wins/losses statistics (optional optimization)
-- This can be refreshed periodically for better performance on large datasets
DO $$ BEGIN
  CREATE MATERIALIZED VIEW IF NOT EXISTS wins_losses_stats AS
  WITH trade_stats AS (
    SELECT 
      COUNT(*) FILTER (WHERE pnl >= 0) as win_count,
      COUNT(*) FILTER (WHERE pnl < 0) as loss_count,
      COUNT(*) as total_count,
      COALESCE(SUM(pnl) FILTER (WHERE pnl >= 0), 0) as wins_total_pnl,
      COALESCE(SUM(pnl) FILTER (WHERE pnl < 0), 0) as losses_total_pnl,
      COALESCE(AVG(pnl) FILTER (WHERE pnl >= 0), 0) as wins_avg_pnl,
      COALESCE(AVG(pnl) FILTER (WHERE pnl < 0), 0) as losses_avg_pnl,
      MAX(executed_at) as last_trade_at
    FROM trades
    WHERE pnl IS NOT NULL
  )
  SELECT 
    win_count,
    loss_count,
    total_count,
    CASE 
      WHEN total_count > 0 THEN ROUND((win_count::numeric / total_count::numeric * 100)::numeric, 2)
      ELSE 0
    END as win_pct,
    CASE 
      WHEN total_count > 0 THEN ROUND((loss_count::numeric / total_count::numeric * 100)::numeric, 2)
      ELSE 0
    END as loss_pct,
    wins_total_pnl,
    losses_total_pnl,
    wins_avg_pnl,
    losses_avg_pnl,
    last_trade_at,
    NOW() as calculated_at
  FROM trade_stats;
EXCEPTION WHEN others THEN NULL;
END $$;

-- Create index on the materialized view
CREATE UNIQUE INDEX IF NOT EXISTS wins_losses_stats_unique ON wins_losses_stats (calculated_at);

-- Create a function to refresh the materialized view
-- Note: CONCURRENTLY requires a unique index, which we created above
CREATE OR REPLACE FUNCTION refresh_wins_losses_stats()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  -- Check if the materialized view exists before refreshing
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'wins_losses_stats') THEN
    REFRESH MATERIALIZED VIEW CONCURRENTLY wins_losses_stats;
  END IF;
END;
$$;

-- Add comment explaining usage
COMMENT ON MATERIALIZED VIEW wins_losses_stats IS 'Materialized view of wins/losses statistics. Refresh using refresh_wins_losses_stats() function.';
COMMENT ON INDEX idx_trades_pnl IS 'Index on trades.pnl for faster wins/losses filtering';
COMMENT ON INDEX idx_trades_symbol_pnl_executed IS 'Composite index for filtered trades queries by symbol, pnl, and execution time';



-- >>> 20250106_add_triggered_status.sql
-- Add "triggered" status to orders table
-- This migration updates the orders table to support all Hyperliquid order statuses

-- Drop the existing CHECK constraint
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check;

-- Add new CHECK constraint with all statuses
ALTER TABLE orders ADD CONSTRAINT orders_status_check 
CHECK (status IN ('open', 'filled', 'canceled', 'rejected', 'triggered'));

-- Add comment
COMMENT ON COLUMN orders.status IS 'Order status: open, filled, canceled, rejected, or triggered';



-- >>> SKIPPED 20250106_clear_binance_data.sql (data wipe; not needed on empty DB)

-- >>> 20250106_wallet_balance_history.sql
-- Create wallet_balance_history table to track account value over time
-- This allows us to calculate Day/Week/Month percentage changes

CREATE TABLE IF NOT EXISTS wallet_balance_history (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  account_value DECIMAL(20, 8) NOT NULL,
  available_balance DECIMAL(20, 8) NOT NULL,
  total_positions_value DECIMAL(20, 8) DEFAULT 0,
  unrealized_pnl DECIMAL(20, 8) DEFAULT 0,
  network VARCHAR(20) DEFAULT 'mainnet', -- 'mainnet' or 'testnet'
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create unique constraint on (timestamp, network) to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS wallet_balance_history_timestamp_network_unique 
ON wallet_balance_history(timestamp, network);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_wallet_balance_history_timestamp ON wallet_balance_history(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_balance_history_network ON wallet_balance_history(network);
CREATE INDEX IF NOT EXISTS idx_wallet_balance_history_network_timestamp ON wallet_balance_history(network, timestamp DESC);

-- Enable RLS
ALTER TABLE wallet_balance_history ENABLE ROW LEVEL SECURITY;

-- Policy: Allow public read access
CREATE POLICY "Allow public read access" ON wallet_balance_history FOR SELECT USING (true);

-- Function to get percentage change between two timestamps
CREATE OR REPLACE FUNCTION get_balance_change_percent(
  p_network VARCHAR DEFAULT 'mainnet',
  p_hours_ago INTEGER DEFAULT 24
)
RETURNS DECIMAL(10, 4) AS $$
DECLARE
  current_balance DECIMAL(20, 8);
  past_balance DECIMAL(20, 8);
BEGIN
  -- Get current balance (most recent)
  SELECT account_value INTO current_balance
  FROM wallet_balance_history
  WHERE network = p_network
  ORDER BY timestamp DESC
  LIMIT 1;
  
  -- Get past balance (p_hours_ago hours ago)
  SELECT account_value INTO past_balance
  FROM wallet_balance_history
  WHERE network = p_network
    AND timestamp <= NOW() - (p_hours_ago || ' hours')::INTERVAL
  ORDER BY timestamp DESC
  LIMIT 1;
  
  -- Calculate percentage change
  IF past_balance IS NULL OR past_balance = 0 THEN
    RETURN 0;
  END IF;
  
  RETURN ((current_balance - past_balance) / past_balance) * 100;
END;
$$ LANGUAGE plpgsql;

-- Comment on table
COMMENT ON TABLE wallet_balance_history IS 'Historical wallet balance snapshots for calculating percentage changes (Day/Week/Month)';



-- >>> SKIPPED 20250107_clear_all_tables_aster.sql (data wipe; not needed on empty DB)

-- >>> 20250107_trading_settings.sql
-- Create trading_settings table for persistent trading configuration
-- This allows users to set leverage, TP%, SL% via frontend and persist to database

CREATE TABLE IF NOT EXISTS trading_settings (
    id text PRIMARY KEY DEFAULT 'default',
    leverage integer NOT NULL DEFAULT 10,
    take_profit_percent numeric(5,2) NOT NULL DEFAULT 5.00,
    stop_loss_percent numeric(5,2) NOT NULL DEFAULT 3.00,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    updated_by text,
    CONSTRAINT leverage_range CHECK (leverage >= 1 AND leverage <= 100),
    CONSTRAINT tp_percent_range CHECK (take_profit_percent >= 0.1 AND take_profit_percent <= 100),
    CONSTRAINT sl_percent_range CHECK (stop_loss_percent >= 0.1 AND stop_loss_percent <= 50)
);

-- Insert default settings if not exists
INSERT INTO trading_settings (id, leverage, take_profit_percent, stop_loss_percent)
VALUES ('default', 10, 5.00, 3.00)
ON CONFLICT (id) DO NOTHING;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_trading_settings_id ON trading_settings(id);

-- Add comment
COMMENT ON TABLE trading_settings IS 'Persistent trading configuration: leverage, take profit %, stop loss %';
COMMENT ON COLUMN trading_settings.id IS 'Settings ID (use "default" for global settings)';
COMMENT ON COLUMN trading_settings.leverage IS 'Default leverage multiplier (1-100)';
COMMENT ON COLUMN trading_settings.take_profit_percent IS 'Take profit percentage (e.g., 5.00 = 5%)';
COMMENT ON COLUMN trading_settings.stop_loss_percent IS 'Stop loss percentage (e.g., 3.00 = 3%)';



-- >>> 20250108_position_sizing.sql
-- Add position sizing configuration to trading_settings table
-- This allows users to configure target profit per 1% move and allocation strategy

ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS target_profit_per_1pct_move numeric(10,2) DEFAULT 1.00,
ADD COLUMN IF NOT EXISTS allocation_per_position numeric(10,2) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS max_positions integer DEFAULT 6,
ADD COLUMN IF NOT EXISTS position_sizing_mode text DEFAULT 'auto' CHECK (position_sizing_mode IN ('auto', 'fixed', 'target_profit'));

-- Update constraints
ALTER TABLE trading_settings
DROP CONSTRAINT IF EXISTS target_profit_range,
ADD CONSTRAINT target_profit_range CHECK (target_profit_per_1pct_move >= 0.01 AND target_profit_per_1pct_move <= 1000),
DROP CONSTRAINT IF EXISTS allocation_range,
ADD CONSTRAINT allocation_range CHECK (allocation_per_position IS NULL OR (allocation_per_position >= 1 AND allocation_per_position <= 100000)),
DROP CONSTRAINT IF EXISTS max_positions_range,
ADD CONSTRAINT max_positions_range CHECK (max_positions >= 1 AND max_positions <= 50);

-- Update default row if exists
UPDATE trading_settings
SET 
  target_profit_per_1pct_move = 1.00,
  max_positions = 6,
  position_sizing_mode = 'auto'
WHERE id = 'default'
AND (target_profit_per_1pct_move IS NULL OR max_positions IS NULL OR position_sizing_mode IS NULL);

-- Add comments
COMMENT ON COLUMN trading_settings.target_profit_per_1pct_move IS 'Target profit in USD per 1% price move (e.g., 1.00 = $1 profit on 1% move)';
COMMENT ON COLUMN trading_settings.allocation_per_position IS 'Fixed allocation per position in USD (NULL = auto-calculate based on target profit)';
COMMENT ON COLUMN trading_settings.max_positions IS 'Maximum number of concurrent positions (1-50)';
COMMENT ON COLUMN trading_settings.position_sizing_mode IS 'Position sizing mode: auto (calculate from target profit), fixed (use allocation_per_position), target_profit (calculate from target_profit_per_1pct_move)';



-- >>> 20250109_strategies_backtesting.sql
-- Strategies and Backtesting Schema
-- Run this in your Supabase SQL editor

-- Strategies table
CREATE TABLE IF NOT EXISTS strategies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    source_video_url TEXT,
    source_transcript TEXT,
    strategy_json JSONB,  -- Extracted strategy rules
    code_file_path TEXT,   -- Path to generated Python file
    status VARCHAR(50) DEFAULT 'extracted' CHECK (status IN ('extracted', 'generated', 'backtested', 'active', 'inactive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT
);

-- Backtest results table
CREATE TABLE IF NOT EXISTS backtest_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID REFERENCES strategies(id) ON DELETE CASCADE,
    strategy_name VARCHAR(255) NOT NULL,  -- Denormalized for easier queries
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,  -- e.g., '5m', '15m', '1h'
    initial_capital DECIMAL(20, 2) NOT NULL,
    final_capital DECIMAL(20, 2) NOT NULL,
    total_return DECIMAL(10, 2) NOT NULL,  -- Percentage
    buy_and_hold_return DECIMAL(10, 2),  -- For comparison
    sharpe_ratio DECIMAL(10, 4),
    sortino_ratio DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 2),  -- Percentage
    win_rate DECIMAL(5, 2),  -- Percentage
    profit_factor DECIMAL(10, 4),
    expectancy DECIMAL(10, 2),  -- Average profit per trade
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    avg_hold_time_hours DECIMAL(10, 2),
    metrics_json JSONB,  -- Full metrics object
    equity_curve JSONB,  -- Array of {date, value} points
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Strategy performance tracking (for live strategies)
CREATE TABLE IF NOT EXISTS strategy_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID REFERENCES strategies(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    daily_return DECIMAL(10, 2),
    cumulative_return DECIMAL(10, 2),
    drawdown DECIMAL(10, 2),
    trades_count INTEGER DEFAULT 0,
    pnl DECIMAL(20, 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(strategy_id, date)
);

-- Historical data cache (for faster backtesting)
CREATE TABLE IF NOT EXISTS historical_ohlcv (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, timeframe, timestamp)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status);
CREATE INDEX IF NOT EXISTS idx_strategies_created_at ON strategies(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy_id ON backtest_results(strategy_id);
CREATE INDEX IF NOT EXISTS idx_backtest_results_created_at ON backtest_results(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_results_total_return ON backtest_results(total_return DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_performance_strategy_id ON strategy_performance(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_performance_date ON strategy_performance(date DESC);
CREATE INDEX IF NOT EXISTS idx_historical_ohlcv_symbol_timeframe ON historical_ohlcv(symbol, timeframe, timestamp DESC);

-- Comments
COMMENT ON TABLE strategies IS 'Trading strategies extracted from videos or manually created';
COMMENT ON TABLE backtest_results IS 'Backtest execution results and metrics';
COMMENT ON TABLE strategy_performance IS 'Daily performance tracking for active strategies';
COMMENT ON TABLE historical_ohlcv IS 'Cached historical OHLCV data for backtesting';



-- >>> 20250110_strategy_optimization.sql
-- Strategy Optimization Schema
-- Run this in your Supabase SQL editor

-- Strategy optimizations table
CREATE TABLE IF NOT EXISTS strategy_optimizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID REFERENCES strategies(id) ON DELETE CASCADE,
    original_backtest_id UUID REFERENCES backtest_results(id),
    target_profitability DECIMAL(5, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'stopped', 'failed')),
    best_profitability DECIMAL(10, 2),
    original_profitability DECIMAL(10, 2),
    improvement DECIMAL(10, 2),  -- Percentage improvement
    iterations_completed INTEGER DEFAULT 0,
    max_iterations INTEGER DEFAULT 50,
    parameters_tested JSONB,  -- Array of parameter sets tested
    best_parameters JSONB,  -- Best parameter set found
    original_parameters JSONB,  -- Original parameters
    optimization_method VARCHAR(50) DEFAULT 'llm_guided' CHECK (optimization_method IN ('llm_guided', 'grid_search', 'random_search', 'bayesian')),
    target_met BOOLEAN DEFAULT FALSE,
    stopped_reason TEXT,  -- Why optimization stopped
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optimization iterations table (detailed tracking)
CREATE TABLE IF NOT EXISTS optimization_iterations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    optimization_id UUID REFERENCES strategy_optimizations(id) ON DELETE CASCADE,
    iteration_number INTEGER NOT NULL,
    parameters JSONB NOT NULL,  -- Parameters tested in this iteration
    profitability DECIMAL(10, 2),
    sharpe_ratio DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 2),
    win_rate DECIMAL(5, 2),
    total_trades INTEGER,
    backtest_result_id UUID REFERENCES backtest_results(id),
    llm_reasoning TEXT,  -- LLM's reasoning for these parameters
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(optimization_id, iteration_number)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_optimizations_strategy_id ON strategy_optimizations(strategy_id);
CREATE INDEX IF NOT EXISTS idx_optimizations_status ON strategy_optimizations(status);
CREATE INDEX IF NOT EXISTS idx_optimizations_created_at ON strategy_optimizations(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_optimization_iterations_optimization_id ON optimization_iterations(optimization_id);
CREATE INDEX IF NOT EXISTS idx_optimization_iterations_iteration ON optimization_iterations(optimization_id, iteration_number);

-- Comments
COMMENT ON TABLE strategy_optimizations IS 'Strategy optimization runs and results';
COMMENT ON TABLE optimization_iterations IS 'Detailed tracking of each optimization iteration';



-- >>> 20250111_active_strategies.sql
-- Active Strategies Configuration
-- Run this in your Supabase SQL editor

-- Add active_strategies field to trading_settings
ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS active_strategy_ids JSONB DEFAULT '[]'::jsonb;

-- Add comment
COMMENT ON COLUMN trading_settings.active_strategy_ids IS 'Array of strategy IDs that are active for live trading. User must manually select strategies from settings page.';

-- Update default row
UPDATE trading_settings
SET active_strategy_ids = '[]'::jsonb
WHERE id = 'default'
AND active_strategy_ids IS NULL;



-- >>> 20250112_margin_position_sizing.sql
-- Add margin-based position sizing to trading_settings table
-- This allows users to specify margin (amount to risk) and leverage, system calculates notional

ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS margin_per_position numeric(10,2) DEFAULT NULL;

-- Update position_sizing_mode constraint to include "margin"
ALTER TABLE trading_settings
DROP CONSTRAINT IF EXISTS trading_settings_position_sizing_mode_check;

ALTER TABLE trading_settings
ADD CONSTRAINT trading_settings_position_sizing_mode_check 
CHECK (position_sizing_mode IN ('auto', 'fixed', 'target_profit', 'margin'));

-- Add validation constraint for margin_per_position
ALTER TABLE trading_settings
DROP CONSTRAINT IF EXISTS margin_per_position_range;

ALTER TABLE trading_settings
ADD CONSTRAINT margin_per_position_range 
CHECK (margin_per_position IS NULL OR (margin_per_position >= 1 AND margin_per_position <= 100000));

-- Update default row if exists (set margin_per_position to NULL by default)
UPDATE trading_settings
SET margin_per_position = NULL
WHERE id = 'default'
AND margin_per_position IS NULL;

-- Add comment
COMMENT ON COLUMN trading_settings.margin_per_position IS 'Margin (amount to risk) per position in USD when position_sizing_mode is "margin". System calculates: Notional = Margin × Leverage';



-- >>> 20250113_portfolio_activities.sql
-- Create portfolio_activities table to store all money-related events
-- This includes trades, funding fees, transfers, deposits, withdrawals, etc.

CREATE TABLE IF NOT EXISTS portfolio_activities (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  type VARCHAR(50) NOT NULL CHECK (type IN (
    'trade_pnl', 
    'funding_fee', 
    'transfer', 
    'deposit', 
    'withdrawal', 
    'commission', 
    'realized_pnl', 
    'unrealized_pnl'
  )),
  amount DECIMAL(20, 8) NOT NULL,
  symbol VARCHAR(10),
  description TEXT,
  timestamp TIMESTAMPTZ NOT NULL,
  income_id VARCHAR(255), -- Unique ID from Aster API to prevent duplicates
  synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create unique constraint on income_id to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS portfolio_activities_income_id_unique 
ON portfolio_activities(income_id) 
WHERE income_id IS NOT NULL;

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_portfolio_activities_timestamp ON portfolio_activities(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_activities_type ON portfolio_activities(type);
CREATE INDEX IF NOT EXISTS idx_portfolio_activities_symbol ON portfolio_activities(symbol);

-- Enable RLS
ALTER TABLE portfolio_activities ENABLE ROW LEVEL SECURITY;

-- Policy: Allow public read access
CREATE POLICY "Allow public read access" ON portfolio_activities FOR SELECT USING (true);

-- Comment on table
COMMENT ON TABLE portfolio_activities IS 'All money-related portfolio activities including trades, fees, transfers, deposits, withdrawals';



-- >>> 20250114_portfolio_assets.sql
-- Create portfolio_assets table to store calculated portfolio asset data
-- This table will be synced from the API and used for display

CREATE TABLE IF NOT EXISTS public.portfolio_assets (
  symbol character varying(10) NOT NULL,
  name character varying(100) NOT NULL,
  logo_url text NULL,
  price numeric(20, 8) NULL DEFAULT 0,
  change_24h numeric(10, 4) NULL DEFAULT 0,
  holding_qty numeric(20, 8) NULL DEFAULT 0,
  holding_value numeric(20, 8) NULL DEFAULT 0,
  available_balance numeric(20, 8) NULL DEFAULT 0,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT portfolio_assets_pkey PRIMARY KEY (symbol)
);

-- Add total_available_balance summary field if it doesn't exist
-- We'll use a special symbol 'TOTAL' to store portfolio summary
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = 'portfolio_assets' AND column_name = 'total_available_balance'
  ) THEN
    ALTER TABLE public.portfolio_assets ADD COLUMN total_available_balance numeric(20, 8) NULL DEFAULT 0;
  END IF;
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_portfolio_assets_holding_value 
  ON public.portfolio_assets USING btree (holding_value DESC);

CREATE INDEX IF NOT EXISTS idx_portfolio_assets_updated_at 
  ON public.portfolio_assets USING btree (updated_at DESC);

-- Enable RLS
ALTER TABLE public.portfolio_assets ENABLE ROW LEVEL SECURITY;

-- Policy: Allow public read access
CREATE POLICY "Allow public read access" ON public.portfolio_assets 
  FOR SELECT USING (true);

-- Note: Service role bypasses RLS by default, so no explicit write policy needed
-- But we can add a policy for authenticated users if needed in the future

-- Comment on table
COMMENT ON TABLE public.portfolio_assets IS 'Stores calculated portfolio asset data synced from API for consistent display';


-- >>> 20250115_extended_trading_settings.sql
-- Extended Trading Settings Migration
-- Adds all .env configuration options to database for real-time management
-- Run this in your Supabase SQL editor

-- Add new columns to trading_settings table
ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS multi_exchange_mode boolean DEFAULT false,
ADD COLUMN IF NOT EXISTS assets text DEFAULT 'BTC ETH SOL',
ADD COLUMN IF NOT EXISTS interval text DEFAULT '5m',
ADD COLUMN IF NOT EXISTS strategy text DEFAULT 'auto',
ADD COLUMN IF NOT EXISTS exchange text DEFAULT 'binance',
ADD COLUMN IF NOT EXISTS alert_service_enabled boolean DEFAULT false,
ADD COLUMN IF NOT EXISTS alert_risk_per_trade numeric(10,2) DEFAULT 30.0,
ADD COLUMN IF NOT EXISTS alert_check_interval integer DEFAULT 5,
ADD COLUMN IF NOT EXISTS alert_agent_endpoint text DEFAULT 'http://localhost:5000/api/alert/signal',
ADD COLUMN IF NOT EXISTS alert_assets text DEFAULT 'ZEC,BTC,ETH,SOL,BNB',
ADD COLUMN IF NOT EXISTS alert_timeframe text DEFAULT '15m',
ADD COLUMN IF NOT EXISTS enable_trailing_stop boolean DEFAULT true,
ADD COLUMN IF NOT EXISTS trailing_stop_activation_pct numeric(5,2) DEFAULT 5.0,
ADD COLUMN IF NOT EXISTS trailing_stop_distance_pct numeric(5,2) DEFAULT 3.0,
ADD COLUMN IF NOT EXISTS max_position_hold_hours numeric(10,2) DEFAULT 48.0,
ADD COLUMN IF NOT EXISTS enable_drawdown_protection boolean DEFAULT true,
ADD COLUMN IF NOT EXISTS max_drawdown_from_peak_pct numeric(5,2) DEFAULT 5.0,
ADD COLUMN IF NOT EXISTS scalping_tp_percent numeric(5,2) DEFAULT 5.0,
ADD COLUMN IF NOT EXISTS scalping_sl_percent numeric(5,2) DEFAULT 5.0,
ADD COLUMN IF NOT EXISTS auto_strategy_cache_minutes integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS asset_leverage_overrides jsonb DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS asset_timeframes jsonb DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS llm_model text DEFAULT 'deepseek-reasoner',
ADD COLUMN IF NOT EXISTS deepseek_max_tokens integer DEFAULT 20000,
ADD COLUMN IF NOT EXISTS next_public_base_url text DEFAULT 'http://localhost:3001';

-- Update default settings with current .env values
UPDATE trading_settings
SET 
  multi_exchange_mode = false,
  assets = 'BTC ETH SOL BNB ZEC DOGE AVAX XLM XMR',
  interval = '5m',
  strategy = 'auto',
  exchange = 'binance',
  alert_service_enabled = true,
  alert_risk_per_trade = 30.0,
  alert_check_interval = 5,
  alert_agent_endpoint = 'http://localhost:5000/api/alert/signal',
  alert_assets = 'ZEC,BTC,ETH,SOL,BNB',
  alert_timeframe = '15m',
  enable_trailing_stop = true,
  trailing_stop_activation_pct = 5.0,
  trailing_stop_distance_pct = 3.0,
  max_position_hold_hours = 48.0,
  enable_drawdown_protection = true,
  max_drawdown_from_peak_pct = 5.0,
  scalping_tp_percent = 5.0,
  scalping_sl_percent = 5.0,
  auto_strategy_cache_minutes = 0,
  asset_leverage_overrides = '{"ZEC": 5, "BTC": 25, "ETH": 25, "BNB": 25, "DOGE": 25, "SOL": 25}'::jsonb,
  asset_timeframes = '{"BTC": "15m", "ETH": "15m", "SOL": "15m", "BNB": "15m", "ZEC": "5m"}'::jsonb,
  llm_model = 'deepseek-reasoner',
  deepseek_max_tokens = 20000,
  next_public_base_url = 'http://localhost:3001'
WHERE id = 'default';

-- Insert default if not exists
INSERT INTO trading_settings (
  id, 
  multi_exchange_mode, 
  assets, 
  interval, 
  strategy, 
  exchange,
  alert_service_enabled,
  alert_risk_per_trade,
  alert_check_interval,
  alert_agent_endpoint,
  alert_assets,
  alert_timeframe,
  enable_trailing_stop,
  trailing_stop_activation_pct,
  trailing_stop_distance_pct,
  max_position_hold_hours,
  enable_drawdown_protection,
  max_drawdown_from_peak_pct,
  scalping_tp_percent,
  scalping_sl_percent,
  auto_strategy_cache_minutes,
  asset_leverage_overrides,
  asset_timeframes,
  llm_model,
  deepseek_max_tokens,
  next_public_base_url
)
VALUES (
  'default',
  false,
  'BTC ETH SOL BNB ZEC DOGE AVAX XLM XMR',
  '5m',
  'auto',
  'binance',
  true,
  30.0,
  5,
  'http://localhost:5000/api/alert/signal',
  'ZEC,BTC,ETH,SOL,BNB',
  '15m',
  true,
  5.0,
  3.0,
  48.0,
  true,
  5.0,
  5.0,
  5.0,
  0,
  '{"ZEC": 5, "BTC": 25, "ETH": 25, "BNB": 25, "DOGE": 25, "SOL": 25}'::jsonb,
  '{"BTC": "15m", "ETH": "15m", "SOL": "15m", "BNB": "15m", "ZEC": "5m"}'::jsonb,
  'deepseek-reasoner',
  20000,
  'http://localhost:3001'
)
ON CONFLICT (id) DO NOTHING;

-- Add comments for documentation
COMMENT ON COLUMN trading_settings.multi_exchange_mode IS 'Enable trading on multiple exchanges simultaneously';
COMMENT ON COLUMN trading_settings.assets IS 'Space-separated list of assets to trade (e.g., "BTC ETH SOL")';
COMMENT ON COLUMN trading_settings.interval IS 'Trading interval (e.g., "5m", "1h")';
COMMENT ON COLUMN trading_settings.strategy IS 'Trading strategy: auto, scalping, llm_trend, or empty for default';
COMMENT ON COLUMN trading_settings.exchange IS 'Exchange to use: binance or aster';
COMMENT ON COLUMN trading_settings.alert_service_enabled IS 'Enable PineScript alert monitoring service';
COMMENT ON COLUMN trading_settings.alert_risk_per_trade IS 'Default risk per trade for alerts (USD)';
COMMENT ON COLUMN trading_settings.alert_check_interval IS 'Alert check interval in seconds';
COMMENT ON COLUMN trading_settings.alert_agent_endpoint IS 'Agent endpoint URL for alert signals';
COMMENT ON COLUMN trading_settings.alert_assets IS 'Comma-separated list of assets to monitor for alerts';
COMMENT ON COLUMN trading_settings.alert_timeframe IS 'Default timeframe for alert monitoring';
COMMENT ON COLUMN trading_settings.enable_trailing_stop IS 'Enable trailing stop loss';
COMMENT ON COLUMN trading_settings.trailing_stop_activation_pct IS 'Start trailing after X% profit';
COMMENT ON COLUMN trading_settings.trailing_stop_distance_pct IS 'Keep SL X% below peak profit';
COMMENT ON COLUMN trading_settings.max_position_hold_hours IS 'Maximum hours to hold a position';
COMMENT ON COLUMN trading_settings.enable_drawdown_protection IS 'Enable drawdown protection';
COMMENT ON COLUMN trading_settings.max_drawdown_from_peak_pct IS 'Maximum drawdown from peak (percentage)';
COMMENT ON COLUMN trading_settings.scalping_tp_percent IS 'Take profit percentage for scalping strategy';
COMMENT ON COLUMN trading_settings.scalping_sl_percent IS 'Stop loss percentage for scalping strategy';
COMMENT ON COLUMN trading_settings.auto_strategy_cache_minutes IS 'Cache auto strategy selection for X minutes (0 = re-evaluate every cycle)';
COMMENT ON COLUMN trading_settings.asset_leverage_overrides IS 'JSON object with per-asset leverage overrides (e.g., {"BTC": 25, "ZEC": 5})';
COMMENT ON COLUMN trading_settings.asset_timeframes IS 'JSON object with per-asset timeframe overrides (e.g., {"BTC": "15m", "ZEC": "5m"})';
COMMENT ON COLUMN trading_settings.llm_model IS 'LLM model to use (e.g., deepseek-reasoner, deepseek-chat)';
COMMENT ON COLUMN trading_settings.deepseek_max_tokens IS 'Maximum tokens for DeepSeek API';
COMMENT ON COLUMN trading_settings.next_public_base_url IS 'Base URL for the dashboard/API';


-- >>> 20250120_stop_loss_enforcement.sql
-- Stop Loss Enforcement Migration
-- Adds stop_loss_usd and take_profit_strict_enforcement fields

-- Add new columns to trading_settings table
ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS stop_loss_usd numeric(10,2) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS take_profit_strict_enforcement boolean DEFAULT false;

-- Add comments for documentation
COMMENT ON COLUMN trading_settings.stop_loss_usd IS 'Stop loss in USD (e.g., -18 means close if loss >= $18). NULL = use percentage only';
COMMENT ON COLUMN trading_settings.take_profit_strict_enforcement IS 'If true, take profit percentage must be strictly enforced. If false, use market conditions for exits';

-- Update default settings
UPDATE trading_settings
SET 
  stop_loss_usd = NULL,
  take_profit_strict_enforcement = false
WHERE id = 'default';


-- >>> 20251103_fix_schema.sql
-- Ayo Trading Dashboard - Idempotent schema alignment
-- Safe to run multiple times; guards ensure no hard failures on existing state

-- 1) Ensure uuid extension exists (harmless if already present)
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
EXCEPTION WHEN others THEN NULL; END $$;

-- 2) prices: use composite PK (symbol, timestamp); allow multiple rows per symbol over time
-- Drop conflicting constraints if present
DO $$ BEGIN
  ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_symbol_key;
  ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_pkey;
  EXCEPTION WHEN undefined_table THEN NULL;
END $$;

-- Add PK on (symbol, timestamp)
DO $$ BEGIN
  ALTER TABLE prices ADD CONSTRAINT prices_pkey PRIMARY KEY (symbol, timestamp);
EXCEPTION WHEN others THEN NULL; END $$;

-- Helpful indexes (idempotent)
CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp DESC);

-- 3) positions: upsert by id (text)
DO $$ BEGIN
  ALTER TABLE positions ALTER COLUMN id DROP DEFAULT;
EXCEPTION WHEN undefined_table THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE positions ALTER COLUMN id TYPE text USING id::text;
EXCEPTION WHEN others THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_pkey;
  ALTER TABLE positions ADD CONSTRAINT positions_pkey PRIMARY KEY (id);
EXCEPTION WHEN others THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at DESC);

-- 4) decisions: upsert by id (text)
DO $$ BEGIN
  ALTER TABLE decisions ALTER COLUMN id DROP DEFAULT;
EXCEPTION WHEN undefined_table THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE decisions ALTER COLUMN id TYPE text USING id::text;
EXCEPTION WHEN others THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_pkey;
  ALTER TABLE decisions ADD CONSTRAINT decisions_pkey PRIMARY KEY (id);
EXCEPTION WHEN others THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_asset ON decisions(asset);

-- 5) orders: upsert by order_id (text), not composite
DO $$ BEGIN
  ALTER TABLE orders ALTER COLUMN order_id TYPE text USING order_id::text;
EXCEPTION WHEN others THEN NULL; END $$;

-- Drop any old unique constraints that include symbol
DO $$ BEGIN
  ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_order_id_symbol_key;
  DROP INDEX IF EXISTS orders_order_id_symbol_key;
EXCEPTION WHEN others THEN NULL; END $$;

-- Ensure unique index on order_id alone
CREATE UNIQUE INDEX IF NOT EXISTS orders_order_id_key ON orders (order_id);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);

-- 6) trading_logs: helpful indexes
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON trading_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON trading_logs(level);

-- 7) pnl/performance auxiliary tables (optional; create if missing)
DO $$ BEGIN
  CREATE TABLE IF NOT EXISTS pnl_series (
    timestamp timestamptz PRIMARY KEY,
    daily_pnl numeric,
    cumulative_pnl numeric
  );
EXCEPTION WHEN others THEN NULL; END $$;

DO $$ BEGIN
  CREATE TABLE IF NOT EXISTS performance_series (
    date timestamptz PRIMARY KEY,
    value numeric,
    pnl numeric
  );
EXCEPTION WHEN others THEN NULL; END $$;

-- 8) account_metrics: ensure basic indexes
CREATE INDEX IF NOT EXISTS idx_account_metrics_timestamp ON account_metrics(timestamp DESC);

-- 9) RLS note: writes use service role. No policy changes required if service role is used.
-- If you wish to disable RLS in dev (optional), uncomment below:
-- ALTER TABLE prices DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE positions DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE orders DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE trades DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE account_metrics DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE trading_logs DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE decisions DISABLE ROW LEVEL SECURITY;




-- >>> 20260303_pair_hunter_exclusions.sql
-- Pair Hunter exclusions for repeatedly invalid symbols
-- Safe to run on existing deployments

ALTER TABLE pair_hunter_stats
ADD COLUMN IF NOT EXISTS data_fail_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS excluded_until TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS exclusion_reason TEXT,
ADD COLUMN IF NOT EXISTS last_data_error TEXT;


-- >>> 20260303_pair_hunter_stats.sql
-- Pair Hunter performance stats persistence
-- Stores per-asset hit rate + expectancy so hunting quality survives restarts/redeploys

CREATE TABLE IF NOT EXISTS pair_hunter_stats (
  asset VARCHAR(16) PRIMARY KEY,
  total_trades INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  win_rate NUMERIC(7, 3) NOT NULL DEFAULT 0,
  total_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
  total_pnl_percent NUMERIC(20, 8) NOT NULL DEFAULT 0,
  expectancy_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
  expectancy_percent NUMERIC(20, 8) NOT NULL DEFAULT 0,
  data_fail_count INTEGER NOT NULL DEFAULT 0,
  excluded_until TIMESTAMPTZ,
  exclusion_reason TEXT,
  last_data_error TEXT,
  last_close_reason TEXT,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pair_hunter_stats_updated_at
  ON pair_hunter_stats(updated_at DESC);

ALTER TABLE pair_hunter_stats ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'pair_hunter_stats'
      AND policyname = 'Allow public read access'
  ) THEN
    CREATE POLICY "Allow public read access"
      ON pair_hunter_stats
      FOR SELECT
      USING (true);
  END IF;
END
$$;


-- >>> 20260304_enable_stop_loss_orders.sql
-- Add explicit exchange stop-loss order toggle
-- This is separate from take_profit_strict_enforcement.

ALTER TABLE trading_settings
ADD COLUMN IF NOT EXISTS enable_stop_loss_orders boolean DEFAULT true;

COMMENT ON COLUMN trading_settings.enable_stop_loss_orders IS
'If true, place and maintain exchange-native stop-loss orders (recommended). If false, rely on bot-side stop checks only.';

UPDATE trading_settings
SET enable_stop_loss_orders = true
WHERE id = 'default' AND enable_stop_loss_orders IS NULL;


