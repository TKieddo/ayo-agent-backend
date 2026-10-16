-- Free-plan hygiene for AYO trading DB
-- Short retention so 500MB DB + 5GB egress stay healthy.
-- Safe to re-run. Never touches trading_settings values.

CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA extensions;

CREATE OR REPLACE FUNCTION public.prune_trading_history()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  n_logs int := 0;
  n_decisions int := 0;
  n_metrics int := 0;
  n_ohlcv int := 0;
  n_activities int := 0;
  n_wallet int := 0;
  n_trades int := 0;
  n_orders int := 0;
BEGIN
  BEGIN
    DELETE FROM trading_logs WHERE timestamp < NOW() - INTERVAL '3 days';
    GET DIAGNOSTICS n_logs = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_logs := 0; END;

  BEGIN
    DELETE FROM decisions WHERE timestamp < NOW() - INTERVAL '7 days';
    GET DIAGNOSTICS n_decisions = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_decisions := 0; END;

  BEGIN
    DELETE FROM account_metrics WHERE timestamp < NOW() - INTERVAL '14 days';
    GET DIAGNOSTICS n_metrics = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_metrics := 0; END;

  BEGIN
    DELETE FROM historical_ohlcv WHERE timestamp < NOW() - INTERVAL '7 days';
    GET DIAGNOSTICS n_ohlcv = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_ohlcv := 0; END;

  BEGIN
    DELETE FROM portfolio_activities WHERE created_at < NOW() - INTERVAL '30 days';
    GET DIAGNOSTICS n_activities = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_activities := 0; END;

  BEGIN
    DELETE FROM wallet_balance_history WHERE recorded_at < NOW() - INTERVAL '30 days';
    GET DIAGNOSTICS n_wallet = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN
    BEGIN
      DELETE FROM wallet_balance_history WHERE created_at < NOW() - INTERVAL '30 days';
      GET DIAGNOSTICS n_wallet = ROW_COUNT;
    EXCEPTION WHEN undefined_table OR undefined_column THEN n_wallet := 0; END;
  END;

  BEGIN
    DELETE FROM trades WHERE executed_at < NOW() - INTERVAL '60 days';
    GET DIAGNOSTICS n_trades = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_trades := 0; END;

  BEGIN
    DELETE FROM orders WHERE created_at < NOW() - INTERVAL '60 days';
    GET DIAGNOSTICS n_orders = ROW_COUNT;
  EXCEPTION WHEN undefined_table OR undefined_column THEN n_orders := 0; END;

  RETURN jsonb_build_object(
    'trading_logs', n_logs,
    'decisions', n_decisions,
    'account_metrics', n_metrics,
    'historical_ohlcv', n_ohlcv,
    'portfolio_activities', n_activities,
    'wallet_balance_history', n_wallet,
    'trades', n_trades,
    'orders', n_orders,
    'ran_at', NOW()
  );
END;
$$;

COMMENT ON FUNCTION public.prune_trading_history() IS
  'Free-plan retention: logs 3d, decisions 7d, metrics/ohlcv 14/7d, activities/wallet 30d, trades/orders 60d.';

SELECT public.prune_trading_history();

DO $cronsetup$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'prune_trading_history_daily') THEN
    PERFORM cron.unschedule((SELECT jobid FROM cron.job WHERE jobname = 'prune_trading_history_daily' LIMIT 1));
  END IF;
  PERFORM cron.schedule(
    'prune_trading_history_daily',
    '17 3 * * *',
    $cmd$SELECT public.prune_trading_history();$cmd$
  );
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_cron schedule skipped: % — call prune_trading_history() from Coolify cron if needed', SQLERRM;
END $cronsetup$;

CREATE INDEX IF NOT EXISTS idx_trading_logs_timestamp ON trading_logs (timestamp);
CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions (timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at ON trades (executed_at);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
