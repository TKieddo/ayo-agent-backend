-- Live extras captured from cloud project rwcamygafesbuwvwbqrz

CREATE OR REPLACE FUNCTION public.get_balance_change_percent(p_network character varying DEFAULT 'mainnet'::character varying, p_hours_ago integer DEFAULT 24)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
  current_balance DECIMAL(20, 8);
  past_balance DECIMAL(20, 8);
BEGIN
  SELECT account_value INTO current_balance
  FROM wallet_balance_history
  WHERE network = p_network
  ORDER BY timestamp DESC
  LIMIT 1;

  SELECT account_value INTO past_balance
  FROM wallet_balance_history
  WHERE network = p_network
    AND timestamp <= NOW() - (p_hours_ago || ' hours')::INTERVAL
  ORDER BY timestamp DESC
  LIMIT 1;

  IF past_balance IS NULL OR past_balance = 0 THEN
    RETURN 0;
  END IF;

  RETURN ((current_balance - past_balance) / past_balance) * 100;
END;
$function$;

CREATE OR REPLACE FUNCTION public.refresh_wins_losses_stats()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'wins_losses_stats') THEN
    REFRESH MATERIALIZED VIEW CONCURRENTLY wins_losses_stats;
  END IF;
END;
$function$;

-- Materialized view (if missing)
CREATE MATERIALIZED VIEW IF NOT EXISTS public.wins_losses_stats AS
 WITH trade_stats AS (
         SELECT count(*) FILTER (WHERE trades.pnl >= 0::numeric) AS win_count,
            count(*) FILTER (WHERE trades.pnl < 0::numeric) AS loss_count,
            count(*) AS total_count,
            COALESCE(sum(trades.pnl) FILTER (WHERE trades.pnl >= 0::numeric), 0::numeric) AS wins_total_pnl,
            COALESCE(sum(trades.pnl) FILTER (WHERE trades.pnl < 0::numeric), 0::numeric) AS losses_total_pnl,
            COALESCE(avg(trades.pnl) FILTER (WHERE trades.pnl >= 0::numeric), 0::numeric) AS wins_avg_pnl,
            COALESCE(avg(trades.pnl) FILTER (WHERE trades.pnl < 0::numeric), 0::numeric) AS losses_avg_pnl,
            max(trades.executed_at) AS last_trade_at
           FROM trades
          WHERE trades.pnl IS NOT NULL
        )
 SELECT win_count,
    loss_count,
    total_count,
        CASE
            WHEN total_count > 0 THEN round(win_count::numeric / total_count::numeric * 100::numeric, 2)
            ELSE 0::numeric
        END AS win_pct,
        CASE
            WHEN total_count > 0 THEN round(loss_count::numeric / total_count::numeric * 100::numeric, 2)
            ELSE 0::numeric
        END AS loss_pct,
    wins_total_pnl,
    losses_total_pnl,
    wins_avg_pnl,
    losses_avg_pnl,
    last_trade_at,
    now() AS calculated_at
   FROM trade_stats;

CREATE UNIQUE INDEX IF NOT EXISTS wins_losses_stats_unique ON public.wins_losses_stats USING btree (calculated_at);


CREATE OR REPLACE FUNCTION public.upsert_order_metrics_summary(p_timestamp timestamp with time zone, p_total_orders integer, p_open_orders integer, p_filled_orders integer, p_canceled_orders integer, p_rejected_orders integer, p_total_fees numeric, p_total_pnl numeric, p_net_profit numeric, p_avg_fee_per_trade numeric, p_fee_to_pnl_ratio numeric, p_profit_margin numeric)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  p_open_pct DECIMAL(5, 2);
  p_filled_pct DECIMAL(5, 2);
  p_canceled_pct DECIMAL(5, 2);
  p_rejected_pct DECIMAL(5, 2);
BEGIN
  p_open_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_open_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_filled_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_filled_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_canceled_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_canceled_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;
  p_rejected_pct := CASE WHEN p_total_orders > 0 THEN ROUND((p_rejected_orders::numeric / p_total_orders::numeric * 100)::numeric, 2) ELSE 0 END;

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
$function$;

