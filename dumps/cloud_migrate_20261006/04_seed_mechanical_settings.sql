-- Fresh mechanical scalp book for cloud Supabase.
-- Skips old trade history on purpose. Upserts the single `default` settings row.
-- Adjust risk_per_trade_usd / take_profit_usd / stop_loss_usd if you want $6 instead of $2.

INSERT INTO trading_settings (
  id,
  leverage,
  take_profit_percent,
  stop_loss_percent,
  llm_model,
  tp_mode,
  position_sizing_mode,
  risk_per_trade_usd,
  take_profit_usd,
  stop_loss_usd,
  interval,
  asset_leverage_overrides,
  mech_cycle_seconds,
  mech_max_positions,
  mech_leverage,
  mech_adx_min,
  mech_atr_min_pct,
  mech_atr_max_pct,
  mech_session_start_utc,
  mech_session_end_utc,
  mech_trade_weekends,
  mech_entry_ttl_candles,
  updated_at,
  updated_by
) VALUES (
  'default',
  10,
  40,            -- % unused when tp_mode=usd (must stay within DB check limits)
  12,            -- % unused when tp_mode=usd (sl_percent_range max 50)
  'mechanical',
  'usd',
  'risk',
  2,             -- risk $ (change to 6 if that is your book)
  2,             -- target $
  -2,            -- stop $
  '5m',
  '{}'::jsonb,
  60,
  1,
  10,
  20,
  0.5,
  1.5,
  6,
  24,
  false,
  2,
  NOW(),
  'cloud_migrate_20261006'
)
ON CONFLICT (id) DO UPDATE SET
  leverage = EXCLUDED.leverage,
  take_profit_percent = EXCLUDED.take_profit_percent,
  stop_loss_percent = EXCLUDED.stop_loss_percent,
  llm_model = EXCLUDED.llm_model,
  tp_mode = EXCLUDED.tp_mode,
  position_sizing_mode = EXCLUDED.position_sizing_mode,
  risk_per_trade_usd = EXCLUDED.risk_per_trade_usd,
  take_profit_usd = EXCLUDED.take_profit_usd,
  stop_loss_usd = EXCLUDED.stop_loss_usd,
  interval = EXCLUDED.interval,
  asset_leverage_overrides = EXCLUDED.asset_leverage_overrides,
  mech_cycle_seconds = EXCLUDED.mech_cycle_seconds,
  mech_max_positions = EXCLUDED.mech_max_positions,
  mech_leverage = EXCLUDED.mech_leverage,
  mech_adx_min = EXCLUDED.mech_adx_min,
  mech_atr_min_pct = EXCLUDED.mech_atr_min_pct,
  mech_atr_max_pct = EXCLUDED.mech_atr_max_pct,
  mech_session_start_utc = EXCLUDED.mech_session_start_utc,
  mech_session_end_utc = EXCLUDED.mech_session_end_utc,
  mech_trade_weekends = EXCLUDED.mech_trade_weekends,
  mech_entry_ttl_candles = EXCLUDED.mech_entry_ttl_candles,
  updated_at = NOW(),
  updated_by = EXCLUDED.updated_by;

-- Confirm
SELECT id, llm_model, tp_mode, position_sizing_mode,
       risk_per_trade_usd, take_profit_usd, stop_loss_usd,
       leverage, interval, mech_leverage, mech_cycle_seconds
FROM trading_settings
WHERE id = 'default';
