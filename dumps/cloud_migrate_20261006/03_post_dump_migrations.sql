-- Applied AFTER RESTORE_ALL.sql + 02_live_extras.sql
-- Schema deltas since the last cloud dump (mechanical scalp era).

-- >>> 20260920_adaptive_risk_settings.sql
-- Volatility-adaptive exits, risk-based sizing, scale-out ladder, and circuit breakers.
--
-- Background: the mechanical stop compared against margin ROI, so a "3%" stop at 10x
-- leverage was a 0.3% price move and fired on noise. These columns let the stop, the
-- target, the position size, and the entry filters all be tuned from the dashboard
-- instead of requiring an agent redeploy.

ALTER TABLE trading_settings
  -- Volatility-adaptive exits
  ADD COLUMN IF NOT EXISTS exit_mode text DEFAULT 'atr',
  ADD COLUMN IF NOT EXISTS sl_atr_mult numeric DEFAULT 2.5,
  ADD COLUMN IF NOT EXISTS tp_rr_ratio numeric DEFAULT 2.0,
  ADD COLUMN IF NOT EXISTS atr_period integer DEFAULT 14,
  ADD COLUMN IF NOT EXISTS min_stop_price_pct numeric DEFAULT 0.8,
  ADD COLUMN IF NOT EXISTS max_stop_price_pct numeric DEFAULT 5.0,

  -- Risk-based position sizing
  ADD COLUMN IF NOT EXISTS risk_per_trade_usd numeric,
  ADD COLUMN IF NOT EXISTS risk_per_trade_pct numeric DEFAULT 0.5,
  ADD COLUMN IF NOT EXISTS max_notional_per_position numeric,
  ADD COLUMN IF NOT EXISTS min_notional_per_position numeric DEFAULT 100,

  -- Partial take-profit ladder, "R:percent_of_size" pairs
  ADD COLUMN IF NOT EXISTS enable_profit_ladder boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS profit_ladder text DEFAULT '1.0:50,2.0:30',
  ADD COLUMN IF NOT EXISTS breakeven_after_first_fill boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS enable_breakeven_stop boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS breakeven_trigger_r numeric DEFAULT 1.0,
  ADD COLUMN IF NOT EXISTS trailing_stop_activation_r numeric DEFAULT 1.5,
  ADD COLUMN IF NOT EXISTS trailing_stop_distance_r numeric DEFAULT 1.0,

  -- Re-entry control (anti-chop)
  ADD COLUMN IF NOT EXISTS reentry_cooldown_minutes numeric DEFAULT 45,
  ADD COLUMN IF NOT EXISTS loss_reentry_cooldown_minutes numeric DEFAULT 90,
  ADD COLUMN IF NOT EXISTS block_direction_flip boolean DEFAULT true,

  -- Account-level circuit breakers
  ADD COLUMN IF NOT EXISTS max_daily_loss_usd numeric,
  ADD COLUMN IF NOT EXISTS max_daily_loss_pct numeric DEFAULT 3.0,
  ADD COLUMN IF NOT EXISTS max_consecutive_losses integer DEFAULT 4,
  ADD COLUMN IF NOT EXISTS loss_streak_pause_minutes numeric DEFAULT 120,

  -- Higher-timeframe trend agreement
  ADD COLUMN IF NOT EXISTS enable_htf_trend_filter boolean DEFAULT true,
  ADD COLUMN IF NOT EXISTS htf_trend_timeframe text DEFAULT '1h',
  ADD COLUMN IF NOT EXISTS htf_trend_fast_ema integer DEFAULT 20,
  ADD COLUMN IF NOT EXISTS htf_trend_slow_ema integer DEFAULT 50,
  ADD COLUMN IF NOT EXISTS htf_trend_min_separation_pct numeric DEFAULT 0.15,
  ADD COLUMN IF NOT EXISTS htf_block_when_flat boolean DEFAULT true,

  -- Analysis timeframes (previously hardcoded in the agent)
  ADD COLUMN IF NOT EXISTS intraday_timeframe text DEFAULT '15m',
  ADD COLUMN IF NOT EXISTS longterm_timeframe text DEFAULT '4h',

  -- Pair hunter tuning
  ADD COLUMN IF NOT EXISTS pair_hunter_min_volatility numeric DEFAULT 1.0,
  ADD COLUMN IF NOT EXISTS pair_hunter_ideal_volatility numeric DEFAULT 2.0,
  ADD COLUMN IF NOT EXISTS pair_hunter_max_volatility numeric DEFAULT 3.5,
  ADD COLUMN IF NOT EXISTS pair_hunter_min_volume_24h numeric DEFAULT 50000000,
  ADD COLUMN IF NOT EXISTS pair_hunter_min_trend_strength numeric DEFAULT 25,
  ADD COLUMN IF NOT EXISTS pair_hunter_min_price numeric DEFAULT 0.01,
  ADD COLUMN IF NOT EXISTS pair_hunter_max_spread_pct numeric DEFAULT 0.5,
  ADD COLUMN IF NOT EXISTS pair_hunter_timeframe text DEFAULT '15m',
  ADD COLUMN IF NOT EXISTS pair_hunter_blacklist text DEFAULT 'SHIB PEPE FLOKI BONK WIF MEME DOGE 1000SATS LUNA FTT',
  ADD COLUMN IF NOT EXISTS pair_hunter_weight_volatility numeric DEFAULT 0.25,
  ADD COLUMN IF NOT EXISTS pair_hunter_weight_trend numeric DEFAULT 0.40,
  ADD COLUMN IF NOT EXISTS pair_hunter_weight_setup numeric DEFAULT 0.35;

COMMENT ON COLUMN trading_settings.exit_mode IS
  'atr = stop derived from sl_atr_mult x ATR% of price; fixed = legacy take_profit_percent/stop_loss_percent as margin ROI.';
COMMENT ON COLUMN trading_settings.profit_ladder IS
  'Scale-out plan as comma separated R:size_pct pairs, e.g. "1.0:50,2.0:30". Remainder rides the trailing stop.';
COMMENT ON COLUMN trading_settings.risk_per_trade_usd IS
  'Fixed USD risked per trade. Takes precedence over risk_per_trade_pct. Notional is solved from the stop distance.';
COMMENT ON COLUMN trading_settings.min_notional_per_position IS
  'Floor so a very tight stop cannot size an order below the exchange minimum.';
COMMENT ON COLUMN trading_settings.max_consecutive_losses IS
  'Pause new entries after this many consecutive losing closes. 0 disables the breaker.';
COMMENT ON COLUMN trading_settings.htf_trend_min_separation_pct IS
  'Minimum EMA separation on the confirmation timeframe. Below this the market is treated as a range.';

-- The original column was created with an inline CHECK that may be named differently from
-- trading_settings_position_sizing_mode_check. Drop every check that mentions the column,
-- then recreate the named one with 'risk' included.
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE rel.relname = 'trading_settings'
      AND nsp.nspname = 'public'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%position_sizing_mode%'
  LOOP
    EXECUTE format('ALTER TABLE trading_settings DROP CONSTRAINT IF EXISTS %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE trading_settings
DROP CONSTRAINT IF EXISTS trading_settings_position_sizing_mode_check;

ALTER TABLE trading_settings
ADD CONSTRAINT trading_settings_position_sizing_mode_check
CHECK (position_sizing_mode IN ('auto', 'fixed', 'target_profit', 'margin', 'risk'));

-- Move the existing row onto the new profile. Legacy TP/SL stay as the fixed-mode fallback,
-- expressed as margin ROI, and are only consulted when ATR data is unavailable.
UPDATE trading_settings
SET
  exit_mode = COALESCE(exit_mode, 'atr'),
  position_sizing_mode = 'risk',
  take_profit_percent = 20,
  stop_loss_percent = 12,
  leverage = 10,
  max_positions = 3,
  margin_per_position = COALESCE(margin_per_position, 30),
  interval = '15m',
  exchange = 'okx',
  updated_at = now()
WHERE id = 'default';

-- >>> 20260920_fix_position_sizing_mode_check.sql
-- Paste this alone into Supabase SQL Editor first.
-- Your DB still has the old position_sizing_mode check (without 'risk'),
-- possibly under a different constraint name than we were dropping.

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE rel.relname = 'trading_settings'
      AND nsp.nspname = 'public'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%position_sizing_mode%'
  LOOP
    EXECUTE format('ALTER TABLE trading_settings DROP CONSTRAINT IF EXISTS %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE trading_settings
ADD CONSTRAINT trading_settings_position_sizing_mode_check
CHECK (position_sizing_mode IN ('auto', 'fixed', 'target_profit', 'margin', 'risk'));

UPDATE trading_settings
SET
  exit_mode = COALESCE(exit_mode, 'atr'),
  position_sizing_mode = 'risk',
  take_profit_percent = 20,
  stop_loss_percent = 12,
  leverage = 10,
  max_positions = 3,
  margin_per_position = COALESCE(margin_per_position, 30),
  interval = '15m',
  exchange = 'okx',
  updated_at = now()
WHERE id = 'default';

-- Confirm
SELECT position_sizing_mode, exit_mode, sl_atr_mult, tp_rr_ratio, interval, exchange
FROM trading_settings
WHERE id = 'default';

-- >>> 20260924_tp_mode_independent.sql
-- Independent take-profit mode (stop stays ATR/fixed via exit_mode).
ALTER TABLE trading_settings
  ADD COLUMN IF NOT EXISTS tp_mode TEXT NOT NULL DEFAULT 'roi_percent',
  ADD COLUMN IF NOT EXISTS take_profit_usd NUMERIC NULL;

COMMENT ON COLUMN trading_settings.tp_mode IS
  'Take-profit mode independent of stop: roi_percent | usd | atr_rr';
COMMENT ON COLUMN trading_settings.take_profit_usd IS
  'Absolute USD profit lock when tp_mode=usd (e.g. 5 = close at +$5)';

-- Keep existing rows on the scalping-friendly default.
UPDATE trading_settings
SET tp_mode = COALESCE(NULLIF(tp_mode, ''), 'roi_percent')
WHERE tp_mode IS NULL OR tp_mode = '';

-- >>> 20260927_max_loss_usd_default.sql
-- Default max loss per trade to $6 (risk sizing + hard USD stop ceiling).
-- Safe to re-run. Only fills blanks / updates the default row profile.

UPDATE trading_settings
SET
  position_sizing_mode = COALESCE(NULLIF(position_sizing_mode, ''), 'risk'),
  risk_per_trade_usd = COALESCE(risk_per_trade_usd, 6),
  risk_per_trade_pct = COALESCE(risk_per_trade_pct, 0.5),
  stop_loss_usd = COALESCE(stop_loss_usd, -6),
  min_notional_per_position = COALESCE(min_notional_per_position, 50),
  updated_at = now()
WHERE id = 'default';

COMMENT ON COLUMN trading_settings.risk_per_trade_usd IS
  'Max USD loss if ATR stop is hit. Position size shrinks when stop is wider. Editable in Trading Settings UI.';

COMMENT ON COLUMN trading_settings.stop_loss_usd IS
  'Hard USD stop ceiling (negative). Closes if unrealized PnL hits this even before ATR stop. Prefer matching -risk_per_trade_usd.';

-- >>> 20260928_align_tp_2r.sql
-- Coolify: fix lose-more-than-we-make by aligning TP to ~2R vs $6 hard stop.
-- $30 margin + -$6 stop + $12 take-profit (USD mode).

UPDATE trading_settings
SET
  position_sizing_mode = 'margin',
  margin_per_position = 30,
  risk_per_trade_usd = 6,
  stop_loss_usd = -6,
  tp_mode = 'usd',
  take_profit_usd = 12,
  take_profit_percent = 40,
  exit_mode = COALESCE(exit_mode, 'atr'),
  updated_at = now()
WHERE id = 'default';

SELECT id, position_sizing_mode, margin_per_position, stop_loss_usd,
       tp_mode, take_profit_usd, take_profit_percent
FROM trading_settings
WHERE id = 'default';

-- >>> 20260928_choppy_atr_risk.sql
-- Coolify: choppy-market profile — wide ATR stops + risk sizing (not a 2% yank).
-- Example: 7% ATR stop with $6 risk → ~$86 notional / ~$8–9 margin at 10x (not $30).

UPDATE trading_settings
SET
  position_sizing_mode = 'risk',
  risk_per_trade_usd = 6,
  stop_loss_usd = -9,
  exit_mode = 'atr',
  tp_mode = 'atr_rr',
  tp_rr_ratio = 2.0,
  sl_atr_mult = 2.0,
  min_stop_price_pct = 1.5,
  max_stop_price_pct = 7.0,
  take_profit_usd = 12,
  margin_per_position = COALESCE(margin_per_position, 30),
  updated_at = now()
WHERE id = 'default';

SELECT id, position_sizing_mode, risk_per_trade_usd, stop_loss_usd,
       exit_mode, tp_mode, min_stop_price_pct, max_stop_price_pct
FROM trading_settings
WHERE id = 'default';

-- >>> 20260928_force_max_loss_6.sql
-- Coolify-hosted Supabase: force $6 max-loss profile on the default settings row.
-- Run in Coolify Supabase SQL editor (not cloud Supabase).

UPDATE trading_settings
SET
  position_sizing_mode = 'risk',
  risk_per_trade_usd = 6,
  risk_per_trade_pct = COALESCE(risk_per_trade_pct, 0.5),
  stop_loss_usd = -6,
  min_notional_per_position = LEAST(COALESCE(min_notional_per_position, 50), 50),
  updated_at = now()
WHERE id = 'default';

SELECT id, position_sizing_mode, risk_per_trade_usd, stop_loss_usd, min_notional_per_position
FROM trading_settings
WHERE id = 'default';

-- >>> 20260928_margin_30_max_loss_6.sql
-- Coolify Supabase: fixed $30 margin + $6 hard max loss (not risk-sized shrink).
-- Run in Coolify SQL editor, then redeploy the agent.

UPDATE trading_settings
SET
  position_sizing_mode = 'margin',
  margin_per_position = 30,
  risk_per_trade_usd = 6,
  stop_loss_usd = -6,
  updated_at = now()
WHERE id = 'default';

SELECT id, position_sizing_mode, margin_per_position, risk_per_trade_usd, stop_loss_usd
FROM trading_settings
WHERE id = 'default';

-- >>> 20261001_hard_max_loss.sql
-- Hard-cap dollar stop to risk_per_trade_usd (remove the old -9 / 1.5× buffer).
-- Also leave take-profit in price_percent if already set.

UPDATE trading_settings
SET
  stop_loss_usd = -ABS(COALESCE(NULLIF(risk_per_trade_usd, 0), 6)),
  risk_per_trade_usd = COALESCE(NULLIF(risk_per_trade_usd, 0), 6),
  updated_at = NOW()
WHERE id = 'default';

SELECT id, risk_per_trade_usd, stop_loss_usd, stop_loss_percent, tp_mode, take_profit_percent
FROM trading_settings
WHERE id = 'default';

-- >>> 20261001_tp_price_percent.sql
-- Respect UI take_profit_percent as % of entry price (not ATR R:R, not margin ROI).
-- Previous choppy/align migrations set tp_mode=atr_rr which placed TPs ~2× ATR stop
-- (often 10–14% away) so a user 5%/7% setting never filled.
-- Note: trading_settings.id is TEXT ('default'), not integer 1.

ALTER TABLE trading_settings
  ADD COLUMN IF NOT EXISTS tp_mode TEXT NOT NULL DEFAULT 'price_percent';

UPDATE trading_settings
SET
  tp_mode = 'price_percent',
  take_profit_percent = CASE
    WHEN take_profit_percent IS NULL OR take_profit_percent <= 0 THEN 7
    WHEN take_profit_percent > 20 THEN 7  -- was likely ROI% (e.g. 40); reset to a sane price %
    ELSE take_profit_percent
  END,
  updated_at = NOW()
WHERE id = 'default';

COMMENT ON COLUMN trading_settings.tp_mode IS
  'Take-profit mode independent of stop: price_percent | roi_percent | usd | atr_rr. price_percent = TAKE_PROFIT_PERCENT is % of entry price.';

SELECT id, tp_mode, take_profit_percent, exit_mode, risk_per_trade_usd, max_stop_price_pct
FROM trading_settings
WHERE id = 'default';

-- >>> 20261006_mechanical_scalp.sql
-- Mechanical scalp book (AI parked). The switch itself is llm_model = 'mechanical' (column
-- already exists); these are the knobs the helper reads. Defaults match
-- src/utils/mechanical_scalp.py DEFAULTS, so a NULL column means "use the code default".
-- Nothing here changes the live row: the dashboard toggle writes the values when the
-- user chooses the mechanical engine.

ALTER TABLE trading_settings
  ADD COLUMN IF NOT EXISTS mech_cycle_seconds INTEGER,
  ADD COLUMN IF NOT EXISTS mech_max_positions INTEGER,
  ADD COLUMN IF NOT EXISTS mech_leverage INTEGER,
  ADD COLUMN IF NOT EXISTS mech_adx_min NUMERIC(6, 2),
  ADD COLUMN IF NOT EXISTS mech_atr_min_pct NUMERIC(6, 3),
  ADD COLUMN IF NOT EXISTS mech_atr_max_pct NUMERIC(6, 3),
  ADD COLUMN IF NOT EXISTS mech_rvol_full NUMERIC(6, 2),
  ADD COLUMN IF NOT EXISTS mech_rvol_half NUMERIC(6, 2),
  ADD COLUMN IF NOT EXISTS mech_funding_max NUMERIC(10, 6),
  ADD COLUMN IF NOT EXISTS mech_session_start_utc INTEGER,
  ADD COLUMN IF NOT EXISTS mech_session_end_utc INTEGER,
  ADD COLUMN IF NOT EXISTS mech_trade_weekends BOOLEAN,
  ADD COLUMN IF NOT EXISTS mech_entry_ttl_candles INTEGER,
  ADD COLUMN IF NOT EXISTS mech_min_stop_pct NUMERIC(6, 3),
  ADD COLUMN IF NOT EXISTS mech_max_stop_pct NUMERIC(6, 3),
  ADD COLUMN IF NOT EXISTS mech_win_cooldown_minutes NUMERIC(8, 2),
  ADD COLUMN IF NOT EXISTS mech_loss_cooldown_minutes NUMERIC(8, 2);

COMMENT ON COLUMN trading_settings.mech_cycle_seconds IS 'Mechanical book: seconds between decision cycles (default 60).';
COMMENT ON COLUMN trading_settings.mech_leverage IS 'Mechanical book: leverage, isolated margin (default 10).';
COMMENT ON COLUMN trading_settings.mech_adx_min IS 'Mechanical book: 15m ADX(14) floor for a tradable regime (default 20).';
COMMENT ON COLUMN trading_settings.mech_atr_min_pct IS 'Mechanical book: 15m ATR%% floor; quieter pairs are skipped (default 0.5).';
COMMENT ON COLUMN trading_settings.mech_atr_max_pct IS 'Mechanical book: 15m ATR%% cap; hotter pairs spike through the $ stop (default 1.5).';
COMMENT ON COLUMN trading_settings.mech_session_start_utc IS 'Mechanical book: first UTC hour new entries are allowed (default 6).';
COMMENT ON COLUMN trading_settings.mech_session_end_utc IS 'Mechanical book: UTC hour after which no new entries (default 24).';
COMMENT ON COLUMN trading_settings.mech_entry_ttl_candles IS 'Mechanical book: cancel the resting post-only entry after this many 5m candles (default 2).';

SELECT id, llm_model, risk_per_trade_usd, tp_mode, take_profit_usd, stop_loss_usd, leverage
FROM trading_settings
WHERE id = 'default';
