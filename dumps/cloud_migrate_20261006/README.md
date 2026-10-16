# Apply to https://euibalqhfilbwsqpxtri.supabase.co

## Blocker for me (agent)

Cursor MCP is **not** linked to `euibalqhfilbwsqpxtri` yet (still bound to other projects). Browser needs your Supabase login. I **cannot** run DDL on this project until you either:

1. **Paste** the project **Database password** (Settings → Database) **or** `postgresql://…` URI here, **or**
2. Open **SQL Editor** and run `APPLY_ALL_euibalqhfilbwsqpxtri.sql` yourself, **or**
3. Reconnect MCP “Supabase Trading Agent” and pick **this** project specifically.

## Fastest path (you, ~2 min)

1. Open: https://supabase.com/dashboard/project/euibalqhfilbwsqpxtri/sql/new  
2. Paste contents of `APPLY_ALL_euibalqhfilbwsqpxtri.sql` → **Run**  
3. Confirm result shows `llm_model = mechanical` from the seed `SELECT`

That file creates **all tables**, seeds **mechanical settings** (no old trades/logs), and installs **daily prune** for free-plan limits.

## Free-plan guards included

| Table | Retention |
|-------|-----------|
| trading_logs | 3 days |
| decisions | 7 days |
| account_metrics / historical_ohlcv | 14 / 7 days |
| portfolio_activities / wallet history | 30 days |
| trades / orders | 60 days |
| trading_settings | **kept forever** |

Also: do **not** turn on heavy Realtime subscriptions for logs; keep dashboard polling moderate; agent should prefer local `diary.jsonl` / `trading_agent.log` with rotation rather than dumping every log line to Supabase.

## Coolify env after SQL succeeds

```bash
SUPABASE_URL=https://euibalqhfilbwsqpxtri.supabase.co
SUPABASE_KEY=<anon key from Project Settings → API>
SUPABASE_SERVICE_KEY=<service_role key>
```

Dashboard:

```bash
NEXT_PUBLIC_SUPABASE_URL=https://euibalqhfilbwsqpxtri.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=<anon key>
SUPABASE_SERVICE_ROLE_KEY=<service_role key>
```

Then redeploy agent + dashboard, stop self-hosted Supabase on the VPS.

## Seed defaults (edit before run if needed)

`04_seed_mechanical_settings.sql` sets risk/target **$2**. Change to **6** in that file / APPLY_ALL if you want the $6 book from your UI.
