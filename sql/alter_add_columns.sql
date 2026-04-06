-- Add typed columns for pipeline tracking and company status.
-- Run in Supabase SQL Editor or via psycopg2.

ALTER TABLE companies ADD COLUMN IF NOT EXISTS pipeline_status TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS filing_format TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_accounts_date DATE;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_status TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS accounts_category TEXT;
