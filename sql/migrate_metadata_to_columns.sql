-- One-time migration: copy data from metadata JSONB to typed columns.
-- Run this in the Supabase SQL Editor BEFORE deploying the updated pipeline code.
-- Leaves metadata copies in place (no destructive changes).

-- ─── Pipeline tracking ──────────────────────────────────────────────────────
UPDATE companies
SET pipeline_status = metadata->'pipeline'->>'status'
WHERE pipeline_status IS NULL
  AND metadata->'pipeline'->>'status' IS NOT NULL;

UPDATE companies
SET filing_format = metadata->>'filing_format'
WHERE filing_format IS NULL
  AND metadata->>'filing_format' IS NOT NULL;

UPDATE companies
SET last_accounts_date = metadata->>'last_accounts_date'
WHERE last_accounts_date IS NULL
  AND metadata->>'last_accounts_date' IS NOT NULL;

-- ─── CSV-seeded fields ──────────────────────────────────────────────────────
UPDATE companies
SET sic_code_1 = metadata->>'sic_code_1'
WHERE sic_code_1 IS NULL
  AND metadata->>'sic_code_1' IS NOT NULL;

UPDATE companies
SET sic_code_2 = metadata->>'sic_code_2'
WHERE sic_code_2 IS NULL
  AND metadata->>'sic_code_2' IS NOT NULL;

UPDATE companies
SET company_status = metadata->>'company_status'
WHERE company_status IS NULL
  AND metadata->>'company_status' IS NOT NULL;

UPDATE companies
SET company_category = metadata->>'company_category'
WHERE company_category IS NULL
  AND metadata->>'company_category' IS NOT NULL;

UPDATE companies
SET accounts_category = metadata->>'accounts_category'
WHERE accounts_category IS NULL
  AND metadata->>'accounts_category' IS NOT NULL;

UPDATE companies
SET incorporation_date = metadata->>'incorporation_date'
WHERE incorporation_date IS NULL
  AND metadata->>'incorporation_date' IS NOT NULL;

-- ─── Verification ───────────────────────────────────────────────────────────
-- Run this after migration to confirm counts match:
--
-- SELECT
--   COUNT(*) FILTER (WHERE pipeline_status IS NOT NULL) AS pipeline_status_filled,
--   COUNT(*) FILTER (WHERE metadata->'pipeline'->>'status' IS NOT NULL) AS pipeline_status_in_meta,
--   COUNT(*) FILTER (WHERE filing_format IS NOT NULL) AS filing_format_filled,
--   COUNT(*) FILTER (WHERE metadata->>'filing_format' IS NOT NULL) AS filing_format_in_meta,
--   COUNT(*) FILTER (WHERE sic_code_1 IS NOT NULL) AS sic_code_1_filled,
--   COUNT(*) FILTER (WHERE metadata->>'sic_code_1' IS NOT NULL) AS sic_code_1_in_meta
-- FROM companies;
