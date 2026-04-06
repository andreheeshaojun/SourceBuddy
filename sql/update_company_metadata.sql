-- RPC function: deep-merge a JSONB patch into companies.metadata
-- Run this in the Supabase SQL Editor before using the pipeline.
--
-- NOTE: This function is ONLY used for writing to the metadata JSONB column
-- (derived ratios, cross-period metrics, validation_warnings, CSV overflow
-- fields). All typed columns (revenue, pipeline_status, etc.) are written
-- via direct UPDATE statements — see update_company() in pipeline.py.
--
-- Usage from Python (via update_company_metadata_blob):
--   supabase.rpc("update_company_metadata", {
--       "p_company_number": "12345678",
--       "p_patch": {"gross_margin_pct": 0.42, "current_ratio": 1.8}
--   }).execute()
--
-- Merge behaviour:
--   - Top-level keys in the patch overwrite the same key in metadata.
--   - If BOTH sides of a key are JSON objects, they are shallow-merged
--     (so nested objects merge without wiping sibling keys).
--   - All other types (strings, numbers, arrays, nulls) are overwritten.

CREATE OR REPLACE FUNCTION update_company_metadata(
    p_company_number text,
    p_patch          jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    current_meta jsonb;
    key          text;
BEGIN
    SELECT metadata INTO current_meta
    FROM   companies
    WHERE  company_number = p_company_number;

    IF current_meta IS NULL THEN
        current_meta := '{}'::jsonb;
    END IF;

    FOR key IN SELECT jsonb_object_keys(p_patch)
    LOOP
        IF  jsonb_typeof(current_meta -> key) = 'object'
        AND jsonb_typeof(p_patch -> key)      = 'object'
        THEN
            -- merge sub-objects one level deep
            current_meta := jsonb_set(
                current_meta,
                ARRAY[key],
                (current_meta -> key) || (p_patch -> key)
            );
        ELSE
            current_meta := jsonb_set(
                current_meta,
                ARRAY[key],
                p_patch -> key
            );
        END IF;
    END LOOP;

    UPDATE companies
    SET    metadata = current_meta
    WHERE  company_number = p_company_number;
END;
$$;
