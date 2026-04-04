-- RPC function: deep-merge a JSONB patch into companies.metadata
-- Run this in the Supabase SQL Editor before using the pipeline.
--
-- Usage from Python:
--   supabase.rpc("update_company_metadata", {
--       "p_company_number": "12345678",
--       "p_patch": {"pipeline": {"status": "failed"}}
--   }).execute()
--
-- Merge behaviour:
--   - Top-level keys in the patch overwrite the same key in metadata.
--   - If BOTH sides of a key are JSON objects, they are shallow-merged
--     (so {"pipeline": {"status": "failed"}} merges into an existing
--      pipeline object without wiping other pipeline keys).
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
