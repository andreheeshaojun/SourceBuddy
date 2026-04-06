"""Run the pipeline on 5 pending MEDIUM/LARGE companies.
Filters by accounts_category to verify A2 works on well-tagged filings.
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

import requests
from pipeline import (
    _check_env, _init_ch_session, log,
    get_accounts_filings, get_document_metadata, determine_filing_format,
    parse_ixbrl_multi, parse_pdf_multi, calculate_derived_metrics,
    _build_write_payload, update_company, update_company_metadata_blob,
    SUPABASE_URL, SUPABASE_KEY,
)
from supabase import create_client

log.setLevel(logging.INFO)

_check_env()
_init_ch_session()
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
log.info("Connected to Supabase. Fetching MEDIUM/LARGE pending companies...")

# ── Query for MEDIUM/LARGE pending companies ─────────────────────────────────
result = (
    supabase.table("companies")
    .select("company_number, company_name, accounts_category")
    .eq("pipeline_status", "pending")
    .in_("accounts_category", ["MEDIUM", "LARGE"])
    .limit(5)
    .execute()
)
companies = result.data
log.info("Got %d MEDIUM/LARGE companies to process.", len(companies))

counts = {"extracted_ixbrl": 0, "extracted_pdf": 0, "no_filing": 0, "failed": 0}

# ── Process (mirrors process_batch logic) ────────────────────────────────────
for i, company in enumerate(companies, 1):
    cn = company["company_number"]
    name = company.get("company_name", "Unknown")
    cat = company.get("accounts_category")
    log.info("[%d/%d] Processing %s (%s) [%s]", i, len(companies), cn, name, cat)
    t_start = time.monotonic()

    try:
        filings = get_accounts_filings(cn, count=1)
        if not filings:
            log.info("  No accounts filing found — marking no_filing")
            update_company(supabase, cn, {"pipeline_status": "no_filing"})
            counts["no_filing"] += 1
            continue

        first_metadata = get_document_metadata(filings[0])
        if first_metadata is None:
            log.warning("  No document metadata link — marking failed")
            update_company(supabase, cn, {"pipeline_status": "failed"})
            counts["failed"] += 1
            continue

        fmt = determine_filing_format(first_metadata, filings[0])
        log.info("  Filing format: %s", fmt)

        pdf_sections = None
        if fmt == "ixbrl":
            extracted, filing_format, last_date, ixbrl_sections = parse_ixbrl_multi(cn, filings)
            if extracted is None:
                log.warning("  iXBRL parsing returned no data — marking failed")
                update_company(supabase, cn, {
                    "pipeline_status": "failed",
                    "filing_format": fmt,
                })
                counts["failed"] += 1
                continue
            count_key = "extracted_ixbrl"
        else:
            extracted, filing_format, last_date, pdf_sections = parse_pdf_multi(cn, filings)
            if extracted is None:
                log.warning("  PDF parsing returned no data — marking failed")
                update_company(supabase, cn, {
                    "pipeline_status": "failed",
                    "filing_format": fmt,
                    "last_accounts_date": filings[0].get("date"),
                })
                counts["failed"] += 1
                continue
            count_key = "extracted_pdf"

        extracted = calculate_derived_metrics(extracted)
        columns, meta_patch = _build_write_payload(extracted, filing_format, last_date)
        if pdf_sections is not None:
            meta_patch["pdf_sections"] = pdf_sections
        update_company(supabase, cn, columns)
        if meta_patch:
            update_company_metadata_blob(supabase, cn, meta_patch)

        log.info("  Extracted and saved — revenue=%s ebitda=%s",
                 columns.get("revenue"), columns.get("ebitda"))
        counts[count_key] += 1

    except requests.exceptions.HTTPError as e:
        log.error("  HTTP error for %s: %s", cn, e)
        update_company(supabase, cn, {"pipeline_status": "failed"})
        counts["failed"] += 1
    except Exception as e:
        log.error("  Unexpected error for %s: %s", cn, e, exc_info=True)
        update_company(supabase, cn, {"pipeline_status": "failed"})
        counts["failed"] += 1
    finally:
        elapsed = time.monotonic() - t_start
        log.info("  [timing] company %s took %.1fs", cn, elapsed)

log.info("--- Batch summary ---")
for status, count in counts.items():
    if count:
        log.info("  %-16s %d", status, count)
log.info("---------------------")

# ── Verification: read back the processed rows ───────────────────────────────
processed_cns = [c["company_number"] for c in companies]
log.info("\n=== Post-batch verification ===")
result = (
    supabase.table("companies")
    .select(
        "company_number, company_name, accounts_category, pipeline_status, "
        "filing_format, last_accounts_date, revenue, ebitda, ebitda_margin, "
        "fcf, cash_conversion, employees, revenue_cagr_5y"
    )
    .in_("company_number", processed_cns)
    .execute()
)

for row in result.data:
    print(f"\n  {row['company_number']} ({row['company_name']}) [{row['accounts_category']}]")
    print(f"    pipeline_status:    {row['pipeline_status']}")
    print(f"    filing_format:      {row['filing_format']}")
    print(f"    last_accounts_date: {row['last_accounts_date']}")
    print(f"    revenue:            {row['revenue']}")
    print(f"    ebitda:             {row['ebitda']}")
    print(f"    ebitda_margin:      {row['ebitda_margin']}")
    print(f"    fcf:                {row['fcf']}")
    print(f"    cash_conversion:    {row['cash_conversion']}")
    print(f"    employees:          {row['employees']}")
    print(f"    revenue_cagr_5y:    {row['revenue_cagr_5y']}")
