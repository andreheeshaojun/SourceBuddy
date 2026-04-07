"""Iterative test batch runner.
Fetches 5 pending companies per batch, processes them, logs detailed results.
"""
import logging
import os
import sys
import time
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Merge Data"))

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

# ── Configuration ────────────────────────────────────────────────────────────
BATCH_SIZE = 5

# The same 20 companies from the previous 4 batches, in order.
ALL_20 = [
    # Batch 1
    "06922118", "03688878", "08965471", "10958084", "02609110",
    # Batch 2
    "02318517", "01030234", "00444134", "03075817", "OC421575",
    # Batch 3
    "08892086", "06930633", "04437266", "SC315316", "OC401440",
    # Batch 4
    "04366797", "05070981", "04551594", "06718612", "06759649",
]

# Determine which batch we're on by checking pipeline_status
already = supabase.table("companies").select("company_number").neq("pipeline_status", "pending").in_("company_number", ALL_20).execute()
done_set = {r["company_number"] for r in already.data}
remaining = [cn for cn in ALL_20 if cn not in done_set]
batch_cns = remaining[:BATCH_SIZE]
batch_num = (len(done_set) // BATCH_SIZE) + 1
log.info("Batch %d: processing %d companies (%d already done)", batch_num, len(batch_cns), len(done_set))

# ── Query ────────────────────────────────────────────────────────────────────
result = (
    supabase.table("companies")
    .select("company_number, company_name, accounts_category")
    .in_("company_number", batch_cns)
    .execute()
)
companies = result.data
log.info("Got %d companies to process.", len(companies))

counts = {"extracted_ixbrl": 0, "extracted_pdf": 0, "no_filing": 0, "failed": 0}
batch_details: list[dict] = []

# ── Process ──────────────────────────────────────────────────────────────────
for i, company in enumerate(companies, 1):
    cn = company["company_number"]
    name = company.get("company_name", "Unknown")
    cat = company.get("accounts_category")
    log.info("[%d/%d] Processing %s (%s) [%s]", i, len(companies), cn, name, cat)
    t_start = time.monotonic()
    detail = {"company_number": cn, "company_name": name, "category": cat}

    try:
        filings = get_accounts_filings(cn, count=5)
        if not filings:
            log.info("  No accounts filing found — marking no_filing")
            update_company(supabase, cn, {"pipeline_status": "no_filing"})
            counts["no_filing"] += 1
            detail["status"] = "no_filing"
            batch_details.append(detail)
            continue

        first_metadata = get_document_metadata(filings[0])
        if first_metadata is None:
            log.warning("  No document metadata link — marking failed")
            update_company(supabase, cn, {"pipeline_status": "failed"})
            counts["failed"] += 1
            detail["status"] = "failed"
            detail["reason"] = "no_metadata"
            batch_details.append(detail)
            continue

        fmt = determine_filing_format(first_metadata, filings[0])
        log.info("  Filing format: %s", fmt)
        detail["format"] = fmt

        pdf_sections = None
        ixbrl_sections = None
        if fmt == "ixbrl":
            extracted, filing_format, last_date, ixbrl_sections = parse_ixbrl_multi(cn, filings)
            if extracted is None:
                log.warning("  iXBRL parsing returned no data — marking failed")
                update_company(supabase, cn, {
                    "pipeline_status": "failed",
                    "filing_format": fmt,
                })
                counts["failed"] += 1
                detail["status"] = "failed"
                detail["reason"] = "ixbrl_no_data"
                batch_details.append(detail)
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
                detail["status"] = "failed"
                detail["reason"] = "pdf_no_data"
                batch_details.append(detail)
                continue
            count_key = "extracted_pdf"

        extracted = calculate_derived_metrics(extracted)
        columns, meta_patch = _build_write_payload(extracted, filing_format, last_date)

        # Part B qualitative sections → company_profile JSONB
        sections_output = pdf_sections or ixbrl_sections
        if sections_output is not None:
            columns["company_profile"] = sections_output

        update_company(supabase, cn, columns)
        if meta_patch:
            update_company_metadata_blob(supabase, cn, meta_patch)

        # Capture key metrics for analysis
        detail["status"] = "extracted"
        detail["revenue"] = columns.get("revenue")
        detail["ebitda"] = columns.get("ebitda")
        detail["ebitda_margin"] = columns.get("ebitda_margin")
        detail["fcf"] = columns.get("fcf")
        detail["cash_conversion"] = columns.get("cash_conversion")
        detail["employees"] = columns.get("employees")
        detail["ebitda_method"] = extracted.get("derivation_log", {}).get("ebitda_method")

        # Flag issues
        issues = []
        if detail["revenue"] is None:
            issues.append("no_revenue")
        if detail["ebitda"] is None and detail["revenue"] is not None:
            issues.append("no_ebitda_with_revenue")
        if detail["ebitda"] is not None and detail["ebitda_margin"] is None:
            issues.append("no_margin_with_ebitda")
        if detail.get("ebitda_method"):
            issues.append("ebitda_approx")
        if detail["cash_conversion"] == 0 and detail["fcf"] is not None and detail["fcf"] < 0:
            issues.append("cash_conv_zero_neg_fcf")
        if detail["ebitda"] is not None and detail["ebitda"] < 0 and detail["cash_conversion"] is not None:
            issues.append("cash_conv_with_neg_ebitda")

        # Capture statement completeness for diagnostics
        inc_years = list((extracted.get("income_statement") or {}).keys())
        bs_years = list((extracted.get("balance_sheet") or {}).keys())
        cf_years = list((extracted.get("cash_flow_statement") or {}).keys())
        detail["stmt_years"] = {"inc": inc_years, "bs": bs_years, "cf": cf_years}

        # Check for operating_profit presence
        if inc_years:
            latest_yr = sorted(inc_years)[-1]
            yr_row = (extracted.get("income_statement") or {}).get(latest_yr, {})
            detail["has_operating_profit"] = yr_row.get("operating_profit") is not None
            detail["has_depreciation"] = yr_row.get("depreciation") is not None
        detail["issues"] = issues

        log.info("  Extracted — revenue=%s ebitda=%s issues=%s",
                 detail["revenue"], detail["ebitda"], issues or "none")
        counts[count_key] += 1

    except requests.exceptions.HTTPError as e:
        log.error("  HTTP error for %s: %s", cn, e)
        update_company(supabase, cn, {"pipeline_status": "failed"})
        counts["failed"] += 1
        detail["status"] = "failed"
        detail["reason"] = f"http_{e.response.status_code}" if hasattr(e, 'response') and e.response else str(e)
    except Exception as e:
        log.error("  Unexpected error for %s: %s", cn, e, exc_info=True)
        update_company(supabase, cn, {"pipeline_status": "failed"})
        counts["failed"] += 1
        detail["status"] = "failed"
        detail["reason"] = str(e)[:200]

    elapsed = time.monotonic() - t_start
    detail["elapsed_s"] = round(elapsed, 1)
    log.info("  [timing] company %s took %.1fs", cn, elapsed)
    batch_details.append(detail)

# ── Summary ──────────────────────────────────────────────────────────────────
log.info("--- Batch summary ---")
for status, count in counts.items():
    if count:
        log.info("  %-16s %d", status, count)
log.info("---------------------")

print("\n" + "=" * 80)
print("BATCH RESULTS")
print("=" * 80)
for d in batch_details:
    fmt_str = d.get("format", "n/a")
    status = d.get("status", "unknown")
    rev = d.get("revenue")
    ebitda = d.get("ebitda")
    margin = d.get("ebitda_margin")
    fcf = d.get("fcf")
    cc = d.get("cash_conversion")
    emp = d.get("employees")
    issues = d.get("issues", [])
    reason = d.get("reason", "")

    rev_str = f"£{rev:,.0f}" if rev else "None"
    ebitda_str = f"£{ebitda:,.0f}" if ebitda else "None"
    margin_str = f"{margin:.1%}" if margin else "None"

    print(f"\n  {d['company_number']} ({d['company_name']}) [{d.get('category')}]")
    print(f"    format={fmt_str}  status={status}  time={d.get('elapsed_s', '?')}s")
    if status == "extracted":
        print(f"    revenue={rev_str}  ebitda={ebitda_str}  margin={margin_str}")
        print(f"    fcf={fcf}  cash_conv={cc}  employees={emp}")
        has_op = d.get("has_operating_profit", "?")
        has_dep = d.get("has_depreciation", "?")
        print(f"    operating_profit={has_op}  depreciation={has_dep}  ebitda_method={d.get('ebitda_method', 'standard')}")
        if issues:
            print(f"    *** ISSUES: {', '.join(issues)}")
    elif reason:
        print(f"    reason: {reason}")

print("\n" + "=" * 80)
total = len(batch_details)
extracted = sum(1 for d in batch_details if d.get("status") == "extracted")
with_revenue = sum(1 for d in batch_details if d.get("revenue") is not None)
with_ebitda = sum(1 for d in batch_details if d.get("ebitda") is not None)
with_issues = sum(1 for d in batch_details if d.get("issues"))
print(f"Total: {total}  Extracted: {extracted}  Revenue: {with_revenue}  EBITDA: {with_ebitda}  Issues: {with_issues}")
print("=" * 80)
