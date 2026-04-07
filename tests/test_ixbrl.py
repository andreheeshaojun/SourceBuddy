"""Dry-run test: fetch 5 iXBRL-eligible companies, parse 5 filings each, print results.
Shows full JSON for the first company, summary for the rest.
Does NOT write to Supabase.
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from pipeline import (
    _check_env, _init_ch_session, log,
    get_accounts_filings, get_document_metadata,
    determine_filing_format, parse_ixbrl_multi,
    calculate_derived_metrics,
    SUPABASE_URL, SUPABASE_KEY,
)
from supabase import create_client


def print_summary(name, cn, last_date, extracted):
    """Print a compact summary of the extracted data."""
    print(f"\n{'='*60}")
    print(f"Company: {name} ({cn})")
    print(f"Latest filing date: {last_date}")
    print(f"{'='*60}")
    for key, val in extracted.items():
        if key in ("income_statement", "balance_sheet", "cash_flow_statement"):
            years = sorted(val.keys()) if val else []
            non_null = sum(1 for yr in val.values() for v in yr.values() if v is not None)
            print(f"  {key}: {len(years)} years {years}, {non_null} non-null values")
        elif key == "derivation_log":
            print(f"  derivation_log: {list(val.keys())}")
        elif isinstance(val, dict):
            print(f"  {key}: {json.dumps(val)}")
        elif isinstance(val, float):
            print(f"  {key}: {val:,.2f}")
        else:
            print(f"  {key}: {val}")


def main():
    _check_env()
    _init_ch_session()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    log.info("Finding iXBRL-eligible companies...")
    result = (
        supabase.table("companies")
        .select("company_number, company_name, accounts_category, pipeline_status")
        .in_("accounts_category", ["MEDIUM", "LARGE"])
        .eq("pipeline_status", "pending")
        .limit(200)
        .execute()
    )

    candidates = result.data

    ixbrl_count = 0
    for company in candidates:
        if ixbrl_count >= 5:
            break

        cn = company["company_number"]
        name = company["company_name"]
        log.info("--- %s (%s) ---", cn, name)

        filings = get_accounts_filings(cn, count=5)
        if not filings:
            log.info("  No filings, skipping")
            continue

        metadata = get_document_metadata(filings[0])
        if not metadata:
            log.info("  No metadata, skipping")
            continue
        fmt = determine_filing_format(metadata, filings[0])
        if fmt != "ixbrl":
            log.info("  Latest format is %s, skipping", fmt)
            continue

        log.info("  Found %d filings, parsing...", len(filings))
        extracted, filing_format, last_date, ixbrl_sections = parse_ixbrl_multi(cn, filings)
        if extracted is None:
            log.info("  Parser returned None")
            continue

        extracted = calculate_derived_metrics(extracted)
        ixbrl_count += 1

        if ixbrl_count == 1:
            # Full JSON dump for the first company
            print(f"\n{'='*60}")
            print(f"FULL JSON OUTPUT: {name} ({cn})")
            print(f"Latest filing date: {last_date}")
            print(f"{'='*60}")
            print(json.dumps(extracted, indent=2, default=str))
        else:
            print_summary(name, cn, last_date, extracted)

    if ixbrl_count == 0:
        log.warning("No iXBRL companies found.")
    else:
        log.info("\nTested %d iXBRL companies (dry run).", ixbrl_count)


if __name__ == "__main__":
    main()
