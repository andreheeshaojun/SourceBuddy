"""
Comprehensive iXBRL parser test.

Picks 10 pending/Active companies from companies, triages their filings,
parses iXBRL-only filings with full provenance tracking, validates output
against the DB schema, and writes results to a Test_Parser_Results table.

NO LLM calls anywhere.  Pure parser only.
"""

import json
import os
import sys
import time
import logging
import re

# ---------------------------------------------------------------------------
# Allow imports from Merge Data/
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "Merge Data"))

from pipeline import (
    _check_env, _init_ch_session, ch_get, log,
    get_accounts_filings, get_document_metadata, determine_filing_format,
    download_document, parse_ixbrl, parse_ixbrl_multi, calculate_derived_metrics,
    IXBRL_TAG_MAP, INCOME_STATEMENT_FIELDS, BALANCE_SHEET_FIELDS, CASH_FLOW_FIELDS,
    _parse_ixbrl_value, _resolve_contexts, _extract_year,
    SUPABASE_URL, SUPABASE_KEY, CH_BASE_URL, REQUEST_DELAY,
)
from bs4 import BeautifulSoup
from supabase import create_client

# ---------------------------------------------------------------------------
# Logging — verbose for this test
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
log = logging.getLogger("test_parser")
log.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# The canonical schema keys that companies is expected to have.
# These are the columns that the pipeline populates (beyond the CSV-seeded ones).
# ---------------------------------------------------------------------------
companies_SCHEMA_KEYS = {
    # CSV-seeded columns
    "company_number", "company_name", "sic_code_1", "sic_code_2",
    "registered_address", "company_category", "incorporation_date",
    "accounts_category", "company_status",
    # Pipeline-populated columns
    "revenue", "ebitda", "ebitda_margin", "fcf", "cash_conversion",
    "employees", "revenue_history", "ebitda_history", "fcf_history",
    "employees_history", "revenue_cagr", "ebitda_cagr",
    "accounts_type", "filing_format", "last_accounts_date",
    "income_statement", "balance_sheet", "cash_flow_statement",
    "derivation_log", "pipeline_status",
    # Derived metrics (from financial_computations)
    "gross_margin_pct", "operating_margin_pct", "net_margin_pct",
    "return_on_assets", "return_on_equity",
    "total_capex", "capex_to_revenue",
    "net_debt", "net_debt_to_ebitda", "current_ratio", "quick_ratio",
    "debt_to_equity", "interest_cover", "asset_turnover",
    "revenue_per_employee", "profit_per_employee",
    # Cross-period metrics
    "revenue_yoy_growth", "ebitda_yoy_growth", "profit_yoy_growth",
    "revenue_cagr_3yr", "revenue_cagr_5yr",
    # Validation
    "validation_warnings",
}

# Reverse map: canonical field -> list of XBRL tag names that map to it
REVERSE_TAG_MAP = {}
for tag_name, field_name in IXBRL_TAG_MAP.items():
    REVERSE_TAG_MAP.setdefault(field_name, []).append(tag_name)


# ============================================================================
# Enhanced iXBRL parsing with provenance tracking
# ============================================================================

def parse_ixbrl_with_provenance(content, company_number):
    """Parse iXBRL and return (standard_result, provenance).

    provenance is a list of dicts, one per extracted value:
      {field, xbrl_tag, year, raw_text, scale, sign, value_before_scale_sign, value_after}
    """
    soup = BeautifulSoup(content, "html.parser")
    contexts, years_seen = _resolve_contexts(soup)

    if not contexts:
        return None, []

    provenance = []
    field_values = {}

    for tag in soup.find_all("ix:nonfraction"):
        name_attr = tag.get("name", "")
        local_name = name_attr.split(":")[-1] if ":" in name_attr else name_attr

        field = IXBRL_TAG_MAP.get(local_name)
        if not field:
            continue

        ctx_id = tag.get("contextref", "")
        ctx = contexts.get(ctx_id)
        if not ctx or not ctx["year"]:
            continue
        if ctx.get("dimensional"):
            continue

        # ---- raw extraction with provenance ----
        raw_text = tag.text.strip()
        raw_clean = raw_text.replace(",", "").replace(" ", "")
        if not raw_clean or raw_clean == "-":
            continue
        try:
            base_value = float(raw_clean)
        except ValueError:
            continue

        scale = int(tag.get("scale", "0"))
        sign = tag.get("sign", "")

        value_before = base_value  # before scale/sign
        value_after = base_value
        if scale:
            value_after *= 10 ** scale
        if sign == "-":
            value_after = -value_after

        year = ctx["year"]

        # Only keep first occurrence per field-year (same rule as pipeline)
        if field not in field_values:
            field_values[field] = {}
        if year not in field_values[field]:
            field_values[field][year] = value_after
            provenance.append({
                "field": field,
                "xbrl_tag": local_name,
                "full_tag": name_attr,
                "year": year,
                "raw_text": raw_text,
                "scale": scale,
                "sign": sign or "(none)",
                "value_before_scale_sign": value_before,
                "value_after": value_after,
            })

    if not field_values:
        return None, []

    # Now call the normal parser to get the full structured result
    result = parse_ixbrl(content, company_number)
    return result, provenance


# ============================================================================
# Step 0 — Connect & pick 10 companies
# ============================================================================

def get_test_companies(supabase, limit=10):
    """Get up to `limit` companies from companies where pipeline_status=pending and company_status=Active."""
    result = (
        supabase.table("companies")
        .select("company_number, company_name")
        .eq("metadata->pipeline->>status", "pending")
        .eq("metadata->>company_status", "Active")
        .limit(limit)
        .execute()
    )
    return result.data


# ============================================================================
# Step 1 — Filing triage
# ============================================================================

def triage_filings(companies, count=3):
    """For each company, fetch the N most recent accounts filings and determine format.

    Returns list of dicts:
      {company_number, company_name, filings: [{date, format, metadata, filing}]}
    """
    results = []
    for company in companies:
        cn = company["company_number"]
        name = company["company_name"]
        log.info("Triaging %s (%s)...", cn, name)

        filings_raw = get_accounts_filings(cn, count=count)
        triage_items = []

        if not filings_raw:
            log.info("  No accounts filings found")
            results.append({
                "company_number": cn,
                "company_name": name,
                "filings": [],
                "triage_result": "no_filing",
            })
            continue

        for f in filings_raw:
            metadata = get_document_metadata(f)
            if not metadata:
                triage_items.append({
                    "date": f.get("date", "?"),
                    "format": "no_metadata",
                    "metadata": None,
                    "filing": f,
                })
                continue

            fmt = determine_filing_format(metadata, f)
            triage_items.append({
                "date": f.get("date", "?"),
                "format": fmt,
                "metadata": metadata,
                "filing": f,
            })

        # Overall triage result = format of the latest filing
        overall = triage_items[0]["format"] if triage_items else "no_filing"
        results.append({
            "company_number": cn,
            "company_name": name,
            "filings": triage_items,
            "triage_result": overall,
        })

    return results


def print_triage_summary(triage_results):
    """Print a summary table of triage results."""
    print("\n" + "=" * 80)
    print("STEP 1 — FILING TRIAGE SUMMARY")
    print("=" * 80)

    # Count formats across all filings
    format_counts = {}
    for tr in triage_results:
        for f in tr["filings"]:
            fmt = f["format"]
            format_counts[fmt] = format_counts.get(fmt, 0) + 1

    # Per-company table
    print(f"\n{'Company':<40} {'Number':<12} {'#Filings':<10} {'Formats'}")
    print("-" * 80)
    for tr in triage_results:
        formats = [f["format"] for f in tr["filings"]]
        fmt_str = ", ".join(formats) if formats else "no_filing"
        print(f"{tr['company_name'][:39]:<40} {tr['company_number']:<12} {len(tr['filings']):<10} {fmt_str}")

    # Overall breakdown
    print(f"\n{'Format':<25} {'Count'}")
    print("-" * 35)
    for fmt, count in sorted(format_counts.items()):
        print(f"{fmt:<25} {count}")
    no_filing = sum(1 for tr in triage_results if not tr["filings"])
    if no_filing:
        print(f"{'no_filing':<25} {no_filing}")
    print()


# ============================================================================
# Step 2 — Parse iXBRL only
# ============================================================================

def parse_ixbrl_companies(triage_results):
    """For companies with at least one iXBRL filing, download and parse.

    Returns:
      parsed_companies: list of {company_number, company_name, result, provenance_all, filing_format, last_date}
      skipped: list of {company_number, company_name, reason}
    """
    parsed_companies = []
    skipped = []

    for tr in triage_results:
        cn = tr["company_number"]
        name = tr["company_name"]

        ixbrl_filings = [f for f in tr["filings"] if f["format"] == "ixbrl"]

        if not ixbrl_filings:
            reason = "no_filing" if not tr["filings"] else f"all filings are {tr['triage_result']}"
            skipped.append({"company_number": cn, "company_name": name, "reason": reason})
            log.info("Skipping %s (%s): %s", cn, name, reason)
            continue

        log.info("Parsing %s (%s) — %d iXBRL filing(s)...", cn, name, len(ixbrl_filings))

        # Parse all iXBRL filings with provenance
        all_provenance = []
        first_result = None
        last_date = None
        filing_format = "ixbrl"

        for item in ixbrl_filings:
            try:
                content, _ = download_document(item["metadata"], "ixbrl")
            except Exception as e:
                log.warning("  Download failed for filing %s: %s", item["date"], e)
                continue

            log.info("  Filing %s: %d bytes", item["date"], len(content))
            result, provenance = parse_ixbrl_with_provenance(content, cn)

            if result is not None:
                if first_result is None:
                    first_result = result
                    last_date = item["date"]
                all_provenance.extend(provenance)

        if first_result is None:
            skipped.append({"company_number": cn, "company_name": name, "reason": "parser returned None for all filings"})
            continue

        # Also run the multi-filing parser for the merged result (used for DB write)
        # Re-use the raw filing objects from triage
        raw_filings = [item["filing"] for item in ixbrl_filings]
        merged_result, _, merged_last_date = parse_ixbrl_multi(cn, raw_filings)

        if merged_result is None:
            skipped.append({"company_number": cn, "company_name": name, "reason": "multi-parse merge returned None"})
            continue

        merged_result = calculate_derived_metrics(merged_result)

        parsed_companies.append({
            "company_number": cn,
            "company_name": name,
            "result": merged_result,
            "provenance": all_provenance,
            "filing_format": filing_format,
            "last_date": merged_last_date or last_date,
        })

    # Print skip log
    if skipped:
        print("\n" + "=" * 80)
        print("STEP 2 — SKIPPED COMPANIES")
        print("=" * 80)
        for s in skipped:
            print(f"  {s['company_name'][:40]:<42} {s['company_number']:<12} Reason: {s['reason']}")
    print()

    return parsed_companies, skipped


# ============================================================================
# Step 3 — Validate extracted fields (detailed report)
# ============================================================================

def print_field_report(parsed_companies):
    """For each parsed company, print every field with XBRL tag, raw/scaled values."""
    print("\n" + "=" * 80)
    print("STEP 3 — DETAILED FIELD REPORTS")
    print("=" * 80)

    for pc in parsed_companies:
        cn = pc["company_number"]
        name = pc["company_name"]
        result = pc["result"]
        provenance = pc["provenance"]

        print(f"\n{'#' * 80}")
        print(f"# {name} ({cn})")
        print(f"# Last filing: {pc['last_date']}")
        print(f"{'#' * 80}")

        # Build provenance lookup: (field, year) -> provenance entry
        prov_lookup = {}
        for p in provenance:
            key = (p["field"], p["year"])
            if key not in prov_lookup:
                prov_lookup[key] = p

        # --- Income Statement ---
        _print_statement_detail("INCOME STATEMENT", result.get("income_statement", {}),
                                INCOME_STATEMENT_FIELDS, prov_lookup)

        # --- Balance Sheet ---
        _print_statement_detail("BALANCE SHEET", result.get("balance_sheet", {}),
                                BALANCE_SHEET_FIELDS, prov_lookup)

        # --- Cash Flow Statement ---
        _print_statement_detail("CASH FLOW STATEMENT", result.get("cash_flow_statement", {}),
                                CASH_FLOW_FIELDS, prov_lookup)

        # --- Null fields ---
        all_null_fields = _find_always_null_fields(result)
        if all_null_fields:
            print(f"\n  Fields that came back NULL across ALL years:")
            for f in sorted(all_null_fields):
                print(f"    - {f}")

        # --- Revenue & EBITDA history ---
        print(f"\n  Revenue history:  {json.dumps(result.get('revenue_history', {}))}")
        print(f"  EBITDA history:   {json.dumps(result.get('ebitda_history', {}))}")
        print(f"  FCF history:      {json.dumps(result.get('fcf_history', {}))}")
        print(f"  Employees hist:   {json.dumps(result.get('employees_history', {}))}")

        # --- Derived metrics ---
        print(f"\n  DERIVED METRICS:")
        _print_derived_metric("EBITDA", result.get("ebitda"), result.get("derivation_log", {}).get("ebitda"))
        _print_derived_metric("EBITDA margin", result.get("ebitda_margin"),
                              f"ebitda ({result.get('ebitda')}) / revenue ({result.get('revenue')})" if result.get("ebitda_margin") else None)
        _print_derived_metric("FCF", result.get("fcf"), result.get("derivation_log", {}).get("fcf"))
        _print_derived_metric("Cash conversion", result.get("cash_conversion"),
                              f"fcf ({result.get('fcf')}) / ebitda ({result.get('ebitda')})" if result.get("cash_conversion") else None)
        _print_derived_metric("Revenue CAGR", result.get("revenue_cagr"),
                              f"CAGR over revenue_history: {json.dumps(result.get('revenue_history', {}))}" if result.get("revenue_cagr") else None)
        _print_derived_metric("EBITDA CAGR", result.get("ebitda_cagr"),
                              f"CAGR over ebitda_history: {json.dumps(result.get('ebitda_history', {}))}" if result.get("ebitda_cagr") else None)

        # --- Full derivation log ---
        print(f"\n  FULL DERIVATION LOG:")
        deriv = result.get("derivation_log", {})
        if deriv:
            for key, val in deriv.items():
                print(f"    {key}: {val}")
        else:
            print("    (empty)")


def _print_statement_detail(title, statement, field_list, prov_lookup):
    """Print detail for one financial statement."""
    print(f"\n  --- {title} ---")
    years = sorted(statement.keys()) if statement else []
    if not years:
        print("    (no data)")
        return

    print(f"  Years: {', '.join(years)}")
    print(f"  {'Field':<35} {'Year':<6} {'XBRL Tag':<50} {'Raw Text':<18} {'Scale':<6} {'Sign':<8} {'Before':<18} {'After'}")
    print(f"  {'-'*35} {'-'*6} {'-'*50} {'-'*18} {'-'*6} {'-'*8} {'-'*18} {'-'*18}")

    for field in field_list:
        for yr_str in years:
            val = statement[yr_str].get(field)
            yr_int = int(yr_str)
            prov = prov_lookup.get((field, yr_int))

            if val is not None and prov:
                print(f"  {field:<35} {yr_str:<6} {prov['xbrl_tag']:<50} {prov['raw_text']:<18} "
                      f"{prov['scale']:<6} {prov['sign']:<8} {prov['value_before_scale_sign']:<18,.2f} {prov['value_after']:>18,.2f}")
            elif val is not None:
                # Value exists but no provenance (came from merge of another filing)
                print(f"  {field:<35} {yr_str:<6} {'(from earlier filing)':<50} {'':<18} "
                      f"{'':<6} {'':<8} {'':<18} {val:>18,.2f}")
            else:
                print(f"  {field:<35} {yr_str:<6} {'—':<50} {'null'}")


def _find_always_null_fields(result):
    """Return set of fields that are null in every year across all 3 statements."""
    always_null = set()

    for stmt_name, field_list in [
        ("income_statement", INCOME_STATEMENT_FIELDS),
        ("balance_sheet", BALANCE_SHEET_FIELDS),
        ("cash_flow_statement", CASH_FLOW_FIELDS),
    ]:
        stmt = result.get(stmt_name, {})
        for field in field_list:
            all_null = True
            for yr_str, row in stmt.items():
                if row.get(field) is not None:
                    all_null = False
                    break
            if all_null:
                always_null.add(field)

    return always_null


def _print_derived_metric(label, value, formula):
    """Print one derived metric with its formula."""
    if value is not None:
        if isinstance(value, float) and abs(value) >= 1:
            print(f"    {label}: {value:,.2f}")
        else:
            print(f"    {label}: {value}")
        if formula:
            print(f"      Formula: {formula}")
    else:
        print(f"    {label}: null (inputs missing)")


# ============================================================================
# Step 4 — Structure check against schema
# ============================================================================

def check_schema(parsed_companies):
    """Validate that parsed output matches companies schema keys."""
    print("\n" + "=" * 80)
    print("STEP 4 — SCHEMA STRUCTURE CHECK")
    print("=" * 80)

    # The keys we expect in the pipeline output (the ones that get written to the DB)
    expected_pipeline_keys = {
        "revenue", "ebitda", "ebitda_margin", "fcf", "cash_conversion",
        "employees", "revenue_history", "ebitda_history", "fcf_history",
        "employees_history", "revenue_cagr", "ebitda_cagr",
        "filing_format", "last_accounts_date",
        "income_statement", "balance_sheet", "cash_flow_statement",
        "derivation_log", "pipeline_status",
        # Derived metrics (from financial_computations)
        "gross_margin_pct", "operating_margin_pct", "net_margin_pct",
        "return_on_assets", "return_on_equity",
        "total_capex", "capex_to_revenue",
        "net_debt", "net_debt_to_ebitda", "current_ratio", "quick_ratio",
        "debt_to_equity", "interest_cover", "asset_turnover",
        "revenue_per_employee", "profit_per_employee",
        # Cross-period metrics
        "revenue_yoy_growth", "ebitda_yoy_growth", "profit_yoy_growth",
        "revenue_cagr_3yr", "revenue_cagr_5yr",
        # Validation
        "validation_warnings",
    }

    for pc in parsed_companies:
        cn = pc["company_number"]
        name = pc["company_name"]
        result = pc["result"]

        output_keys = set(result.keys())

        # Keys in output but not in schema
        extra = output_keys - expected_pipeline_keys
        # Keys in schema but not in output
        missing = expected_pipeline_keys - output_keys

        print(f"\n  {name} ({cn}):")
        if extra:
            print(f"    EXTRA keys (in output, not in schema): {sorted(extra)}")
        else:
            print(f"    No extra keys.")
        if missing:
            print(f"    MISSING keys (in schema, not in output): {sorted(missing)}")
        else:
            print(f"    No missing keys.")

        if not extra and not missing:
            print(f"    PASS — output matches schema exactly.")


# ============================================================================
# Step 5 — Write to Test_Parser_Results table
# ============================================================================

def create_test_table(supabase):
    """Create Test_Parser_Results table via Supabase RPC (SQL).

    Falls back gracefully if the table already exists.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS "Test_Parser_Results" (
        company_number TEXT PRIMARY KEY,
        company_name TEXT,
        sic_code_1 TEXT,
        sic_code_2 TEXT,
        registered_address TEXT,
        company_category TEXT,
        incorporation_date TEXT,
        accounts_category TEXT,
        company_status TEXT,
        revenue DOUBLE PRECISION,
        ebitda DOUBLE PRECISION,
        ebitda_margin DOUBLE PRECISION,
        fcf DOUBLE PRECISION,
        cash_conversion DOUBLE PRECISION,
        employees INTEGER,
        revenue_history JSONB,
        ebitda_history JSONB,
        fcf_history JSONB,
        employees_history JSONB,
        revenue_cagr DOUBLE PRECISION,
        ebitda_cagr DOUBLE PRECISION,
        accounts_type TEXT,
        filing_format TEXT,
        last_accounts_date TEXT,
        income_statement JSONB,
        balance_sheet JSONB,
        cash_flow_statement JSONB,
        derivation_log JSONB,
        pipeline_status TEXT,
        gross_margin_pct DOUBLE PRECISION,
        operating_margin_pct DOUBLE PRECISION,
        net_margin_pct DOUBLE PRECISION,
        return_on_assets DOUBLE PRECISION,
        return_on_equity DOUBLE PRECISION,
        total_capex DOUBLE PRECISION,
        capex_to_revenue DOUBLE PRECISION,
        net_debt DOUBLE PRECISION,
        net_debt_to_ebitda DOUBLE PRECISION,
        current_ratio DOUBLE PRECISION,
        quick_ratio DOUBLE PRECISION,
        debt_to_equity DOUBLE PRECISION,
        interest_cover DOUBLE PRECISION,
        asset_turnover DOUBLE PRECISION,
        revenue_per_employee DOUBLE PRECISION,
        profit_per_employee DOUBLE PRECISION,
        revenue_yoy_growth DOUBLE PRECISION,
        ebitda_yoy_growth DOUBLE PRECISION,
        profit_yoy_growth DOUBLE PRECISION,
        revenue_cagr_3yr DOUBLE PRECISION,
        revenue_cagr_5yr DOUBLE PRECISION,
        validation_warnings JSONB
    );
    """
    try:
        supabase.rpc("exec_sql", {"query": sql}).execute()
        log.info("Test_Parser_Results table created (or already exists) via RPC.")
    except Exception as e:
        log.warning("RPC exec_sql failed (%s). Trying direct psycopg2 connection...", e)
        _create_table_psycopg2(sql)


def _create_table_psycopg2(sql):
    """Fallback: create table via direct PostgreSQL connection."""
    import psycopg2

    db_password = os.getenv("SUPABASE_DB_PASSWORD")
    if not db_password:
        log.error("SUPABASE_DB_PASSWORD not set in config/keys.env — cannot create table via psycopg2.")
        log.error("Please create Test_Parser_Results manually in Supabase SQL Editor with same schema as companies.")
        return

    # Extract project ref from SUPABASE_URL
    # e.g. https://inqwlidpatcqckvdqtcn.supabase.co -> inqwlidpatcqckvdqtcn
    project_ref = SUPABASE_URL.replace("https://", "").split(".")[0]

    conn_str = (
        f"host=aws-0-eu-west-1.pooler.supabase.com "
        f"port=6543 "
        f"dbname=postgres "
        f"user=postgres.{project_ref} "
        f"password={db_password}"
    )

    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.close()
        log.info("Test_Parser_Results table created via psycopg2.")
    except Exception as e:
        log.error("psycopg2 table creation failed: %s", e)
        log.error("Please create Test_Parser_Results manually in Supabase SQL Editor.")


def write_to_test_table(supabase, parsed_companies, source_companies):
    """Write parsed results to Test_Parser_Results.

    source_companies: list of dicts with company_number, company_name from companies
    """
    # Build a lookup of source company info
    source_lookup = {c["company_number"]: c for c in source_companies}

    rows_written = 0
    for pc in parsed_companies:
        cn = pc["company_number"]
        result = pc["result"]
        source = source_lookup.get(cn, {})

        row = {
            "company_number": cn,
            "company_name": pc["company_name"],
            "filing_format": pc["filing_format"],
            "last_accounts_date": pc["last_date"],
            "pipeline_status": "extracted",
            # Pipeline-extracted fields
            "revenue": result.get("revenue"),
            "ebitda": result.get("ebitda"),
            "ebitda_margin": result.get("ebitda_margin"),
            "fcf": result.get("fcf"),
            "cash_conversion": result.get("cash_conversion"),
            "employees": result.get("employees"),
            "revenue_history": result.get("revenue_history"),
            "ebitda_history": result.get("ebitda_history"),
            "fcf_history": result.get("fcf_history"),
            "employees_history": result.get("employees_history"),
            "revenue_cagr": result.get("revenue_cagr"),
            "ebitda_cagr": result.get("ebitda_cagr"),
            "income_statement": result.get("income_statement"),
            "balance_sheet": result.get("balance_sheet"),
            "cash_flow_statement": result.get("cash_flow_statement"),
            "derivation_log": result.get("derivation_log"),
            # Derived metrics
            "gross_margin_pct": result.get("gross_margin_pct"),
            "operating_margin_pct": result.get("operating_margin_pct"),
            "net_margin_pct": result.get("net_margin_pct"),
            "return_on_assets": result.get("return_on_assets"),
            "return_on_equity": result.get("return_on_equity"),
            "total_capex": result.get("total_capex"),
            "capex_to_revenue": result.get("capex_to_revenue"),
            "net_debt": result.get("net_debt"),
            "net_debt_to_ebitda": result.get("net_debt_to_ebitda"),
            "current_ratio": result.get("current_ratio"),
            "quick_ratio": result.get("quick_ratio"),
            "debt_to_equity": result.get("debt_to_equity"),
            "interest_cover": result.get("interest_cover"),
            "asset_turnover": result.get("asset_turnover"),
            "revenue_per_employee": result.get("revenue_per_employee"),
            "profit_per_employee": result.get("profit_per_employee"),
            # Cross-period metrics
            "revenue_yoy_growth": result.get("revenue_yoy_growth"),
            "ebitda_yoy_growth": result.get("ebitda_yoy_growth"),
            "profit_yoy_growth": result.get("profit_yoy_growth"),
            "revenue_cagr_3yr": result.get("revenue_cagr_3yr"),
            "revenue_cagr_5yr": result.get("revenue_cagr_5yr"),
            # Validation
            "validation_warnings": result.get("validation_warnings"),
        }

        try:
            supabase.table("Test_Parser_Results").upsert(row).execute()
            rows_written += 1
            log.info("  Wrote %s to Test_Parser_Results", cn)
        except Exception as e:
            log.error("  Failed to write %s: %s", cn, e)

    return rows_written


# ============================================================================
# Step 6 — Final summary
# ============================================================================

def print_final_summary(companies, triage_results, parsed_companies, skipped, rows_written):
    """Print the final summary."""
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)

    total = len(companies)
    ixbrl_available = sum(1 for tr in triage_results
                          if any(f["format"] == "ixbrl" for f in tr["filings"]))
    parsed_ok = len(parsed_companies)

    print(f"  Companies processed:         {total}")
    print(f"  Companies with iXBRL:        {ixbrl_available}")
    print(f"  Parsed successfully:         {parsed_ok}")
    print(f"  Rows written to test table:  {rows_written}")

    # Average non-null fields per company
    if parsed_companies:
        total_non_null = 0
        for pc in parsed_companies:
            result = pc["result"]
            for stmt_key in ("income_statement", "balance_sheet", "cash_flow_statement"):
                stmt = result.get(stmt_key, {})
                for yr_row in stmt.values():
                    total_non_null += sum(1 for v in yr_row.values() if v is not None)
        avg_non_null = total_non_null / parsed_ok
        print(f"  Avg non-null fields/company: {avg_non_null:.1f} (across all 3 statements, all years)")

    # Fields that are null for EVERY company
    universally_null = None
    for pc in parsed_companies:
        company_nulls = _find_always_null_fields(pc["result"])
        if universally_null is None:
            universally_null = company_nulls.copy()
        else:
            universally_null &= company_nulls

    if universally_null:
        print(f"\n  Fields NULL for every company (possible tag mapping gap):")
        for f in sorted(universally_null):
            known_tags = REVERSE_TAG_MAP.get(f, [])
            print(f"    - {f}  (mapped from: {', '.join(known_tags[:3])}{'...' if len(known_tags) > 3 else ''})")
    else:
        print(f"\n  No fields were NULL across every company.")

    print()


# ============================================================================
# Main
# ============================================================================

def main():
    _check_env()
    _init_ch_session()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Connected to Supabase.")

    # --------------------------------------------------
    # Step 0: Pick 10 companies
    # --------------------------------------------------
    log.info("Fetching 10 pending/Active companies from companies...")
    companies = get_test_companies(supabase, limit=10)

    if not companies:
        log.error("No pending/Active companies found in companies. Exiting.")
        return

    log.info("Selected %d companies.", len(companies))
    for c in companies:
        log.info("  %s  %s", c["company_number"], c["company_name"])

    # --------------------------------------------------
    # Step 1: Filing triage
    # --------------------------------------------------
    log.info("Starting filing triage (3 most recent filings each)...")
    triage_results = triage_filings(companies, count=3)
    print_triage_summary(triage_results)

    # --------------------------------------------------
    # Step 2: Parse iXBRL only
    # --------------------------------------------------
    log.info("Parsing iXBRL filings...")
    parsed_companies, skipped = parse_ixbrl_companies(triage_results)

    if not parsed_companies:
        log.warning("No companies were successfully parsed. Nothing to report.")
        print_final_summary(companies, triage_results, parsed_companies, skipped, 0)
        return

    # --------------------------------------------------
    # Step 3: Detailed field reports
    # --------------------------------------------------
    print_field_report(parsed_companies)

    # --------------------------------------------------
    # Step 4: Schema structure check
    # --------------------------------------------------
    check_schema(parsed_companies)

    # --------------------------------------------------
    # Step 5: Write to test table
    # --------------------------------------------------
    log.info("Creating Test_Parser_Results table (if needed)...")
    create_test_table(supabase)

    log.info("Writing results to Test_Parser_Results...")
    rows_written = write_to_test_table(supabase, parsed_companies, companies)

    # --------------------------------------------------
    # Step 6: Final summary
    # --------------------------------------------------
    print_final_summary(companies, triage_results, parsed_companies, skipped, rows_written)

    log.info("Test complete.")


if __name__ == "__main__":
    main()
