"""
One-time backfill: compute revenue_cagr_3y for all extracted companies.

Reads revenue_history from each company where pipeline_status = 'extracted',
computes the 3-year CAGR, and writes it to the revenue_cagr_3y column.
"""

import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE = "companies"
PAGE_SIZE = 1000


def compute_cagr_3y(revenue_history: dict) -> float | None:
    """Compute 3-year CAGR from a {year_str: value} dict.

    Uses the most recent year as the end point and looks back 3 years.
    Mirrors _cagr_from_history() in financial_computations.py.
    """
    if not revenue_history:
        return None

    # Parse to {int_year: float_value}, keeping only positive values
    parsed = {}
    for yr, val in revenue_history.items():
        try:
            y = int(yr)
            v = float(val) if val is not None else None
        except (ValueError, TypeError):
            continue
        if v is not None and v > 0:
            parsed[y] = v

    if len(parsed) < 2:
        return None

    current_yr = max(parsed)
    current_val = parsed[current_yr]

    target_yr = current_yr - 3

    # Find closest year at or before target
    candidates = {yr: val for yr, val in parsed.items() if yr <= target_yr}
    if not candidates:
        return None

    base_yr = max(candidates)
    base_val = candidates[base_yr]
    years = current_yr - base_yr
    if years < 1:
        return None

    try:
        cagr = (current_val / base_val) ** (1.0 / years) - 1.0
        return round(cagr, 4)
    except (ZeroDivisionError, ValueError):
        return None


def fetch_extracted(supabase):
    """Fetch all extracted companies with revenue_history, paginated."""
    all_rows = []
    offset = 0
    while True:
        resp = (
            supabase.table(TABLE)
            .select("company_number, revenue_history")
            .eq("pipeline_status", "extracted")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        rows = resp.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Fetching extracted companies...")
    rows = fetch_extracted(supabase)
    print(f"Found {len(rows)} extracted companies.")

    updated = 0
    skipped = 0

    for row in rows:
        cn = row["company_number"]
        rev_hist = row.get("revenue_history")

        # revenue_history may come back as a JSON string or dict
        if isinstance(rev_hist, str):
            try:
                rev_hist = json.loads(rev_hist)
            except (json.JSONDecodeError, TypeError):
                rev_hist = None

        cagr = compute_cagr_3y(rev_hist)
        if cagr is None:
            skipped += 1
            continue

        supabase.table(TABLE).update(
            {"revenue_cagr_3y": cagr}
        ).eq("company_number", cn).execute()
        updated += 1

        if updated % 50 == 0:
            print(f"  Updated {updated} so far...")

    print(f"\nDone. Updated: {updated}, Skipped (insufficient data): {skipped}")


if __name__ == "__main__":
    main()
