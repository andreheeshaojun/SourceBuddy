"""Test employee extraction from scanned PDF notes pages.

Standalone script — does NOT modify pipeline code. Tests the extraction
logic in isolation, then shows what would be written.
"""
import sys
import os
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Merge Data"))

from pipeline import (
    _check_env, _init_ch_session,
    get_accounts_filings, get_document_metadata, determine_filing_format,
    download_document,
)
import pdf_parser

_check_env()
_init_ch_session()

# ---------------------------------------------------------------------------
# Employee extraction logic (candidate for pdf_parser.py)
# ---------------------------------------------------------------------------

# Labels that indicate an employee headcount row (not cost rows)
EMPLOYEE_HEADCOUNT_LABELS = {
    "total staff": True,
    "total": True,            # bare total row after sub-categories
    "average number": True,
    "number of employees": True,
    "average headcount": True,
    # Sub-category labels (will be summed if no total found)
    "production staff": False,
    "production": False,
    "management staff": False,
    "management": False,
    "administration staff": False,
    "administration": False,
    "administrative staff": False,
    "sales staff": False,
    "sales": False,
    "warehouse staff": False,
    "warehouse": False,
    "office staff": False,
    "operations": False,
    "technical": False,
    "directors": False,
}
# True = total label, False = sub-category label


def extract_employees_from_notes(all_pages, page_width, page_height, notes_pages,
                                  current_year, prior_year):
    """Extract employee headcount from notes pages by finding the employee
    section and parsing the table.

    Strategy:
    1. Find the notes page containing employee/staff costs section
    2. Isolate the rows in that section
    3. Look for a total row first; if not found, sum sub-category rows
    4. Return {current_year: count, prior_year: count} or None
    """
    # Step 1: Find the employee page
    employee_page = None
    for pnum in notes_pages:
        page_txt = pdf_parser.page_text(all_pages, pnum).lower()
        if any(kw in page_txt for kw in ['staff costs', 'employees', 'average number of persons',
                                          'average number of employees', 'average headcount']):
            # Confirm it has "No." or actual small numbers (headcount indicators)
            if 'no.' in page_txt or 'average number' in page_txt or 'average headcount' in page_txt:
                employee_page = pnum
                break

    if employee_page is None:
        return None, None

    # Step 2: Get all items on the page
    items = all_pages.get(employee_page, [])
    if not items:
        return None, None

    # Step 3: Find the "staff costs" or "employees" section boundary.
    # The section header must be left-aligned (x < 40% page width) to avoid
    # matching "employees" in prose text (e.g. accounting policies).
    items_sorted = sorted(items, key=lambda it: pdf_parser.get_y_center(it[0]))

    section_start_y = None
    table_start_y = None
    section_end_y = None

    for poly, text, conf in items_sorted:
        y = pdf_parser.get_y_center(poly)
        x = pdf_parser.get_x_center(poly)
        lower = text.lower().strip()

        # Find section header — must be left-aligned heading
        if section_start_y is None:
            if x < page_width * 0.35:
                if lower in ('staff', 'employees') or 'staff costs' in lower:
                    section_start_y = y
                    continue

        # After section header, find "No." or "average"/"number" as table start
        if section_start_y is not None and table_start_y is None:
            if lower in ('no.', 'no', 'average', 'number'):
                table_start_y = y
                continue

        # Find end of headcount section — cost/wage section or next note heading
        if section_start_y is not None and table_start_y is not None and section_end_y is None:
            if x < page_width * 0.35:
                if any(kw in lower for kw in ['aggregate', 'payroll', 'wages',
                                               'salaries', 'incurred', 'costs',
                                               'remuneration', 'auditor']):
                    section_end_y = y
                    break

    if table_start_y is None:
        # Fallback: if we found the section header but no "No." marker,
        # use section_start_y as the table start (some filings put numbers
        # inline with prose immediately after the header)
        if section_start_y is not None:
            table_start_y = section_start_y
        else:
            return None, None

    # Always scan from section_start_y (not table_start_y) because some
    # filings place headcount numbers above the "No." marker row
    scan_start_y = section_start_y if section_start_y else table_start_y

    if section_end_y is None:
        # Use a generous window — 300px below scan start
        section_end_y = scan_start_y + 300

    # Step 4: Extract rows in the headcount table region
    # Detect column positions (same logic as parse_financial_page but simpler)
    year_ints = set()
    for y in (current_year, prior_year):
        if isinstance(y, str) and y.isdigit():
            year_ints.add(int(y))

    # Collect items in the employee section region
    table_items = []
    for poly, text, conf in items_sorted:
        y = pdf_parser.get_y_center(poly)
        if y < scan_start_y or y > section_end_y:
            continue
        table_items.append((poly, text, conf))

    if not table_items:
        return None, None

    # Cluster into rows by y-proximity
    rows = []
    current_row = []
    current_y = -999
    for poly, text, conf in sorted(table_items, key=lambda it: pdf_parser.get_y_center(it[0])):
        y = pdf_parser.get_y_center(poly)
        if abs(y - current_y) > 20:
            if current_row:
                rows.append(current_row)
            current_row = [(poly, text, conf)]
            current_y = y
        else:
            current_row.append((poly, text, conf))
    if current_row:
        rows.append(current_row)

    # Step 5: Parse each row — separate labels from numbers
    total_values = None
    sub_totals = []

    for row in rows:
        label_parts = []
        numbers = []
        for poly, text, conf in sorted(row, key=lambda it: pdf_parser.get_x_center(it[0])):
            x = pdf_parser.get_x_center(poly)
            if pdf_parser.is_number_text(text):
                val = pdf_parser.parse_number(text)
                if val is not None and val not in year_ints and 0 < val < 100000:
                    numbers.append(val)
            elif x < page_width * 0.50:
                label_parts.append(text)

        if not numbers:
            continue

        label = " ".join(label_parts).strip().lower()

        # Check if this matches a headcount label
        is_total = None
        for pattern, is_total_flag in EMPLOYEE_HEADCOUNT_LABELS.items():
            if pattern in label:
                is_total = is_total_flag
                break

        # Bare number row (no label) after sub-categories = total
        if is_total is None and not label_parts and sub_totals:
            is_total = True

        if is_total is True:
            # Take the first number as current year
            total_values = numbers
            break  # Total found, stop looking
        elif is_total is False:
            sub_totals.append(numbers)

    # Step 6: Resolve — prefer total, fall back to sum of sub-categories
    result = {}
    if total_values:
        if len(total_values) >= 1:
            result[current_year] = int(total_values[0])
        if len(total_values) >= 2:
            result[prior_year] = int(total_values[1])
    elif sub_totals:
        # Sum sub-categories
        col1_sum = sum(nums[0] for nums in sub_totals if len(nums) >= 1)
        col2_sum = sum(nums[1] for nums in sub_totals if len(nums) >= 2)
        if col1_sum > 0:
            result[current_year] = int(col1_sum)
        if col2_sum > 0:
            result[prior_year] = int(col2_sum)

    if not result:
        return None, employee_page

    return result, employee_page


# ---------------------------------------------------------------------------
# Test on the 4 scanned PDF companies (excluding 4C Hotels — no notes pages)
# ---------------------------------------------------------------------------

test_companies = [
    ("02609110", "A & D INSTRUMENTS"),
    ("03688878", "A C GEORGIADES"),
    ("OC421575", "BREGAL MILESTONE"),
    ("OC401440", "4C HOTELS MINORIES"),
]

print("=" * 70)
print("EMPLOYEE EXTRACTION TEST — Scanned PDFs")
print("=" * 70)

for cn, name in test_companies:
    print(f"\n--- {name} ({cn}) ---")

    filings = get_accounts_filings(cn, count=1)
    metadata = get_document_metadata(filings[0])
    fmt = determine_filing_format(metadata, filings[0])
    content, _ = download_document(metadata, fmt)

    all_pages, page_width, page_height, source = pdf_parser.load_pages(content)
    income_page, balance_page, cashflow_page, notes_pages, is_filleted = \
        pdf_parser.identify_pages(all_pages, page_height=page_height)
    current_year, prior_year = pdf_parser.detect_years(
        all_pages, [income_page, balance_page, cashflow_page])

    print(f"  notes_pages={notes_pages}  years={current_year}/{prior_year}")

    if not notes_pages:
        print(f"  SKIP: no notes pages detected")
        continue

    result, page_found = extract_employees_from_notes(
        all_pages, page_width, page_height, notes_pages, current_year, prior_year)

    if result:
        print(f"  FOUND on page {page_found}: {result}")
    else:
        print(f"  NOT FOUND (searched page {page_found})")

print("\n" + "=" * 70)
