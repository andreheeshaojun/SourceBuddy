"""Backfill sector and sub_sector for all companies using SIC codes.

Reads metadata.sic_code_1..4 from the database, classifies using the
deterministic SIC 2007 lookup from Claude skills/SiC-classification.md,
and writes:
  - sector (typed column)
  - sub_sector (typed column)
  - metadata.sic_codes (structured JSONB: primary + secondary codes)

This is a standalone one-time script — not part of the financial pipeline.
Re-run safely; it overwrites sector/sub_sector on every row.
"""
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

log = logging.getLogger("backfill_sectors")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# SIC classification logic (from Claude skills/SiC-classification.md)
# ---------------------------------------------------------------------------

def parse_sic(raw):
    """Extract 5-digit SIC code from Companies House format 'NNNNN Description'."""
    if not raw or raw in ("EMPTY", ""):
        return None
    code = str(raw).strip().split()[0]
    if not code.isdigit() or len(code) != 5:
        return None
    return code


def parse_sic_description(raw):
    """Extract description text after the 5-digit code."""
    if not raw:
        return None
    code = parse_sic(raw)
    if not code:
        return None
    return raw[raw.index(code) + len(code):].strip()


EXCLUDED = {"99999", "74990"}


def is_excluded(code):
    if code in EXCLUDED:
        return True
    div = int(code[:2])
    return div in (97, 98, 99)


FOUR_DIGIT_OVERRIDES = {
    # Technology — Hardware
    2610: ("Technology", "Hardware & Semiconductors"),
    2620: ("Technology", "Hardware & Semiconductors"),
    2630: ("Technology", "Hardware & Semiconductors"),
    2640: ("Technology", "Hardware & Semiconductors"),
    2650: ("Technology", "Hardware & Semiconductors"),
    2651: ("Technology", "Hardware & Semiconductors"),
    2670: ("Technology", "Hardware & Semiconductors"),
    2680: ("Technology", "Hardware & Semiconductors"),
    # Healthcare — Medical Devices (overrides Hardware for div 26)
    2660: ("Healthcare", "Medical Devices & Equipment"),
    # Consumer — from div 26
    2652: ("Consumer", "Consumer Products"),
    # Consumer — from div 32
    3211: ("Consumer", "Consumer Products"),
    3212: ("Consumer", "Consumer Products"),
    3213: ("Consumer", "Consumer Products"),
    3220: ("Consumer", "Consumer Products"),
    3230: ("Consumer", "Leisure & Hospitality"),
    3240: ("Consumer", "Consumer Products"),
    # Healthcare — Medical Devices from div 32
    3250: ("Healthcare", "Medical Devices & Equipment"),
    # Industrial — remainder of div 32
    3291: ("Industrial", "Engineering & Manufacturing"),
    3299: ("Industrial", "Engineering & Manufacturing"),
    # Financials — Asset Management overrides within div 64
    6420: ("Financials", "Asset Management"),
    6430: ("Financials", "Asset Management"),
    6499: ("Financials", "Asset Management"),
}

DIVISION_MAP = {
    # Technology
    62: ("Technology", "Software"),
    63: ("Technology", "Software"),
    61: ("Technology", "Telecommunications"),
    58: ("Technology", "Media & Digital Content"),
    59: ("Technology", "Media & Digital Content"),
    60: ("Technology", "Media & Digital Content"),
    # Financials
    64: ("Financials", "Banking & Lending"),
    65: ("Financials", "Insurance & Pensions"),
    66: ("Financials", "Capital Markets & Brokerages"),
    # Healthcare
    21: ("Healthcare", "Pharma & Biotech"),
    72: ("Healthcare", "Pharma & Biotech"),
    86: ("Healthcare", "Healthcare Services"),
    87: ("Healthcare", "Care & Social Services"),
    88: ("Healthcare", "Care & Social Services"),
    # Consumer
    10: ("Consumer", "Food & Beverage"),
    11: ("Consumer", "Food & Beverage"),
    12: ("Consumer", "Food & Beverage"),
    56: ("Consumer", "Food & Beverage"),
    45: ("Consumer", "Retail & E-commerce"),
    46: ("Consumer", "Retail & E-commerce"),
    47: ("Consumer", "Retail & E-commerce"),
    55: ("Consumer", "Leisure & Hospitality"),
    90: ("Consumer", "Leisure & Hospitality"),
    91: ("Consumer", "Leisure & Hospitality"),
    92: ("Consumer", "Leisure & Hospitality"),
    93: ("Consumer", "Leisure & Hospitality"),
    13: ("Consumer", "Consumer Products"),
    14: ("Consumer", "Consumer Products"),
    15: ("Consumer", "Consumer Products"),
    31: ("Consumer", "Consumer Products"),
    75: ("Consumer", "Personal Services"),
    95: ("Consumer", "Personal Services"),
    96: ("Consumer", "Personal Services"),
    85: ("Consumer", "Education"),
    # Business Services
    69: ("Business Services", "Professional Services"),
    70: ("Business Services", "Professional Services"),
    71: ("Business Services", "Professional Services"),
    74: ("Business Services", "Professional Services"),
    78: ("Business Services", "Staffing & HR"),
    73: ("Business Services", "Marketing & Communications"),
    77: ("Business Services", "Facilities & Support Services"),
    79: ("Business Services", "Facilities & Support Services"),
    80: ("Business Services", "Facilities & Support Services"),
    81: ("Business Services", "Facilities & Support Services"),
    82: ("Business Services", "Facilities & Support Services"),
    # Industrial
    16: ("Industrial", "Engineering & Manufacturing"),
    17: ("Industrial", "Engineering & Manufacturing"),
    18: ("Industrial", "Engineering & Manufacturing"),
    22: ("Industrial", "Engineering & Manufacturing"),
    23: ("Industrial", "Engineering & Manufacturing"),
    24: ("Industrial", "Engineering & Manufacturing"),
    25: ("Industrial", "Engineering & Manufacturing"),
    27: ("Industrial", "Engineering & Manufacturing"),
    28: ("Industrial", "Engineering & Manufacturing"),
    33: ("Industrial", "Engineering & Manufacturing"),
    20: ("Industrial", "Chemicals & Materials"),
    30: ("Industrial", "Aerospace & Defence"),
    29: ("Industrial", "Automotive & Transport Equipment"),
    41: ("Industrial", "Construction"),
    42: ("Industrial", "Construction"),
    43: ("Industrial", "Construction"),
    # Real Assets
    68: ("Real Assets", "Real Estate"),
    5:  ("Real Assets", "Energy"),
    6:  ("Real Assets", "Energy"),
    7:  ("Real Assets", "Energy"),
    8:  ("Real Assets", "Energy"),
    9:  ("Real Assets", "Energy"),
    19: ("Real Assets", "Energy"),
    35: ("Real Assets", "Energy"),
    36: ("Real Assets", "Infrastructure & Utilities"),
    37: ("Real Assets", "Infrastructure & Utilities"),
    38: ("Real Assets", "Infrastructure & Utilities"),
    39: ("Real Assets", "Infrastructure & Utilities"),
    49: ("Real Assets", "Infrastructure & Utilities"),
    50: ("Real Assets", "Infrastructure & Utilities"),
    51: ("Real Assets", "Infrastructure & Utilities"),
    52: ("Real Assets", "Infrastructure & Utilities"),
    53: ("Real Assets", "Infrastructure & Utilities"),
    1:  ("Real Assets", "Agriculture & Natural Resources"),
    2:  ("Real Assets", "Agriculture & Natural Resources"),
    3:  ("Real Assets", "Agriculture & Natural Resources"),
    # Public Services
    84: ("Public Services", "Public Administration & Defence"),
}


def classify(code):
    """Return (sector, sub_sector) for a 5-digit SIC code, or None if excluded."""
    if is_excluded(code):
        return None
    div = int(code[:2])
    if div in (26, 32, 64):
        cls = int(code[:4])
        if cls in FOUR_DIGIT_OVERRIDES:
            return FOUR_DIGIT_OVERRIDES[cls]
    return DIVISION_MAP.get(div)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fetch all companies — paginate in chunks of 1000
    all_companies = []
    offset = 0
    page_size = 1000
    while True:
        result = (
            supabase.table("companies")
            .select("company_number, metadata")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data
        if not batch:
            break
        all_companies.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    log.info("Loaded %d companies from database.", len(all_companies))

    counts = {
        "classified": 0,
        "excluded": 0,
        "no_sic": 0,
        "unknown_division": 0,
        "multi_sector": 0,
    }
    sector_dist = {}

    # --- Phase 1: classify all companies in memory ---
    updates = []  # (cn, sector, sub_sector, sic_metadata)

    for company in all_companies:
        cn = company["company_number"]
        meta = company.get("metadata") or {}

        primary_raw = meta.get("sic_code_1", "")
        primary_code = parse_sic(primary_raw)

        if not primary_code:
            counts["no_sic"] += 1
            continue

        if is_excluded(primary_code):
            sector = None
            sub_sector = None
            excluded = True
            counts["excluded"] += 1
        else:
            result = classify(primary_code)
            if result:
                sector, sub_sector = result
                excluded = False
                counts["classified"] += 1
                sector_dist[sector] = sector_dist.get(sector, 0) + 1
            else:
                sector = None
                sub_sector = None
                excluded = False
                counts["unknown_division"] += 1

        secondary_codes = []
        is_multi_sector = False
        for col in ("sic_code_2", "sic_code_3", "sic_code_4"):
            raw = meta.get(col, "")
            code = parse_sic(raw)
            if code:
                desc = parse_sic_description(raw)
                secondary_codes.append({"code": code, "description": desc})
                sec_result = classify(code)
                if sec_result and sec_result[0] != sector:
                    is_multi_sector = True

        if is_multi_sector:
            counts["multi_sector"] += 1

        sic_metadata = {
            "primary": {
                "code": primary_code,
                "description": parse_sic_description(primary_raw),
                "sector": sector,
                "sub_sector": sub_sector,
                "excluded": excluded,
            },
            "secondary": secondary_codes,
        }
        if is_multi_sector:
            sic_metadata["multi_sector"] = True

        updates.append((cn, sector, sub_sector, sic_metadata))

    log.info("Classification complete. %d companies to update.", len(updates))

    # --- Phase 2: batch write to database ---
    # Group by (sector, sub_sector) to do bulk updates
    from collections import defaultdict
    sector_groups = defaultdict(list)
    for cn, sector, sub_sector, _ in updates:
        sector_groups[(sector, sub_sector)].append(cn)

    log.info("Writing sector/sub_sector in %d batches...", len(sector_groups))
    for (sector, sub_sector), cns in sector_groups.items():
        # Supabase .in_() supports up to ~300 items; chunk if needed
        for i in range(0, len(cns), 200):
            chunk = cns[i:i + 200]
            supabase.table("companies").update({
                "sector": sector,
                "sub_sector": sub_sector,
            }).in_("company_number", chunk).execute()
    log.info("Typed columns written.")

    # Write sic_codes metadata individually (RPC doesn't support batch)
    log.info("Writing sic_codes metadata for %d companies...", len(updates))
    for idx, (cn, _, _, sic_metadata) in enumerate(updates):
        supabase.rpc("update_company_metadata", {
            "p_company_number": cn,
            "p_patch": {"sic_codes": sic_metadata},
        }).execute()
        if (idx + 1) % 500 == 0:
            log.info("  %d / %d metadata writes done", idx + 1, len(updates))
    log.info("Metadata written.")

    # --- Summary ---
    log.info("")
    log.info("=== Backfill Summary ===")
    log.info("  Classified:        %d", counts["classified"])
    log.info("  Excluded:          %d", counts["excluded"])
    log.info("  No SIC code:       %d", counts["no_sic"])
    log.info("  Unknown division:  %d", counts["unknown_division"])
    log.info("  Multi-sector:      %d", counts["multi_sector"])
    log.info("")
    log.info("=== Sector Distribution ===")
    for sector, count in sorted(sector_dist.items(), key=lambda x: -x[1]):
        log.info("  %-25s %d", sector, count)
    log.info("========================")


if __name__ == "__main__":
    main()
