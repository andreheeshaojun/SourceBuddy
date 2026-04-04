"""
Upload FilteredCompanyData.csv to the Supabase `companies` table.

Maps:  CompanyNumber  → company_number  (top-level column)
       CompanyName    → company_name    (top-level column)
       everything else → metadata JSONB

Every row gets  metadata.pipeline.status = 'pending'.
"""

import json
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "CompanyData", "FilteredCompanyData.csv"
)
TABLE_NAME = "companies"
BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# Read CSV
# ---------------------------------------------------------------------------
df = pd.read_csv(CSV_PATH, dtype=str)
df.columns = df.columns.str.strip()
df = df.fillna("").replace(["nan", "NaN", "None"], "")

# ---------------------------------------------------------------------------
# Build rows for the new schema
# ---------------------------------------------------------------------------

# CSV columns that map to metadata keys
METADATA_MAP = {
    "SICCode.SicText_1":            "sic_code_1",
    "SICCode.SicText_2":            "sic_code_2",
    "SICCode.SicText_3":            "sic_code_3",
    "SICCode.SicText_4":            "sic_code_4",
    "CompanyStatus":                "company_status",
    "Accounts.AccountCategory":     "accounts_category",
    "IncorporationDate":            "incorporation_date",
    "RegAddress.PostCode":          "postcode",
    "RegAddress.AddressLine1":      "address_line_1",
    "RegAddress.AddressLine2":      "address_line_2",
    "RegAddress.PostTown":          "post_town",
    "RegAddress.County":            "county",
    "RegAddress.Country":           "country",
    "RegAddress.CareOf":            "care_of",
    "RegAddress.POBox":             "po_box",
    "CompanyCategory":              "company_category",
    "CountryOfOrigin":              "country_of_origin",
    "Accounts.AccountRefDay":       "account_ref_day",
    "Accounts.AccountRefMonth":     "account_ref_month",
    "Accounts.NextDueDate":         "accounts_next_due_date",
    "Accounts.LastMadeUpDate":      "accounts_last_made_up_date",
}


def row_to_record(row):
    """Convert a CSV row to the {company_number, company_name, metadata} shape."""
    metadata = {}

    for csv_col, meta_key in METADATA_MAP.items():
        val = row.get(csv_col, "")
        if val:                       # skip blanks
            metadata[meta_key] = val

    # Pipeline status — always pending for fresh uploads
    metadata["pipeline"] = {"status": "pending"}

    return {
        "company_number": row["CompanyNumber"],
        "company_name":   row["CompanyName"],
        "metadata":       metadata,
    }


# ---------------------------------------------------------------------------
# Preview a sample row before uploading
# ---------------------------------------------------------------------------
sample = row_to_record(df.iloc[0])
print("=== SAMPLE ROW ===")
print(json.dumps(sample, indent=2))
print("===================\n")

input("Press Enter to upload all rows, or Ctrl-C to cancel...")

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print(f"Uploading {len(df)} rows to '{TABLE_NAME}'...")

for i in range(0, len(df), BATCH_SIZE):
    batch = [row_to_record(df.iloc[j]) for j in range(i, min(i + BATCH_SIZE, len(df)))]
    try:
        supabase.table(TABLE_NAME).insert(batch).execute()
        print(f"  Inserted {min(i + BATCH_SIZE, len(df))} / {len(df)}")
    except Exception as e:
        print(f"  Error at row {i}: {e}")
        exit(1)

print("\nDone! All data uploaded successfully.")
