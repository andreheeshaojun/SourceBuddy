# Filtering - Target Universe of Companies

## Overview

This skill filters the downloaded Companies House CSVs to the target universe of companies and merges them into a single file. Run this as **Step 2** after downloading the main CSVs using the `download-main-csv` skill.

## Instructions

### Step 1 - Locate the extracted CSVs

1. Find all extracted CSV files inside the `CompanyData` directory (created by the download skill).
2. There will be multiple CSV files corresponding to the parts downloaded. **Do not assume a fixed number of files** — process every CSV found in the directory.
3. Confirm the files exist and contain the expected header rows.

### Step 2 - Strip whitespace from column headers

After loading each CSV, strip all leading and trailing whitespace from every column header. The raw data contains some column names with leading spaces (e.g. ` CompanyNumber` instead of `CompanyNumber`).

### Step 3 - Filter by Accounts.AccountCategory

Filter each CSV to only retain companies where the `Accounts.AccountCategory` column has one of the following values:

- **SMALL**
- **MEDIUM**
- **LARGE**

Remove all rows that do not match one of these three categories (e.g. MICRO, DORMANT, TOTAL EXEMPTION FULL, TOTAL EXEMPTION SMALL, NO ACCOUNTS FILED, etc.).

### Step 4 - Filter by CompanyStatus

Filter to only keep companies where the `CompanyStatus` column equals **Active**. Remove all other rows (e.g. Liquidation, In Administration, Proposal to Strike off, etc.).

### Step 5 - Replace hyphens and underscores with spaces

After filtering, replace every hyphen (`-`) and underscore (`_`) with a space (` `) across all string values in the data.

### Step 6 - Fill blank numeric columns with 0

Replace blank/NaN values with `0` in the following numeric columns only. This ensures valid integers for database import (e.g. Supabase). Do **not** fill text columns with 0.

Numeric columns to fill:
- `Mortgages.NumMortCharges`
- `Mortgages.NumMortOutstanding`
- `Mortgages.NumMortPartSatisfied`
- `Mortgages.NumMortSatisfied`
- `LimitedPartnerships.NumGenPartners`
- `LimitedPartnerships.NumLimPartners`

### Step 7 - Convert remaining NaN values to None

For all other columns, convert remaining `NaN` values to `None` using `df.where(df.notna(), None)`. This ensures that missing/empty cells in text columns are written as truly blank in the CSV output rather than as the string `NaN`.

### Step 8 - Add empty qualitative columns

After all cleaning, add three new empty text columns to the right side of the dataframe:

- `company_description` — free-text company description (to be populated later)
- `risks` — free-text risk notes (to be populated later)
- `qualitative_data` — free-text qualitative data (to be populated later)

```python
df_merged["company_description"] = None
df_merged["risks"] = None
df_merged["qualitative_data"] = None
```

### Step 9 - Merge into a single CSV

After filtering and cleaning, merge all filtered parts into one combined CSV file.

Use Python to perform the filtering, hyphen/underscore replacement, and merging:

```python
import pandas as pd
import glob

# Find all CSV files in CompanyData
csv_files = sorted(glob.glob("CompanyData/*.csv"))

target_categories = ["SMALL", "MEDIUM", "LARGE"]
filtered_parts = []

for csv_path in csv_files:
    print(f"Processing: {csv_path}")
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df_filtered = df[df["Accounts.AccountCategory"].str.strip().str.upper().isin(target_categories)]
    df_filtered = df_filtered[df_filtered["CompanyStatus"].str.strip() == "Active"]
    print(f"  Rows: {len(df)} -> {len(df_filtered)}")
    filtered_parts.append(df_filtered)

# Merge all filtered parts into one CSV
df_merged = pd.concat(filtered_parts, ignore_index=True)

# Replace all hyphens and underscores with spaces in string columns
for col in df_merged.select_dtypes(include=["object"]).columns:
    df_merged[col] = df_merged[col].str.replace("-", " ", regex=False).str.replace("_", " ", regex=False)

# Fill blank numeric columns with 0
numeric_columns = [
    "Mortgages.NumMortCharges",
    "Mortgages.NumMortOutstanding",
    "Mortgages.NumMortPartSatisfied",
    "Mortgages.NumMortSatisfied",
    "LimitedPartnerships.NumGenPartners",
    "LimitedPartnerships.NumLimPartners",
]
for col in numeric_columns:
    df_merged[col] = df_merged[col].fillna(0).astype(int)

# Convert remaining NaN values to None for clean CSV output
df_merged = df_merged.where(df_merged.notna(), None)

# Add empty qualitative columns
df_merged["company_description"] = None
df_merged["risks"] = None
df_merged["qualitative_data"] = None

df_merged.to_csv("CompanyData/FilteredCompanyData.csv", index=False)

print(f"\nTotal filtered rows: {len(df_merged)}")
```

### Step 10 - Verify the merged data

1. Confirm the merged CSV was saved to `CompanyData/FilteredCompanyData.csv`.
2. Check that only SMALL, MEDIUM, and LARGE values remain in the `Accounts.AccountCategory` column:
   ```python
   print(df_merged["Accounts.AccountCategory"].value_counts())
   ```
3. Check that only `Active` remains in the `CompanyStatus` column:
   ```python
   print(df_merged["CompanyStatus"].value_counts())
   ```
4. Confirm the three new columns (`company_description`, `risks`, `qualitative_data`) exist and are empty/None.
5. Confirm all column headers have no leading or trailing whitespace.
4. Confirm no hyphens or underscores remain in string values.
5. Confirm numeric columns contain integers with no blanks — all should be `0` or a valid number.
6. Confirm no `NaN` strings appear in the CSV — remaining missing values should be truly blank.
7. Confirm the total row count is reasonable relative to the original datasets.

## Notes

- The column in the raw data is called `Accounts.AccountCategory` (referred to as `accounts_type_desc` in the Companies House schema). This contains the company size classification (SMALL, MEDIUM, LARGE, MICRO, etc.).
- The `CompanyCategory` column contains the legal entity type (e.g. "Private Limited Company") and is not used for this filter.
- The original extracted CSVs are left untouched. The filtered and merged output is saved as a separate file.
