# Download Main CSV - Companies House Free Company Data Product

## Overview

This skill downloads the Free Company Data Product from Companies House as multiple smaller files. This is a downloadable data snapshot containing basic company data of live companies on the UK register. The snapshot is provided as multiple ZIP files containing data in CSV format.

**Source:** https://download.companieshouse.gov.uk/en_output.html

The snapshot is updated within 5 working days of the previous month end. Data is compiled up to the end of the previous month.

## Instructions

### Step 1 - Identify the current download files

1. Fetch the Companies House download page: `https://download.companieshouse.gov.uk/en_output.html`
2. Look for the **"Company data as multiple files:"** heading on the right-hand side of the page.
3. Under that heading, find **all** the bullet point links listed between the **"Company data as multiple files:"** heading and the **"Last Updated:"** line. They follow the naming pattern:
   ```
   BasicCompanyData-YYYY-MM-DD-part1_N.zip
   BasicCompanyData-YYYY-MM-DD-part2_N.zip
   ...
   BasicCompanyData-YYYY-MM-DD-partN_N.zip
   ```
   The date portion changes each month (e.g. `BasicCompanyData-2026-03-02-part1_7.zip`). **Do not assume a fixed number of parts** — the total number of parts may change over time. Always check the page and download every file listed under this heading.
4. Extract the full download URLs for every part listed. The download links follow this pattern:
   ```
   https://download.companieshouse.gov.uk/BasicCompanyData-YYYY-MM-DD-partX_N.zip
   ```
   where `X` is the part number and `N` is the total number of parts.

### Step 2 - Download all ZIP files

1. Download **every** ZIP file identified in Step 1 to the project working directory using `curl`. For each part:
   ```bash
   curl -L -o BasicCompanyData-partX_N.zip "https://download.companieshouse.gov.uk/BasicCompanyData-YYYY-MM-DD-partX_N.zip"
   ```
   Replace `YYYY-MM-DD` with the date identified in Step 1, and `X`/`N` with the part number and total from each filename.
2. Each file is approximately 47-70 MB. Allow sufficient time for all downloads.

### Step 3 - Extract the CSVs

1. Unzip all downloaded files into the same directory. For each part:
   ```bash
   unzip BasicCompanyData-partX_N.zip -d CompanyData
   ```
2. The extracted CSV files will be inside the `CompanyData` directory.
3. Optionally remove the ZIP files after extraction to save disk space:
   ```bash
   rm BasicCompanyData-part*.zip
   ```

### Step 4 - Verify the data

Confirm the CSVs contain the expected columns by reading the header row of one of the files. Each CSV should contain the following data fields:

| Category | Field | Max Size |
|---|---|---|
| | CompanyName | 160 |
| | CompanyNumber | 8 |
| Registered Office Address | CareOf | 100 |
| | POBox | 10 |
| | AddressLine1 (HouseNumber and Street) | 300 |
| | AddressLine2 (area) | 300 |
| | PostTown | 50 |
| | County (region) | 50 |
| | Country | 50 |
| | PostCode | 20 |
| | CompanyCategory (corporate_body_type_desc) | 100 |
| | CompanyStatus (action_code_desc) | 70 |
| | CountryOfOrigin | 50 |
| | DissolutionDate | 10 |
| | IncorporationDate | 10 |
| Accounts | AccountingRefDay | 2 |
| | AccountingRefMonth | 2 |
| | NextDueDate | 10 |
| | LastMadeUpDate | 10 |
| | AccountsCategory (accounts_type_desc) | 30 |
| Returns | NextDueDate | 10 |
| | LastMadeUpDate | 10 |
| Mortgages | NumMortCharges | 6 |
| | NumMortOutstanding | 6 |
| | NumMortPartSatisfied | 6 |
| | NumMortSatisfied | 6 |
| SIC Codes (occurs max 4) | SICCode1 | 170 |
| | SICCode2 | 170 |
| | SICCode3 | 170 |
| | SICCode4 | 170 |
| Limited Partnerships | NumGenPartners | 6 |
| | NumLimPartners | 6 |
| | URI | 47 |
| Previous Names (occurs max 10) | Change of Name Date | 10 |
| | Company name (previous) | 160 |
| Confirmation Statement | ConfStmtNextDueDate | 10 |
| | ConfStmtLastMadeUpDate | 10 |

## Notes

- This snapshot is provided free of charge and will not be supported.
- Up-to-date company information can be obtained by following the URI links in the data.
- If files are viewed with Microsoft Excel, it is recommended to use version 2007 or later.
- The Previous Names fields (Change of Name Date and Company name) can occur up to 10 times in the data.
- The SIC Code fields can occur up to 4 times.
