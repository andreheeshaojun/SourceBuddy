# Companies House Financial Extraction Pipeline — Project Context

## What This Project Is

This project extracts financial data from UK Companies House filings and populates a Supabase database. The end product is a table where each row is a company with structured financial metrics (revenue, EBITDA, margins, growth rates, etc.) used by a frontend for PE deal sourcing.

---

## Infrastructure

**Database:** Supabase (PostgreSQL). One primary table called `companies`. Connection credentials are in `config/keys.env` at the project root under `SUPABASE_URL` and `SUPABASE_KEY` (service role key). Use the `supabase-py` client to connect. Never hardcode credentials.

**Companies House API:** Free REST API. Key is in `config/keys.env` under `CH_API_KEY`. Auth is HTTP Basic with the API key as username and empty password. Rate limit is 600 requests per 5 minutes — always add a delay between calls to stay under this.

**Local repo:** Contains pipeline scripts only. No data is stored locally. The raw CSV that originally seeded the database is no longer used — everything reads from and writes to Supabase.

---

## Database Schema — `companies` table

This is the only table. One row per company. It serves both the pipeline and the frontend.

**Columns populated by the initial CSV import (already filled):**
company_number (text, primary key), company_name, sic_code_1, sic_code_2, registered_address, company_category, incorporation_date, accounts_category, company_status

**Columns populated by the extraction pipeline (start as null):**
revenue, ebitda, ebitda_margin, fcf, cash_conversion, employees, revenue_history (jsonb, keyed by year), ebitda_history (jsonb, keyed by year), revenue_cagr, ebitda_cagr, accounts_type, filing_format, last_accounts_date

**Pipeline tracking column:**
pipeline_status — values are `pending`, `extracted`, `failed`, `no_filing`. The pipeline queries for `pending` rows, processes them, and updates to one of the other three values.

**Conventions:**
- All monetary values are in GBP, stored as plain numbers (no formatting).
- Margins and percentages are stored as decimals (0.15 means 15%). Frontend multiplies by 100 for display.
- JSONB history fields are objects keyed by year string, e.g. `{"2021": 1500000, "2022": 1700000}`.
- CAGR is calculated from the history values for all metrics (revenue, EBITDA). The pipeline fetches the 5 most recent accounts filings per company to build ~5 years of history. CAGR is computed over the full span available.

---

## Pipeline Logic — How Extraction Works

The pipeline processes companies in batches of 50. For each company, the sequence is:

**1. Get filing history.**
Hit the Companies House filing history endpoint for the company_number. Filter by category `accounts`. Fetch the 5 most recent filings to build ~5 years of historical data. If no accounts filings exist, mark the company as `no_filing` and move on.

**2. Check the filing metadata.**
Each filing item has a `paper_filed` boolean and a link to document metadata. The document metadata response contains a `resources` object listing available formats.

**3. Triage the document type.** This determines the parsing strategy:
- If `application/xhtml+xml` is listed in resources → this is iXBRL (structured data). Set filing_format to `ixbrl`. This is the cheapest and most reliable path.
- If only `application/pdf` is available and `paper_filed` is false → this is an electronically generated PDF with a text layer. Set filing_format to `electronic_pdf`.
- If only `application/pdf` and `paper_filed` is true → this is a scanned document with no text layer. Set filing_format to `scanned_pdf`. This is the hardest and most expensive to process.

**4. Download the document.**
Request the document content endpoint with an `Accept` header matching the desired format. Same auth as all other CH API calls.

**5. Parse based on format:**

- **iXBRL:** Parse as HTML. Financial values are embedded in inline XBRL tags (ix:nonfraction, ix:nonnumeric) with taxonomy names identifying what each number represents. Common tags include Turnover, GrossProfit, OperatingProfit, DepreciationAmortisationImpairment, CashBankInHand, NetAssetsLiabilities, AverageNumberEmployeesDuringPeriod. Watch for `scale` attributes on tagged values — they indicate multipliers (scale="3" means thousands, scale="6" means millions). Check `sign` attributes for negation. Context references distinguish current year from prior year. Do not use an LLM for iXBRL — it is already structured data.

- **Electronic PDF:** Extract text using pdfplumber. Apply rule-based matching — build a dictionary of label variations that map to canonical financial fields (e.g. "Turnover", "Revenue", "Net turnover" all map to revenue). Use fuzzy matching for OCR-like imperfections. Use table extraction and column position logic to assign numbers to the correct year. If rule-based extraction fails on key fields, do NOT call an LLM — flag the company as `failed` with a note that it needs LLM review, and ask the user before proceeding. LLM fallback will be added later.

- **Scanned PDF:** Requires OCR first (Tesseract for free, AWS Textract or Google Document AI for quality). After OCR, apply the same rule-based extraction as electronic PDFs. If the project is cost-sensitive, skip scanned PDFs entirely — mark as `failed` and revisit later. Most recent filings are electronic.

**6. Calculate derived metrics.**
EBITDA margin = EBITDA / revenue. CAGR = compound annual growth rate from the earliest and latest years in the history object, calculated for all metrics with history (revenue, EBITDA). The pipeline targets 5 years of history via multi-filing extraction. Cash conversion = FCF / EBITDA. Only calculate if the required inputs are present — do not infer or estimate missing values.

**7. Write back to Supabase.**
Update the company's row with all extracted and calculated values. Set pipeline_status to `extracted`. If any step failed, set pipeline_status to `failed` and log the error. Do not create new rows or new tables.

---

## Companies House API — Endpoint Reference

All endpoints use the same auth (API key as Basic auth username, empty password).

**Filing history:**
`GET https://api.companieshouse.gov.uk/company/{company_number}/filing-history`
Supports query params: `category` (filter by type, use `accounts`), `items_per_page`.

**Document metadata:**
URL comes from the filing history response under `links.document_metadata`. Do not construct this URL manually.
Response contains `resources` (available formats) and `links.document` (download URL).

**Document download:**
URL comes from document metadata under `links.document`. Set the `Accept` header to the desired format (`application/pdf` or `application/xhtml+xml`).

---

## Rules and Constraints

- Never create new tables. All data lives in the `companies` table.
- Never store downloaded files permanently. Process in memory or temp files, then discard.
- Never use an LLM to parse iXBRL files. They are structured — using an LLM is wasteful.
- Never process more than 50 companies per batch without verifying rate limits.
- Never hardcode API keys. Always read from `config/keys.env`.
- Never estimate or fabricate financial values. If a field cannot be extracted, leave it null.
- Always update pipeline_status when done with a company, whether success or failure.
- Always handle Companies House 429 responses by backing off for 5 minutes before retrying.
- Always check for and apply XBRL scale and sign attributes. Ignoring these produces values off by orders of magnitude.
- Never call an LLM as a fallback for extraction. If rule-based parsing fails, flag the company as `failed` and ask the user first. LLM integration will be added later.

---

## When I Ask You To Do Things

- "Process the next batch" → write a script that queries pending companies from Supabase, runs the full pipeline, and writes results back.
- "Test on 5 companies" → same as above but with a limit of 5 and verbose logging so I can verify outputs.
- "Add a new financial field" → tell me what column to add in Supabase, what XBRL tag to look for, and update the extraction logic.
- "Debug a failed company" → query Supabase for that company, re-run the pipeline on just that row with detailed error output.
- "Skip scanned PDFs" → set all scanned_pdf companies to failed and only process ixbrl and electronic_pdf.
- "Show pipeline progress" → query Supabase for counts grouped by pipeline_status.
