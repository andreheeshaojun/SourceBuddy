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

This is the primary table. One row per company. It serves both the pipeline and the frontend.

### Typed columns

**Identity & classification:**
| Column | Type | Source |
|---|---|---|
| `company_number` | text, PK | CSV import |
| `company_name` | text, NOT NULL | CSV import |
| `company_status` | text | CSV import (via migration) |
| `accounts_category` | text | CSV import (via migration) |
| `sector` | text | Manual / frontend |
| `ownership` | text | Manual / frontend |
| `location` | text | Manual / frontend |

**Pipeline tracking:**
| Column | Type | Source |
|---|---|---|
| `pipeline_status` | text | Pipeline. Values: `pending`, `extracted`, `failed`, `no_filing` |
| `filing_format` | text | Pipeline. Values: `ixbrl`, `electronic_pdf`, `scanned_pdf` |
| `last_accounts_date` | date | Pipeline. Date of most recent filing processed |

**Financial display metrics (current year):**
| Column | Type | Source |
|---|---|---|
| `revenue` | numeric | Pipeline (iXBRL extraction) |
| `ebitda` | numeric | Pipeline (derived: operating_profit + depreciation + amortisation; fallback: operating_profit alone when D&A unavailable) |
| `ebitda_margin` | numeric | Pipeline (derived: ebitda / revenue) |
| `fcf` | numeric | Pipeline (derived: operating_cash_flow - capex) |
| `cash_conversion` | numeric | Pipeline (derived: fcf / ebitda; None when EBITDA ≤ 0 or FCF < 0) |
| `employees` | integer | Pipeline (iXBRL: `AverageNumberEmployeesDuringPeriod` tag; PDF: `extract_employees_from_notes()` parses Staff costs / Employees note, sums sub-categories if no total row) |
| `revenue_cagr_5y` | numeric | Pipeline (5-year compound annual growth rate) |

**History (JSONB, keyed by year string e.g. `{"2021": 1500000, "2022": 1700000}`):**
| Column | Type |
|---|---|
| `revenue_history` | jsonb |
| `ebitda_history` | jsonb |
| `fcf_history` | jsonb |
| `employees_history` | jsonb |

**Full financial statements (JSONB, keyed by year, each year containing all line items):**
| Column | Type | Contents |
|---|---|---|
| `income_statement` | jsonb | revenue, cost_of_sales, gross_profit, operating_profit, depreciation, amortisation, finance_costs, tax_expense, profit_before_tax, profit_after_tax, etc. |
| `balance_sheet` | jsonb | total_assets, total_liabilities, net_assets, cash, trade_receivables, trade_payables, borrowings, equity components, etc. |
| `cash_flow_statement` | jsonb | operating_cash_flow, capex_ppe, capex_intangibles, net_cash_investing, net_cash_financing, opening/closing cash, etc. |

**Other JSONB columns:**
| Column | Type | Contents |
|---|---|---|
| `derivation_log` | jsonb | Audit trail: how EBITDA, FCF, etc. were calculated, plus computation_audit (sign corrections, gap-fills) |
| `company_profile` | jsonb | Reserved for enrichment data |
| `metadata` | jsonb | Overflow storage — see below |

**Timestamps:**
| Column | Type |
|---|---|
| `created_at` | timestamptz |
| `updated_at` | timestamptz |

### The `metadata` JSONB column

This column holds two categories of data:

**1. CSV-imported fields that don't have typed columns:**
SIC codes (`sic_code_1` through `sic_code_4`), address fields (`address_line_1`, `post_town`, `postcode`, `county`, `country`, etc.), `incorporation_date`, `company_category`, `country_of_origin`, account reference dates.

**2. Pipeline-computed derived ratios and cross-period metrics:**
`gross_margin_pct`, `operating_margin_pct`, `net_margin_pct`, `return_on_assets`, `return_on_equity`, `total_capex`, `capex_to_revenue`, `net_debt`, `net_debt_to_ebitda`, `current_ratio`, `quick_ratio`, `debt_to_equity`, `interest_cover`, `asset_turnover`, `revenue_per_employee`, `profit_per_employee`, `revenue_yoy_growth`, `ebitda_yoy_growth`, `profit_yoy_growth`, `revenue_cagr`, `ebitda_cagr`, `revenue_cagr_3yr`, `revenue_cagr_5yr`, `validation_warnings`.

### Conventions

- All monetary values are in GBP, stored as plain numbers (no formatting).
- Margins and percentages are stored as decimals (0.15 means 15%). Frontend multiplies by 100 for display.
- JSONB history fields are objects keyed by year string, e.g. `{"2021": 1500000, "2022": 1700000}`.
- JSONB statement fields are objects keyed by year string, each year containing a dict of line items.
- The pipeline writes display metrics to typed columns and derived ratios to `metadata`. Use `update_company()` for typed columns and `update_company_metadata_blob()` for the metadata JSONB.

---

## Pipeline Logic — How Extraction Works

The pipeline processes companies in batches of 50. For each company, the sequence is:

**1. Get filing history.**
Hit the Companies House filing history endpoint for the company_number. Fetch the 5 most recent accounts filings to build multi-year history. If no accounts filings exist, mark the company as `no_filing` and move on.

**2. Check the filing metadata.**
Each filing item has a `paper_filed` boolean and a link to document metadata. The document metadata response contains a `resources` object listing available formats.

**3. Triage the document type.** This determines the parsing strategy:
- If `application/xhtml+xml` is listed in resources → this is iXBRL (structured data). Set filing_format to `ixbrl`. This is the cheapest and most reliable path.
- If only `application/pdf` is available and `paper_filed` is false → this is an electronically generated PDF with a text layer. Set filing_format to `electronic_pdf`.
- If only `application/pdf` and `paper_filed` is true → this is a scanned document with no text layer. Set filing_format to `scanned_pdf`. This is the hardest and most expensive to process.

**4. Download and parse based on format:**

- **iXBRL (implemented):** Parse all 5 filings via `parse_ixbrl_multi`. Financial values are embedded in inline XBRL tags (ix:nonfraction) with taxonomy names identifying what each number represents. The tag-to-field mapping covers UK GAAP, FRS 102, FRS 105, and IFRS taxonomies (~200 tag mappings). Watch for `scale` attributes (scale="3" means thousands, scale="6" means millions) and `sign` attributes for negation. Context references distinguish current year from prior year. Data from all 5 filings is merged to build multi-year history. Do not use an LLM for iXBRL — it is already structured data.

- **Electronic PDF (not yet implemented):** PDF extraction is planned but not wired into the pipeline. Companies with this format get their `filing_format` recorded and are skipped. See `Claude skills/sme-extraction.md` for the planned approach.

- **Scanned PDF (not yet implemented):** Same as electronic PDF — format recorded, company skipped for now.

**5. Compute derived metrics** (via `financial_computations.py`).

The computation pipeline runs on the extracted data in this order:
1. **Sign normalisation** — flip values that violate sign conventions (e.g. costs must be negative, revenue must be positive).
2. **Gap-fills** — derive missing values algebraically (e.g. gross_profit = revenue + cost_of_sales, net_assets = total_assets + total_liabilities). 15 rules, never overwrites existing values.
3. **Single-row derivations** — compute 20 analytical metrics per year: margins (gross, operating, net, EBITDA), returns (ROA, ROE), cash flow metrics (FCF, cash conversion, capex ratios), leverage (net debt, debt-to-equity, interest cover), liquidity (current ratio, quick ratio), efficiency (asset turnover, revenue/profit per employee).
   - **EBITDA** = `operating_profit + |depreciation or 0| + |amortisation or 0|`. Single rule — missing D&A treated as 0, not blocking. Logged as: `ebitda_method: "approximated"` (both D&A absent), `"partial"` (one of D&A absent), or omitted (both present, standard calculation). Key iXBRL tags for D&A: `DepreciationAmortisationExpense` (combined, most common), `IncreaseFromDepreciationChargeForYearPropertyPlantEquipment` (PPE note total), `IncreaseFromAmortisationChargeForYearIntangibleAssets`.
   - **Revenue gap-fill for non-trading entities**: when `TurnoverRevenue` is nil/absent but `other_operating_income` or `income_from_group_undertakings` exist, revenue is gap-filled as their sum (holding companies, management fee entities).
   - **Cash conversion** = `fcf / ebitda`, but guarded: returns `None` when EBITDA ≤ 0 (meaningless ratio) or when FCF < 0. The legacy fallback in `calculate_derived_metrics` applies the same guards (ebitda > 0, fcf < 0 → None).
   - **PDF D&A sourcing**: For scanned PDFs, depreciation/amortisation come from cash flow add-back lines or notes pages. `_normalise_pdf_extraction` lifts D&A values from the nested `notes.creditors.depreciation` structure into the `income_statement` year rows where `compute()` can find them.
4. **Validations** — 6 consistency checks (balance sheet balances, P&L ties out, cash flow reconciles). Failures are logged to `validation_warnings`, not treated as extraction errors.
5. **Cross-period metrics** — YoY growth (revenue, EBITDA, profit) and CAGR (3-year, 5-year) computed across years.

**6. Write results to Supabase.**

Results are split into two writes:
- **Typed columns** (via `update_company`): revenue, ebitda, ebitda_margin, fcf, cash_conversion, employees, revenue_cagr_5y, all history JSONB, all statement JSONB, derivation_log, filing_format, last_accounts_date, pipeline_status = `extracted`. **All typed columns are always written, including `None` values**, so that re-runs clear stale data from previous extractions (e.g. a cash_conversion that was incorrectly set).
- **Metadata JSONB** (via `update_company_metadata_blob`): all derived ratios, YoY growth rates, CAGR variants, validation_warnings.

If any step fails, set `pipeline_status` to `failed` and log the error.

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
- Always update `pipeline_status` when done with a company, whether success or failure.
- Always handle Companies House 429 responses by backing off for 5 minutes before retrying.
- Always check for and apply XBRL scale and sign attributes. Ignoring these produces values off by orders of magnitude.
- Write display metrics to typed columns. Write derived ratios and cross-period metrics to the `metadata` JSONB column.
- Use `update_company()` for typed column writes and `update_company_metadata_blob()` for metadata JSONB writes. Do not mix them.

---

## When I Ask You To Do Things

- "Process the next batch" → run `process_batch` which queries pending companies from Supabase, runs the full pipeline (extraction + computation), and writes results back.
- "Test on 5 companies" → same as above but with a limit of 5 and verbose logging so I can verify outputs.
- "Add a new financial field" → tell me what column to add in Supabase (if typed) or what key to add in metadata, what XBRL tag to look for, and update the extraction logic.
- "Debug a failed company" → query Supabase for that company, re-run the pipeline on just that row with detailed error output.
- "Skip scanned PDFs" → set all scanned_pdf companies to failed and only process ixbrl and electronic_pdf.
- "Show pipeline progress" → query Supabase for counts grouped by `pipeline_status`.
