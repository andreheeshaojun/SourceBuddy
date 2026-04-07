# iXBRL Extraction — UK Company Filings (Script-Based)

## Overview

This skill extracts structured data from UK Companies House **inline-XBRL (iXBRL)** filings across **two complementary paths** sharing infrastructure (BeautifulSoup parsing, Companies House API, taxonomy-version detection):

- **Part A — Quantitative financial extraction:** all `ix:nonFraction` (numeric) facts → three statement JSONBs, display columns, history series, derived metrics. Tag-name lookup driven.
- **Part B — Qualitative section extraction:** all `ix:nonNumeric` (narrative) facts plus text-layer fallback from the rendered XHTML body → segmented into the 12 statutory sections using the same output contract as PDF-extraction Part B. Hybrid tag-layer + text-layer pipeline.

**Trigger:** Use this skill when given a UK iXBRL filing (`application/xhtml+xml` from Companies House).
- If the user asks for financial data, metrics, or line items → Part A.
- If the user asks for narrative content, risks, MD&A, governance, strategy, or section text → Part B.
- If the user asks for both, run both independently and merge the outputs. Both parts parse the same document once via `BeautifulSoup`; share the soup object to avoid re-parsing.

**Scope:**
- UK Companies House filings only.
- Handles UK GAAP (legacy), FRS 102, FRS 105, and IFRS (as adopted for UK filing), across taxonomy versions from 2014 onwards.
- Part A: designed to process **multiple filings per company** and merge their time series to build 3–5 year history.
- Part B: processes a single filing at a time. Multi-filing narrative merging is out of scope — downstream should handle narrative by filing date.

**Dependencies:** `beautifulsoup4` (HTML parser), `requests` (Companies House API), `supabase` (persistence), `python-dotenv` (credentials).

**Reference implementation:** `Merge Data/pipeline.py` contains the working code. `Merge Data/test_ixbrl.py` is a read-only dry-run harness that calls the parser against real filings without writing to Supabase.

**Design rule for this skill:** every enhancement over the baseline parser must be **observable** (logged to `derivation_log`), **fail-soft** (never crash on a filing that uses unexpected tags or units — skip the fact, log, move on), and **additive** (no change to the output shape that downstream consumers depend on). Version drift in the UK iXBRL taxonomies is the rule, not the exception, and the parser must degrade gracefully rather than silently produce wrong numbers.

---

## Pipeline stages

The full pipeline runs in the following order. Each stage is a named function in `pipeline.py`.

### Stage 1 — Filing discovery

Use the Companies House API to pull the accounts filing history for a company number.

```python
def get_accounts_filings(company_number, count=5):
    """Return the most recent N filings of category='accounts'."""
```

- **Endpoint:** `GET /company/{company_number}/filing-history?category=accounts&items_per_page={count}`
- **Rate limits:** Companies House enforces 600 requests per 5 minutes = 2 req/sec. Use `REQUEST_DELAY = 0.6` seconds between calls and `RATE_LIMIT_BACKOFF = 300` seconds on HTTP 429.
- The pipeline pulls up to 5 filings per company so it can build a time series by merging across years.

### Stage 2 — Document metadata and format detection

For each filing, fetch the document metadata link and classify the filing format.

```python
def get_document_metadata(filing):
    """Follow filing.links.document_metadata and return the JSON descriptor."""

def determine_filing_format(metadata, filing):
    """Return 'ixbrl' | 'electronic_pdf' | 'scanned_pdf'."""
```

**Format rules (in order):**
1. If `metadata.resources` contains `application/xhtml+xml` → **`ixbrl`**
2. Else if `filing.paper_filed == False` → **`electronic_pdf`**
3. Else → **`scanned_pdf`**

Only filings classified as `ixbrl` proceed to Stage 3 in this skill. PDF filings are routed to `PDF-extraction.md`.

### Stage 3 — Download

Download the iXBRL document bytes from Companies House.

```python
def download_document(metadata, filing_format):
    """Download document content. For iXBRL, set Accept: application/xhtml+xml."""
```

- The URL is `metadata.links.document`.
- For iXBRL, send `Accept: application/xhtml+xml` so Companies House serves the inline-XBRL body rather than a rendered PDF.
- On HTTP 429, back off for 300 seconds and retry once.
- Returns `(content_bytes, content_type)`.

### Stage 4 — Parse iXBRL

The core extraction. Five sub-steps. Sub-steps 4.0, 4b-unit, 4b-decimals, and 4b-logging are **additions over the baseline parser** — see "Design rule" above.

#### 4.0 — Detect taxonomy version (new)

Before extracting any facts, identify which taxonomy version the filing uses. This is cheap (one-time XML lookup per filing) and pays for itself in every subsequent step, including debugging.

```python
def _detect_taxonomy_version(soup):
    """Return a version tag like 'frc-uk-2024' | 'frc-uk-2022' | 'frc-uk-2018' |
    'frc-uk-2014' | 'ifrs-<year>' | 'unknown'. Never raises."""
```

**Detection rules:**
1. Look for `<link:schemaRef xlink:href="...">` elements. The href usually contains a version string, e.g. `http://xbrl.frc.org.uk/fr/2024-01-01/core` → `frc-uk-2024`.
2. Fall back to inspecting the document root's `xmlns:*` declarations. Namespace URIs contain the same version strings.
3. If multiple version signals disagree (e.g. a filing mixes FRC UK core with IFRS), record the **primary** taxonomy (the one used by the statement elements, not the entity-info elements) and flag the others in `derivation_log.taxonomy_mix`.
4. If no version can be determined, return `"unknown"` — **do not fail**. Parsing proceeds with the `_any` fallback tier (see 4b).

Record the detected version in `derivation_log.taxonomy_version`. This field must always be populated on every successful parse.

#### 4a — Resolve contexts

iXBRL wraps every fact in a `contextRef` that ties it to a reporting period and an entity. Build a lookup of every context element in the document.

```python
def _resolve_contexts(soup):
    """Return (contexts, years_seen) where contexts[ctx_id] = {
        'year': int,
        'type': 'duration' | 'instant',
        'dimensional': bool,
        'dimension_members': list[str]
    }"""
```

**Rules:**
- Iterate every `xbrli:context` element.
- For `xbrli:period/xbrli:enddate` → `type='duration'`, year = 4-digit year from the end date.
- For `xbrli:period/xbrli:instant` → `type='instant'`, year = 4-digit year from the instant date.
- A context is flagged `dimensional=True` if it contains `xbrli:segment` **and** that segment's dimension members are not all entity-wide defaults. See 4c for the "entity-wide default" recogniser.
- When `dimensional=True`, also store the list of dimension member local names in `dimension_members`. This is required for the smarter-dimension rule in 4c.

#### 4b — Extract field values

Walk every `ix:nonFraction` tag in the document, look up its taxonomy name via the tiered tag map, verify its unit and context, and record its numeric value against the correct year.

```python
def _extract_field_values(soup, contexts, taxonomy_version, units):
    """Returns (field_values, source_tags, unknown_tags, collisions, unit_skips)."""
```

**Rules in exact order:**

1. **Name parsing.** Read `tag["name"]` (e.g. `"core:TurnoverRevenue"`). Split into `prefix` and `local_name` on the first `:`. If there is no prefix, treat as `_any`.

2. **Resolve namespace to family.** Using the document's `xmlns` declarations, map the prefix to a taxonomy family:
   - `xbrl.frc.org.uk/.../core` → `core`
   - `xbrl.frc.org.uk/.../FRS-102` → `frs-102`
   - `xbrl.frc.org.uk/.../FRS-105` → `frs-105`
   - `xbrl.frc.org.uk/.../aurep` → `aurep` (auditor report)
   - `xbrl.frc.org.uk/.../direp` → `direp` (directors report)
   - `xbrl.frc.org.uk/.../bus` → `bus` (business entity)
   - `xbrl.ifrs.org/...` → `ifrs`
   - Unrecognised prefixes → `_any`

3. **Tiered lookup.** Look up the canonical field in `IXBRL_TAG_MAP[family][local_name]`. If not found, fall back to `IXBRL_TAG_MAP["_any"][local_name]`. If still not found, **record the unknown** (family, local_name) in `unknown_tags` Counter and skip.

4. **Resolve context.** Read `tag["contextref"]`. Look up in the contexts table. If unknown or `year is None`, skip.

5. **Dimensional guard (smarter version).** If `ctx["dimensional"] == True`, **check the dimension members**:
   - If all members are in the entity-wide default set (`ConsolidatedGroup`, `EntityOfficersDirectorsMember`, `EntityOfficersMember`, any member whose local name matches `^Consolidated`, `^EntityWide`, `^Group` ), treat the context as non-dimensional and proceed.
   - Otherwise skip the fact — it's a genuine segmental or class-of-share breakdown.

6. **Unit verification (new).** Read `tag["unitref"]`. Resolve against the document's `xbrli:unit` elements. Require the unit measure to be `iso4217:GBP`.
   - If the unit is a non-GBP currency (`iso4217:USD`, `iso4217:EUR`, etc.) → skip the fact, record in `unit_skips` with `(field, unit_measure, year)`.
   - If the unit is non-monetary (shares, pure number, percent) and the field is expected to be monetary → skip and record.
   - If the unit is non-monetary and the field is expected to be non-monetary (e.g. `employees`) → allow.
   - If `unitref` is missing → allow with a warning logged (older filings sometimes omit).

7. **Parse the value with scale OR decimals fallback (new).**
   ```python
   def _parse_ixbrl_value(tag):
       raw = tag.text.strip().replace(",", "").replace(" ", "")
       if not raw or raw == "-": return None
       try: value = float(raw)
       except ValueError: return None

       # Prefer @scale; fall back to -@decimals when scale is absent.
       scale_attr = tag.get("scale")
       decimals_attr = tag.get("decimals")
       if scale_attr is not None:
           scale = int(scale_attr)
       elif decimals_attr is not None and decimals_attr != "INF":
           # decimals="-3" means reported to the thousand -> scale = 3
           d = int(decimals_attr)
           scale = -d if d < 0 else 0
       else:
           scale = 0
       if scale: value *= 10 ** scale

       if tag.get("sign") == "-": value = -value
       return value
   ```

8. **Record the value and its provenance.**
   - `field_values[field][year] = value` — first-wins.
   - `source_tags[field][year] = local_name` — so downstream can audit which tag produced the field.
   - If `field_values[field][year]` is already set from an earlier tag with a **different** value, append the collision to `collisions` list with `(field, year, kept_tag, kept_value, dropped_tag, dropped_value)`.

#### 4c — Build statements and derived metrics

Group the extracted values into three JSONB statements, compute EBITDA and FCF, build history series.

```python
def _build_statement(field_values, field_list, years):
    """Build {year_str: {field: value}} with null for missing fields."""

def parse_ixbrl(content, company_number):
    """Full parser — returns the result dict or None if nothing extracted."""
```

**Statement field lists** (defined at module level):

```python
INCOME_STATEMENT_FIELDS = [
    "revenue", "cost_of_sales", "gross_profit", "distribution_costs",
    "admin_expenses", "other_operating_income", "employee_costs",
    "depreciation", "amortisation", "operating_profit",
    "finance_income", "finance_costs", "profit_before_tax",
    "tax_expense", "profit_after_tax",
]

BALANCE_SHEET_FIELDS = [
    "intangible_assets", "goodwill", "tangible_fixed_assets",
    "right_of_use_assets", "investment_properties", "investments_fixed",
    "total_fixed_assets", "inventories", "trade_receivables",
    "other_receivables", "cash", "short_term_investments",
    "total_current_assets", "total_assets",
    "trade_payables", "other_payables", "short_term_borrowings",
    "long_term_borrowings", "lease_liabilities_noncurrent",
    "provisions", "pension_obligations",
    "total_current_liabilities", "total_noncurrent_liabilities",
    "total_liabilities", "net_assets",
    "share_capital", "share_premium", "retained_earnings",
    "other_reserves", "minority_interest", "total_equity",
]

CASH_FLOW_FIELDS = [
    "operating_cash_flow", "net_cash_operating",
    "capex_ppe", "capex_intangibles", "net_cash_investing",
    "repayment_borrowings", "proceeds_borrowings",
    "proceeds_disposal_ppe", "dividends_paid_cf",
    "lease_payments", "tax_paid", "net_cash_financing",
    "opening_cash", "closing_cash", "net_change_cash",
]
```

**Display columns (scalar, current year):**
- `revenue` — current-year revenue. For non-trading entities (holding companies) where `TurnoverRevenue` is nil, a gap-fill sums `other_operating_income + income_from_group_undertakings` as a proxy.
- `ebitda` — derived as `operating_profit + |depreciation| + |amortisation|`. Missing D&A treated as 0 (not blocking). When both D&A are absent, EBITDA = operating_profit (logged as `ebitda_method: "approximated"`). When only one of D&A is present, the missing one is treated as 0 (logged as `ebitda_method: "partial"`). Key tag additions: `DepreciationAmortisationExpense` (combined D&A, most common), `IncreaseFromDepreciationChargeForYearPropertyPlantEquipment` (PPE note total), `IncreaseFromAmortisationChargeForYearIntangibleAssets` (intangibles note total).
- `employees` — current-year `AverageNumberEmployeesDuringPeriod`.
- `fcf` — derived as `operating_cash_flow - |capex_ppe| - |capex_intangibles|`.
- `cash_conversion` — `fcf / ebitda`. Returns `None` when EBITDA ≤ 0 or FCF < 0 (meaningless ratios).

**History columns (JSONB, one value per year):**
- `revenue_history`, `ebitda_history`, `fcf_history`, `employees_history` — same formulas applied per year where the inputs exist.

**`derivation_log`** — this is where all the observability lives. Every `parse_ixbrl` call emits:
- `taxonomy_version` — from Stage 4.0.
- `taxonomy_mix` — populated only if multiple taxonomies were detected.
- `ebitda`, `fcf` — plain-English formula strings with the actual values substituted, for audit.
- `ebitda_history`, `fcf_history` — the formula descriptions.
- `source_tags` — nested dict `{canonical_field: {year_str: local_tag_name}}` identifying which tag produced each extracted value. This lets downstream answer "why did revenue come through as X?" without re-running the parser.
- `unknown_tags` — top 20 `(family, local_name)` pairs seen in the filing that weren't in the tag map, with their counts. Used for tag-map maintenance: if a field gap shows up repeatedly in this log across multiple filings, it's a candidate for the map.
- `collisions` — list of `(field, year, kept_tag, kept_value, dropped_tag, dropped_value)` for every first-wins decision where the competing tags carried different values. A non-empty collisions list is a signal to review the tag map ordering.
- `unit_skips` — list of `(field_guess, unit_measure, year)` for facts dropped because their unit wasn't `iso4217:GBP`. A non-empty list should trigger manual review of the filing.
- `computation_audit` — the existing trail from `financial_computations`, preserved unchanged.

`parse_ixbrl` returns `None` if no contexts resolve or no mapped tags are found.

### Stage 5 — Multi-filing merge

A single iXBRL filing usually contains 2 years (current + prior year comparatives). To build a 3–5 year history, merge across multiple filings.

```python
def parse_ixbrl_multi(company_number, filings):
    """Fetch up to 5 iXBRL filings, parse each, merge results.
    Returns (merged_result, filing_format, last_accounts_date)."""
```

**Merge rules:**
- Iterate the list of filings in the order Companies House returned them (newest first).
- Skip any non-iXBRL filings in the list.
- The **first** parsed filing becomes the base (`latest_result`). Its `filing_format` and filing date are recorded as the canonical "latest" for the company.
- For each subsequent filing, merge its statements and histories into the base using **earliest-wins** rules:
  - `_merge_statement(target, source)`: add year rows that don't exist in target; for existing year rows, fill only fields where target is `None`.
  - `_merge_history(target, source)`: add year keys that don't exist in target. Never overwrite.
- **Merge `derivation_log` too.** The base's `derivation_log` is kept; any `unknown_tags` / `collisions` / `unit_skips` from subsequent filings are appended under sub-keys `filing_2_unknown_tags`, etc., so nothing is lost.
- Final step: sort all merged statement dicts and history dicts by year ascending.

Result: up to 5 years of time series from 2–5 filings, with no data being overwritten by older filings once captured.

### Stage 6 — Derived metrics and cross-period calculations

```python
def calculate_derived_metrics(data):
    """Run the full financial computation pipeline on extracted data."""
```

Delegates to `financial_computations.compute(data)` and `compute_cross_period(data)` which run, in order:
1. **Sign normalisation** (per year) — ensures expense fields are negative, income fields positive, per taxonomy convention.
2. **Gap-fills** (per year) — derive missing fields from available identities (e.g. if `gross_profit` is missing but `revenue` and `cost_of_sales` are present, compute `gross_profit = revenue + cost_of_sales` since `cost_of_sales` is stored negative).
3. **Single-row derivations** — margins (gross, operating, EBITDA), FCF, leverage ratios.
4. **Validations** — internal consistency checks (`assets == liabilities + equity`, etc.) logged to `derivation_log.computation_audit`.
5. **Cross-period metrics** — YoY growth, CAGR.

After the main computation, two legacy derivations are run as fallbacks (in case the main pipeline left them null):
- `ebitda_margin = ebitda / revenue`
- `cash_conversion = fcf / ebitda`
- `revenue_cagr` and `ebitda_cagr` via `_cagr(history)` on the history dicts.

**`_cagr` rules:**
- Requires ≥2 history points with different years.
- Requires both endpoint values to be positive (CAGR is undefined for negative bases).
- Formula: `(last_val / first_val) ** (1 / n_years) - 1`, rounded to 4 dp.

### Stage 7 — Persist to Supabase

Results are split into two writes:

```python
# Typed columns (revenue, ebitda, pipeline_status, statements, etc.)
update_company(supabase, company_number, column_dict)

# Derived ratios, YoY/CAGR, validation_warnings → metadata JSONB
update_company_metadata_blob(supabase, company_number, metadata_patch)
```

`update_company` performs a direct column UPDATE. `update_company_metadata_blob` calls the `update_company_metadata` Postgres RPC (defined in `sql/update_company_metadata.sql`) which deep-merges into the `metadata` JSONB column only. See `_build_write_payload()` in `pipeline.py` for the exact routing of fields.

---

## Tag map — `IXBRL_TAG_MAP` (tiered structure)

The tag map is organised as a nested dict keyed by taxonomy family, with an `_any` fallback tier:

```python
IXBRL_TAG_MAP = {
    "core":    { "TurnoverRevenue": "revenue", ... },     # FRC UK core
    "frs-102": { ... },                                    # FRS 102 specifics
    "frs-105": { ... },                                    # FRS 105 micro-entities
    "ifrs":    { "RevenueFromContractsWithCustomers": "revenue", ... },
    "aurep":   { ... },                                    # auditor report tags (future qualitative use)
    "direp":   { ... },                                    # directors report tags (future qualitative use)
    "bus":     { ... },                                    # business entity tags
    "_any":    { "Turnover": "revenue", ... },             # taxonomy-agnostic fallbacks
}
```

**Current state (post-refactor from flat layout):**
- **~280 tag keys** across all tiers → **65+ canonical fields**.
- Keys are the **local name** of the iXBRL tag (the part after the namespace colon).
- Sourced from: FRC taxonomy schemas at `xbrl.frc.org.uk`, HMRC CT taxonomy, IFRS taxonomy (as adopted for UK filing), and tag names observed in real Companies House filings.

**Recent additions (high-impact tags found missing during 20-company batch testing):**
- `DepreciationAmortisationExpense` → `depreciation` — combined D&A tag, present in ~60% of filings. Was the #1 reason for `ebitda_approx`.
- `IncreaseFromDepreciationChargeForYearPropertyPlantEquipment` → `depreciation` — PPE note total (non-dimensional context).
- `IncreaseFromAmortisationChargeForYearIntangibleAssets` → `amortisation` — intangibles note total.
- `OtherOperatingIncomeFormat1` / `Format2` → `other_operating_income` — UK GAAP Format 1/2 variants.
- `IncomeFromSharesInGroupUndertakings` → `income_from_group_undertakings` — holding company dividend/management fee income.
- `OtherInterestReceivableSimilarIncomeFinanceIncome` → `finance_income` — FRS 102 interest receivable variant.

**Lookup order at parse time (Stage 4b step 3):**
1. Resolve the tag's prefix → taxonomy family.
2. Look up in `IXBRL_TAG_MAP[family]` if that family exists.
3. If no hit, look up in `IXBRL_TAG_MAP["_any"]`.
4. If still no hit, record in `unknown_tags` and skip.

**Top fields by number of synonymous tags (across all tiers):**

| Canonical field | # tags | Reason for breadth |
|---|---|---|
| `revenue` | 10 | Turnover/Revenue/RevenueFromContractsWithCustomers across UK GAAP, FRS 102, FRS 105, IFRS |
| `total_equity` | 8 | Multiple equity-total tags in different taxonomies |
| `finance_costs` | 7 | Different names in old UK GAAP vs FRS 102 vs IFRS |
| `tax_expense` | 7 | IncomeTaxExpense, TaxOnProfit, CurrentTaxation variants |
| `cash` | 7 | Cash vs CashAndCashEquivalents vs CashBankInHand |
| `short_term_borrowings` | 7 | Current liabilities breakdown |
| `profit_after_tax` | 7 | ProfitLoss, ProfitLossForPeriod, ProfitLossAttributableToOwnersOfParent |

The long tail reflects real taxonomy drift: **the same business concept has different tag names in different taxonomy versions**, and filers stick with the taxonomy version that was current when their template was generated. Without multiple synonymous keys per field, coverage across filings drops sharply.

**Tag map maintenance process:**
1. Run the parser over a batch of filings with `test_ixbrl.py` or the batch pipeline.
2. Inspect the aggregated `derivation_log.unknown_tags` counts across all parses.
3. Any `(family, local_name)` pair that appears in ≥3% of filings and maps clearly to an existing canonical field should be added to the appropriate tier in `IXBRL_TAG_MAP`.
4. Tags that appear frequently but don't fit an existing canonical field are candidates for a new field — discuss with downstream consumers before adding.
5. Tags that appear rarely (< 1% of filings) are usually filer-specific extensions and should not be added.

---

## Output contract

A single company's extracted data is a dict with this shape:

```json
{
  "income_statement": {
    "2023": {"revenue": 12345000, "cost_of_sales": -6789000, ...},
    "2024": {"revenue": 13456000, "cost_of_sales": -7123000, ...}
  },
  "balance_sheet": {
    "2023": {...}, "2024": {...}
  },
  "cash_flow_statement": {
    "2023": {...}, "2024": {...}
  },
  "revenue": 13456000,
  "ebitda": 2100000,
  "employees": 450,
  "fcf": 1750000,
  "revenue_history": {"2020": ..., "2021": ..., "2022": ..., "2023": ..., "2024": ...},
  "ebitda_history":  {...},
  "fcf_history":     {...},
  "employees_history": {...},
  "derivation_log": {
    "taxonomy_version": "frc-uk-2024",
    "taxonomy_mix": null,
    "source_tags": {
      "revenue": {"2023": "TurnoverRevenue", "2024": "TurnoverRevenue"},
      "operating_profit": {"2023": "OperatingProfitLoss", "2024": "OperatingProfitLoss"},
      ...
    },
    "unknown_tags": [
      ["core", "SomeNewTagName", 2],
      ["ifrs", "AnotherUnmappedTag", 1]
    ],
    "collisions": [],
    "unit_skips": [],
    "ebitda": "operating_profit (1234000) + |depreciation| (567000) + |amortisation| (89000)",
    "fcf": "operating_cash_flow (1800000) - |capex_ppe| (45000) - |capex_intangibles| (5000)",
    "computation_audit": { ... per-field trail from financial_computations ... }
  },
  "ebitda_margin": 0.1560,
  "cash_conversion": 0.8333,
  "revenue_cagr": 0.0823,
  "ebitda_cagr": 0.1101
}
```

After `parse_ixbrl_multi` merges across filings, every statement JSONB will carry up to 5 years keyed by string year (`"2020"` … `"2024"`) in ascending order.

**The `derivation_log` structure is the only place where the v2 enhancements are visible to downstream consumers.** The statement JSONBs, display columns, and history columns are unchanged in shape from the baseline parser — this is intentional so the database schema does not need migration.

---

## Edge cases

- **Dimensional contexts with entity-wide default members are now allowed.** Context members matching `^Consolidated`, `^EntityWide`, `^Group`, `EntityOfficersDirectors`, `EntityOfficers`, etc., are treated as non-dimensional. This recovers consolidated facts that legacy filers tag with an explicit consolidation dimension. Genuine segmental breakdowns (class of share, geographic segment, product line) are still skipped.
- **Non-GBP filings are skipped with audit.** If a filing's numeric facts are tagged with a non-GBP unit, those facts are excluded from extraction and recorded in `derivation_log.unit_skips`. A non-empty `unit_skips` list should trigger manual review — we don't auto-convert currencies.
- **Unknown tag names are logged, not silently dropped.** Every iXBRL tag whose local name isn't in the map lands in `derivation_log.unknown_tags` with its family and frequency. This is the canonical source for tag map maintenance.
- **First-wins collisions are now visible.** When two differently-named tags both map to the same `(canonical_field, year)` with different values, the first-encountered value wins (for schema stability) but the loser is recorded in `derivation_log.collisions`.
- **Taxonomy version unknown is a soft failure.** If Stage 4.0 cannot determine the taxonomy version, parsing proceeds using only the `_any` fallback tier. Coverage will be lower but the parser does not crash, and the missing version info is visible in the output (`derivation_log.taxonomy_version = "unknown"`).
- **`@scale` and `@decimals`.** `@scale` wins if both are present. If only `@decimals` is present and it's negative, `scale = -decimals`. If both are absent, the raw value is used as-is.
- **Missing `unitRef`.** Older filings sometimes omit `unitRef` entirely. The fact is allowed through but logged as `derivation_log.unit_missing` (separate from `unit_skips`) for audit.
- **Narrative content is ignored.** `ix:nonNumeric` elements (Strategic Report, Principal Risks, Directors' Report narrative, accounting policies text) are never read. Scope for a future qualitative iXBRL skill — see "Planned follow-up" below.
- **Paper-filed accounts never reach this path.** Small and micro companies typically file on paper, which the pipeline classifies as `scanned_pdf` and routes to `PDF-extraction.md`.

---

## Validation

End-to-end validation is via `test_ixbrl.py`, which:
1. Pulls up to 200 MEDIUM/LARGE pending companies from Supabase.
2. Filters to those whose latest filing is iXBRL format.
3. Calls `parse_ixbrl_multi` on up to 5 of them.
4. For company #1: dumps the full JSON output to stdout.
5. For companies #2–5: prints a compact summary (year count, non-null cell count per statement, derivation_log keys).
6. **Does not write to Supabase.** Read-only.

**v2 verification targets.** Before the enhanced parser can be declared ready:
1. Run `test_ixbrl.py` over ≥20 filings spanning small medium-to-large companies and both FRS 102 and IFRS preparers.
2. Every parsed filing must have a populated `derivation_log.taxonomy_version` that is not `"unknown"` (or if `"unknown"`, the reason must be verifiable manually).
3. `derivation_log.unit_skips` should be empty for ≥95% of filings — a higher rate indicates the unit resolver is broken.
4. `derivation_log.unknown_tags` should be **non-empty** for a meaningful fraction of filings. If it's always empty, the unknown-tag logger isn't wired. If the same tag shows up in a majority of filings with no match, the tag map has a real gap.
5. `derivation_log.collisions` should be empty or rare. Frequent collisions indicate the tag map has ordering bugs.

---

## Planned follow-up

### Wiring `process_batch` to actually parse iXBRL

`pipeline.py:process_batch` currently stops at format triage (classifies and records `filing_format`) and does not call `parse_ixbrl_multi` in the batch loop. All parsing currently runs through `test_ixbrl.py` as a dry-run. A follow-up task is to extend `process_batch` with: "if `fmt == 'ixbrl'`, call `parse_ixbrl_multi`, run `calculate_derived_metrics`, call `update_company` to persist." This is a pure wiring task — all the functions already exist.

*(Part B — Qualitative Section Extraction — is documented below as a first-class section of this skill, not a separate follow-up.)*

---

# PART B — Qualitative Section Extraction (Narrative)

## Purpose

Segment a UK iXBRL filing into the **12 statutory sections** and return raw text per section, matching the output contract of `PDF-extraction.md` Part B exactly so downstream consumers don't care whether a company's narrative came from a PDF or an iXBRL source. The 12 sections are identical to the PDF Part B list: Strategic Report, Section 172, Principal Risks, Viability Statement, Directors' Report, Principal Activity, Going Concern (Directors' Report), Statement of Directors' Responsibilities, Independent Auditor's Report, Accounting Policies, Critical Estimates, Going Concern (Note).

Part B is **hybrid**, combining two sources within the same document:
1. **Tag layer** — `ix:nonNumeric` elements with authoritative FRC taxonomy names. Pre-segmented, clean, no heuristics.
2. **Text layer** — the rendered XHTML body (after stripping `ix:header`) processed with the same statutory phrase locators as PDF Part B.

The tag layer is authoritative for the sections it covers. The text layer fills gaps for sections the FRC taxonomy doesn't tag (notably Strategic Report body, Principal Risks body, Section 172 body when not separately tagged, Directors' Report narrative).

## Validation baselines

Part B has been verified on two filings of the same company spanning the small/medium size boundary:

- **"V" Installations Mechanical Handling Limited (04372047), 2025 filing** — medium company, FRS 102 / 2024-01-01 taxonomy, 283 KB, 106 `ix:nonNumeric` elements across 76 unique tags, 42,504 chars of body text after `ix:header` strip. Tag layer populates accounting_policies (13 policy sub-tags), auditor_report (6 `aurep:*` sub-tags), directors_responsibilities, critical_estimates, principal_activity, going_concern (flag). Text layer populates strategic_report body (4,579 chars via offsets `[912:5491]`), directors_report body (2,721 chars via `[5589:8310]`). Section 172, Principal Risks, Viability correctly resolved as `not_present` per Stage 0 classification.
- **Same company, 2024 filing** — small "Total exemption full accounts", 120 KB, 64 `ix:nonNumeric` elements across 57 unique tags, 16,019 chars of body text. Stage 0 correctly identifies small + filleted + audit-exempt via `direp:Statement*` tag presence (boolean flags missing or empty on this filing). Tag layer populates accounting_policies, critical_estimates, directors_responsibilities, going_concern. Principal Activity, Strategic Report, Directors' Report body, Auditor's Report all correctly resolved as `not_present` (filleted — not delivered).

Both filings demonstrate graceful degradation: the architecture works identically across size boundaries, and every "missing" section has a legitimate statutory reason that Stage 0 can explain.

---

## Pipeline

Part B runs in six fixed stages. Stage 1 produces inputs that every subsequent stage reads.

### Stage 1 — Ingest, taxonomy detect, strip `ix:header`, extract body text

```python
def prepare_ixbrl_document(content: bytes):
    """Parse the iXBRL document and return a dict containing:
       - soup: the parsed BeautifulSoup (shared with Part A)
       - taxonomy_version: via _detect_taxonomy_version (from Part A Stage 4.0)
       - body_text: the full rendered XHTML body as plain text
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")

    taxonomy_version = _detect_taxonomy_version(soup)  # reused from Part A

    # CRITICAL: strip <ix:header> before extracting body text.
    # Without this, soup.get_text() leaks 3kb+ of metadata (boolean flags,
    # unit declarations, hidden numeric facts) at the top of the output,
    # which corrupts offset calculations and pollutes every early locator match.
    soup_for_text = BeautifulSoup(content, "html.parser")
    for header in soup_for_text.find_all("ix:header"):
        header.decompose()
    body_element = soup_for_text.find("body") or soup_for_text
    body_text = body_element.get_text("\n", strip=True)

    return {
        "soup": soup,                       # still contains ix:header for tag walks
        "taxonomy_version": taxonomy_version,
        "body_text": body_text,             # ix:header already removed
    }
```

**Rules:**
- **Always strip `ix:header` from a separate soup copy before extracting body text.** The original `soup` must keep `ix:header` intact so Stage 2 can walk the `ix:nonNumeric` elements inside it. Use two soup objects, not one.
- **Never use `soup.get_text()` directly on the document root** — this returns mixed metadata and body content and is unusable for locator matching. Always take the `<body>` element after header stripping.
- **Taxonomy version detection is shared with Part A Stage 4.0.** Don't re-implement.
- **Do not attempt to use `<h1>`/`<h2>`/`<h3>` elements as section boundaries.** iXBRL documents are laid out with `<div>` + CSS classes; semantic heading elements are almost always absent. Verified on both V Installations filings (zero `<h1>`–`<h4>` elements in each). Text-based regex locators are the only reliable path.

### Stage 2 — Filing classification (flags + declaration tags)

Replaces PDF Part B's heuristic Stage 0 with **authoritative declarations** from the preparer. Two complementary signal sources, because neither alone is reliable across all filing sizes.

#### Source A — Boolean flag tags

Read every declarative `*Truefalse` element and every short-string declaration element:

```python
FLAG_TAGS = {
    "bus:EntityDormantTruefalse":                           "entity_dormant",
    "bus:AccountsStatusAuditedOrUnaudited":                 "audit_status",
    "bus:AccountsType":                                     "accounts_type",
    "bus:ReportIncludesStrategicReportTruefalse":           "has_strategic_report",
    "bus:ReportIncludesDetailedProfitLossStatementTruefalse": "has_detailed_pl",
    "direp:EntityHasTakenExemptionUnderCompaniesActInNotPublishingItsOwnProfitLossAccountTruefalse": "pl_omitted_exemption",
    "core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse": "going_concern_asserted",
    "bus:ApplicableLegislation":                            "applicable_legislation",
    "bus:AccountingStandardsApplied":                       "accounting_standards",
}

def read_flags(soup):
    flags = {}
    for tag_name, field in FLAG_TAGS.items():
        el = soup.find("ix:nonnumeric", {"name": tag_name})
        if el is None:
            flags[field] = None            # MISSING — distinct from False
        else:
            txt = el.get_text(" ", strip=True)
            flags[field] = txt if txt else ""   # empty string kept distinct from None
    return flags
```

**Critical distinction — three possible states per flag:**
- `None` — tag is not present in the document at all. Common on small filings where the flag is not mandatory.
- `""` (empty string) — tag is present but has no content. Seen on small filings where the preparer generated a stub.
- `"true"` / `"false"` / `"audited"` / etc. — tag has an explicit value.

Never collapse these three states into a boolean. Collapse loses information that Stage 4 needs.

#### Source B — `direp:Statement…` presence tags (more reliable for small filings)

Small and filleted filings often have missing or empty boolean flags but ALWAYS carry specific statement declaration tags whose **mere presence** is the signal:

```python
DECLARATION_TAGS = {
    "direp:StatementThatAccountsHaveBeenPreparedInAccordanceWithProvisionsSmallCompaniesRegime": "small_regime",
    "direp:StatementThatCompanyEntitledToExemptionFromAuditUnderSection477CompaniesAct2006RelatingToSmallCompanies": "audit_exempt_s477",
    "direp:StatementThatMembersHaveNotRequiredCompanyToObtainAnAudit":                 "members_waived_audit",
    "direp:StatementThatDirectorsHaveElectedNotToDeliverProfitLossAccountUnderSection4445ACompaniesAct2006": "filleted_s444_5a",
    "direp:StatementThatCompanyHasActedAsSmallCompanyForPreparationAccounts":          "prepared_as_small",
    "direp:StatementThatAccountsHaveBeenPreparedInAccordanceWithProvisionsMediumSizedCompaniesRegime": "medium_regime",
}

def read_declaration_presence(soup):
    return {field: (soup.find("ix:nonnumeric", {"name": tag}) is not None)
            for tag, field in DECLARATION_TAGS.items()}
```

Verified on the V Installations 2024 small filing where flags were missing/empty but `small_regime`, `audit_exempt_s477`, `members_waived_audit`, and `filleted_s444_5a` were all present.

#### Combined classification

```python
def classify_filing(soup):
    flags = read_flags(soup)
    decl  = read_declaration_presence(soup)

    # Size: prefer declaration tags, fall back to flag
    if decl.get("small_regime") or decl.get("prepared_as_small") or decl.get("audit_exempt_s477"):
        size = "small"
    elif decl.get("medium_regime") or flags.get("has_strategic_report") == "true":
        size = "medium"
    else:
        size = "unknown"

    # Mode: filleted takes precedence over everything else
    if decl.get("filleted_s444_5a") or flags.get("pl_omitted_exemption") == "true":
        mode = "filleted"
    else:
        mode = "full"

    # Audit: explicit declaration beats flag beats unknown
    if decl.get("audit_exempt_s477") or decl.get("members_waived_audit"):
        audited = False
    elif flags.get("audit_status") == "audited":
        audited = True
    else:
        audited = None  # genuinely unknown — treat as audit_report not_found (not not_present)

    return {"size": size, "mode": mode, "audited": audited,
            "dormant": flags.get("entity_dormant") == "true",
            "going_concern_asserted": flags.get("going_concern_asserted") == "true",
            "flags": flags, "declarations": decl}
```

The `(size, mode, audited)` triple drives the expected-sections matrix in Stage 5.

### Stage 3 — Tag-layer extraction

Walk every `ix:nonNumeric` element. Map each tag via the narrative tag map to one of the 12 sections. Concatenate multi-tag sections in canonical order and expose the per-tag breakdown as `subsections`.

```python
def extract_tag_layer(soup):
    """Return (sections_dict, source_tags_dict, subsections_dict)."""
    sections = {k: [] for k in TWELVE_SECTIONS}
    source_tags = {k: [] for k in TWELVE_SECTIONS}
    subsections = {k: {} for k in TWELVE_SECTIONS}

    for el in soup.find_all("ix:nonnumeric"):
        name = el.get("name", "")
        section_id, subsection_key = resolve_narrative_tag(name)  # lookup in NARRATIVE_TAG_MAP
        if section_id is None:
            continue
        text = el.get_text(" ", strip=True)
        if not text:
            continue
        sections[section_id].append((AUDITOR_SUBSECTION_ORDER.get(subsection_key, 999), text))
        source_tags[section_id].append(name)
        if subsection_key:
            subsections[section_id][subsection_key] = text

    # Sort sub-tags into canonical order and concatenate
    for sid in sections:
        sections[sid].sort(key=lambda x: x[0])
        sections[sid] = "\n\n".join(t for _, t in sections[sid])

    return sections, source_tags, subsections
```

**Rules:**
- **Tag resolution is prefix-aware**, just like Part A Stage 4b. Use the document's `xmlns:*` declarations to map the prefix (`bus`, `core`, `direp`, `aurep`, …) to a taxonomy family, then look up `(family, local_name)` in the narrative tag map.
- **Multi-tag sections concatenate in canonical order**, not document order. Audit report subsections should always appear in the order: Opinion → Basis for Opinion → Scope → Management Responsibilities → Auditors' Responsibilities → Matters by Exception. Accounting policies can follow document order since preparer ordering is already meaningful.
- **Subsections dict is first-class output**, not debug data. Downstream may want to address individual policies (`accounting_policies.subsections["revenue_recognition"]`) without re-parsing.

### Stage 4 — Text-layer fallback (reuses PDF Part B locators)

For every section that Stage 3 did **not** populate, run the PDF Part B locators against the `body_text` from Stage 1. This handles the sections the FRC taxonomy does not tag — notably Strategic Report body, Principal Risks body, Section 172 body (when not separately tagged), Directors' Report narrative, and KPIs / Business Review / Future Developments sub-sections inside the Strategic Report.

```python
def extract_text_layer(body_text, sections_already_found: set):
    """Run PDF Part B Locators B, E, and A against body_text for any slot
    not already populated by Stage 3."""
    results = {}

    # Locator B — statutory phrase dictionary (same regex dict as PDF Part B)
    heading_hits = find_heading_like_matches(body_text, HARD_ANCHORS)

    # Locator E — statutory sign-off phrases (section end boundaries)
    signoff_offsets = find_signoff_phrases(body_text)

    # Locator A — printed TOC parse if present (same line-pair regex as PDF Part B)
    toc_entries = parse_toc(body_text[:5000])  # top ~5k chars only

    for section_id in TWELVE_SECTIONS:
        if section_id in sections_already_found:
            continue
        # Find this section's heading hit, then find the next sign-off or next-section start
        start_offset, end_offset = resolve_section_boundaries(
            section_id, heading_hits, signoff_offsets, toc_entries)
        if start_offset is not None:
            results[section_id] = {
                "text": body_text[start_offset:end_offset],
                "offsets": [start_offset, end_offset],
                "source": "ixbrl_text",
                "signals": ["hard_anchor", "signoff_boundary"],
            }
    return results
```

**Rules — unchanged from PDF Part B:**
- **Same statutory phrase dictionary** as `PDF-extraction.md` Part B Locator B.
- **Same sign-off phrase regex** as Locator E.
- **Same heading-like filter:** line starts with the phrase after stripping leading numbering, ≤12 words OR line is all-caps.
- **Same parent scoping:** Level-2 sub-sections (KPIs, Section 172, Principal Risks, Principal Activity text fallback) must be located **inside the parent Strategic Report slice**, not against the full document.
- **Offsets replace pages.** Character offsets into `body_text` are the boundary unit. Downstream can re-slice using the same offsets if they preserve the exact `ix:header` strip method from Stage 1.

### Stage 5 — Three-way resolution using the expected-sections matrix

For each of the 12 slots, decide status:

```python
def resolve_section(section_id, tag_result, text_result, classification):
    if tag_result:
        return {"status": "found", "source": "ixbrl_tag", ...}
    if text_result:
        return {"status": "found", "source": "ixbrl_text", ...}
    # Neither path found content — decide between not_present and not_found
    expected = is_section_expected(section_id, classification)
    if not expected:
        return {"status": "not_present", "reason": classification_reason(section_id, classification)}
    return {"status": "not_found"}
```

**Expected-sections matrix** — which `(size, mode, audited)` combinations should have which sections:

| Section | micro | small-filleted | small-full | medium | large |
|---|---|---|---|---|---|
| strategic_report | ✗ | ✗ | ✗ | ✓ | ✓ |
| section_172 | ✗ | ✗ | ✗ | ✗ | ✓ |
| principal_risks | ✗ | ✗ | ✗ | ✓ | ✓ |
| viability_statement | ✗ | ✗ | ✗ | ✗ | listed only |
| directors_report | ✗ | ✗ | ✓ | ✓ | ✓ |
| principal_activity | ✗ | ✗ | ✓ | ✓ | ✓ |
| going_concern_dir_rpt | ✗ | ✗ | ✓ | ✓ | ✓ |
| directors_responsibilities | ✓ | ✓ | ✓ | ✓ | ✓ |
| auditor_report | only if audited=true | ✗ | ✓ if audited | ✓ | ✓ |
| accounting_policies | ✓ | ✓ | ✓ | ✓ | ✓ |
| critical_estimates | ✗ | varies | ✓ | ✓ | ✓ |
| going_concern_note | ✓ | ✓ | ✓ | ✓ | ✓ |

**Rules:**
- **`not_present` must include a human-readable `reason`** so downstream can display "Strategic Report: not required for small filleted filings" rather than a silent null.
- **`audited == None` is a distinct state** from `audited == False`. If we don't know, auditor_report is `not_found`, not `not_present`. Surfacing "we don't know if this company is audited" is preferable to silently hiding it.
- **Stage 5 never fills content** — it only decides status based on what Stages 3 and 4 produced and what Stage 2's classification says was expected.

### Stage 6 — Emit

Output contract matches PDF-extraction Part B with three additions: `source`, `offsets`, `subsections`.

```python
def emit(sections_tag, sections_text, classification, taxonomy_version, source_tags, subsections):
    output = {"_classification": classification, "_taxonomy": taxonomy_version}
    for section_id in TWELVE_SECTIONS:
        slot = resolve_section(section_id, ...)
        if slot["status"] == "found":
            output[section_id] = {
                "status": "found",
                "text": slot["text"],
                "source": slot["source"],              # "ixbrl_tag" | "ixbrl_text" | "ixbrl_flag"
                "offsets": slot.get("offsets"),        # None for tag source, [start,end] for text
                "pages": None,                          # iXBRL has no pages; null for contract compat
                "signals": slot["signals"],
                "source_tags": source_tags[section_id], # list of "family:local_name"
                "subsections": subsections[section_id] or None,
                "confidence": "high" if len(slot["signals"]) >= 2 else "medium",
            }
        else:
            output[section_id] = {
                "status": slot["status"],               # "not_present" | "not_found"
                "reason": slot.get("reason"),
                "source": None, "text": None, "offsets": None, "pages": None,
            }
    return output
```

**Field semantics:**
- `source`: which path populated the slot.
  - `"ixbrl_tag"` — content came from one or more `ix:nonNumeric` elements.
  - `"ixbrl_text"` — content came from the rendered XHTML body via regex locators.
  - `"ixbrl_flag"` — content is a boolean assertion (going_concern slot when only the flag is set).
- `offsets`: `[char_start, char_end]` into the Stage 1 `body_text`. Present only for `ixbrl_text` source.
- `pages`: always `null` for iXBRL — included to keep the output contract type-stable with PDF Part B.
- `source_tags`: list of `(family:local_name)` strings for the tag path. Empty for `ixbrl_text` slots.
- `subsections`: per-tag dict for multi-tag slots. `None` for single-tag or text-source slots.
- `confidence`: `high` when ≥2 independent locators agreed, `medium` otherwise. Always `high` for tag-source slots because the tag is authoritative.

---

## Narrative tag map

The narrative tag map is keyed by `(family, local_name) → (section_id, subsection_key)`. The second element is the subsection key used for the per-tag breakdown in the `subsections` output field; it is `None` for single-tag sections.

**Observed coverage from V Installations 2025 (medium, FRS 102 2024-01-01):**

```python
NARRATIVE_TAG_MAP = {
    # ----- Principal Activity -----
    ("bus",    "DescriptionPrincipalActivities"):                                    ("principal_activity", None),

    # ----- Directors' Responsibilities -----
    ("direp",  "StatementThatDirectorsAcknowledgeTheirResponsibilitiesUnderCompaniesAct"): ("directors_responsibilities", None),
    ("direp",  "StatementOnQualityCompletenessInformationProvidedToAuditors"):      ("directors_responsibilities", "audit_info_quality"),

    # ----- Auditor's Report (6 sub-sections) -----
    ("aurep",  "OpinionAuditorsOnEntity"):                                          ("auditor_report", "opinion"),
    ("aurep",  "BasisForOpinionAuditorsOnEntity"):                                  ("auditor_report", "basis_for_opinion"),
    ("aurep",  "StatementOnScopeAuditReport"):                                      ("auditor_report", "scope"),
    ("aurep",  "StatementResponsibilitiesManagementThoseChargedWithCorporateGovernance"): ("auditor_report", "management_responsibilities"),
    ("aurep",  "StatementAuditorsResponsibilitiesRelatingToOtherInformation"):      ("auditor_report", "auditors_responsibilities_other_info"),
    ("aurep",  "StatementOnMattersOnWhichAuditorReportsByException"):               ("auditor_report", "matters_by_exception"),

    # ----- Critical Estimates and Judgements -----
    ("core",   "GeneralDescriptionCriticalEstimatesJudgements"):                    ("critical_estimates", None),

    # ----- Accounting Policies (per-topic, multi-tag concatenation) -----
    ("core",   "RevenueRecognitionPolicy"):                                         ("accounting_policies", "revenue_recognition"),
    ("core",   "PropertyPlantEquipmentPolicy"):                                     ("accounting_policies", "ppe"),
    ("core",   "ProvisionsPolicy"):                                                 ("accounting_policies", "provisions"),
    ("core",   "ImpairmentNon-financialAssetsPolicy"):                              ("accounting_policies", "impairment"),
    ("core",   "FinancialInstrumentsRecognitionMeasurementPolicy"):                 ("accounting_policies", "financial_instruments"),
    ("core",   "CurrentIncomeTaxPolicy"):                                           ("accounting_policies", "current_tax"),
    ("core",   "DeferredTaxPolicy"):                                                ("accounting_policies", "deferred_tax"),
    ("core",   "DefinedContributionPensionsPolicy"):                                ("accounting_policies", "pensions_dc"),
    ("core",   "DefinedBenefitPensionsPolicy"):                                     ("accounting_policies", "pensions_db"),
    ("core",   "LesseeFinanceLeasePolicy"):                                         ("accounting_policies", "leases_finance_lessee"),
    ("core",   "LessorOperatingLeasePolicy"):                                       ("accounting_policies", "leases_operating_lessor"),
    ("core",   "ForeignCurrencyTranslationOperationsPolicy"):                       ("accounting_policies", "foreign_currency"),
    ("core",   "FunctionalPresentationCurrencyPolicy"):                             ("accounting_policies", "functional_currency"),
    ("core",   "GovernmentGrantsOtherGovernmentAssistancePolicy"):                  ("accounting_policies", "government_grants"),
    ("core",   "StatementComplianceWithApplicableReportingFramework"):              ("accounting_policies", "compliance_framework"),
    ("core",   "GeneralDescriptionBasisMeasurementUsedInPreparingFinancialStatements"): ("accounting_policies", "basis_of_preparation"),

    # ----- Section 172 (large filings only) -----
    ("core",   "StatementOnSection172CompaniesAct2006"):                            ("section_172", None),

    # ----- Principal Risks (when tagged; rare below large) -----
    ("core",   "DescriptionPrincipalRisksUncertaintiesFacingEntity"):               ("principal_risks", None),

    # ----- Viability Statement (large listed only) -----
    ("core",   "StatementViabilityEntity"):                                         ("viability_statement", None),

    # ----- Going Concern (flag only — no narrative tag) -----
    # Handled via FLAG_TAGS["core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse"]
    # in Stage 2, emitted as source="ixbrl_flag" in Stage 6.

    # ----- Reserves and share capital (not in the 12 sections; reserved for future) -----
    # ("core",  "DescriptionNaturePurposeReservesWithinEquity"):                    ("reserves_note", None),
    # ("core",  "DescriptionRightsPreferencesRestrictionsAttachingToClassShareCapital"): ("share_capital_note", None),
}

AUDITOR_SUBSECTION_ORDER = {
    "opinion": 1,
    "basis_for_opinion": 2,
    "scope": 3,
    "management_responsibilities": 4,
    "auditors_responsibilities_other_info": 5,
    "matters_by_exception": 6,
}
```

**Maintenance rules:**
- Map organised by taxonomy family (`bus`, `core`, `direp`, `aurep`) so a filing using a different family falls through to the `_any` tier cleanly.
- When extending: always observe the tag in a real filing first. Do not add speculative mappings from taxonomy documentation — observed coverage is what matters.
- **Unknown narrative tags must be logged** to `derivation_log.unknown_narrative_tags` per parse, same pattern as Part A's unknown-tag telemetry. This is how the map grows over time.
- Auditor sub-sections have a canonical display order (Opinion first, Matters by Exception last) — preserve via `AUDITOR_SUBSECTION_ORDER` at concatenation time.

---

## Output contract

```json
{
  "strategic_report": {
    "status": "found",
    "text": "Strategic Report\nYear ended 30 April 2025\nThe Directors...",
    "source": "ixbrl_text",
    "offsets": [912, 5491],
    "pages": null,
    "signals": ["hard_anchor", "signoff_boundary"],
    "source_tags": [],
    "subsections": null,
    "confidence": "high"
  },
  "principal_activity": {
    "status": "found",
    "text": "Principle Activities At V Installations Mechanical Handling Ltd...",
    "source": "ixbrl_tag",
    "offsets": null,
    "pages": null,
    "signals": ["ixbrl_tag"],
    "source_tags": ["bus:DescriptionPrincipalActivities"],
    "subsections": null,
    "confidence": "high"
  },
  "auditor_report": {
    "status": "found",
    "text": "We have audited...\n\nWe conducted our audit in accordance with...\n\n...",
    "source": "ixbrl_tag",
    "source_tags": [
      "aurep:OpinionAuditorsOnEntity",
      "aurep:BasisForOpinionAuditorsOnEntity",
      "aurep:StatementOnScopeAuditReport",
      "aurep:StatementResponsibilitiesManagementThoseChargedWithCorporateGovernance",
      "aurep:StatementAuditorsResponsibilitiesRelatingToOtherInformation",
      "aurep:StatementOnMattersOnWhichAuditorReportsByException"
    ],
    "subsections": {
      "opinion": "We have audited the financial statements of...",
      "basis_for_opinion": "We conducted our audit in accordance with...",
      "scope": "Our objectives are to obtain reasonable assurance...",
      "management_responsibilities": "As explained more fully in the directors'...",
      "auditors_responsibilities_other_info": "The other information comprises...",
      "matters_by_exception": "In the light of the knowledge and understanding..."
    },
    "confidence": "high"
  },
  "accounting_policies": {
    "status": "found",
    "text": "...concatenated from 13 per-topic tags...",
    "source": "ixbrl_tag",
    "source_tags": ["core:RevenueRecognitionPolicy", "core:PropertyPlantEquipmentPolicy", ...],
    "subsections": {
      "revenue_recognition": "Turnover is measured at the fair value...",
      "ppe": "Tangible assets are initially recorded at cost...",
      "provisions": "Provisions are recognised when the entity has an obligation...",
      ...
    },
    "confidence": "high"
  },
  "section_172": {
    "status": "not_present",
    "reason": "Not required for small/medium filings — only large companies per s414CZA",
    "source": null, "text": null, "offsets": null, "pages": null
  },
  "principal_risks": {
    "status": "not_present",
    "reason": "Not required for small companies — Strategic Report exemption",
    "source": null, "text": null, "offsets": null, "pages": null
  },
  "_classification": {
    "size": "medium",
    "mode": "full",
    "audited": true,
    "dormant": false,
    "going_concern_asserted": true
  },
  "_taxonomy": "frc-uk-2024"
}
```

---

## Part B — Edge cases

- **Small filleted filings.** Verified on V Installations 2024 (`direp:StatementThatDirectorsHaveElectedNotToDeliverProfitLossAccountUnderSection4445A...`). Directors' Report body, Auditor's Report body, and Principal Activity are all legitimately `not_present` with reason "filleted — Directors' Report not delivered". Do NOT mark these as `not_found`.
- **Missing boolean flags on small filings.** `bus:ReportIncludesStrategicReportTruefalse` may be entirely absent from small-company filings. Do not treat absence as `false` — treat it as `None` and let the declaration-tag presence (`direp:Statement*SmallCompaniesRegime`) drive classification.
- **Empty-string flag values.** Some preparers generate the flag tag with no content (`bus:AccountsStatusAuditedOrUnaudited` empty on the small V Installations filing). Treat as `None` for classification, log to `_classification.flag_warnings` so downstream can see the data quality issue.
- **Principal Activity missing in filleted smalls.** `bus:DescriptionPrincipalActivities` is only emitted when the Directors' Report is delivered. For filleted filings the tag is absent — mark the slot `not_present` with reason "filleted".
- **Going Concern slot is almost always flag-only.** The FRC taxonomy has `core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse` but no dedicated narrative tag. The slot is populated from the flag (source `"ixbrl_flag"`) with a synthetic text like `"Going concern basis: asserted"`. Narrative prose about going concern lives in the accounting policies section and should not be duplicated into the going_concern slot.
- **Strategic Report body when the flag is true but no tag exists.** Common case for medium companies. Stage 3 returns empty for strategic_report, Stage 4 text fallback takes over using the statutory phrase dictionary. Verified on V Installations 2025 — text slice `[912:5491]` produced 4,579 chars of clean body.
- **KPIs, Principal Activity (text fallback), Section 172 (text fallback), Business Review, Future Developments.** All Level-2 sub-sections inside the Strategic Report body slice. Run a second locator pass **inside the parent slice** using the same regex dictionary — parent scoping is mandatory to avoid false matches.
- **Auditor's Report when audit-exempt.** For audit-exempt small filings, all `aurep:*` tags are absent. Stage 3 returns empty, Stage 4 text layer returns empty, Stage 5 consults classification: `audited == False` → `not_present` with reason "audit-exempt under s477".
- **No semantic HTML headings.** Verified on both V Installations filings — zero `<h1>`–`<h4>` elements. Do not attempt heading-based segmentation. The text layer must use regex locators exclusively.
- **Taxonomy version drift.** The tag map is keyed by `(family, local_name)`. Older taxonomy versions use slightly different local names (`ProfitLoss*` vs `ProfitOrLoss*`, `Description*` vs `General*Description*`). When extending the map, add version-specific entries to the appropriate family tier — do NOT create a combined entry that guesses.
- **Charity filings.** Use `char` and related FRC charity taxonomy families. Out of scope for v1 of Part B — charity filings will map to an empty narrative set until the charity family is added.

---

## Part B — Verification targets

Before declaring Part B ready for production:

1. Run Part B over ≥10 filings spanning the size spectrum (3 small, 2 small-filleted, 3 medium, 2 large where available). Verified baseline: 2 filings (V Installations medium 2025, V Installations small 2024).
2. For every filing, `_classification.size` must match the Companies House `accounts_category` metadata.
3. For every filing, the sum of `found + not_present + not_found` must equal 12.
4. `not_found` count should be 0 on representative filings — any non-zero `not_found` indicates either a classification gap (Stage 2) or a tag map gap (Stage 3).
5. For filings with `_classification.size == "large"` and `mode == "full"`, the `strategic_report`, `directors_report`, and `auditor_report` slots must all resolve to `found` via either tag or text path.
6. `unknown_narrative_tags` should be non-empty on a meaningful fraction — if it's always empty, the unknown-tag logger isn't wired.
7. Compare the `strategic_report.text` output of an iXBRL filing against the same company's PDF Part B output. They should be the same prose (allowing for rendering differences).

---

## Relationship to PDF-extraction Part B

Both skills produce the **identical output contract** — the 12-slot dict with the same `status` / `text` / `source` / `signals` / `subsections` fields. Downstream consumers must not need different code paths for iXBRL-sourced and PDF-sourced narrative. The differences are all in auxiliary metadata:

| Field | PDF Part B | iXBRL Part B |
|---|---|---|
| `source` | `"pdf"` | `"ixbrl_tag"` \| `"ixbrl_text"` \| `"ixbrl_flag"` |
| `pages` | `[start, end]` 1-indexed | `null` |
| `offsets` | not used | `[char_start, char_end]` into body text (text source only) |
| `signals` | `"toc"`, `"hard_anchor"`, `"running_header"`, `"all_caps_first_line"`, `"signoff_phrase"`, `"numbered_note"` | `"ixbrl_tag"`, `"hard_anchor"`, `"signoff_boundary"`, `"toc"` |
| `source_tags` | not used | list of `"family:local_name"` |
| `subsections` | not used | dict for multi-tag slots (auditor_report, accounting_policies, directors_responsibilities) |

Where both skills run on the same company (medium+ filings that have both a PDF and an iXBRL on Companies House), prefer iXBRL output for the sections that iXBRL populates via the tag layer — they are authoritative and require no heuristics. Fall back to PDF for sections where iXBRL returns `not_present` or `not_found`. Merge strategy is downstream's responsibility, not either skill's.
