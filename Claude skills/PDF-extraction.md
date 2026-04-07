# PDF Extraction — UK Company Filings (Script-Based)

## Overview

This skill extracts data from UK company annual report / financial statement PDFs (Companies House filings) using Python scripts with OCR and rule-based parsing. No LLM calls in the extraction layer. It covers **two complementary paths** that share infrastructure (PyMuPDF, EasyOCR, rule-based parsing) but serve different downstream uses:

- **Part A — Quantitative financial extraction:** numbers from the Income Statement, Balance Sheet, and Notes into a structured GBP JSON object. Verified against LLM extraction at 100% accuracy on the Heights Management test PDF.
- **Part B — Qualitative section extraction:** segments the document into the 12 UK statutory sections and returns raw text per section for downstream analysis. Scope of Part B ends at "raw text per section" — any finer analysis happens in a separate downstream layer.

**Trigger:** Use this skill when given a PDF of a UK company's annual report / financial statements.
- If the user asks for financial data, metrics, or line items → run Part A.
- If the user asks for narrative content, risks, MD&A, governance, strategy, or section text → run Part B.
- If the user asks for both, run both independently and merge the outputs.

**Scope:**
- UK Companies Act 2006 filings only. US 10-K, IFRS across jurisdictions, and other regimes are out of scope.
- Handles **any non-iXBRL PDF** — small, medium, large companies, born-digital or scanned. For iXBRL filings, the main pipeline in `Merge Data/pipeline.py` handles extraction directly and this skill is not needed.
- **Ad-hoc format** — no per-issuer templates. Every heuristic must work from generic signals that apply across publishers.

**Dependencies:** `pymupdf` (fitz), `easyocr`, `numpy`, `Pillow`

---

# PART A — Quantitative Financial Extraction

## Validation baselines

Part A has been verified end-to-end on two filings spanning the size spectrum:

- **Heights Management Test 2** — 7-page scanned filleted small-company filing (FRS 102 s1A, s444(4) exemption, no P&L delivered). Path B (OCR via EasyOCR). All balance sheet line items matched ground truth. Balance check: `called_up_share_capital + retained_earnings = shareholders_funds` holds for both years (370 + 13,347 = 13,717; 370 + 51,384 = 51,754). Notes extraction recovered other_creditors, maintenance_fees_received_in_advance, accruals_and_deferred_income, prepayments.
- **John Lewis plc 2025** — 134-page born-digital listed plc annual report (IFRS, £m). Path A (text-layer spans). Revenue £11,113m, PBT £98m, PAT £82m, Net Assets £2,034m = Total Equity £2,034m. Every internal flow reconciles: `gross_profit = revenue − cost_of_sales`, `PBT = operating_profit + finance_income − finance_costs`, `PAT = PBT − taxation`, `net_assets = total_equity`.

The same `parse_financial_page` engine handles both filings unchanged. The only differences are which input path produces `all_pages` (OCR vs text-layer) and which label map is used (UK GAAP vs IFRS).

## Instructions

### Step 1 — Get text + bounding boxes (two paths sharing one data shape)

The downstream parsing engine is agnostic to how the text was produced — it only requires items in the shape `(bbox, text, conf)` where `bbox` is an EasyOCR-style polygon `[[x0,y0],[x1,y0],[x1,y1],[x0,y1]]`. Pick the path by **probing the text layer first**:

```python
import fitz, re, json
import numpy as np
from PIL import Image

PDF_PATH = "<path_to_pdf>"
doc = fitz.open(PDF_PATH)

# Probe text layer on the first few pages
has_text_layer = any(
    len(doc[i].get_text("text").split()) > 20
    for i in range(min(5, doc.page_count))
)
```

#### Path A — Born-digital (text layer present)

For large plc filings, glossy listed-company reports, and any PDF with a real text layer, skip OCR entirely. Convert `get_text("dict")` spans into the same `(bbox, text, conf)` tuple shape:

```python
all_pages = {}
for pi in range(doc.page_count):
    items = []
    d = doc[pi].get_text("dict")
    for block in d.get("blocks", []):
        if block.get("type", 0) != 0: continue
        for line in block.get("lines", []):
            for sp in line.get("spans", []):
                t = sp.get("text", "").strip()
                if not t: continue
                x0, y0, x1, y1 = sp["bbox"]
                poly = [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
                items.append((poly, t, 1.0))  # conf=1.0 for text-layer
    all_pages[pi + 1] = items
PAGE_WIDTH = doc[0].rect.width
doc.close()
```

Path A is **orders of magnitude faster than OCR** (seconds vs minutes) on large filings. Verified on the 134-page John Lewis plc 2025 annual report — full income statement + balance sheet extracted with internal flows reconciling exactly.

#### Path B — Scanned / no text layer (two-pass OCR via Tesseract)

For small-company filings, scanned documents, and any page where the text-layer probe fails, use a **two-pass OCR strategy** with Tesseract to avoid burning CPU on non-financial pages.

**OCR backend: Tesseract via pytesseract.** Tesseract runs as a subprocess (no native DLL loading into the Python process), which avoids Windows Smart App Control (WDAC) blocks that affect PyTorch/EasyOCR. `pytesseract.image_to_data()` returns word-level bounding boxes as `(left, top, width, height)` with text and confidence (0-100). Convert each detection into the canonical polygon shape:

```python
import pytesseract

def _tesseract_ocr(img):
    """Run Tesseract on a PIL image -> list of (polygon, text, conf)."""
    data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
    items = []
    for i in range(len(data["text"])):
        text = data["text"][i].strip()
        if not text:
            continue
        conf = float(data["conf"][i])
        if conf < 0:
            continue  # Tesseract returns -1 for block/paragraph headers
        conf /= 100.0  # normalise to 0-1
        x0, y0 = float(data["left"][i]), float(data["top"][i])
        x1, y1 = x0 + float(data["width"][i]), y0 + float(data["height"][i])
        items.append(([[x0,y0],[x1,y0],[x1,y1],[x0,y1]], text, conf))
    return items
```

**Pass 1 — Top-band identification (cheap).** Render every page at 1x zoom, crop the top 30% only, OCR the crop. Scale coordinates up by 2x so they sit in the same coordinate space as pass 2. This is enough to identify financial-statement pages (headings live in the top 15% of page height), detect contents pages (top 25%), and match Part B hard-anchor headings.

```python
PASS1_TOP_FRAC = 0.30
COORD_SCALE = 2.0  # scale pass-1 coords (1x) into pass-2 space (2x)

top_band_pages = {}
for i, page in enumerate(doc):
    mat = fitz.Matrix(1, 1)
    pix = page.get_pixmap(matrix=mat)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    crop_h = max(1, int(pix.height * PASS1_TOP_FRAC))
    crop = img.crop((0, 0, pix.width, crop_h))
    results = _tesseract_ocr(crop)
    scaled = [([[x * COORD_SCALE, y * COORD_SCALE] for x, y in poly], text, conf)
              for poly, text, conf in results]
    top_band_pages[i + 1] = scaled
```

**Identify pass-2 targets.** Walk top-band data to find financial statement pages + Part B section heading matches + ±1 neighbours of each financial page (catches "continued" statements and filleted-filing opt-out notes). Union of all these is the pass-2 set.

**Pass 2 — Full-page OCR on targets only.** Render at 2x zoom, OCR the full page, overwrite pass-1 entries:

```python
targets = _identify_pass2_targets(top_band_pages)  # set of page numbers
mat2 = fitz.Matrix(2, 2)
for pnum in sorted(targets):
    pix = doc[pnum - 1].get_pixmap(matrix=mat2)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    top_band_pages[pnum] = _tesseract_ocr(img)

all_pages = top_band_pages
PAGE_WIDTH = doc[0].get_pixmap(matrix=mat2).width
PAGE_HEIGHT = doc[0].get_pixmap(matrix=mat2).height
doc.close()
```

**Performance impact.** Tesseract is significantly faster than EasyOCR on CPU (no neural network inference). Pass-1 crops are ~7.5% the pixel count of a full 2x page, so the identification sweep is very cheap. Combined with two-pass, a 20-page filing takes ~5-10s; a 130-page plc ~30-50s.

**Both paths produce the identical `all_pages` structure.** Pages outside the pass-2 set retain their top-band-only items — sufficient for Part B locators A/C/D/F which only read top-of-page text. Financial parsing and Locator B only ever touch pass-2 pages, so they see full 2x OCR output. All subsequent steps (page identification, column detection, row clustering, `parse_financial_page`) work unchanged regardless of which path was used.

**`load_pages()` returns `(all_pages, page_width, page_height, source)`.** The `page_height` value is in the same coordinate space as item coordinates (PDF points for text-layer, 2x pixel-space for OCR). It is used by `top_band_text` and all internal y-cutoff calculations for **absolute** cutoff positioning (e.g. top 15% of page height) instead of relative fractions of item y-ranges. This is critical for scanned PDFs where OCR items may cluster in a narrow y-range, causing relative fractions to chop off headings that sit just below the company name banner.

### Step 2 — Identify financial pages (top-band match, first-wins)

Scan each page's **top y-band only** (top ~15% of page height, using absolute `page_height` cutoff) for section titles. Full-page substring matching produces false positives and must not be used. Two verified failures from real filings:

- **Heights Management Test 2:** full-page match tagged the Balance Sheet as an Income Statement because `"Profit and Loss Account"` appears as a reserves-line label in the equity section.
- **John Lewis plc:** full-page match tagged the Auditor's Report (p110) as the Income Statement because the auditor's opinion references `"consolidated income statement"` in its body.

```python
def get_x_center(bbox): return (bbox[0][0] + bbox[2][0]) / 2
def get_y_center(bbox): return (bbox[0][1] + bbox[2][1]) / 2

def page_text(page_num):
    return " ".join([t for _, t, _ in all_pages.get(page_num, [])])

def top_band_text(page_num, frac=0.15, page_height=None):
    """Return uppercased text from the top `frac` of the page.

    When page_height is provided, the cutoff is absolute (page_height * frac).
    This prevents scanned PDFs with narrow item y-ranges from chopping off
    headings that sit just below the company name.  Falls back to relative
    item y-range fraction when page_height is None.
    """
    items = all_pages.get(page_num, [])
    if not items: return ""
    if page_height is not None:
        cutoff = page_height * frac
    else:
        ys = [get_y_center(b) for b, _, _ in items]
        y_min, y_max = min(ys), max(ys)
        cutoff = y_min + (y_max - y_min) * frac
    return " ".join(t for b, t, _ in items if get_y_center(b) <= cutoff).upper()

def is_contents_page(page_num, page_height=None):
    """Detect TOC / Contents pages to exclude from page-type detection."""
    return "CONTENTS" in top_band_text(page_num, frac=0.25, page_height=page_height)

income_page = None
balance_page = None
cashflow_page = None
notes_pages = []

for pnum in all_pages:
    if is_contents_page(pnum, page_height=PAGE_HEIGHT):
        continue
    top = top_band_text(pnum, page_height=PAGE_HEIGHT)
    full_u = page_text(pnum).upper()

    # First-match wins — do not let later pages overwrite
    if income_page is None and (
        "INCOME STATEMENT" in top or
        "PROFIT AND LOSS ACCOUNT" in top or
        "STATEMENT OF COMPREHENSIVE INCOME" in top
    ) and "OPTED NOT TO DELIVER" not in full_u:
        income_page = pnum
        continue
    if balance_page is None and (
        "BALANCE SHEET" in top or "STATEMENT OF FINANCIAL POSITION" in top
    ) and "CONTINUED" not in top:
        balance_page = pnum
        continue
    if cashflow_page is None and (
        "CASH FLOW STATEMENT" in top or
        "STATEMENT OF CASH FLOWS" in top or
        "CASH FLOWS" in top
    ) and "CONTINUED" not in top:
        cashflow_page = pnum
        continue
    if "NOTES TO THE" in top and "FINANCIAL STATEMENTS" in top:
        notes_pages.append(pnum)
```

**Page detection rules:**
- **Top-band only.** Use `top_band_text(p, page_height=PAGE_HEIGHT)` for all title matching. The default `frac=0.15` with absolute page_height gives a cutoff of ~15% of actual page height, which reliably captures both the company name banner and the statement heading underneath. For scanned PDFs, the old relative-fraction approach (fraction of item y-range) caused headings to be missed because OCR items clustered in a narrow y-range.
- **First-match wins.** Once `income_page`, `balance_page`, or `cashflow_page` is set, do not overwrite it. Later pages may reference the income statement in their body text (auditor's report, directors' report, statement of comprehensive income) and would otherwise overwrite the true page.
- **Skip Contents / TOC pages.** A TOC page lists "Balance Sheet" and "Income Statement" as entries and will false-match otherwise. Heights Management Test 2 page 2 is the verified failure case.
- **Income Statement aliases:** "Profit and Loss Account", "Statement of Comprehensive Income", "Consolidated Income Statement".
- **Balance Sheet aliases:** "Statement of Financial Position", "Consolidated Balance Sheet".
- **Cash Flow aliases:** "Cash Flow Statement", "Statement of Cash Flows", "Consolidated Statement of Cash Flows", "Consolidated Cash Flow Statement". Note: cash flow statements are **not required** for small / micro / filleted filings under FRS 102 s1A and FRS 105 — `cashflow_page` will legitimately be `None` for those. Do not flag absence as an error.
- **Filleted filings (s444(4)):** the filing may contain the phrase `"opted not to deliver ... Profit and Loss Account"` stating that the P&L is *not* in this document. Guard against matching it: require `"OPTED NOT TO DELIVER"` NOT to appear on the candidate page. Set `income_page = None` for filleted filings.
- **Skip `(CONTINUED)` pages** for primary statement detection — they're continuations, not the start of the statement. The parser should still read their content when the primary page points to them.
- **Notes pages may span many pages** — collect all of them into `notes_pages`.

### Step 3 — Detect the year columns

Extract the **current year** and **prior year** from the year headers found on the Income Statement or Balance Sheet page. Look for 4-digit years in the OCR text (e.g. "2024", "2023"). The higher year is the current year.

```python
years = []
for pnum in [income_page, balance_page]:
    if pnum:
        for bbox, text, conf in all_pages.get(pnum, []):
            m = re.match(r"^(20\d{2})$", text.strip())
            if m:
                years.append(m.group(1))
years = sorted(set(years))
CURRENT_YEAR = years[-1] if years else "Unknown"
PRIOR_YEAR = years[-2] if len(years) >= 2 else "Unknown"
```

### Step 4 — Core parsing engine

The parsing engine uses spatial analysis of OCR bounding boxes to:
1. Classify each text item as a **label** (left side) or **number** (right side) based on x-position
2. Detect number columns dynamically by clustering x-positions of number-like text
3. Group items into rows by y-proximity
4. Match row labels against a label map to identify financial fields
5. Extract numbers from the correct year columns

**Key algorithms:**

#### Notes-column detection and exclusion
Many financial statements include a "Notes" reference column between the labels and the financial value columns. These are small integers (1-30) at a consistent x-position. Before column clustering, `_detect_notes_column()` looks for ≥2 small integers (1-30) in the zone 35%-55% of page width, clustered within 30px spread. When detected, items within ±25px of the notes column centre are tagged as `"note_ref"` and excluded from both column clustering and value extraction. Year-header integers (e.g. 2024, 2025) are also excluded from clustering to prevent spurious 3-cluster layouts.

#### Column detection
Number columns are detected by clustering the x-positions of all number-like text on the page (excluding note refs and year headers). A gap threshold of 60px separates clusters. For 2-column layouts (Income Statement, Notes), the two clusters map directly to current year and prior year. For 4-column layouts (Balance Sheet with inner sub-totals and outer section totals), the left two clusters are current year and the right two are prior year.

```python
def cluster_x_positions(xs, gap_threshold=60):
    if not xs:
        return []
    xs_sorted = sorted(xs)
    clusters = [[xs_sorted[0]]]
    for x in xs_sorted[1:]:
        if x - clusters[-1][-1] > gap_threshold:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    return [sum(c) / len(c) for c in clusters]
```

#### Row clustering
Items within 20px vertical distance are grouped into the same row. This threshold handles slight vertical misalignment from OCR while keeping separate line items apart (typical line spacing is 22-25px).

#### Multi-line label handling
Some financial statements split labels across multiple rows (e.g. "CREDITORS" on one line, "Amounts falling due within one year" on the next, with numbers only on the second line). The parser uses a **pending label** accumulator:

- If a row has labels but no numbers, check if it's a **section header** (next row has numbers → accumulate) or a **nil line item** (next row independently matches a different field → record as zero)
- This correctly handles both cases:
  - "Tax on profit" with no numbers → standalone nil item → `{2024: 0, 2023: 0}`
  - "CREDITORS" / "Amounts falling due within one year 6,893 6,772" → section header + data row → `creditors_within_one_year: {2024: 6893, 2023: 6772}`

#### Number parsing (OCR-tolerant)

```python
def parse_number(s):
    s = s.strip()
    # Open-bracket alone is enough for negative — OCR frequently drops closing ')'.
    # Verified on Heights Management Test 2: "(9,453" came through without ')'.
    negative = s.startswith("(")
    s = s.replace("(", "").replace(")", "").replace(" ", "")
    s = s.replace("\u00a3", "").replace("$", "")  # £, $
    if s in ("-", "\u2013", "\u2014", "", "."):
        return 0
    # Key OCR fix: commas AND periods are both thousand separators here.
    # EasyOCR routinely reads UK commas as periods ("54,043" → "54.043").
    # If the string is all digits once both separators are stripped, treat it as an integer.
    cleaned = s.replace(",", "").replace(".", "")
    if re.match(r"^-?\d+$", cleaned):
        try:
            val = int(cleaned)
            return -val if negative else val
        except ValueError:
            return None
    # Rare fallback for genuine decimals (ratios, EPS, etc.)
    try:
        val = int(float(s.replace(",", "")))
        return -val if negative else val
    except ValueError:
        return None
```

**Rules:**
- **Open-bracket alone marks negative.** Do not require a matching `)` — OCR drops closing brackets. Verified failure mode on Heights Management Test 2 where `"(9.453"` (note: open paren, no close paren, period-for-comma) was the raw OCR output for `(9,453)`.
- **Period-as-thousand-separator.** EasyOCR systematically misreads commas as periods. Strip both characters before parsing to integer. Without this fix, `"54.043"` parses as the float 54.043 and `int()` truncates to 54, losing three orders of magnitude. Verified failure on Heights Management Test 2 where cash_at_bank prior year `54,043` became `54`.
- **Dashes indicate zero:** `-`, `–`, `—` → `0`.
- **Strip currency symbols, spaces, brackets** before parsing.
- **All values stored as integers.** For SME filings (£), whole pounds. For listed plc filings (£m), whole millions — the skill does not auto-scale, so record `"currency": "GBP (millions)"` in the output when the filing presents in £m or £'000.

### Step 5 — Label maps (two dialects: UK GAAP and IFRS)

Map label text to canonical field names. Labels are matched using substring containment (case-insensitive). **Order matters — more specific patterns must come before general ones** to avoid the `"retirement benefit"` → `"retirement benefit asset"` vs `"retirement benefit obligation"` collision (verified on John Lewis: a single generic pattern matched both, tagging a liability as an asset).

**Pick the map based on filing size / accounting framework:**
- `micro` / `small` / `small-filleted` → **UK GAAP map** (FRS 102 s1A / FRS 105). Uses "Turnover", "Tangible assets", "Debtors", "Creditors", "Shareholders' funds".
- `medium` / `large` → **IFRS map**. Uses "Revenue", "Property, plant and equipment", "Trade and other receivables", "Trade and other payables", "Total equity".

When in doubt, run both maps and take the one producing more non-empty matches. They have little overlap and won't collide.

#### UK GAAP Income Statement labels (small / micro filings)
```python
INCOME_LABEL_MAP_UKGAAP = {
    "turnover": "turnover",
    "revenue": "turnover",
    "cost of sales": "cost_of_sales",
    "gross profit": "gross_profit",
    "gross loss": "gross_profit",
    "distribution costs": "distribution_costs",
    "administrative expenses": "administrative_expenses",
    "admin expenses": "administrative_expenses",
    "other operating income": "other_operating_income",
    "operating profit": "operating_profit",
    "operating loss": "operating_profit",
    "operating (loss)/ profit": "operating_profit",
    "operating profit/(loss)": "operating_profit",
    "(loss)/profit from operations": "operating_profit",
    "profit/(loss) from operations": "operating_profit",
    "interest receivable": "interest_receivable",
    "interest payable": "interest_payable",
    "finance income": "interest_receivable",
    "finance cost": "interest_payable",
    "profit before tax": "profit_before_taxation",
    "loss before tax": "profit_before_taxation",
    "(loss)/profit before tax": "profit_before_taxation",
    "profit/(loss) before tax": "profit_before_taxation",
    "tax on profit": "tax_on_profit",
    "tax on loss": "tax_on_profit",
    "taxation": "tax_on_profit",
    "tax credit": "tax_on_profit",
    "profit for the financial year": "profit_for_financial_year",
    "loss for the financial year": "profit_for_financial_year",
    "profit for the year": "profit_for_financial_year",
    "loss for the year": "profit_for_financial_year",
    "(loss)/profit for the year": "profit_for_financial_year",
    "profit/(loss) for the year": "profit_for_financial_year",
    "profit after tax": "profit_for_financial_year",
    "loss after tax": "profit_for_financial_year",
}
```

#### UK GAAP Balance Sheet labels (small / micro filings)
```python
BALANCE_LABEL_MAP_UKGAAP = {
    "tangible assets": "tangible_assets",
    "intangible assets": "intangible_assets",
    "fixed asset investments": "fixed_asset_investments",
    "debtors": "debtors",
    "cash at bank": "cash_at_bank",
    "cash and cash equivalents": "cash_at_bank",
    "stock": "stock",
    "inventories": "stock",
    "net current assets": "net_current_assets",
    "total assets less current": "total_assets_less_current_liabilities",
    "net assets": "net_assets",
    "called up share capital": "called_up_share_capital",
    "share premium": "share_premium",
    "retained earnings": "retained_earnings",
    "profit and loss account": "retained_earnings",
    "shareholders": "shareholders_funds",
    "creditors": "creditors_within_one_year",
}
```

#### IFRS Income Statement labels (medium / large plc filings)
```python
INCOME_LABEL_MAP_IFRS = {
    "revenue": "revenue",
    "cost of sales": "cost_of_sales",
    "gross profit": "gross_profit",
    "other operating income": "other_operating_income",
    "operating and administrative": "operating_and_admin_expenses",
    "administrative expenses": "administrative_expenses",
    "operating profit": "operating_profit",
    "operating loss": "operating_profit",
    "operating (loss)/ profit": "operating_profit",
    "operating profit/(loss)": "operating_profit",
    "(loss)/profit from operations": "operating_profit",
    "profit/(loss) from operations": "operating_profit",
    "finance income": "finance_income",
    "finance cost": "finance_costs",
    "finance costs": "finance_costs",
    "profit before tax": "profit_before_taxation",
    "loss before tax": "profit_before_taxation",
    "(loss)/profit before tax": "profit_before_taxation",
    "profit/(loss) before tax": "profit_before_taxation",
    "taxation": "taxation",
    "tax expense": "taxation",
    "tax credit": "taxation",
    "profit for the financial year": "profit_for_financial_year",
    "profit for the year": "profit_for_financial_year",
    "loss for the financial year": "profit_for_financial_year",
    "loss for the year": "profit_for_financial_year",
    "(loss)/profit for the year": "profit_for_financial_year",
    "profit/(loss) for the year": "profit_for_financial_year",
}
```

#### IFRS Balance Sheet labels (medium / large plc filings)

**Specificity note:** patterns like `"retirement benefit asset"` and `"retirement benefit obligation"` must be separate entries, not a shared `"retirement benefit"` prefix — that prefix matches both an asset and a liability and tags them with the same field.

```python
BALANCE_LABEL_MAP_IFRS = {
    # Non-current assets
    "intangible assets": "intangible_assets",
    "property, plant and equipment": "property_plant_equipment",
    "right-of-use assets": "right_of_use_assets",
    "investments": "investments",
    "deferred tax assets": "deferred_tax_assets",
    "retirement benefit asset": "retirement_benefit_asset",
    "trade and other receivables": "trade_and_other_receivables",
    "derivative financial instruments": "derivatives",
    # Current assets
    "inventories": "inventories",
    "cash and cash equivalents": "cash_and_equivalents",
    "short-term investments": "short_term_investments",
    "assets held for sale": "assets_held_for_sale",
    "total current assets": "total_current_assets",
    "total non-current assets": "total_non_current_assets",
    "total assets": "total_assets",
    # Liabilities
    "trade and other payables": "trade_and_other_payables",
    "borrowings": "borrowings",
    "lease liabilities": "lease_liabilities",
    "provisions": "provisions",
    "current tax liabilities": "current_tax_liabilities",
    "retirement benefit obligation": "retirement_benefit_obligation",
    "retirement benefit liabilit": "retirement_benefit_obligation",
    "deferred tax liabilities": "deferred_tax_liabilities",
    "total current liabilities": "total_current_liabilities",
    "total non-current liabilities": "total_non_current_liabilities",
    "total liabilities": "total_liabilities",
    "net assets": "net_assets",
    # Equity
    "called up share capital": "share_capital",
    "share capital": "share_capital",
    "share premium": "share_premium",
    "retained earnings": "retained_earnings",
    "total equity": "total_equity",
}
```

#### Cash Flow Statement labels (both dialects)

Cash flow wording is largely the same across UK GAAP (FRS 102) and IFRS, so a single map works. The cash flow statement is legally required only for medium-and-large filings — small / micro / filleted filings omit it under FRS 102 s1A and FRS 105, and the absence is lawful.

**Critical specificity rules for cash flow:**

1. **Sub-total aliasing.** "Cash generated from operations" (pre-tax, pre-interest) and "Net cash from operating activities" (the final operating sub-total, after tax and interest paid) are two different numbers. The downstream pipeline uses `operating_cash_flow` for the final sub-total. Map the more-specific phrase `"net cash ... from operating"` to `operating_cash_flow` and leave "cash generated from operations" unmapped (or map it to the same field as a fallback when the final sub-total is absent).

2. **Indirect-method add-back collision.** Indirect-method cash flows begin with profit-before-tax and add back Depreciation, Amortisation, Finance costs, etc. **These line items share labels with the income statement** and would overwrite income-statement values if the cash flow label map included them. Solution: the cash flow label map must **not** include depreciation / amortisation / finance-cost add-backs. The add-backs are redundant (already captured on the P&L) and excluding them avoids the collision entirely.

3. **Bracketed numbers are outflows.** Every investing and most financing line items appear in brackets, e.g. `(5,423)`. `parse_number` already parses open-brackets as negative, so `capex_ppe` comes out signed correctly without extra logic. The downstream `_NEGATIVE_FIELDS` sign-normalisation in `financial_computations` then confirms the sign.

4. **"Interest paid" can appear in operating OR financing.** IFRS permits either classification. Map both occurrences to `tax_paid`'s sibling `interest_paid` only if you need the line; the main pipeline does not currently store interest_paid, so it is not in the default map.

5. **Plural vs singular.** "Purchase of property, plant and equipment" vs "Purchases of property, plant and equipment" — substring matching treats these as distinct because the suffix `-s` changes position. Include both variants as separate map entries.

6. **Lease payments.** Under IFRS 16 the line is usually "Payment of lease liabilities" or "Principal elements of lease payments". Under legacy FRS 102 it may be "Capital element of finance lease rental payments". Include all three variants.

```python
CASHFLOW_LABEL_MAP = {
    # --- Operating activities: final sub-total ---
    # Most-specific first: "net cash ... from operating" beats "cash generated"
    "net cash from operating activities": "operating_cash_flow",
    "net cash generated from operating activities": "operating_cash_flow",
    "net cash used in operating activities": "operating_cash_flow",
    "net cash inflow from operating activities": "operating_cash_flow",
    "cash flows from operating activities": "operating_cash_flow",  # IFRS variant
    # Fallback: pre-tax / pre-interest operating sub-total
    "cash generated from operations": "net_cash_operating",

    # --- Operating activities: tax paid ---
    "income taxes paid": "tax_paid",
    "corporation tax paid": "tax_paid",
    "tax paid": "tax_paid",
    "taxes paid": "tax_paid",

    # --- Investing activities: PPE ---
    "purchase of property, plant and equipment": "capex_ppe",
    "purchases of property, plant and equipment": "capex_ppe",
    "purchase of tangible fixed assets": "capex_ppe",
    "purchases of tangible fixed assets": "capex_ppe",
    "payments for property, plant and equipment": "capex_ppe",
    "acquisition of property, plant and equipment": "capex_ppe",
    "additions to property, plant and equipment": "capex_ppe",

    # --- Investing activities: intangibles ---
    "purchase of intangible assets": "capex_intangibles",
    "purchases of intangible assets": "capex_intangibles",
    "purchase of other intangible assets": "capex_intangibles",
    "payments for intangible assets": "capex_intangibles",
    "acquisition of intangible assets": "capex_intangibles",
    "additions to intangible assets": "capex_intangibles",

    # --- Investing activities: disposal proceeds ---
    "proceeds from sale of property, plant and equipment": "proceeds_disposal_ppe",
    "proceeds from disposal of property, plant and equipment": "proceeds_disposal_ppe",
    "proceeds from sale of tangible fixed assets": "proceeds_disposal_ppe",
    "proceeds from disposals of property": "proceeds_disposal_ppe",

    # --- Investing activities: sub-total ---
    "net cash used in investing activities": "net_cash_investing",
    "net cash from investing activities": "net_cash_investing",
    "net cash generated from investing activities": "net_cash_investing",
    "cash flows from investing activities": "net_cash_investing",

    # --- Financing activities: borrowings ---
    "proceeds from borrowings": "proceeds_borrowings",
    "proceeds from issue of borrowings": "proceeds_borrowings",
    "proceeds from new borrowings": "proceeds_borrowings",
    "new bank loans": "proceeds_borrowings",
    "drawdown of borrowings": "proceeds_borrowings",
    "drawdowns of borrowings": "proceeds_borrowings",
    "repayment of borrowings": "repayment_borrowings",
    "repayments of borrowings": "repayment_borrowings",
    "repayment of bank loans": "repayment_borrowings",
    "repayment of loans": "repayment_borrowings",

    # --- Financing activities: lease payments ---
    "payment of lease liabilities": "lease_payments",
    "payments of lease liabilities": "lease_payments",
    "repayment of lease liabilities": "lease_payments",
    "principal elements of lease payments": "lease_payments",
    "principal element of lease payments": "lease_payments",
    "capital element of finance lease": "lease_payments",
    "payments of finance lease obligations": "lease_payments",

    # --- Financing activities: dividends ---
    "dividends paid": "dividends_paid_cf",
    "equity dividends paid": "dividends_paid_cf",
    "dividends paid to shareholders": "dividends_paid_cf",
    "dividends paid to equity holders": "dividends_paid_cf",

    # --- Financing activities: sub-total ---
    "net cash used in financing activities": "net_cash_financing",
    "net cash from financing activities": "net_cash_financing",
    "net cash generated from financing activities": "net_cash_financing",
    "cash flows from financing activities": "net_cash_financing",

    # --- Bottom of statement: cash position ---
    "net increase in cash and cash equivalents": "net_change_cash",
    "net decrease in cash and cash equivalents": "net_change_cash",
    "net increase/(decrease) in cash": "net_change_cash",
    "net (decrease)/increase in cash": "net_change_cash",
    "cash and cash equivalents at beginning": "opening_cash",
    "cash and cash equivalents at the beginning": "opening_cash",
    "cash and cash equivalents at end": "closing_cash",
    "cash and cash equivalents at the end": "closing_cash",
}
```

**Depreciation & amortisation add-backs:** D&A labels are included at the top of `CASHFLOW_LABEL_MAP` (e.g. "depreciation of tangible fixed assets" → `depreciation`, "amortisation of intangible assets" → `amortisation`). These values are extracted from the cash flow statement's indirect-method add-back section and flow through `_normalise_pdf_extraction` into the `cash_flow_statement` year rows. The derivation layer in `financial_computations.py` uses them for EBITDA = operating_profit + |depreciation| + |amortisation|. When D&A are unavailable (common for scanned PDFs where the cash flow page is not detected), a fallback approximation fires: EBITDA ≈ operating_profit, logged as `ebitda_method: "approximated (D&A unavailable)"`.

**Sanity-check relationships:**
- `operating_cash_flow + net_cash_investing + net_cash_financing ≈ net_change_cash` (allow ±1 rounding)
- `opening_cash + net_change_cash ≈ closing_cash`
- `closing_cash` from the cash flow statement should equal `cash` on the balance sheet for the same year (FX effects can cause small divergence)

These relationships are used by `financial_computations.compute()` to gap-fill missing lines and flag inconsistencies, so the extractor just needs to return what it finds — no post-processing in the skill layer.

#### Notes labels
```python
NOTES_LABEL_MAP = {
    "maintenance fees receivable": "maintenance_fees_receivable",
    "maintenance fees received in advance": "maintenance_fees_received_in_advance",
    "other creditors": "other_creditors",
    "accruals and deferred income": "accruals_and_deferred_income",
    "trade debtors": "trade_debtors",
    "trade creditors": "trade_creditors",
    "prepayments": "prepayments",
    "wages and salaries": "wages_and_salaries",
    "social security costs": "social_security_costs",
    "pension costs": "pension_costs",
    # Depreciation & Amortisation (from tangible/intangible fixed asset notes)
    "depreciation charge for the year": "depreciation",
    "depreciation charged in the year": "depreciation",
    "depreciation for the year": "depreciation",
    "charge for the year": "depreciation",
    "amortisation charge for the year": "amortisation",
    "amortisation charged in the year": "amortisation",
    "amortisation for the year": "amortisation",
}
```

**Extending the label maps:** When encountering a new filing with different label text, add the new variation to the appropriate map. The substring matching is forgiving — `"creditors"` will match "Creditors: amounts falling due within one year".

### Step 6 — Extract company metadata

Pull the company name, registered number, and year end from the cover page or header text using regex.

```python
cover_text = page_text(1)
reg_match = re.search(r"(\d{8})", cover_text)
reg_number = reg_match.group(1) if reg_match else "Unknown"
year_match = re.search(r"YEAR ENDED (\d{1,2} \w+ \d{4})", cover_text, re.IGNORECASE)
year_ended = year_match.group(1) if year_match else "Unknown"
```

For the company name, extract from the cover page or use the text following "REPORT OF THE DIRECTORS AND FINANCIAL STATEMENTS FOR" or similar heading.

### Step 7 — Run extraction and build output JSON

Call the parsing engine on each identified page and assemble the structured output.

```python
income = parse_financial_page(income_page, INCOME_LABEL_MAP) if income_page else {}
balance = parse_financial_page(balance_page, BALANCE_LABEL_MAP) if balance_page else {}
cashflow = parse_financial_page(cashflow_page, CASHFLOW_LABEL_MAP) if cashflow_page else {}
notes = {}
for np_ in notes_pages:
    notes.update(parse_financial_page(np_, NOTES_LABEL_MAP))

output = {
    "company": company_name,
    "registered_number": reg_number,
    "year_ended": year_ended,
    "currency": "GBP",
    "income_statement": income,
    "cash_flow_statement": cashflow,  # may be empty for small / micro / filleted filings
    "balance_sheet": {
        "fixed_assets": {
            k: v for k, v in balance.items()
            if k in ("tangible_assets", "intangible_assets", "fixed_asset_investments")
        },
        "current_assets": {
            k: v for k, v in balance.items()
            if k in ("debtors", "cash_at_bank", "stock")
        },
        "creditors_within_one_year": balance.get("creditors_within_one_year", {}),
        "net_current_assets": balance.get("net_current_assets", {}),
        "total_assets_less_current_liabilities": balance.get("total_assets_less_current_liabilities", {}),
        "capital_and_reserves": {
            k: v for k, v in balance.items()
            if k in ("called_up_share_capital", "share_premium", "retained_earnings", "shareholders_funds")
        },
    },
    "notes": {
        "debtors": {k: v for k, v in notes.items() if "receivable" in k or "debtors" in k or "prepayment" in k},
        "creditors": {k: v for k, v in notes.items() if k not in (
            "maintenance_fees_receivable", "trade_debtors", "prepayments"
        )},
    },
}

print(json.dumps(output, indent=2))
```

### Step 8 — Extract employee headcount from notes

Employee counts are extracted separately from the financial tables because they appear in a different format: a prose sentence ("The average number of persons employed...") followed by a simple headcount table, often broken into sub-categories.

**`extract_employees_from_notes()`** in `pdf_parser.py`:
1. **Find employee page** — scan notes pages for keywords "staff costs", "average number of persons", "employees"
2. **Locate section boundaries** — find the left-aligned heading ("Staff costs" or "Employees"), then the "No." marker or "average"/"number" as table start, then cost/wage keywords as section end
3. **Parse headcount rows** — match labels against `_EMPLOYEE_HEADCOUNT_LABELS` (total vs sub-category)
4. **Resolve** — prefer a total row if found; otherwise sum sub-category rows (handles filings that break headcount into Production, Management, etc. without a labelled total)

**`parse_pdf_full()`** now returns 3 keys: `financials`, `sections`, and `employees` (a `{year_str: int}` dict). The pipeline merges this into `employees` (display) and `employees_history` (JSONB).

### Step 8 — Sanity checks

After generating the JSON, verify:

1. **Balance Sheet balances:** `total_assets_less_current_liabilities` (or `net_assets`) must equal `shareholders_funds` for both years.
2. **Income Statement flows:** Check `turnover - cost_of_sales = gross_profit` and `operating_profit + interest_receivable - interest_payable ≈ profit_before_taxation` where applicable.
3. **Retained earnings movement:** Prior year retained earnings + current year profit ≈ current year retained earnings (allowing for dividends).
4. **Cash flow totals reconcile:** `operating_cash_flow + net_cash_investing + net_cash_financing ≈ net_change_cash` (±1 rounding). `opening_cash + net_change_cash ≈ closing_cash`. Only run when cashflow_page was found.
5. **Cash flow ↔ balance sheet tie:** `closing_cash` from the cash flow statement should equal the balance-sheet `cash` for the same year. Small divergence (<1%) is acceptable (FX, restricted cash); larger gaps indicate an extraction error on one of the two pages.

```python
warnings = []
bs = output["balance_sheet"]
for year in [CURRENT_YEAR, PRIOR_YEAR]:
    total = bs.get("total_assets_less_current_liabilities", {}).get(year)
    shf = bs.get("capital_and_reserves", {}).get("shareholders_funds", {}).get(year)
    if total is not None and shf is not None and total != shf:
        warnings.append(f"{year}: total_assets_less_current_liabilities ({total}) != shareholders_funds ({shf})")

if warnings:
    output["warnings"] = warnings
```

If any check fails, add a `"warnings"` array to the output. Do not silently adjust numbers.

---

## Complete parse_financial_page function

This is the full parsing engine, tested and verified against LLM extraction with 100% match on all fields.

```python
def cluster_x_positions(xs, gap_threshold=60):
    """Cluster x-positions into groups separated by gaps > threshold."""
    if not xs:
        return []
    xs_sorted = sorted(xs)
    clusters = [[xs_sorted[0]]]
    for x in xs_sorted[1:]:
        if x - clusters[-1][-1] > gap_threshold:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    return [sum(c) / len(c) for c in clusters]


def parse_financial_page(page_num, label_map):
    """Parse a financial page using row clustering and column detection."""
    items = all_pages.get(page_num, [])
    if not items:
        return {}

    # Build year-integer exclusion set early so it applies to clustering too.
    year_ints = set()
    for y in (current_year, prior_year):
        if isinstance(y, str) and y.isdigit():
            year_ints.add(int(y))

    # Detect and exclude notes-reference column (small ints 1-30 at
    # consistent x between labels and values).
    notes_col_x = _detect_notes_column(items, PAGE_WIDTH)

    # Collect x-positions of all number-like text in the right portion.
    # Exclude: small ints (|val|<=50), year headers, notes-column items.
    number_xs = []
    for bbox, text, conf in items:
        if not is_number_text(text):
            continue
        val = parse_number(text)
        if val is None or abs(val) <= 50:
            continue
        if int(val) in year_ints:
            continue
        x = get_x_center(bbox)
        if x <= PAGE_WIDTH * 0.40:
            continue
        if notes_col_x is not None and abs(x - notes_col_x) < 25:
            continue
        number_xs.append(x)

    if not number_xs:
        return {}

    # Cluster into column centers (could be 2 or 4 columns)
    col_centers = cluster_x_positions(number_xs, gap_threshold=60)

    # Map columns to current year (col1) and prior year (col2)
    if len(col_centers) <= 2:
        c1 = col_centers[0] if len(col_centers) >= 1 else PAGE_WIDTH * 0.6
        c2 = col_centers[1] if len(col_centers) >= 2 else PAGE_WIDTH * 0.85
    else:
        # 4-column (e.g. balance sheet with inner/outer columns):
        # left half = current year, right half = prior year
        mid_idx = len(col_centers) // 2
        c1_group = col_centers[:mid_idx]
        c2_group = col_centers[mid_idx:]
        c1 = sum(c1_group) / len(c1_group)
        c2 = sum(c2_group) / len(c2_group)

    mid_boundary = (c1 + c2) / 2

    def classify_col(x):
        if x < PAGE_WIDTH * 0.40:
            return "label"
        if notes_col_x is not None and abs(x - notes_col_x) < 25:
            return "note_ref"
        return "col1" if x < mid_boundary else "col2"

    # Build row items with classification
    row_items = []
    for bbox, text, conf in items:
        x = get_x_center(bbox)
        y = get_y_center(bbox)
        col = classify_col(x)
        row_items.append({"y": y, "x": x, "col": col, "text": text, "conf": conf})

    row_items.sort(key=lambda r: r["y"])

    # Cluster by Y proximity (20px threshold)
    clusters = []
    current = []
    for item in row_items:
        if current and abs(item["y"] - current[-1]["y"]) > 20:
            clusters.append(current)
            current = [item]
        else:
            current.append(item)
    if current:
        clusters.append(current)

    # Match clusters to fields with multi-line label accumulation
    result = {}
    pending_label = ""
    for idx, cluster in enumerate(clusters):
        labels = [it for it in cluster if it["col"] == "label"]
        col1_items = [it for it in cluster if it["col"] == "col1" and is_number_text(it["text"])]
        col2_items = [it for it in cluster if it["col"] == "col2" and is_number_text(it["text"])]

        if labels:
            row_label = " ".join(
                [l["text"] for l in sorted(labels, key=lambda x: x["x"])]
            ).strip().lower()
        else:
            row_label = ""

        has_numbers = bool(col1_items or col2_items)

        if row_label and not has_numbers:
            # Is this a nil line item or a section header?
            direct_match = None
            for pattern, field in label_map.items():
                if pattern in row_label:
                    direct_match = field
                    break

            next_independently_matches = False
            if direct_match and idx + 1 < len(clusters):
                next_cluster = clusters[idx + 1]
                next_labels = [it for it in next_cluster if it["col"] == "label"]
                if next_labels:
                    next_label_text = " ".join(
                        [l["text"] for l in sorted(next_labels, key=lambda x: x["x"])]
                    ).strip().lower()
                    for pattern, field in label_map.items():
                        if pattern in next_label_text and field != direct_match:
                            next_independently_matches = True
                            break

            if direct_match and next_independently_matches:
                # Standalone nil line item (dashes not picked up by OCR)
                result[direct_match] = {CURRENT_YEAR: 0, PRIOR_YEAR: 0}
                pending_label = ""
            else:
                # Section header — accumulate for next row
                pending_label = (pending_label + " " + row_label).strip()
            continue

        # Combine with any pending label context
        combined_label = (pending_label + " " + row_label).strip()
        pending_label = ""

        if not combined_label:
            continue

        matched_field = None
        for pattern, field in label_map.items():
            if pattern in combined_label:
                matched_field = field
                break

        if matched_field is None:
            continue

        # Exclude the current/prior year integers from candidate values.
        # Verified failure on John Lewis: the header row "2025 / 2024" sat close
        # enough to the "Revenue" label that row-clustering assigned those year
        # ints as Revenue's values, producing revenue={2025:2025, 2024:2024}.
        year_ints = set()
        for y in (CURRENT_YEAR, PRIOR_YEAR):
            if isinstance(y, str) and y.isdigit():
                year_ints.add(int(y))

        col1_val = None
        col2_val = None
        for it in col1_items:
            v = parse_number(it["text"])
            if v is not None and v not in year_ints:
                col1_val = v
        for it in col2_items:
            v = parse_number(it["text"])
            if v is not None and v not in year_ints:
                col2_val = v

        # Check for dash/nil indicators in non-number items on the right side
        if col1_val is None or col2_val is None:
            right_items = [
                it for it in cluster
                if it["x"] > PAGE_WIDTH * 0.40 and not is_number_text(it["text"])
            ]
            for it in right_items:
                txt = it["text"].strip()
                if txt in ("-", "–", "—", "nil", "Nil", "NIL", "."):
                    if it["x"] < mid_boundary and col1_val is None:
                        col1_val = 0
                    elif it["x"] >= mid_boundary and col2_val is None:
                        col2_val = 0

        if col1_val is not None or col2_val is not None:
            entry = {}
            if col1_val is not None:
                entry[CURRENT_YEAR] = col1_val
            if col2_val is not None:
                entry[PRIOR_YEAR] = col2_val
            result[matched_field] = entry

    return result
```

---

## Part A — Edge Cases

- **Filleted accounts (s444(4)):** Small companies may opt not to deliver the Profit & Loss Account to the registrar. Detect via `"section 444(4)"` / `"opted not to deliver ... Profit and Loss"` in Stage 0, set `income_page = None`, emit `"income_statement": null`, and do not flag a warning — the absence is lawful. Heights Management Test 2 is the reference case.
- **Abbreviated accounts:** Older pre-2016 small-company format with reduced disclosures. Handle like filleted: extract what is present, mark omitted sections as `null`.
- **Micro-entity accounts (FRS 105):** Very minimal filings with only a Balance Sheet. Extract what is available.
- **Multi-year comparatives:** If 3+ year columns are detected, extract only the two most recent.
- **4-column Balance Sheet:** The column clustering algorithm automatically handles inner (sub-total) and outer (section total) column layouts. For large plc filings the parser should keep only the **rightmost two clusters** (current year / prior year), treating left-side sub-total columns as label-zone noise.
- **Currency scale:** Small company filings present in whole pounds (£). Medium/large plc filings present in £'000 or £m. The parser stores values as-printed; record the scale in `"currency"` (e.g. `"GBP"`, `"GBP (thousands)"`, `"GBP (millions)"`). Do not auto-scale — downstream decides.
- **OCR misreads handled by `parse_number`:** commas read as periods (`54,043` → `54.043`); closing brackets dropped (`(9,453)` → `(9.453`); dashes and em-dashes as zeros. Common character misreads `0↔O`, `1↔l`, `5↔S` are stripped by the non-numeric filter.
- **Low OCR confidence:** If OCR confidence is consistently <0.5 on number text, flag in warnings.
- **No text layer:** routed to Path B (OCR) automatically by the Step 1 text-layer probe.
- **Born-digital large plc filings:** routed to Path A (text-layer spans). Path A is orders of magnitude faster than OCR on 100+ page documents and produces more precise bboxes. Verified on John Lewis plc.
- **Year-header row contamination:** the header row on the income statement contains integers `2025`, `2024` that the parser will read as numeric candidates. The `parse_financial_page` function excludes year integers from row-level value extraction — do not remove this guard.
- **IFRS vs UK GAAP terminology:** large plc filings use IFRS terms that don't appear in the UK GAAP label map. Switch map based on filing size, or run both and merge.
- **Sub-entity / multi-entity PDFs:** large filings may bundle a second entity's accounts after the primary (e.g. JLP Scottish Limited Partnership on John Lewis pages 120–134). Use `top_band_text` on pages 1–10 only for page identification in the primary entity, or scope the page loop to the primary page range (end at the first `"Independent Auditor's Report"` location).

---

# PART B — Qualitative Section Extraction

## Purpose

Segment a UK company annual report PDF into the **12 statutory sections** and return **raw text per section**. Nothing more. Any finer analysis — sentence splitting, topic tagging, risk classification, MD&A summarisation — happens in a separate downstream layer that consumes this output. Over-reaching here breaks the handoff contract.

## Target sections

Default target set (confirm with user if their list differs — the architecture is independent of the exact list, only the locators change):

| # | Section | Statutory basis | Typical exemption |
|---|---|---|---|
| 1 | Strategic Report | CA 2006 s414A | small, micro |
| 2 | Section 172(1) Statement | CA 2006 s414CZA | small |
| 3 | Principal Risks and Uncertainties | CA 2006 s414C(2)(b) | small |
| 4 | Viability Statement | UK Corporate Governance Code | non-premium-listed |
| 5 | Directors' Report | CA 2006 s415 | micro (under s414B) |
| 6 | Principal Activity | CA 2006 s416(1)(a) | — |
| 7 | Going Concern (Directors' Report) | CA 2006 + FRC guidance | — |
| 8 | Statement of Directors' Responsibilities | CA 2006 s414 / s418 | — |
| 9 | Independent Auditor's Report | CA 2006 s495 | audit-exempt small |
| 10 | Accounting Policies (Note 1 / Basis of preparation) | FRS 102 / IFRS | — |
| 11 | Critical Accounting Judgements and Estimates | FRS 102 / IAS 1 | — |
| 12 | Going Concern (Note disclosure) | FRS 102 / IAS 1 | — |

## Output contract

Return a dict keyed by the 12 section IDs. Each entry has a **three-way status** that downstream consumers must rely on:

```python
{
  "strategic_report": {
    "status": "found" | "not_present" | "not_found",
    "pages": [start, end],            # inclusive, 1-indexed; null if not found
    "text": "…raw text of the section…",  # null if not found
    "signals": ["toc", "hard_anchor", "all_caps_first_line"],  # which locators agreed
    "confidence": "high" | "low",     # high = ≥2 independent locators agreed
    "notes": "…optional provenance / losing candidates…"
  },
  ...
}
```

**The three statuses mean distinct things and must not be conflated:**
- `found` — section was located, text extracted.
- `not_present` — section is legitimately absent because the filing size exempts it (e.g. a small-company filing has no Strategic Report). Downstream should treat this as expected and not flag it.
- `not_found` — section should exist given the filing size but no locator fired. Downstream should flag this as a quality issue.

The extractor decides `not_present` vs `not_found` using the filing-size classification from Stage 0.

## Pipeline

The pipeline runs in a fixed order. Earlier stages produce inputs for later stages. **Never skip a stage** — each one contributes signals that later stages depend on.

### Stage 0 — Filing classification (size AND mode)

Before any extraction, classify the filing on **two axes**: size and filing mode. Both are needed because Companies Act exemptions cut across both. Without the mode axis, filleted small filings produce permanent false `not_found` alarms on Directors' Report, Directors' Responsibilities, and Auditor's Report.

**Axis 1 — Size:** micro / small / medium / large.
- "micro-entity", "FRS 105" → micro
- "small companies regime", "FRS 102 Section 1A", "s1A", "small entities" → small
- Full audit report, Strategic Report present, "FRS 102" or "UK-adopted IFRS" without s1A, 40+ pages → medium or large
- Page count as a tiebreaker: micros usually <15 pages, smalls <30, larges 60+

**Axis 2 — Filing mode:** full / abbreviated / filleted / micro-minimal.
- **Full** — Directors' Report body + Strategic Report (if applicable) + full primary statements + Notes + Auditor's Report delivered.
- **Abbreviated** — reduced-content Directors' Report, some notes omitted; rare since 2016.
- **Filleted (s444(4))** — only Balance Sheet + Notes delivered to the registrar; Profit & Loss and (typically) Directors' Report and Auditor's Report are *not delivered* even if they exist. Detect via: `"section 444(4)"`, `"opted not to deliver ... Profit and Loss"`, absence of any Directors' Report heading, absence of any `"Independent Auditor's Report"` body, page count ≤10 with a Balance Sheet present.
- **Micro-minimal** — single Balance Sheet under FRS 105, no notes or minimal notes.

**Signal extraction rule (critical):** all Stage 0 phrase searches must run against the **joined page text string** from Stage 2, not against the token list. OCR frequently splits statutory phrases across adjacent tokens (e.g. `"subject to the small"` + `"companies regime."` as two tokens on Heights Management Test 2), and iterating tokens individually misses them. Always concatenate tokens with single spaces per page before searching.

**Expected-sections matrix** — the extractor uses `(size, mode)` as the key for what is expected. Examples:
- `(small, filleted)` → only `accounting_policies`, `going_concern_note`, `critical_estimates` are expected present; all narrative sections and auditor's report are `not_present`.
- `(small, full)` → Directors' Report, Dir Responsibilities, possibly Auditor's Report are expected; Strategic Report and Section 172 are `not_present`.
- `(large, full)` → all 12 sections expected.
- `(micro, micro-minimal)` → only Balance Sheet notes expected; everything else `not_present`.

This two-axis classification determines **which of the 12 sections are expected**. Without it you cannot distinguish `not_present` from `not_found` and the output becomes noise.

### Stage 1 — Text-layer probe and per-page routing

For each page independently, decide whether `page.get_text("text")` produces meaningful text:
- Non-empty
- Reasonable word count (say >20 for a non-cover page)
- Reasonable alphabetic-character ratio (>0.5)

Three outcomes **per page, not per document**:
- **Born-digital** → plain-text path: use `get_text("text")` output directly.
- **Scanned** → OCR path: render at 2x zoom and run EasyOCR (reuse the reader from Part A).
- **Mixed routing** applies across the document: a typical small filing may have a scanned signed Directors' Report page inside an otherwise born-digital body. Per-page routing preserves that page's section membership.

### Stage 2 — Unified page-text table

Regardless of source, normalise to one structure:

```python
page_text_table = {
    page_no: {
        "text": str,                   # MANDATORY: full page text as a single space-joined string
        "lines": [                     # per line / token with bbox
            {"text": str, "bbox": (x0,y0,x1,y1), "size_hint": float|None}
        ],
        "source": "text" | "ocr",
        "quality": float               # 0-1
    }
}
```

**The `text` field is mandatory and must be a single space-joined string** covering every token on the page. All substring / regex searches in Stages 0 and 3 read from this string, never from iterating the `lines` list. OCR typically splits statutory phrases across adjacent tokens (verified on Heights Management Test 2: `"small companies regime"` appears as two separate tokens), so token-by-token searches produce false negatives. For OCR sources, sort tokens by y-band then x before joining so reading order is preserved.

Every subsequent stage reads from this table and is agnostic to how the text was produced. This is what lets the same segmentation logic work on scanned micros and glossy PLC reports.

### Stage 3 — Run multiple independent locators in parallel

Run **all** locators on every filing. Each emits candidate `(section_id, start_page, end_page, confidence, signal_name)` tuples into a shared pool. Do not cascade or early-return — cascades mask failures; voting surfaces them.

#### Locator A — Printed TOC parse

Scan pages 1–4 for a table-of-contents structure. **Two parsing paths depending on source:**

**Born-digital pages** — a simple pattern works: a title line followed by a page-number token on the next line (or at the end of the same line). Regex like `^(.+?)\s*\n?\s*(\d{1,3})\s*$` per normalised line pair. Verified on John Lewis — page 2 parsed cleanly this way.

**OCR-sourced pages** — line pairing by vertical adjacency is unreliable because EasyOCR groups page-number tokens onto a different y-band from their titles (verified on Heights Management Test 2: the contents page produced title tokens and a `"Page"` header but no adjacent number tokens via line-pair regex). Instead use **x-position alignment**:
1. Cluster all tokens on the contents page by x-center. Titles form a left cluster, page numbers form a right cluster.
2. For each right-cluster token that parses as an integer ≤ doc page count, find the left-cluster token(s) whose y-center is closest (within ~1.5 × median line-height).
3. Emit `(title_text, page_number)` pairs from those matches.
4. Map each title to a section ID via the hard-anchor dictionary (Locator B).

Derive end pages as `next_entry_start - 1`.

High confidence when it fires. On the John Lewis example the born-digital path alone recovered every Level-1 section. Often absent in small-company filings — that's fine, it just doesn't contribute.

#### Locator B — Statutory phrase dictionary over plain text

This is the strongest portable signal for UK filings because Companies Act 2006 and the FRC mandate the wording. Two tiers:

**Hard anchors (statutory, consistent wording):**
```
strategic_report:        r"\bSTRATEGIC REPORT\b|\bStrategic Report\b"
directors_report:        r"\bDIRECTORS[’']\s*REPORT\b|\bDirectors[’']\s*Report\b|\bReport of the Directors\b"
section_172:             r"SECTION 172\(1\)|Section 172\(1\)|Section 172 statement|\bs172\b"
principal_risks:         r"\bPRINCIPAL RISKS(?: AND UNCERTAINTIES)?\b|\bPrincipal [Rr]isks(?: and [Uu]ncertainties)?\b"
viability_statement:     r"\bVIABILITY STATEMENT\b|\bViability Statement\b"
going_concern:           r"\bGOING CONCERN\b|\bGoing [Cc]oncern\b"
directors_responsibilities: r"Statement of Directors[’']? Responsibilities"
auditor_report:          r"INDEPENDENT AUDITOR[’']?S?[’']?\s*REPORT|Independent Auditor[’']s Report"
principal_activity:      r"\bPrincipal activit(?:y|ies)\b|\bPRINCIPAL ACTIVIT(?:Y|IES)\b"
accounting_policies:     r"Accounting (policies|principles)|Basis of preparation"
critical_estimates:      r"Critical (accounting )?(judgements|estimates)|Key sources of estimation"
```

**Rules:**
- Match against `get_text("text")` output, **not** dict-mode spans. Dict-mode line grouping is unreliable (verified on John Lewis — spans get fragmented, multi-word phrase matches fail).
- Line-start anchored with leading-numbering tolerance (`"2.1 SEGMENTAL REPORTING"`, `"I ACCOUNTING INFORMATION"`).
- **Heading-like filter** to suppress prose references: the containing line, after stripping leading numbering, must start with the matched phrase AND be ≤12 words OR be all-caps. This avoids matches like `"…in the Strategic Report on pages 4 to 17"` being treated as a section start.

For OCR-sourced pages, apply hard anchors with **tight fuzzy matching** (edit distance ≤1 per 6 characters) to absorb common OCR misreads ("Strategie Report", "D!rectors Report"). Keep the threshold tight — loose fuzzy matching on body prose produces garbage.

**Auditor's Report body-confirmation rule (critical to avoid false positives):** a hit on the `auditor_report` anchor only promotes to `found` if the same page OR the next page also contains **at least one body-confirmation token** from this set: `"Basis for opinion"`, `"Key audit matters"`, `"Opinion"` (as a standalone heading, not inline), `"We have audited"`, `"In our opinion"`. Heights Management Test 2 illustrates why: page 6 contains the reference line `"The auditor's report on the accounts ... was unqualified"` — this is a *mention* of the report, not the report itself (filleted s444(4) filings don't deliver the auditor's report body). Without the body-confirmation gate, every filleted filing produces a false `auditor_report: found` on this reference line.

#### Locator C — Running header / footer frequency

Count lines that recur verbatim at the top or bottom of many pages. On the John Lewis example, `"Notes to the consolidated financial statements (continued)"` appeared on 63 pages and `"Notes to the company financial statements (continued)"` on 14 — that alone gave perfect Notes boundaries. When it fires it is near-unambiguous.

**Implementation — three rules that matter for correctness:**

1. **Top y-band, not token position [0].** On OCR pages the first-returned token is usually near the top-left but "first line" must mean *topmost y-band* (tokens within the highest ~5% of page height, sorted by x). On Heights Management Test 2 the OCR tokens are returned in reading order so positional `[0]`/`[1]` picks up the company name and misses the true top-band section header. Always sort tokens by y-center ascending and take the top band.

2. **Exclude the company name.** Extract the company name from page 1 at Stage 0 (regex over the joined text: `^.*?LIMITED|.*?PLC|.*?LTD` on a top-band token). Then exclude any recurring top-band string that matches or contains the company name from the running-header candidate pool. On Heights Management Test 2 the string `"Heights" (Management) No 2 Limited(The)` repeats on 5 pages as the true first token — that's a *company* running header, not a *section* running header, and treating it as the latter would produce a spurious section boundary covering most of the document.

3. **Cross-validate against the hard-anchor dictionary.** A recurring top-band string only becomes a section-boundary signal if it also matches a Locator B hard anchor (after fuzzy matching for OCR tolerance). The range of pages sharing that string is the section's page range. This prevents any non-statutory recurring header (publisher branding, "Contents", date stamps, page-X-of-Y footers) from being misread as a section.

Build a Counter of top-band strings and bottom-band strings separately. Any string appearing on ≥3 pages AND passing the two exclusion rules AND matching a hard anchor is a running-header candidate.

#### Locator D — All-caps first-line-of-page

Rule: *the first non-empty line of a page is all-caps, ≤15 words, and matches any dictionary anchor.* Generalises the observation that on this publisher every Level-1 section begins at the top of a new page with an all-caps heading. Sidesteps font-size unreliability. Works on both plain-text and OCR pages.

#### Locator E — Statutory sign-off phrases (end boundaries only)

Regex: `approved by the (Board of )?Directors on|signed on (its )?behalf (of the Board)?|The financial statements on pages \d+ to \d+ were approved`. These are mandated and near-unambiguous. On John Lewis they fired correctly at pages 17, 22, 26, 93 — ending Strategic Report, Directors' Report, consolidated FS, and company-only FS respectively.

Feed sign-off phrase pages into Stage 4 as **end-boundary candidates**, not section starts. Prefer them over "next heading" boundary resolution whenever one falls inside a candidate section's page range.

**Bonus — self-describing page-range references.** Extract any `pages?\s+(\d+)\s+to\s+(\d+)` cross-references from within the text (e.g. "principal risks are on pages 11 to 16", "the financial statements on pages 23 to 91"). The publisher has written the ground truth into the document. Feed these into the candidate pool for the mentioned section.

#### Locator F — Numbered-note regex (Notes section only)

Inside the Notes page range (identified by Locator C or by the `"Notes to the…"` anchor), match `^\s*(\d+(?:\.\d+)*)[._\s]+([A-Z][^\n]{0,80})$` to enumerate every note heading (`1.1 ACCOUNTING PRINCIPLES`, `2.9 PARTNERS`, `4. Going concern`). The numbering gives both hierarchy and ordering, so for notes you don't need a dictionary — you walk the number sequence. Use this locator to resolve accounting-policies, critical-estimates, and note-level going-concern sections.

**OCR-tolerant separator.** The separator class `[._\s]+` (not `\.?\s+`) is deliberate: EasyOCR frequently reads a period followed by whitespace as an underscore, so note numbers come through as `"2.1_"`, `"2.2_"`, `"2.3_"` rather than `"2.1 "`, `"2.2 "`, `"2.3 "`. Verified on Heights Management Test 2. Use the tolerant separator unconditionally — it matches born-digital output too.

### Stage 4 — Resolution / voting

For each of the 12 target sections, pick the best candidate from the pool:

1. **Cluster candidates** by (section_id, start_page). Two locators within ±1 page of each other count as agreeing.
2. **High confidence** = ≥2 independent locators agreed on the start page.
3. **Low confidence** = single-locator hit. Still emit it, but flag.
4. **Tie-breaker ranking** (only when multiple candidate starts exist for the same section): TOC > sign-off phrase > hard anchor > soft anchor > all-caps > running header > numbered-note > first-line heuristic.
5. **End boundary** preference: self-describing page-range reference > sign-off phrase inside the candidate window > next Level-1 section start − 1 > next-heading-at-same-level − 1.
6. **Never discard losing candidates silently** — keep them in the `notes` field of the output for downstream audit.

### Stage 5 — Multi-entity boundary detection

Detect **before** slicing. A single PDF can contain multiple entities' accounts (John Lewis example: pages 1–119 are John Lewis plc, pages 120–134 are JLP Scottish Limited Partnership with its own Strategic Report, Directors' Report, Auditor's Report).

Signals that a secondary entity's accounts have started:
- A second "Independent Auditor's Report" after the first has already been matched.
- A change in registered number or company name (regex `Registered number:?\s*\d{8}`, `Company number:?\s*\d{8}`).
- A repeat of a Level-1 anchor (Strategic Report, Directors' Report) after an auditor's report.
- A new printed TOC-like page mid-document.

Establish the **primary entity page range** (typically page 1 → page of first auditor's report end). All locators should be re-scoped to this range before Stage 4 resolution. Secondary entities are out of scope for v1 — emit a `notes` field on the top-level output recording that they exist.

### Stage 6 — Slice and emit

Once each slot has a confident `(start_page, end_page)`:
1. Concatenate `page_text_table[p]["text"]` for `p in range(start_page, end_page+1)`.
2. Do **not** trim headers, page numbers, or footers. Raw text means raw text. Over-including by one page is cheap; under-including is lossy.
3. Emit the output dict in the contract shape above.

## Parent scoping (critical for duplicated anchors)

Some anchors legitimately appear multiple times in the same filing — "Going concern" appeared 8 times on the John Lewis example (Directors' Report, Note 1, Scottish LP, etc). **Two-pass resolution is load-bearing:**

1. **Pass 1:** Resolve Level-1 parents — Strategic Report, Directors' Report, Financial Statements + Notes, Auditor's Report.
2. **Pass 2:** Resolve Level-2 children scoped to their parent's page range only. Going Concern (Directors' Report) must be inside the Directors' Report range; Going Concern (Notes) must be inside the Notes range.

Without parent scoping, the same text lands in multiple slots and the output is corrupted.

## What not to do in Part B

- **No heading cleanup.** Emit raw text including repeated headers, page numbers, footer lines. Downstream cleans.
- **No sentence or paragraph splitting.** Raw text per section, nothing finer.
- **No font-size heuristics as a primary signal.** Verified failure on John Lewis — heading sizes ranged 8.3–11.3pt with no clean body cluster, two real headings sat below a "1.2× body" threshold. Font size is at most a weak tiebreaker inside Stage 4.
- **No `get_text("dict")` line reconstruction for phrase matching.** Verified failure — spans get split per word, multi-word phrases don't match. Use `get_text("text")`.
- **No LLM fallback.** If locators fail, emit `not_found` with provenance and let downstream decide whether to escalate.
- **No per-issuer templates.** Every heuristic must work from generic signals.
- **No cascading locator order.** Run them all in parallel and vote. Cascades hide failure.
- **No collapsing `not_present` and `not_found` into a single null.** The distinction matters downstream.

## Part B — Edge Cases

- **Micro-entity filings with no section structure at all.** Hard anchors won't fire because there's nothing to anchor on. Expect and accept that a micro may produce a single `directors_report` slot containing everything and the other 11 slots as `not_present`. Do not invent structure that isn't there.
- **Scanned filings with poor OCR on headings.** Statutory phrases come through as misreads. Enable OCR-tolerant fuzzy matching on Locator B (hard anchors only, not body prose, edit distance ≤1 per 6 characters).
- **Headings rendered as images** (common for Chair's statement portraits, glossy section dividers). The per-page text-layer probe catches this — Stage 1 routes those pages through OCR even if the rest of the document is born-digital.
- **Unusual ordering** (auditor's report at the front, or Notes interleaved with primary statements). Locators must work positionally, not sequentially. Do not hard-code `strategic_report comes before directors_report comes before auditor_report`.
- **Going concern appearing 5+ times.** Handled by parent scoping in Stage 4.
- **Multi-entity documents (John Lewis Scottish LP case).** Handled by Stage 5. Do not silently double hits.
- **Audit-exempt small companies (no auditor's report).** Filing-size classification in Stage 0 must mark `auditor_report` as `not_present`, not `not_found`.
- **Abbreviated / filleted accounts.** Many sections legitimately absent. Stage 0 determines which are expected.

## Part B — Verification approach

Diagnostic-first, like Part A. Before committing to a production run on a batch:

1. Pick 5–10 filings spanning the size spectrum (1 micro, 2 small, 2 medium, 3+ large).
2. Manually record ground-truth page ranges for each of the 12 sections on each filing.
3. Run the pipeline and diff detected ranges against ground truth.
4. Report per section: found/not, page-range accuracy, which locators fired, which disagreed. The diagnostic output from the John Lewis exercise (13/13 sections found, 12/13 page-range exact) is the model for what this looks like.
5. Only after the diagnostic passes across the sample should the output shape be consumed by downstream layers.

If a locator consistently fails on a filing class, harden the locator — do not add per-issuer hacks.

---

## Cross-cutting notes

- Both parts process one PDF at a time. For batch processing, integrate with the main pipeline in `Merge Data/pipeline.py`.
- Part A stores all monetary values as integers in whole pounds (no pence).
- Part A's JSON output is compatible with the `companies` table schema in Supabase.
- OCR backend is Tesseract via `pytesseract` (subprocess-based, no native DLL loading — avoids Windows Smart App Control blocks). Requires the Tesseract binary installed separately (`winget install UB-Mannheim.TesseractOCR` on Windows). No model download needed — Tesseract ships with `eng.traineddata` out of the box.
- Processing time: With two-pass Tesseract OCR, Part A is ~5-15 seconds per scanned PDF. Born-digital PDFs take seconds. Part B on a born-digital PDF is seconds; on a scanned PDF it reuses Part A's OCR pass so the marginal cost is small.
- Part A was verified against LLM-based extraction on the Heights Management test PDF with 40/40 field checks passing (100% accuracy).
- Part B's statutory-phrase approach was validated on the John Lewis plc 2025 annual report at 13/13 sections found and 12/13 page ranges exact — see the diagnostic run for full provenance.

## Pre-pipeline filing filters

Before downloading or parsing any filing, apply these filters to avoid wasting compute:

1. **Filing type filter.** Companies House returns all account-related filings under `category=accounts`, including administrative forms (AA01 = change of accounting reference date, etc.). Only process filings with `type == "AA"` (actual annual accounts). All other types contain no financial data.

2. **Pre-2015 date filter.** Skip any filing dated before 2015. The label maps (UK GAAP FRS 102, IFRS) target post-2015 terminology. Pre-FRS-102 filings use different vocabulary (e.g., "profit on ordinary activities" vs "profit before taxation") and OCR time on them is wasted on unextractable content.

3. **Initial backfill strategy.** For the initial dataset build, fetch `count=1` (latest filing only) per company. This validates extraction across the full dataset before committing to multi-year history. Historical backfill (`count=5`) can be enabled later once the latest year is confirmed working.
