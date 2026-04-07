"""
PDF extraction for UK Companies House filings (non-iXBRL).

Implements both parts of `Claude skills/PDF-extraction.md`:
  - Part A: quantitative financial tables -> structured JSON (GBP)
  - Part B: qualitative segmentation -> raw text per statutory section

Two input paths sharing one parsing engine:
  - Path A: born-digital PDFs via PyMuPDF text-layer spans (fast)
  - Path B: scanned PDFs via Tesseract OCR at 2x zoom (two-pass)

Both produce items in the shape (bbox, text, conf) where bbox is the
polygon [[x0,y0],[x1,y0],[x1,y1],[x0,y1]]. All downstream steps
(page identification, column detection, row clustering, label matching,
section segmentation) are agnostic to the source.

Entry points:
  - `parse_pdf(pdf_path_or_bytes)`       -> Part A only (quantitative)
  - `extract_sections(pdf_path_or_bytes)` -> Part B only (qualitative)
  - `parse_pdf_full(pdf_path_or_bytes)`  -> both, sharing a single OCR pass

Validation baselines (from the skill):
  - Heights Management Test 2 (7-page scanned filleted small filing) via Path B
  - John Lewis plc 2025 (134-page born-digital IFRS plc) via Path A
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any

import fitz  # PyMuPDF
from PIL import Image


# ---------------------------------------------------------------------------
# Label maps — UK GAAP (small/micro) and IFRS (medium/large)
# Ordering matters: more specific patterns must come before general ones.
# ---------------------------------------------------------------------------

INCOME_LABEL_MAP_UKGAAP: dict[str, str] = {
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
    "(loss)/profit on ordinary activities before taxation": "profit_before_taxation",
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

BALANCE_LABEL_MAP_UKGAAP: dict[str, str] = {
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

INCOME_LABEL_MAP_IFRS: dict[str, str] = {
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

BALANCE_LABEL_MAP_IFRS: dict[str, str] = {
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

CASHFLOW_LABEL_MAP: dict[str, str] = {
    # --- Operating activities: depreciation & amortisation add-backs ---
    "depreciation of tangible fixed assets": "depreciation",
    "depreciation of tangible assets": "depreciation",
    "depreciation of property, plant and equipment": "depreciation",
    "depreciation charge": "depreciation",
    "depreciation and amortisation": "depreciation",
    "depreciation and impairment": "depreciation",
    "amortisation of intangible assets": "amortisation",
    "amortisation of intangible fixed assets": "amortisation",
    "amortisation of goodwill": "amortisation",
    "amortisation charge": "amortisation",

    # --- Operating activities: final sub-total ---
    # Most-specific first: "net cash ... from operating" beats "cash generated"
    "net cash from operating activities": "operating_cash_flow",
    "net cash generated from operating activities": "operating_cash_flow",
    "net cash used in operating activities": "operating_cash_flow",
    "net cash inflow from operating activities": "operating_cash_flow",
    "cash flows from operating activities": "operating_cash_flow",
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
    # Specific patterns first (containing "at beginning"/"at end")
    "cash and cash equivalents at beginning": "opening_cash",
    "cash and cash equivalents at the beginning": "opening_cash",
    "cash and cash equivalents at end": "closing_cash",
    "cash and cash equivalents at the end": "closing_cash",
    "net increase in cash and cash equivalents": "net_change_cash",
    "net decrease in cash and cash equivalents": "net_change_cash",
    "net increase/(decrease) in cash": "net_change_cash",
    "net (decrease)/increase in cash": "net_change_cash",
}

NOTES_LABEL_MAP: dict[str, str] = {
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
    # --- Depreciation & Amortisation (tangible/intangible fixed asset notes) ---
    "depreciation charge for the year": "depreciation",
    "depreciation charged in the year": "depreciation",
    "depreciation for the year": "depreciation",
    "charge for the year": "depreciation",
    "amortisation charge for the year": "amortisation",
    "amortisation charged in the year": "amortisation",
    "amortisation for the year": "amortisation",
}


# ---------------------------------------------------------------------------
# Bbox helpers
# ---------------------------------------------------------------------------

def get_x_center(bbox) -> float:
    return (bbox[0][0] + bbox[2][0]) / 2


def get_y_center(bbox) -> float:
    return (bbox[0][1] + bbox[2][1]) / 2


# ---------------------------------------------------------------------------
# Number parsing (OCR-tolerant)
# ---------------------------------------------------------------------------

_NUMBER_LIKE_RE = re.compile(r"[\d\(\)\-\u2013\u2014\.,]")


def is_number_text(s: str) -> bool:
    """Return True if the string looks like it contains a numeric value."""
    s = s.strip()
    if not s:
        return False
    if s in ("-", "\u2013", "\u2014", ".", "nil", "Nil", "NIL"):
        return True
    # Must contain at least one digit to be a real number
    if not any(c.isdigit() for c in s):
        return False
    # Must be dominated by number-like characters (allow currency / brackets)
    allowed = sum(1 for c in s if c.isdigit() or c in "(),.-\u2013\u2014 \u00a3$")
    return allowed / len(s) >= 0.8


def parse_number(s: str):
    """Parse an OCR-produced number string into an int. None if unparseable."""
    s = s.strip()
    # Open-bracket alone is enough for negative — OCR often drops closing ')'.
    negative = s.startswith("(")
    s = s.replace("(", "").replace(")", "").replace(" ", "")
    s = s.replace("\u00a3", "").replace("$", "")
    if s in ("-", "\u2013", "\u2014", "", "."):
        return 0
    # Commas AND periods are both thousand separators here. OCR routinely
    # reads UK commas as periods ("54,043" -> "54.043"). If the string is all
    # digits once both separators are stripped, treat it as an integer.
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


# ---------------------------------------------------------------------------
# Step 1 — Get text + bounding boxes (two paths, one data shape)
# ---------------------------------------------------------------------------

def _load_pages_text_layer(doc: fitz.Document) -> tuple[dict[int, list], float, float]:
    """Path A — born-digital, convert text-layer spans to (bbox, text, conf)."""
    all_pages: dict[int, list] = {}
    for pi in range(doc.page_count):
        items: list = []
        d = doc[pi].get_text("dict")
        for block in d.get("blocks", []):
            if block.get("type", 0) != 0:
                continue
            for line in block.get("lines", []):
                for sp in line.get("spans", []):
                    t = sp.get("text", "").strip()
                    if not t:
                        continue
                    x0, y0, x1, y1 = sp["bbox"]
                    poly = [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
                    items.append((poly, t, 1.0))
        all_pages[pi + 1] = items
    page_width = doc[0].rect.width
    page_height = doc[0].rect.height
    return all_pages, page_width, page_height


def _tesseract_ocr(img: Image.Image) -> list:
    """Run Tesseract OCR on a PIL image, returning items in the canonical
    (polygon, text, conf) shape used by all downstream code.

    pytesseract.image_to_data returns rows with (left, top, width, height,
    conf, text). We convert each word-level detection into the canonical
    polygon [[x0,y0],[x1,y0],[x1,y1],[x0,y1]] so the entire parsing engine
    remains unchanged.
    """
    try:
        import pytesseract
    except ImportError as e:
        raise RuntimeError(
            "pytesseract is required for scanned PDFs but is not installed. "
            "Install with: pip install pytesseract  (also needs Tesseract binary)"
        ) from e

    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

    data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
    items: list = []
    n = len(data["text"])
    for i in range(n):
        text = data["text"][i].strip()
        if not text:
            continue
        conf = float(data["conf"][i])
        if conf < 0:
            continue  # Tesseract returns -1 for block/paragraph headers
        conf = conf / 100.0  # normalise from 0-100 to 0-1
        x0 = float(data["left"][i])
        y0 = float(data["top"][i])
        w = float(data["width"][i])
        h = float(data["height"][i])
        x1, y1 = x0 + w, y0 + h
        poly = [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
        items.append((poly, text, conf))
    return items


# Fraction of the page (from the top) that pass 1 OCRs. Must cover:
#   - the 25% band used by is_contents_page / printed-TOC locator
#   - the 6% band used by top_band_text for heading matching
#   - enough body to catch the first line of ALL-CAPS section headings
_PASS1_TOP_FRAC = 0.30

# Pass-1 zoom factor. Items are scaled to 2x page-space so they are directly
# comparable with pass-2 output (which is rendered at 2x).
_PASS1_ZOOM = 1.0
_PASS2_ZOOM = 2.0
_COORD_SCALE = _PASS2_ZOOM / _PASS1_ZOOM  # multiply pass-1 coords by this


def _ocr_top_bands(
    doc: fitz.Document,
) -> tuple[dict[int, list], float]:
    """Pass 1 — OCR only the top `_PASS1_TOP_FRAC` of every page at 1x zoom.

    Returns (all_pages, page_width_2x). Item coordinates are scaled up to
    2x page-space so downstream code (and pass 2) can compare freely.
    """
    mat = fitz.Matrix(_PASS1_ZOOM, _PASS1_ZOOM)
    all_pages: dict[int, list] = {}
    for i, page in enumerate(doc):
        pix = page.get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        crop_h = max(1, int(pix.height * _PASS1_TOP_FRAC))
        crop = img.crop((0, 0, pix.width, crop_h))
        results = _tesseract_ocr(crop)
        # Scale crop-space coords to 2x page-space. Crop starts at y=0 so no
        # y offset is needed; x is full-width so no x offset either.
        scaled: list = []
        for poly, text, conf in results:
            spoly = [[float(x) * _COORD_SCALE, float(y) * _COORD_SCALE]
                     for x, y in poly]
            scaled.append((spoly, text, conf))
        all_pages[i + 1] = scaled
    # Page width reported at 2x to stay consistent with pass-2 rendering.
    mat2 = fitz.Matrix(_PASS2_ZOOM, _PASS2_ZOOM)
    page_width = doc[0].get_pixmap(matrix=mat2).width
    return all_pages, page_width


def _ocr_full_pages(
    doc: fitz.Document, page_indices: set[int]
) -> dict[int, list]:
    """Pass 2 — full-page OCR at 2x zoom, but only for the given 1-indexed
    page numbers. Returns {pnum: items}."""
    if not page_indices:
        return {}
    mat = fitz.Matrix(_PASS2_ZOOM, _PASS2_ZOOM)
    out: dict[int, list] = {}
    for pnum in sorted(page_indices):
        idx = pnum - 1
        if idx < 0 or idx >= doc.page_count:
            continue
        pix = doc[idx].get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        out[pnum] = _tesseract_ocr(img)
    return out


def _identify_pass2_targets(top_band_pages: dict[int, list]) -> set[int]:
    """Given pass-1 top-band-only data, decide which pages need full OCR.

    Union of:
      - financial-statement pages (income / balance / cashflow / notes)
      - any page whose top band matches a Part B hard-anchor regex
      - ±1 neighbours of the balance sheet (filleted-note / opt-out text)
      - ±1 neighbours of every financial statement (continued statements)
    """
    targets: set[int] = set()

    income_page: int | None = None
    balance_page: int | None = None
    cashflow_page: int | None = None

    for pnum in sorted(top_band_pages.keys()):
        if is_contents_page(top_band_pages, pnum):
            # TOC pages are not parsed as financial statements but are still
            # useful for Locator A — keep them in pass 2 so the printed-TOC
            # locator has full text to work with.
            targets.add(pnum)
            continue

        # Use ALL pass-1 text for heading matching (not just frac=0.06).
        # Pass-1 only covers the top 30% of the page, so all of it is
        # effectively "top band". At 1x zoom, Tesseract often garbles the
        # very top of the page (e.g. DocuSign headers), making frac=0.06
        # miss real headings that sit slightly lower in the top 30%.
        top = " ".join(t for _, t, _ in top_band_pages.get(pnum, [])).upper()

        if income_page is None and (
            "INCOME STATEMENT" in top
            or "PROFIT AND LOSS ACCOUNT" in top
            or "STATEMENT OF COMPREHENSIVE INCOME" in top
            or "STATEMENT OF INCOME AND RETAINED EARNINGS" in top
            or "INCOME AND RETAINED EARNINGS" in top
        ):
            income_page = pnum
            targets.add(pnum)
            continue

        if balance_page is None and (
            "BALANCE SHEET" in top or "STATEMENT OF FINANCIAL POSITION" in top
        ) and "CONTINUED" not in top:
            balance_page = pnum
            targets.add(pnum)
            continue

        if cashflow_page is None and (
            "CASH FLOW STATEMENT" in top
            or "STATEMENT OF CASH FLOWS" in top
            or "CASH FLOWS" in top
        ) and "CONTINUED" not in top:
            cashflow_page = pnum
            targets.add(pnum)
            continue

        if "NOTES TO THE" in top and "FINANCIAL STATEMENTS" in top:
            targets.add(pnum)

    # Part B hard-anchor sweep: any page whose top band hits a statutory
    # heading regex needs full OCR so Locator B / classification can read it.
    # Joined top-band text per page, scanned with each anchor pattern.
    for pnum, items in top_band_pages.items():
        if pnum in targets:
            continue
        joined = " ".join(t for _, t, _ in items)
        if not joined:
            continue
        for pattern in HARD_ANCHORS.values():
            if re.search(pattern, joined, re.IGNORECASE):
                targets.add(pnum)
                break

    # ±1 neighbours of every financial statement page — catches continued
    # statements and the "opted not to deliver" / s444(4) boilerplate note
    # that typically sits next to the balance sheet.
    neighbours: set[int] = set()
    max_page = max(top_band_pages.keys()) if top_band_pages else 0
    for anchor in (income_page, balance_page, cashflow_page):
        if anchor is None:
            continue
        for off in (-1, 1):
            n = anchor + off
            if 1 <= n <= max_page:
                neighbours.add(n)
    targets |= neighbours

    return targets


def _load_pages_ocr(doc: fitz.Document) -> tuple[dict[int, list], float, float]:
    """Path B — scanned / no text layer.

    Two-pass strategy:
      1. OCR only the top 30% of every page at 1x zoom to cheaply identify
         financial-statement pages and Part B section pages.
      2. Full-page OCR at 2x zoom for that targeted subset only.

    Pages outside the pass-2 set retain their pass-1 top-band items, which
    is sufficient for Locators A/C/D/F (all of which only read top-of-page
    text) and for the shared `build_page_text_table` step. Financial
    parsing and Locator B only ever touch pass-2 pages, so they see full
    2x OCR output exactly as in the old single-pass path.
    """
    top_pages, page_width = _ocr_top_bands(doc)
    targets = _identify_pass2_targets(top_pages)
    full_pages = _ocr_full_pages(doc, targets)

    # Merge: full pass-2 items replace pass-1 top-band items where available.
    all_pages: dict[int, list] = dict(top_pages)
    for pnum, items in full_pages.items():
        all_pages[pnum] = items
    # Page height at 2x to stay consistent with page_width and pass-2 coords.
    mat2 = fitz.Matrix(_PASS2_ZOOM, _PASS2_ZOOM)
    page_height = doc[0].get_pixmap(matrix=mat2).height
    return all_pages, page_width, page_height


def _open_doc(pdf_path_or_bytes) -> fitz.Document:
    """Open a PDF from a file path or raw bytes."""
    if isinstance(pdf_path_or_bytes, (bytes, bytearray)):
        return fitz.open(stream=bytes(pdf_path_or_bytes), filetype="pdf")
    return fitz.open(pdf_path_or_bytes)


def load_pages(pdf_path_or_bytes) -> tuple[dict[int, list], float, float, str]:
    """Probe the text layer and route to Path A or Path B.

    Accepts a file path or raw bytes. Returns (all_pages, page_width,
    page_height, source) where source is 'text' or 'ocr'.  page_height is
    in the same coordinate space as item coordinates (PDF points for
    text-layer, 2x pixel-space for OCR) and is used by top_band_text for
    absolute cutoff calculations.
    """
    doc = _open_doc(pdf_path_or_bytes)
    try:
        has_text_layer = any(
            len(doc[i].get_text("text").split()) > 20
            for i in range(min(5, doc.page_count))
        )
        if has_text_layer:
            all_pages, page_width, page_height = _load_pages_text_layer(doc)
            source = "text"
        else:
            all_pages, page_width, page_height = _load_pages_ocr(doc)
            source = "ocr"
    finally:
        doc.close()
    return all_pages, page_width, page_height, source


# ---------------------------------------------------------------------------
# Step 2 — Identify financial pages (top-band match, first-wins)
# ---------------------------------------------------------------------------

def page_text(all_pages: dict[int, list], page_num: int) -> str:
    return " ".join(t for _, t, _ in all_pages.get(page_num, []))


def top_band_text(
    all_pages: dict[int, list],
    page_num: int,
    frac: float = 0.15,
    page_height: float | None = None,
) -> str:
    """Return uppercased text from the top portion of the page.

    When *page_height* is provided the cutoff is ``page_height * frac``
    (absolute — independent of where items happen to sit on the page).
    This is critical for scanned PDFs where item y-ranges can be narrow,
    causing relative fractions to chop off headings.

    When *page_height* is None the legacy relative behaviour is used as
    a fallback (fraction of item y-range).
    """
    items = all_pages.get(page_num, [])
    if not items:
        return ""
    if page_height is not None:
        cutoff = page_height * frac
    else:
        ys = [get_y_center(b) for b, _, _ in items]
        y_min, y_max = min(ys), max(ys)
        cutoff = y_min + (y_max - y_min) * frac
    return " ".join(t for b, t, _ in items if get_y_center(b) <= cutoff).upper()


def _y_cutoff(items: list, frac: float,
              page_height: float | None = None) -> float:
    """Compute the y-cutoff for the top *frac* of a page.

    When *page_height* is available the cutoff is absolute (fraction of
    page height).  Otherwise falls back to a fraction of the item y-range.
    """
    if page_height is not None:
        return page_height * frac
    ys = [get_y_center(b) for b, _, _ in items]
    y_min, y_max = min(ys), max(ys)
    return y_min + (y_max - y_min) * frac


def is_contents_page(all_pages: dict[int, list], page_num: int,
                     page_height: float | None = None) -> bool:
    return "CONTENTS" in top_band_text(all_pages, page_num, frac=0.25,
                                       page_height=page_height)


def identify_pages(
    all_pages: dict[int, list],
    page_height: float | None = None,
) -> tuple[int | None, int | None, int | None, list[int], bool]:
    """Return (income_page, balance_page, cashflow_page, notes_pages, is_filleted)."""
    income_page: int | None = None
    balance_page: int | None = None
    cashflow_page: int | None = None
    notes_pages: list[int] = []

    # Filleted detection: does any page body say "opted not to deliver"
    # the Profit and Loss Account (s444(4))?
    is_filleted = False
    for pnum in all_pages:
        full_u = page_text(all_pages, pnum).upper()
        if "OPTED NOT TO DELIVER" in full_u and "PROFIT AND LOSS" in full_u:
            is_filleted = True
            break
        if "SECTION 444(4)" in full_u:
            is_filleted = True
            break

    for pnum in sorted(all_pages.keys()):
        if is_contents_page(all_pages, pnum, page_height=page_height):
            continue
        top = top_band_text(all_pages, pnum, page_height=page_height)
        full_u = page_text(all_pages, pnum).upper()

        # Income statement — first-match wins, skip if page declares filleted
        if income_page is None and not is_filleted and (
            "INCOME STATEMENT" in top
            or "PROFIT AND LOSS ACCOUNT" in top
            or "STATEMENT OF COMPREHENSIVE INCOME" in top
            or "STATEMENT OF INCOME AND RETAINED EARNINGS" in top
            or "INCOME AND RETAINED EARNINGS" in top
        ) and "OPTED NOT TO DELIVER" not in full_u:
            income_page = pnum
            continue

        if balance_page is None and (
            "BALANCE SHEET" in top or "STATEMENT OF FINANCIAL POSITION" in top
        ) and "CONTINUED" not in top:
            balance_page = pnum
            continue

        if cashflow_page is None and (
            "CASH FLOW STATEMENT" in top
            or "STATEMENT OF CASH FLOWS" in top
            or "CASH FLOWS" in top
        ) and "CONTINUED" not in top:
            cashflow_page = pnum
            continue

        if "NOTES TO THE" in top and "FINANCIAL STATEMENTS" in top:
            notes_pages.append(pnum)

    return income_page, balance_page, cashflow_page, notes_pages, is_filleted


# ---------------------------------------------------------------------------
# Step 3 — Detect year columns
# ---------------------------------------------------------------------------

def detect_years(all_pages: dict[int, list], pages: list[int | None]) -> tuple[str, str]:
    years: list[str] = []
    for pnum in pages:
        if not pnum:
            continue
        for _, text, _ in all_pages.get(pnum, []):
            m = re.match(r"^(20\d{2})$", text.strip())
            if m:
                years.append(m.group(1))
    years = sorted(set(years))
    current = years[-1] if years else "Unknown"
    prior = years[-2] if len(years) >= 2 else "Unknown"
    return current, prior


# ---------------------------------------------------------------------------
# Step 4 — Core parsing engine
# ---------------------------------------------------------------------------

def cluster_x_positions(xs: list[float], gap_threshold: float = 60) -> list[float]:
    """Cluster x-positions into groups separated by gaps > threshold."""
    if not xs:
        return []
    xs_sorted = sorted(xs)
    clusters: list[list[float]] = [[xs_sorted[0]]]
    for x in xs_sorted[1:]:
        if x - clusters[-1][-1] > gap_threshold:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    return [sum(c) / len(c) for c in clusters]


def _detect_notes_column(items: list, page_width: float) -> float | None:
    """Detect the x-position of the notes-reference column.

    Notes columns contain small single-digit integers (1-30) clustered at
    the same x-position, sitting between the label column and the financial
    value columns.  Often headed by the word "Notes" or "Note".

    Returns the centre x of the notes column, or None if not detected.
    """
    # Collect candidate note-ref items: single/double-digit integers in the
    # middle zone (between labels at <40% and values at >55% of page width)
    note_xs: list[float] = []
    for bbox, text, _ in items:
        if not is_number_text(text):
            continue
        val = parse_number(text)
        if val is None or val != int(val) or val < 1 or val > 30:
            continue
        x = get_x_center(bbox)
        if page_width * 0.35 < x < page_width * 0.55:
            note_xs.append(x)

    if len(note_xs) < 2:
        return None

    # Check they cluster tightly (within 30px spread)
    if max(note_xs) - min(note_xs) < 30:
        return sum(note_xs) / len(note_xs)
    return None


def parse_financial_page(
    all_pages: dict[int, list],
    page_width: float,
    page_num: int,
    label_map: dict[str, str],
    current_year: str,
    prior_year: str,
) -> dict[str, dict]:
    """Parse a financial page using spatial row/column clustering."""
    items = all_pages.get(page_num, [])
    if not items:
        return {}

    # Build year-integer exclusion set early so it applies to clustering too.
    year_ints: set[int] = set()
    for y in (current_year, prior_year):
        if isinstance(y, str) and y.isdigit():
            year_ints.add(int(y))

    # Detect the notes-reference column so we can exclude it from clustering
    # and value extraction.  Note refs are small integers (1-30) in a narrow
    # column between the labels and the financial value columns.
    notes_col_x = _detect_notes_column(items, page_width)

    # Collect x-positions of all number-like text in the right portion.
    # Exclude: small integers (|val| ≤ 50) — note refs / page numbers;
    #          year integers (2024, 2025) — column headers, not values;
    #          items within ±25px of the detected notes column.
    number_xs: list[float] = []
    for bbox, text, _ in items:
        if is_number_text(text):
            val = parse_number(text)
            if val is None or abs(val) <= 50:
                continue
            if int(val) in year_ints:
                continue
            x = get_x_center(bbox)
            if x <= page_width * 0.40:
                continue
            if notes_col_x is not None and abs(x - notes_col_x) < 25:
                continue
            number_xs.append(x)

    if not number_xs:
        return {}

    col_centers = cluster_x_positions(number_xs, gap_threshold=60)

    if len(col_centers) <= 2:
        c1 = col_centers[0] if len(col_centers) >= 1 else page_width * 0.6
        c2 = col_centers[1] if len(col_centers) >= 2 else page_width * 0.85
    else:
        # 4-column layout: left half = current year, right half = prior year
        mid_idx = len(col_centers) // 2
        c1_group = col_centers[:mid_idx]
        c2_group = col_centers[mid_idx:]
        c1 = sum(c1_group) / len(c1_group)
        c2 = sum(c2_group) / len(c2_group)

    mid_boundary = (c1 + c2) / 2

    def classify_col(x: float) -> str:
        if x < page_width * 0.40:
            return "label"
        # Items in the notes column are not financial values
        if notes_col_x is not None and abs(x - notes_col_x) < 25:
            return "note_ref"
        return "col1" if x < mid_boundary else "col2"

    row_items = []
    for bbox, text, conf in items:
        x = get_x_center(bbox)
        y = get_y_center(bbox)
        row_items.append({
            "y": y,
            "x": x,
            "col": classify_col(x),
            "text": text,
            "conf": conf,
        })
    row_items.sort(key=lambda r: r["y"])

    # Cluster by Y proximity (20px threshold)
    clusters: list[list[dict]] = []
    current: list[dict] = []
    for item in row_items:
        if current and abs(item["y"] - current[-1]["y"]) > 20:
            clusters.append(current)
            current = [item]
        else:
            current.append(item)
    if current:
        clusters.append(current)

    result: dict[str, dict] = {}
    pending_label = ""

    for idx, cluster in enumerate(clusters):
        labels = [it for it in cluster if it["col"] == "label"]
        col1_items = [
            it for it in cluster
            if it["col"] == "col1" and is_number_text(it["text"])
        ]
        col2_items = [
            it for it in cluster
            if it["col"] == "col2" and is_number_text(it["text"])
        ]

        if labels:
            row_label = " ".join(
                l["text"] for l in sorted(labels, key=lambda x: x["x"])
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
                        l["text"] for l in sorted(next_labels, key=lambda x: x["x"])
                    ).strip().lower()
                    for pattern, field in label_map.items():
                        if pattern in next_label_text and field != direct_match:
                            next_independently_matches = True
                            break

            if direct_match and next_independently_matches:
                # Standalone nil line item (dashes not picked up by OCR)
                result[direct_match] = {current_year: 0, prior_year: 0}
                pending_label = ""
            else:
                # Section header — accumulate for next row
                pending_label = (pending_label + " " + row_label).strip()
            continue

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

        # Dash/nil indicators on the right side
        if col1_val is None or col2_val is None:
            right_items = [
                it for it in cluster
                if it["x"] > page_width * 0.40 and not is_number_text(it["text"])
            ]
            for it in right_items:
                txt = it["text"].strip()
                if txt in ("-", "\u2013", "\u2014", "nil", "Nil", "NIL", "."):
                    if it["x"] < mid_boundary and col1_val is None:
                        col1_val = 0
                    elif it["x"] >= mid_boundary and col2_val is None:
                        col2_val = 0

        if col1_val is not None or col2_val is not None:
            entry: dict = {}
            if col1_val is not None:
                entry[current_year] = col1_val
            if col2_val is not None:
                entry[prior_year] = col2_val
            result[matched_field] = entry

    return result


# ---------------------------------------------------------------------------
# Step 5 — Pick the label-map dialect
# ---------------------------------------------------------------------------

def choose_label_maps(all_pages: dict[int, list]) -> tuple[dict, dict, str]:
    """Choose between UK GAAP and IFRS by scanning for dialect markers.

    Returns (income_map, balance_map, dialect_name).
    """
    joined = " ".join(
        page_text(all_pages, p).lower() for p in all_pages
    )

    ifrs_signals = [
        "uk-adopted ifrs",
        "international financial reporting standards",
        "property, plant and equipment",
        "trade and other receivables",
        "trade and other payables",
        "total equity",
    ]
    ukgaap_signals = [
        "frs 102",
        "frs 105",
        "section 1a",
        "s1a",
        "shareholders' funds",
        "shareholders funds",
        "turnover",
        "tangible assets",
        "called up share capital",
    ]

    ifrs_score = sum(1 for s in ifrs_signals if s in joined)
    ukgaap_score = sum(1 for s in ukgaap_signals if s in joined)

    if ifrs_score > ukgaap_score:
        return INCOME_LABEL_MAP_IFRS, BALANCE_LABEL_MAP_IFRS, "ifrs"
    return INCOME_LABEL_MAP_UKGAAP, BALANCE_LABEL_MAP_UKGAAP, "uk_gaap"


# ---------------------------------------------------------------------------
# Step 6 — Company metadata
# ---------------------------------------------------------------------------

def extract_metadata(all_pages: dict[int, list],
                     page_height: float | None = None) -> dict[str, str]:
    cover = page_text(all_pages, 1)
    reg_match = re.search(r"\b(\d{8})\b", cover)
    reg_number = reg_match.group(1) if reg_match else "Unknown"

    year_match = re.search(
        r"YEAR ENDED\s+(\d{1,2}\s+\w+\s+\d{4})", cover, re.IGNORECASE
    )
    year_ended = year_match.group(1) if year_match else "Unknown"

    # Best-effort company name: first top-band token of page 1 containing
    # LIMITED / PLC / LTD.
    company_name = "Unknown"
    top1 = top_band_text(all_pages, 1, frac=0.25, page_height=page_height)
    m = re.search(r"([A-Z][A-Z0-9 &,\.'()\-]+(?:LIMITED|PLC|LTD|LLP))", top1)
    if m:
        company_name = m.group(1).strip()

    return {
        "company": company_name,
        "registered_number": reg_number,
        "year_ended": year_ended,
    }


# ---------------------------------------------------------------------------
# Step 7 + 8 — Run extraction, assemble output, sanity-check
# ---------------------------------------------------------------------------

def _sanity_check(output: dict, current_year: str, prior_year: str) -> list[str]:
    warnings: list[str] = []
    bs = output.get("balance_sheet", {}) or {}

    # 1) total_assets_less_current_liabilities ?= shareholders_funds (UK GAAP)
    talcl = bs.get("total_assets_less_current_liabilities") or {}
    cap = bs.get("capital_and_reserves") or {}
    shf = (cap.get("shareholders_funds") if isinstance(cap, dict) else None) or {}
    for year in (current_year, prior_year):
        a = talcl.get(year)
        b = shf.get(year)
        if a is not None and b is not None and a != b:
            warnings.append(
                f"{year}: total_assets_less_current_liabilities ({a}) "
                f"!= shareholders_funds ({b})"
            )

    # 2) net_assets ?= total_equity (IFRS)
    na = bs.get("net_assets") or {}
    te = bs.get("total_equity") or {}
    for year in (current_year, prior_year):
        a = na.get(year) if isinstance(na, dict) else None
        b = te.get(year) if isinstance(te, dict) else None
        if a is not None and b is not None and a != b:
            warnings.append(
                f"{year}: net_assets ({a}) != total_equity ({b})"
            )

    # 3) Income statement: turnover - cost_of_sales = gross_profit
    inc = output.get("income_statement") or {}
    if isinstance(inc, dict):
        rev = inc.get("turnover") or inc.get("revenue") or {}
        cos = inc.get("cost_of_sales") or {}
        gp = inc.get("gross_profit") or {}
        for year in (current_year, prior_year):
            r = rev.get(year) if isinstance(rev, dict) else None
            c = cos.get(year) if isinstance(cos, dict) else None
            g = gp.get(year) if isinstance(gp, dict) else None
            if r is not None and c is not None and g is not None:
                # cost_of_sales is usually stored as a negative
                if abs((r + c) - g) > 1 and abs((r - c) - g) > 1:
                    warnings.append(
                        f"{year}: revenue ({r}) - cost_of_sales ({c}) "
                        f"!= gross_profit ({g})"
                    )

    # 4) Cash flow totals: operating + investing + financing ~= net_change_cash
    cf = output.get("cash_flow_statement") or {}
    if isinstance(cf, dict) and cf:
        def _cf_val(field, year):
            entry = cf.get(field)
            if isinstance(entry, dict):
                return entry.get(year)
            return None

        for year in (current_year, prior_year):
            op = _cf_val("operating_cash_flow", year)
            inv = _cf_val("net_cash_investing", year)
            fin = _cf_val("net_cash_financing", year)
            net_change = _cf_val("net_change_cash", year)
            if all(v is not None for v in (op, inv, fin, net_change)):
                if abs((op + inv + fin) - net_change) > 1:
                    warnings.append(
                        f"{year}: operating_cash_flow ({op}) + net_cash_investing "
                        f"({inv}) + net_cash_financing ({fin}) != net_change_cash "
                        f"({net_change})"
                    )

            # 5) opening + net_change ~= closing
            opening = _cf_val("opening_cash", year)
            closing = _cf_val("closing_cash", year)
            if opening is not None and net_change is not None and closing is not None:
                if abs((opening + net_change) - closing) > 1:
                    warnings.append(
                        f"{year}: opening_cash ({opening}) + net_change_cash "
                        f"({net_change}) != closing_cash ({closing})"
                    )

            # 6) closing_cash ~= balance sheet cash (±1% tolerance for FX)
            if closing is not None:
                bs_cash = None
                # UK GAAP nested
                ca = bs.get("current_assets") if isinstance(bs, dict) else None
                if isinstance(ca, dict):
                    cab = ca.get("cash_at_bank")
                    if isinstance(cab, dict):
                        bs_cash = cab.get(year)
                # IFRS flat
                if bs_cash is None:
                    cab = bs.get("cash_and_equivalents") if isinstance(bs, dict) else None
                    if isinstance(cab, dict):
                        bs_cash = cab.get(year)
                if bs_cash is not None and closing != 0:
                    if abs(closing - bs_cash) / max(abs(closing), 1) > 0.01:
                        warnings.append(
                            f"{year}: cash flow closing_cash ({closing}) != "
                            f"balance sheet cash ({bs_cash})"
                        )

    return warnings


def _assemble_output_uk_gaap(
    metadata: dict,
    current_year: str,
    prior_year: str,
    income: dict,
    balance: dict,
    cashflow: dict,
    notes: dict,
    is_filleted: bool,
) -> dict:
    return {
        **metadata,
        "currency": "GBP",
        "dialect": "uk_gaap",
        "current_year": current_year,
        "prior_year": prior_year,
        "filleted": is_filleted,
        "income_statement": None if is_filleted else income,
        # Cash flow statement is not required for small / micro / filleted filings
        # under FRS 102 s1A and FRS 105, so this is commonly empty for UK GAAP.
        "cash_flow_statement": cashflow,
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
            "total_assets_less_current_liabilities":
                balance.get("total_assets_less_current_liabilities", {}),
            "net_assets": balance.get("net_assets", {}),
            "capital_and_reserves": {
                k: v for k, v in balance.items()
                if k in (
                    "called_up_share_capital",
                    "share_premium",
                    "retained_earnings",
                    "shareholders_funds",
                )
            },
        },
        "notes": {
            "debtors": {
                k: v for k, v in notes.items()
                if "receivable" in k or "debtors" in k or "prepayment" in k
            },
            "creditors": {
                k: v for k, v in notes.items()
                if k not in ("maintenance_fees_receivable", "trade_debtors", "prepayments")
            },
        },
    }


def _assemble_output_ifrs(
    metadata: dict,
    current_year: str,
    prior_year: str,
    income: dict,
    balance: dict,
    cashflow: dict,
    notes: dict,
) -> dict:
    return {
        **metadata,
        "currency": "GBP",
        "dialect": "ifrs",
        "current_year": current_year,
        "prior_year": prior_year,
        "filleted": False,
        "income_statement": income,
        "balance_sheet": balance,
        "cash_flow_statement": cashflow,
        "notes": notes,
    }


def parse_pdf(pdf_path_or_bytes, preloaded=None) -> dict[str, Any]:
    """Part A entry point. Parse a UK Companies House PDF and return JSON.

    Routes text-layer PDFs to Path A and scanned PDFs to Path B. Produces
    the same structured output in either case.

    `preloaded` is the tuple returned by `load_pages()`; pass it to avoid
    re-running OCR when Part A and Part B are both needed.
    """
    if preloaded is not None:
        all_pages, page_width, page_height, source = preloaded
    else:
        all_pages, page_width, page_height, source = load_pages(pdf_path_or_bytes)

    income_page, balance_page, cashflow_page, notes_pages, is_filleted = identify_pages(
        all_pages, page_height=page_height)
    current_year, prior_year = detect_years(
        all_pages, [income_page, balance_page, cashflow_page]
    )
    income_map, balance_map, dialect = choose_label_maps(all_pages)

    income: dict = {}
    balance: dict = {}
    cashflow: dict = {}
    notes: dict = {}

    if income_page and not is_filleted:
        income = parse_financial_page(
            all_pages, page_width, income_page, income_map,
            current_year, prior_year,
        )
    if balance_page:
        balance = parse_financial_page(
            all_pages, page_width, balance_page, balance_map,
            current_year, prior_year,
        )
    if cashflow_page:
        cashflow = parse_financial_page(
            all_pages, page_width, cashflow_page, CASHFLOW_LABEL_MAP,
            current_year, prior_year,
        )
    for np_ in notes_pages:
        notes.update(
            parse_financial_page(
                all_pages, page_width, np_, NOTES_LABEL_MAP,
                current_year, prior_year,
            )
        )

    metadata = extract_metadata(all_pages, page_height=page_height)

    if dialect == "ifrs":
        output = _assemble_output_ifrs(
            metadata, current_year, prior_year, income, balance, cashflow, notes,
        )
    else:
        output = _assemble_output_uk_gaap(
            metadata, current_year, prior_year, income, balance, cashflow, notes, is_filleted,
        )

    output["extraction"] = {
        "source": source,
        "income_page": income_page,
        "balance_page": balance_page,
        "cashflow_page": cashflow_page,
        "notes_pages": notes_pages,
    }

    warnings = _sanity_check(output, current_year, prior_year)
    if warnings:
        output["warnings"] = warnings

    return output


# ===========================================================================
# PART B — Qualitative section extraction
# ===========================================================================
#
# Segments a UK filing into the 12 statutory sections and returns raw text
# per section. Reuses the same `all_pages` structure produced by Part A's
# `load_pages()`, so a single OCR pass feeds both parts.
# ---------------------------------------------------------------------------

# Target sections (id -> human label)
SECTION_IDS: list[str] = [
    "strategic_report",
    "section_172",
    "principal_risks",
    "viability_statement",
    "directors_report",
    "principal_activity",
    "going_concern_directors",
    "directors_responsibilities",
    "auditor_report",
    "accounting_policies",
    "critical_estimates",
    "going_concern_note",
]

# Hard anchors (Stage 3 / Locator B) — statutory phrases with mandated wording
# Patterns are applied case-insensitive to joined page text and to line starts.
HARD_ANCHORS: dict[str, str] = {
    "strategic_report":          r"\bSTRATEGIC\s+REPORT\b",
    "directors_report":          r"\bDIRECTORS[\u2019']?\s*REPORT\b|\bREPORT\s+OF\s+THE\s+DIRECTORS\b",
    "section_172":               r"\bSECTION\s*172(?:\s*\(1\))?\b|\bS\.?\s*172\b",
    "principal_risks":           r"\bPRINCIPAL\s+RISKS(?:\s+AND\s+UNCERTAINTIES)?\b",
    "viability_statement":       r"\bVIABILITY\s+STATEMENT\b",
    "going_concern_directors":   r"\bGOING\s+CONCERN\b",
    "going_concern_note":        r"\bGOING\s+CONCERN\b",
    "directors_responsibilities": r"STATEMENT\s+OF\s+DIRECTORS[\u2019']?\s*RESPONSIBILITIES",
    "auditor_report":            r"INDEPENDENT\s+AUDITOR[\u2019']?S?[\u2019']?\s*REPORT",
    "principal_activity":        r"\bPRINCIPAL\s+ACTIVIT(?:Y|IES)\b",
    "accounting_policies":       r"\bACCOUNTING\s+POLICIES\b|\bBASIS\s+OF\s+PREPARATION\b",
    "critical_estimates":        r"\bCRITICAL\s+(?:ACCOUNTING\s+)?(?:JUDGEMENTS|ESTIMATES)\b|\bKEY\s+SOURCES\s+OF\s+ESTIMATION\b",
}

# Body-confirmation tokens for Locator B auditor_report promotion.
# A bare "Independent Auditor's Report" mention in a filleted filing is
# usually a reference line, not the report itself. Require at least one
# of these tokens on the same or next page.
AUDITOR_BODY_TOKENS = [
    "basis for opinion",
    "key audit matters",
    "we have audited",
    "in our opinion",
]


# ---------------------------------------------------------------------------
# Stage 0 — Filing classification (size + mode)
# ---------------------------------------------------------------------------

def _joined_doc_text(all_pages: dict[int, list]) -> str:
    return " ".join(page_text(all_pages, p) for p in sorted(all_pages.keys()))


def classify_filing(all_pages: dict[int, list]) -> dict[str, Any]:
    """Classify the filing on two axes: size (micro/small/medium/large) and
    mode (full/abbreviated/filleted/micro-minimal). Returns a dict with keys
    {size, mode, page_count, reasons}.
    """
    joined = _joined_doc_text(all_pages).lower()
    page_count = max(all_pages.keys()) if all_pages else 0
    reasons: list[str] = []

    # Size
    size = "medium"
    if "frs 105" in joined or "micro-entity" in joined or "micro entity" in joined:
        size = "micro"
        reasons.append("frs_105_marker")
    elif (
        "frs 102 section 1a" in joined
        or "section 1a" in joined
        or "small companies regime" in joined
        or "small entities" in joined
        or " s1a" in joined
    ):
        size = "small"
        reasons.append("frs_102_s1a_marker")
    elif (
        "uk-adopted international accounting standards" in joined
        or "uk-adopted ifrs" in joined
        or "international financial reporting standards" in joined
    ) or page_count >= 60:
        size = "large"
        reasons.append("ifrs_or_60plus_pages")
    else:
        size = "medium"

    # Tiebreak by page count
    if size == "medium" and page_count < 15:
        size = "micro"
    elif size == "medium" and page_count < 30:
        size = "small"

    # Mode
    mode = "full"
    if (
        "section 444(4)" in joined
        or "opted not to deliver" in joined
        and "profit and loss" in joined
    ):
        mode = "filleted"
        reasons.append("s444_filleted")
    elif size == "micro" and page_count <= 10:
        mode = "micro_minimal"
        reasons.append("micro_minimal_by_page_count")
    elif "abbreviated" in joined and "accounts" in joined:
        mode = "abbreviated"
        reasons.append("abbreviated_marker")

    return {
        "size": size,
        "mode": mode,
        "page_count": page_count,
        "reasons": reasons,
    }


def _expected_sections(classification: dict) -> dict[str, str]:
    """Return section_id -> 'expected' | 'not_present' based on (size, mode).

    'expected' sections that are not found become 'not_found' (quality flag).
    'not_present' sections that are not found are legitimately absent.
    """
    size = classification["size"]
    mode = classification["mode"]

    expected: dict[str, str] = {sid: "expected" for sid in SECTION_IDS}

    if size in ("micro", "small"):
        for sid in (
            "strategic_report",
            "section_172",
            "principal_risks",
            "viability_statement",
        ):
            expected[sid] = "not_present"

    if mode == "filleted":
        for sid in (
            "strategic_report",
            "section_172",
            "principal_risks",
            "viability_statement",
            "directors_report",
            "directors_responsibilities",
            "auditor_report",
            "going_concern_directors",
        ):
            expected[sid] = "not_present"

    if mode == "micro_minimal":
        for sid in SECTION_IDS:
            if sid not in ("accounting_policies", "going_concern_note"):
                expected[sid] = "not_present"

    return expected


# ---------------------------------------------------------------------------
# Stage 2 — Unified page-text table (text field is MANDATORY; everything
# downstream in Stages 3-6 reads it via this dict, never by iterating tokens)
# ---------------------------------------------------------------------------

def build_page_text_table(all_pages: dict[int, list], source: str,
                          page_height: float | None = None) -> dict[int, dict]:
    """Build the per-page text table used by Stages 3-6."""
    table: dict[int, dict] = {}
    for pnum, items in all_pages.items():
        # Sort tokens by y then x so the joined string preserves reading order
        sorted_items = sorted(
            items, key=lambda it: (get_y_center(it[0]), get_x_center(it[0]))
        )
        text = " ".join(t for _, t, _ in sorted_items)
        confs = [c for _, _, c in items if isinstance(c, (int, float))]
        quality = float(sum(confs) / len(confs)) if confs else 1.0
        table[pnum] = {
            "text": text,
            "items": sorted_items,  # kept for top-band and y-sorted operations
            "source": source,
            "quality": quality,
            "page_height": page_height,
        }
    return table


# ---------------------------------------------------------------------------
# Stage 3 — Locators
# Each locator returns a list of candidate dicts:
#   {"section_id": str, "start_page": int, "end_page": int | None,
#    "confidence": "high"|"low", "signal": str}
# ---------------------------------------------------------------------------

def _locator_b_hard_anchors(
    table: dict[int, dict], classification: dict
) -> list[dict]:
    """Locator B — statutory phrase dictionary over joined page text.

    Applies a heading-like filter (matched phrase near page top, short or
    all-caps) and an auditor-body-confirmation gate to suppress references.
    """
    candidates: list[dict] = []

    for sid, pattern in HARD_ANCHORS.items():
        regex = re.compile(pattern, re.IGNORECASE)
        for pnum, entry in table.items():
            text = entry["text"]
            if not text:
                continue
            if not regex.search(text):
                continue

            # Heading-like filter: the phrase must appear in the top portion
            # of the page (top 25% of items) AND the first line containing
            # it must be short (<= 12 words) or all-caps.
            items = entry["items"]
            if not items:
                continue
            ph = entry.get("page_height")
            top_cutoff = _y_cutoff(items, 0.25, page_height=ph)
            top_tokens = [t for b, t, _ in items if get_y_center(b) <= top_cutoff]
            top_text = " ".join(top_tokens)

            if not regex.search(top_text):
                # Phrase exists on the page but is body prose, not a heading
                continue

            # Short-or-all-caps filter
            if len(top_tokens) > 40:
                # Stray phrase inside a long top band — keep only if all-caps
                all_caps = sum(1 for t in top_tokens if t.isupper()) / len(top_tokens)
                if all_caps < 0.5:
                    continue

            # Auditor body confirmation
            if sid == "auditor_report":
                same = text.lower()
                next_text = table.get(pnum + 1, {}).get("text", "").lower()
                if not any(tok in same or tok in next_text for tok in AUDITOR_BODY_TOKENS):
                    continue

            candidates.append({
                "section_id": sid,
                "start_page": pnum,
                "end_page": None,
                "confidence": "high",
                "signal": "hard_anchor",
            })

    return candidates


def _locator_a_printed_toc(table: dict[int, dict]) -> list[dict]:
    """Locator A — parse a printed TOC page into (title, page_number) pairs.

    Works for both born-digital and OCR sources because we operate on the
    shared `all_pages` item structure. For each page 1-4 flagged as a TOC
    page, we cluster items into rows (y-proximity) and, within each row,
    pair text tokens (left) with the rightmost integer that looks like a
    plausible page number (≤ doc page count). Titles are then mapped to
    section ids via the hard-anchor dictionary.
    """
    candidates: list[dict] = []
    doc_last_page = max(table.keys()) if table else 0
    if not doc_last_page:
        return candidates

    toc_pages = [p for p in sorted(table.keys())[:4] if is_contents_page(
        {p: [(it["_bbox"], it["text"], 1.0) for it in table[p]["items_full"]]
         if "items_full" in table[p] else []},
        p,
    )]
    # Simpler: use the stored items directly
    toc_pages = []
    for p in sorted(table.keys())[:4]:
        items = table[p]["items"]
        if not items:
            continue
        # Inline top-band "CONTENTS" check
        if not items:
            continue
        ph = table[p].get("page_height")
        cutoff = _y_cutoff(items, 0.25, page_height=ph)
        top = " ".join(t for b, t, _ in items if get_y_center(b) <= cutoff).upper()
        if "CONTENTS" in top:
            toc_pages.append(p)

    for toc_p in toc_pages:
        items = table[toc_p]["items"]
        if not items:
            continue

        # Cluster items by y-proximity into rows
        rows: list[list[tuple]] = []
        current_row: list[tuple] = []
        last_y: float | None = None
        for bbox, text, conf in sorted(items, key=lambda it: (get_y_center(it[0]), get_x_center(it[0]))):
            y = get_y_center(bbox)
            if last_y is not None and abs(y - last_y) > 15:
                rows.append(current_row)
                current_row = []
            current_row.append((bbox, text, conf))
            last_y = y
        if current_row:
            rows.append(current_row)

        for row in rows:
            if not row:
                continue
            # Find the rightmost token that parses as a page number
            row_sorted = sorted(row, key=lambda it: get_x_center(it[0]))
            page_num_token = None
            for it in reversed(row_sorted):
                t = it[1].strip().rstrip(".")
                if t.isdigit():
                    n = int(t)
                    if 1 <= n <= doc_last_page:
                        page_num_token = (it, n)
                        break
            if page_num_token is None:
                continue
            (pn_bbox, _, _), page_num = page_num_token
            # Title is everything to the left of the page-number token
            title_tokens = [
                it[1] for it in row_sorted
                if get_x_center(it[0]) < get_x_center(pn_bbox) - 5
                and not it[1].strip().rstrip(".").isdigit()
            ]
            if not title_tokens:
                continue
            title = " ".join(title_tokens).strip()
            if len(title) < 3 or len(title.split()) > 15:
                continue

            # Map title to a section id via hard anchors
            for sid, pattern in HARD_ANCHORS.items():
                if re.search(pattern, title, re.IGNORECASE):
                    candidates.append({
                        "section_id": sid,
                        "start_page": page_num,
                        "end_page": None,
                        "confidence": "high",
                        "signal": "toc",
                    })
                    break

    return candidates


def _locator_c_running_headers(
    table: dict[int, dict], company_name: str | None
) -> list[dict]:
    """Locator C — recurring top-band strings that match a statutory hard anchor.

    Three rules:
      1. Top y-band (top ~5%), not positional [0]
      2. Exclude the company name
      3. Only emit if the recurring string matches a Locator B hard anchor
    """
    from collections import Counter

    company_norm = (company_name or "").strip().lower() if company_name else ""

    # Collect top-band strings per page
    top_band_by_page: dict[int, str] = {}
    for pnum, entry in table.items():
        items = entry["items"]
        if not items:
            continue
        ph = entry.get("page_height")
        cutoff = _y_cutoff(items, 0.05, page_height=ph)
        band_items = [
            (b, t, c) for b, t, c in items
            if get_y_center(b) <= cutoff
        ]
        if not band_items:
            continue
        band_items.sort(key=lambda it: get_x_center(it[0]))
        band_text = " ".join(t for _, t, _ in band_items).strip()
        if not band_text:
            continue
        top_band_by_page[pnum] = band_text

    # Count recurring strings
    counter: Counter[str] = Counter(top_band_by_page.values())
    candidates: list[dict] = []

    for band_text, count in counter.items():
        if count < 3:
            continue
        lowered = band_text.lower()
        # Rule 2: exclude company name
        if company_norm and company_norm in lowered:
            continue
        # Rule 3: cross-validate against hard anchors
        matched_sid: str | None = None
        for sid, pattern in HARD_ANCHORS.items():
            if re.search(pattern, band_text, re.IGNORECASE):
                matched_sid = sid
                break
        if matched_sid is None:
            continue
        # The range of pages sharing this string IS the section's range
        matching_pages = sorted(
            p for p, txt in top_band_by_page.items() if txt == band_text
        )
        if not matching_pages:
            continue
        candidates.append({
            "section_id": matched_sid,
            "start_page": matching_pages[0],
            "end_page": matching_pages[-1],
            "confidence": "high",
            "signal": "running_header",
        })

    return candidates


def _extract_company_name(table: dict[int, dict]) -> str | None:
    """Extract company name from page 1 top-band for Locator C exclusion."""
    if 1 not in table:
        return None
    items = table[1]["items"]
    if not items:
        return None
    ph = table[1].get("page_height")
    cutoff = _y_cutoff(items, 0.30, page_height=ph)
    top_text = " ".join(t for b, t, _ in items if get_y_center(b) <= cutoff)
    m = re.search(
        r"([A-Z][A-Za-z0-9 &,\.'()\-]+?(?:LIMITED|PLC|LTD|LLP))",
        top_text,
    )
    return m.group(1).strip() if m else None


def _locator_d_allcaps_first_line(table: dict[int, dict]) -> list[dict]:
    """Locator D — all-caps first-line-of-page matching any dictionary anchor."""
    candidates: list[dict] = []
    for pnum, entry in table.items():
        items = entry["items"]
        if not items:
            continue
        # Topmost y-band (top 5%)
        ph = entry.get("page_height")
        band_cutoff = _y_cutoff(items, 0.05, page_height=ph)
        top_band = [t for b, t, _ in items if get_y_center(b) <= band_cutoff]
        if not top_band:
            continue
        first_line = " ".join(top_band).strip()
        if not first_line or len(first_line.split()) > 15:
            continue
        if not first_line.isupper():
            continue
        for sid, pattern in HARD_ANCHORS.items():
            if re.search(pattern, first_line, re.IGNORECASE):
                candidates.append({
                    "section_id": sid,
                    "start_page": pnum,
                    "end_page": None,
                    "confidence": "high",
                    "signal": "all_caps_first_line",
                })
                break
    return candidates


SIGNOFF_RE = re.compile(
    r"approved by the (?:board of )?directors on"
    r"|signed on (?:its )?behalf (?:of the board)?"
    r"|the financial statements on pages \d+ to \d+ were approved",
    re.IGNORECASE,
)


def _locator_e_signoffs(table: dict[int, dict]) -> list[dict]:
    """Locator E — statutory sign-off phrases, emit as end-boundary candidates."""
    candidates: list[dict] = []
    for pnum, entry in table.items():
        if SIGNOFF_RE.search(entry["text"]):
            candidates.append({
                "section_id": "__signoff__",
                "start_page": pnum,
                "end_page": pnum,
                "confidence": "high",
                "signal": "signoff",
            })
    return candidates


NOTE_HEADING_RE = re.compile(
    r"(?m)^\s*(\d+(?:\.\d+)*)[._\s]+([A-Z][^\n]{0,80})$"
)


def _locator_f_numbered_notes(table: dict[int, dict]) -> list[dict]:
    """Locator F — numbered note headings for accounting policies / estimates /
    going concern inside the Notes range."""
    candidates: list[dict] = []
    for pnum, entry in table.items():
        text = entry["text"]
        for _m in NOTE_HEADING_RE.finditer(text):
            heading = _m.group(2).lower()
            for sid in ("accounting_policies", "critical_estimates", "going_concern_note"):
                if re.search(HARD_ANCHORS[sid], heading, re.IGNORECASE):
                    candidates.append({
                        "section_id": sid,
                        "start_page": pnum,
                        "end_page": None,
                        "confidence": "low",
                        "signal": "numbered_note",
                    })
                    break
    return candidates


# ---------------------------------------------------------------------------
# Stage 5 — Multi-entity boundary detection
# ---------------------------------------------------------------------------

def detect_primary_range(table: dict[int, dict]) -> tuple[int, int]:
    """Return (start, end) inclusive page numbers for the primary entity.

    Signal: second auditor-report match after the first promotes a secondary
    entity. Everything before it is primary.
    """
    pages = sorted(table.keys())
    if not pages:
        return 1, 1
    auditor_pages: list[int] = []
    regex = re.compile(HARD_ANCHORS["auditor_report"], re.IGNORECASE)
    for pnum in pages:
        text = table[pnum]["text"]
        if regex.search(text):
            # Require body confirmation to avoid counting references
            lowered = text.lower()
            if any(tok in lowered for tok in AUDITOR_BODY_TOKENS):
                auditor_pages.append(pnum)
    if len(auditor_pages) >= 2:
        return pages[0], auditor_pages[1] - 1
    return pages[0], pages[-1]


# ---------------------------------------------------------------------------
# Stage 4 — Voting / resolution
# ---------------------------------------------------------------------------

_TIE_BREAK_RANK = {
    "toc": 6,
    "signoff": 5,
    "hard_anchor": 4,
    "all_caps_first_line": 3,
    "running_header": 2,
    "numbered_note": 1,
    "first_line": 0,
}


def _resolve_sections(
    candidates: list[dict],
    signoff_pages: list[int],
    expected: dict[str, str],
    page_range: tuple[int, int],
) -> dict[str, dict]:
    """Vote across candidates and produce final section slots."""
    start_of_range, end_of_range = page_range

    # Group candidates by section id
    by_section: dict[str, list[dict]] = {}
    for c in candidates:
        if c["section_id"] == "__signoff__":
            continue
        if not (start_of_range <= c["start_page"] <= end_of_range):
            continue
        by_section.setdefault(c["section_id"], []).append(c)

    results: dict[str, dict] = {}
    chosen_starts: list[tuple[str, int]] = []  # (section_id, start_page)

    for sid in SECTION_IDS:
        cands = by_section.get(sid, [])
        expect = expected.get(sid, "expected")

        if not cands:
            results[sid] = {
                "status": "not_present" if expect == "not_present" else "not_found",
                "pages": None,
                "text": None,
                "signals": [],
                "confidence": "low",
                "notes": None,
            }
            continue

        # Cluster candidates by start_page (±1 counts as agreement)
        cands.sort(key=lambda c: c["start_page"])
        groups: list[list[dict]] = []
        for c in cands:
            if groups and abs(c["start_page"] - groups[-1][-1]["start_page"]) <= 1:
                groups[-1].append(c)
            else:
                groups.append([c])

        # Pick the group with the most distinct signals; tiebreak by rank
        def group_score(grp):
            signals = {c["signal"] for c in grp}
            rank = max(_TIE_BREAK_RANK.get(c["signal"], 0) for c in grp)
            return (len(signals), rank)

        best = max(groups, key=group_score)
        start_page = min(c["start_page"] for c in best)
        signals = sorted({c["signal"] for c in best})
        confidence = "high" if len(signals) >= 2 else "low"

        results[sid] = {
            "status": "found",
            "pages": [start_page, None],  # end filled in below
            "text": None,
            "signals": signals,
            "confidence": confidence,
            "notes": None,
        }
        chosen_starts.append((sid, start_page))

    # Resolve end pages using next section start and sign-off phrases
    chosen_starts.sort(key=lambda t: t[1])
    for i, (sid, start_page) in enumerate(chosen_starts):
        if i + 1 < len(chosen_starts):
            next_start = chosen_starts[i + 1][1]
            end_page = max(start_page, next_start - 1)
        else:
            end_page = end_of_range

        # Prefer a sign-off phrase inside (start_page, end_page]
        signoffs_inside = [p for p in signoff_pages if start_page <= p <= end_page]
        if signoffs_inside:
            end_page = signoffs_inside[0]

        results[sid]["pages"] = [start_page, end_page]

    return results


# ---------------------------------------------------------------------------
# Stage 6 — Slice and emit
# ---------------------------------------------------------------------------

def _slice_text(table: dict[int, dict], start: int, end: int) -> str:
    return "\n\n".join(
        table[p]["text"] for p in range(start, end + 1) if p in table
    )


def extract_sections(pdf_path_or_bytes, preloaded=None) -> dict[str, Any]:
    """Part B entry point. Return the 12-section dict per the skill contract.

    Top-level keys: `classification`, `primary_range`, `sections` (dict),
    and optionally `notes` for multi-entity flags.
    """
    if preloaded is not None:
        all_pages, _page_width, page_height, source = preloaded
    else:
        all_pages, _page_width, page_height, source = load_pages(pdf_path_or_bytes)

    table = build_page_text_table(all_pages, source, page_height=page_height)
    classification = classify_filing(all_pages)
    expected = _expected_sections(classification)
    primary_range = detect_primary_range(table)

    # Extract company name for Locator C exclusion
    company_name = _extract_company_name(table)

    # Run locators in parallel (shared candidate pool)
    candidates: list[dict] = []
    candidates.extend(_locator_a_printed_toc(table))
    candidates.extend(_locator_b_hard_anchors(table, classification))
    candidates.extend(_locator_c_running_headers(table, company_name))
    candidates.extend(_locator_d_allcaps_first_line(table))
    candidates.extend(_locator_f_numbered_notes(table))
    signoff_cands = _locator_e_signoffs(table)
    signoff_pages = [c["start_page"] for c in signoff_cands]

    sections = _resolve_sections(candidates, signoff_pages, expected, primary_range)

    # Stage 6 — slice text for each found section
    for sid, slot in sections.items():
        if slot["status"] != "found" or not slot["pages"]:
            continue
        start, end = slot["pages"]
        slot["text"] = _slice_text(table, start, end)

    output: dict[str, Any] = {
        "classification": classification,
        "primary_range": list(primary_range),
        "sections": sections,
    }

    # Multi-entity note
    all_pages_max = max(all_pages.keys()) if all_pages else 0
    if primary_range[1] < all_pages_max:
        output["notes"] = (
            f"Secondary entity detected after page {primary_range[1]} — "
            f"pages {primary_range[1] + 1}..{all_pages_max} not processed."
        )

    return output


# ===========================================================================
# Employee headcount extraction from notes pages
# ===========================================================================

# Labels that indicate an employee headcount row (not cost rows).
# True = total label, False = sub-category label (summed if no total found).
_EMPLOYEE_HEADCOUNT_LABELS: dict[str, bool] = {
    "total staff": True,
    "total": True,
    "average number": True,
    "number of employees": True,
    "average headcount": True,
    # Sub-category labels
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


def extract_employees_from_notes(
    all_pages: dict[int, list],
    page_width: float,
    page_height: float | None,
    notes_pages: list[int],
    current_year: str,
    prior_year: str,
) -> dict[str, int] | None:
    """Extract employee headcount from the Staff costs / Employees note.

    Scans notes pages for the employee section, finds headcount rows
    (not cost rows), and returns ``{year_str: count}``.  When the note
    breaks headcount into sub-categories (Production, Management, etc.)
    without a labelled total row, the sub-categories are summed.
    """
    if not notes_pages:
        return None

    # --- Step 1: find the notes page with employee data ---
    employee_page = None
    for pnum in notes_pages:
        page_txt = page_text(all_pages, pnum).lower()
        if any(kw in page_txt for kw in [
            "staff costs", "average number of persons",
            "average number of employees", "average headcount",
        ]):
            if "no." in page_txt or "average number" in page_txt:
                employee_page = pnum
                break
        # Fallback: section headed "Employees" with a small integer nearby
        if employee_page is None and "employees" in page_txt:
            items = all_pages.get(pnum, [])
            for poly, text, _ in items:
                x = get_x_center(poly)
                if x < page_width * 0.35 and text.lower().strip() == "employees":
                    employee_page = pnum
                    break
            if employee_page is not None:
                break

    if employee_page is None:
        return None

    items = all_pages.get(employee_page, [])
    if not items:
        return None

    items_sorted = sorted(items, key=lambda it: get_y_center(it[0]))

    # --- Step 2: locate section boundaries ---
    section_start_y = None
    table_start_y = None
    section_end_y = None

    for poly, text, _ in items_sorted:
        y = get_y_center(poly)
        x = get_x_center(poly)
        lower = text.lower().strip()

        # Section header — must be left-aligned
        if section_start_y is None:
            if x < page_width * 0.35:
                if lower in ("staff", "employees") or "staff costs" in lower:
                    section_start_y = y
                    continue

        # Table start markers
        if section_start_y is not None and table_start_y is None:
            if lower in ("no.", "no", "average", "number"):
                table_start_y = y
                continue

        # End of headcount section — left-aligned cost/wage heading
        if section_start_y is not None and table_start_y is not None and section_end_y is None:
            if x < page_width * 0.35:
                if any(kw in lower for kw in [
                    "aggregate", "payroll", "wages", "salaries",
                    "incurred", "costs", "remuneration", "auditor",
                ]):
                    section_end_y = y
                    break

    if table_start_y is None:
        if section_start_y is not None:
            table_start_y = section_start_y
        else:
            return None

    scan_start_y = section_start_y if section_start_y else table_start_y
    if section_end_y is None:
        section_end_y = scan_start_y + 300

    # --- Step 3: collect items in the headcount region ---
    year_ints: set[int] = set()
    for yr in (current_year, prior_year):
        if isinstance(yr, str) and yr.isdigit():
            year_ints.add(int(yr))

    table_items = [
        (p, t, c) for p, t, c in items_sorted
        if scan_start_y <= get_y_center(p) <= section_end_y
    ]
    if not table_items:
        return None

    # Cluster into rows by y-proximity (20px)
    rows: list[list[tuple]] = []
    current_row: list[tuple] = []
    current_y = -999.0
    for poly, text, conf in table_items:
        y = get_y_center(poly)
        if abs(y - current_y) > 20:
            if current_row:
                rows.append(current_row)
            current_row = [(poly, text, conf)]
            current_y = y
        else:
            current_row.append((poly, text, conf))
    if current_row:
        rows.append(current_row)

    # --- Step 4: parse rows into label + numbers ---
    total_values: list[float] | None = None
    sub_totals: list[list[float]] = []

    for row in rows:
        label_parts: list[str] = []
        numbers: list[float] = []
        for poly, text, conf in sorted(row, key=lambda it: get_x_center(it[0])):
            x = get_x_center(poly)
            if is_number_text(text):
                val = parse_number(text)
                if val is not None and int(val) not in year_ints and 0 < val < 100000:
                    numbers.append(val)
            elif x < page_width * 0.50:
                label_parts.append(text)

        if not numbers:
            continue

        label = " ".join(label_parts).strip().lower()

        # Match against headcount labels
        is_total: bool | None = None
        for pattern, is_total_flag in _EMPLOYEE_HEADCOUNT_LABELS.items():
            if pattern in label:
                is_total = is_total_flag
                break

        # Bare number row (no label) after sub-categories = total
        if is_total is None and not label_parts and sub_totals:
            is_total = True

        if is_total is True:
            total_values = numbers
            break
        elif is_total is False:
            sub_totals.append(numbers)

    # --- Step 5: resolve — prefer total, fall back to sum ---
    result: dict[str, int] = {}
    if total_values:
        if len(total_values) >= 1:
            result[current_year] = int(total_values[0])
        if len(total_values) >= 2:
            result[prior_year] = int(total_values[1])
    elif sub_totals:
        col1 = sum(nums[0] for nums in sub_totals if len(nums) >= 1)
        col2 = sum(nums[1] for nums in sub_totals if len(nums) >= 2)
        if col1 > 0:
            result[current_year] = int(col1)
        if col2 > 0:
            result[prior_year] = int(col2)

    return result if result else None


# ===========================================================================
# Combined entry point — one OCR pass, both outputs
# ===========================================================================

def parse_pdf_full(pdf_path_or_bytes) -> dict[str, Any]:
    """Run Part A and Part B against a single shared OCR/text-layer pass.

    Returns a dict with keys `financials` (Part A output), `sections`
    (Part B output), and `employees` (headcount from notes). This is
    what the main pipeline should call.
    """
    preloaded = load_pages(pdf_path_or_bytes)
    all_pages, page_width, page_height, source = preloaded

    financials = parse_pdf(None, preloaded=preloaded)
    sections = extract_sections(None, preloaded=preloaded)

    # Employee extraction from notes pages
    income_page, balance_page, cashflow_page, notes_pages, _ = \
        identify_pages(all_pages, page_height=page_height)
    current_year, prior_year = detect_years(
        all_pages, [income_page, balance_page, cashflow_page])
    employees = extract_employees_from_notes(
        all_pages, page_width, page_height, notes_pages,
        current_year, prior_year)

    return {
        "financials": financials,
        "sections": sections,
        "employees": employees,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pdf_parser.py <pdf_path> [--full|--sections]", file=sys.stderr)
        sys.exit(1)
    mode = sys.argv[2] if len(sys.argv) >= 3 else "--financials"
    if mode == "--full":
        result = parse_pdf_full(sys.argv[1])
    elif mode == "--sections":
        result = extract_sections(sys.argv[1])
    else:
        result = parse_pdf(sys.argv[1])
    print(json.dumps(result, indent=2, default=str))
