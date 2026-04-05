import sys, re, json, fitz
sys.stdout.reconfigure(encoding="utf-8")

PDF = r"C:\Users\Andre Hee\Desktop\SourceBuddy\John Lewis Example.pdf"
doc = fitz.open(PDF)

# Build all_pages in OCR-compatible shape: {page_no: [(polygon_bbox, text, conf), ...]}
# polygon_bbox = [[x0,y0],[x1,y0],[x1,y1],[x0,y1]]  (EasyOCR format so helpers work unchanged)
all_pages = {}
for pi in range(doc.page_count):
    d = doc[pi].get_text("dict")
    items = []
    for block in d.get("blocks", []):
        if block.get("type", 0) != 0: continue
        for line in block.get("lines", []):
            for sp in line.get("spans", []):
                t = sp.get("text","").strip()
                if not t: continue
                x0,y0,x1,y1 = sp["bbox"]
                poly = [[x0,y0],[x1,y0],[x1,y1],[x0,y1]]
                items.append((poly, t, 1.0))
    all_pages[pi+1] = items

PAGE_WIDTH = doc[0].rect.width
doc.close()

def get_x_center(b): return (b[0][0]+b[2][0])/2
def get_y_center(b): return (b[0][1]+b[2][1])/2

def parse_number(s):
    s = s.strip()
    negative = s.startswith("(")
    s = s.replace("(","").replace(")","").replace(" ","").replace("\u00a3","").replace("$","")
    if s in ("-","\u2013","\u2014","","."): return 0
    cleaned = s.replace(",","").replace(".","")
    if re.match(r"^-?\d+$", cleaned):
        try:
            val = int(cleaned)
            return -val if negative else val
        except ValueError:
            return None
    try:
        val = int(float(s.replace(",","")))
        return -val if negative else val
    except ValueError:
        return None

def is_number_text(t):
    t = t.strip()
    if t in ("-","\u2013","\u2014"): return True
    t2 = t.replace("(","").replace(")","").replace(",","").replace(" ","").replace("\u00a3","")
    return bool(re.match(r"^-?\d+(\.\d+)?$", t2))

def cluster_x_positions(xs, gap_threshold=40):
    if not xs: return []
    xs_sorted = sorted(xs)
    clusters=[[xs_sorted[0]]]
    for x in xs_sorted[1:]:
        if x-clusters[-1][-1] > gap_threshold: clusters.append([x])
        else: clusters[-1].append(x)
    return [sum(c)/len(c) for c in clusters]

def page_text(p):
    return " ".join(t for _,t,_ in all_pages.get(p,[]))

def top_band_text(p, frac=0.12):
    items = all_pages.get(p, [])
    if not items: return ""
    ys = [get_y_center(b) for b,_,_ in items]
    y_min, y_max = min(ys), max(ys)
    cutoff = y_min + (y_max-y_min)*frac
    return " ".join(t for b,t,_ in items if get_y_center(b) <= cutoff).upper()

# Identify primary (consolidated) FS pages — first match wins, not last
# (p110 Auditor's Report mentions the income statement in its opinion, must not overwrite)
income_page = balance_page = None
notes_pages = []
for p in range(1, len(all_pages)+1):
    top = top_band_text(p, frac=0.06)  # tight top band so body refs on p110 don't match
    if income_page is None and "CONSOLIDATED INCOME STATEMENT" in top:
        income_page = p
    if balance_page is None and "CONSOLIDATED BALANCE SHEET" in top and "CONTINUED" not in top:
        balance_page = p
    if "NOTES TO THE CONSOLIDATED" in top:
        notes_pages.append(p)

print(f"Identified: income=p{income_page}  balance=p{balance_page}  notes=first {notes_pages[:5]}...")

# Years
years = set()
for p in (income_page, balance_page):
    if p:
        for _,t,_ in all_pages[p]:
            m = re.match(r"^(20\d{2})$", t.strip())
            if m: years.add(m.group(1))
years = sorted(years)
CURRENT_YEAR = years[-1] if years else "Unknown"
PRIOR_YEAR = years[-2] if len(years)>=2 else "Unknown"
print(f"Years: {CURRENT_YEAR} / {PRIOR_YEAR}")

# IFRS-aware label maps for large UK plc
INCOME_LABEL_MAP = {
    "revenue": "revenue",
    "cost of sales": "cost_of_sales",
    "gross profit": "gross_profit",
    "other operating income": "other_operating_income",
    "operating and administrative": "operating_and_admin_expenses",
    "operating profit": "operating_profit",
    "operating loss": "operating_profit",
    "finance income": "finance_income",
    "finance cost": "finance_costs",
    "finance costs": "finance_costs",
    "profit before tax": "profit_before_taxation",
    "profit/(loss) before tax": "profit_before_taxation",
    "loss before tax": "profit_before_taxation",
    "taxation": "taxation",
    "profit for the financial year": "profit_for_financial_year",
    "profit for the year": "profit_for_financial_year",
    "loss for the financial year": "profit_for_financial_year",
}

BALANCE_LABEL_MAP = {
    "intangible assets": "intangible_assets",
    "property, plant and equipment": "property_plant_equipment",
    "right-of-use assets": "right_of_use_assets",
    "trade and other receivables": "trade_and_other_receivables",
    "derivative financial instruments": "derivatives",
    "deferred tax assets": "deferred_tax_assets",
    "retirement benefit asset": "retirement_benefit_asset",
    "retirement benefit obligation": "retirement_benefit_obligation",
    "retirement benefit liabilit": "retirement_benefit_obligation",
    "investments": "investments",
    "inventories": "inventories",
    "cash and cash equivalents": "cash_and_equivalents",
    "short-term investments": "short_term_investments",
    "assets held for sale": "assets_held_for_sale",
    "total current assets": "total_current_assets",
    "total non-current assets": "total_non_current_assets",
    "total assets": "total_assets",
    "trade and other payables": "trade_and_other_payables",
    "borrowings": "borrowings",
    "lease liabilities": "lease_liabilities",
    "provisions": "provisions",
    "current tax liabilities": "current_tax_liabilities",
    "total current liabilities": "total_current_liabilities",
    "total non-current liabilities": "total_non_current_liabilities",
    "total liabilities": "total_liabilities",
    "net assets": "net_assets",
    "called up share capital": "share_capital",
    "share capital": "share_capital",
    "retained earnings": "retained_earnings",
    "total equity": "total_equity",
}

def parse_financial_page(page_num, label_map, y_gap=6):
    items = all_pages.get(page_num, [])
    if not items: return {}
    number_xs = []
    for bbox, text, conf in items:
        if is_number_text(text) and parse_number(text) is not None:
            x = get_x_center(bbox)
            if x > PAGE_WIDTH * 0.45:
                number_xs.append(x)
    if not number_xs: return {}
    col_centers = cluster_x_positions(number_xs, gap_threshold=30)
    # keep only the rightmost 2 columns (current + prior year)
    if len(col_centers) == 0: return {}
    if len(col_centers) >= 2:
        c1, c2 = col_centers[-2], col_centers[-1]
    else:
        c1 = col_centers[0]; c2 = PAGE_WIDTH*0.95
    mid_boundary = (c1 + c2) / 2

    def classify_col(x):
        if x < PAGE_WIDTH * 0.45: return "label"
        # Only the two rightmost cols matter — treat everything left of c1 midzone as ignorable
        if x < (c1 - 20): return "label"  # sub-total columns: treat as label-zone noise
        return "col1" if x < mid_boundary else "col2"

    row_items = []
    for bbox, text, conf in items:
        row_items.append({
            "y": get_y_center(bbox), "x": get_x_center(bbox),
            "col": classify_col(get_x_center(bbox)), "text": text, "conf": conf
        })
    row_items.sort(key=lambda r: r["y"])

    clusters = []; cur = []
    for it in row_items:
        if cur and abs(it["y"] - cur[-1]["y"]) > y_gap:
            clusters.append(cur); cur = [it]
        else:
            cur.append(it)
    if cur: clusters.append(cur)

    result = {}
    pending = ""
    for idx, cluster in enumerate(clusters):
        labels = [it for it in cluster if it["col"] == "label"]
        col1_items = [it for it in cluster if it["col"] == "col1" and is_number_text(it["text"])]
        col2_items = [it for it in cluster if it["col"] == "col2" and is_number_text(it["text"])]
        row_label = " ".join([l["text"] for l in sorted(labels, key=lambda x: x["x"])]).strip().lower() if labels else ""
        has_numbers = bool(col1_items or col2_items)
        if row_label and not has_numbers:
            pending = (pending + " " + row_label).strip()
            continue
        combined = (pending + " " + row_label).strip()
        pending = ""
        if not combined: continue
        matched = None
        for pat, fld in label_map.items():
            if pat in combined:
                matched = fld; break
        if matched is None: continue
        year_ints = {int(CURRENT_YEAR) if CURRENT_YEAR.isdigit() else -1,
                     int(PRIOR_YEAR) if PRIOR_YEAR.isdigit() else -1}
        c1v = c2v = None
        for it in col1_items:
            v = parse_number(it["text"])
            if v is not None and v not in year_ints: c1v = v
        for it in col2_items:
            v = parse_number(it["text"])
            if v is not None and v not in year_ints: c2v = v
        if c1v is not None or c2v is not None:
            entry = {}
            if c1v is not None: entry[CURRENT_YEAR] = c1v
            if c2v is not None: entry[PRIOR_YEAR] = c2v
            if matched not in result:
                result[matched] = entry
    return result

income = parse_financial_page(income_page, INCOME_LABEL_MAP, y_gap=6) if income_page else {}
balance = parse_financial_page(balance_page, BALANCE_LABEL_MAP, y_gap=6) if balance_page else {}

output = {
    "company": "John Lewis plc",
    "registered_number": "00233462",
    "year_ended": "25 January 2025",
    "currency": "GBP (millions)",
    "filing_size": "large",
    "filing_mode": "full",
    "income_statement": income,
    "balance_sheet": balance,
}

print("\n" + "="*60)
print("JOHN LEWIS PLC — PART A OUTPUT")
print("="*60)
print(json.dumps(output, indent=2))

# Sanity checks
print("\n--- Sanity ---")
rev = income.get("revenue", {}).get(CURRENT_YEAR)
cos = income.get("cost_of_sales", {}).get(CURRENT_YEAR)
gp  = income.get("gross_profit", {}).get(CURRENT_YEAR)
if rev is not None and cos is not None and gp is not None:
    print(f"revenue + cost_of_sales = {rev + cos}  (gross_profit = {gp})  {'OK' if rev+cos==gp else 'FAIL'}")
ne = balance.get("net_assets", {}).get(CURRENT_YEAR)
te = balance.get("total_equity", {}).get(CURRENT_YEAR)
if ne is not None and te is not None:
    print(f"net_assets = {ne}  total_equity = {te}  {'OK' if ne==te else 'FAIL'}")
