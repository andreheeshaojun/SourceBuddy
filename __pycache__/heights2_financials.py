import sys, pickle, re, json
sys.stdout.reconfigure(encoding="utf-8")
with open(r"C:\Users\Andre Hee\Desktop\SourceBuddy\__pycache__\heights2_ocr.pkl","rb") as f:
    all_pages = pickle.load(f)

import fitz
doc = fitz.open(r"C:\Users\Andre Hee\Desktop\SourceBuddy\Heights Management Test 2.pdf")
PAGE_WIDTH = doc[0].get_pixmap(matrix=fitz.Matrix(2,2)).width
doc.close()

def parse_number(s):
    s = s.strip()
    # Negative if starts with "(" even if closing ")" was dropped by OCR
    negative = s.startswith("(")
    s = s.replace("(","").replace(")","").replace(" ","").replace("\u00a3","").replace("$","")
    if s in ("-","\u2013","\u2014","","."): return 0
    # UK accounts: commas are thousand separators. OCR frequently reads commas as periods.
    # Heuristic: any period followed by exactly 3 digits (and not the last period) is a thousand separator.
    # Simpler rule: if the string is all digits plus periods/commas and total digit count > 3,
    # strip both periods and commas treating them as thousand separators.
    cleaned = s.replace(",","").replace(".","")
    if re.match(r"^-?\d+$", cleaned):
        try:
            val = int(cleaned)
            return -val if negative else val
        except ValueError:
            return None
    # Fallback: single decimal value
    try:
        val = int(float(s.replace(",","")))
        return -val if negative else val
    except ValueError:
        return None

def is_number_text(t):
    t = t.strip()
    if t in ("-","\u2013","\u2014"): return True
    t2 = t.replace("(","").replace(")","").replace(",","").replace(" ","").replace("\u00a3","").replace(".","",1)
    return bool(re.match(r"^-?\d+$", t2))

def get_x_center(bbox): return (bbox[0][0]+bbox[2][0])/2
def get_y_center(bbox): return (bbox[0][1]+bbox[2][1])/2

def cluster_x_positions(xs, gap_threshold=60):
    if not xs: return []
    xs_sorted = sorted(xs)
    clusters=[[xs_sorted[0]]]
    for x in xs_sorted[1:]:
        if x-clusters[-1][-1]>gap_threshold: clusters.append([x])
        else: clusters[-1].append(x)
    return [sum(c)/len(c) for c in clusters]

def page_text(p):
    return " ".join(t for _,t,_ in all_pages.get(p,[]))

def top_band_text(p, frac=0.15):
    items = all_pages.get(p, [])
    if not items: return ""
    ys = [get_y_center(b) for b,_,_ in items]
    y_min, y_max = min(ys), max(ys)
    cutoff = y_min + (y_max-y_min)*frac
    return " ".join(t for b,t,_ in items if get_y_center(b) <= cutoff).upper()

def is_contents_page(p):
    return "CONTENTS" in top_band_text(p, 0.25)

income_page = None
balance_page = None
notes_pages = []
for p in all_pages:
    top = top_band_text(p)
    full_u = page_text(p).upper()
    if is_contents_page(p): continue
    if "NOTES TO THE FINANCIAL" in top:
        notes_pages.append(p); continue
    if "BALANCE SHEET" in top and "CONTINUED" not in top:
        if balance_page is None: balance_page = p
        continue
    if ("INCOME STATEMENT" in top or "PROFIT AND LOSS ACCOUNT" in top or "STATEMENT OF COMPREHENSIVE INCOME" in top) and "OPTED NOT TO DELIVER" not in full_u:
        income_page = p
        continue

print(f"Identified pages: income={income_page}  balance={balance_page}  notes={notes_pages}")

years=[]
for p in (balance_page, income_page, *notes_pages):
    if p is None: continue
    for _,t,_ in all_pages.get(p,[]):
        m = re.match(r"^(20\d{2})$", t.strip())
        if m: years.append(m.group(1))
years = sorted(set(years))
CURRENT_YEAR = years[-1] if years else "Unknown"
PRIOR_YEAR   = years[-2] if len(years)>=2 else "Unknown"
print(f"Years: current={CURRENT_YEAR}  prior={PRIOR_YEAR}")

BALANCE_LABEL_MAP = {
    "tangible assets":"tangible_assets",
    "intangible assets":"intangible_assets",
    "fixed asset investments":"fixed_asset_investments",
    "debtors":"debtors",
    "cash at bank":"cash_at_bank",
    "cash and cash equivalents":"cash_at_bank",
    "stock":"stock","inventories":"stock",
    "net current assets":"net_current_assets",
    "total assets less current":"total_assets_less_current_liabilities",
    "called up share capital":"called_up_share_capital",
    "share premium":"share_premium",
    "retained earnings":"retained_earnings",
    "profit and loss account":"retained_earnings",
    "shareholders":"shareholders_funds",
    "creditors":"creditors_within_one_year",
    "net assets":"net_assets",
}
NOTES_LABEL_MAP = {
    "maintenance fees receivable":"maintenance_fees_receivable",
    "maintenance charges received in advance":"maintenance_fees_received_in_advance",
    "maintenance fees received in advance":"maintenance_fees_received_in_advance",
    "other creditors":"other_creditors",
    "accruals and deferred income":"accruals_and_deferred_income",
    "trade debtors":"trade_debtors",
    "trade creditors":"trade_creditors",
    "prepayments and accrued income":"prepayments",
    "prepayments":"prepayments",
}

def parse_financial_page(page_num, label_map):
    items = all_pages.get(page_num, [])
    if not items: return {}
    number_xs = []
    for bbox, text, conf in items:
        if is_number_text(text) and parse_number(text) is not None:
            x = get_x_center(bbox)
            if x > PAGE_WIDTH*0.40:
                number_xs.append(x)
    if not number_xs: return {}
    col_centers = cluster_x_positions(number_xs, 60)
    if len(col_centers)<=2:
        c1 = col_centers[0] if len(col_centers)>=1 else PAGE_WIDTH*0.6
        c2 = col_centers[1] if len(col_centers)>=2 else PAGE_WIDTH*0.85
    else:
        mid_idx = len(col_centers)//2
        c1 = sum(col_centers[:mid_idx])/mid_idx
        c2 = sum(col_centers[mid_idx:])/(len(col_centers)-mid_idx)
    mid_boundary = (c1+c2)/2
    def classify_col(x):
        if x < PAGE_WIDTH*0.40: return "label"
        return "col1" if x < mid_boundary else "col2"
    row_items=[]
    for bbox, text, conf in items:
        row_items.append({
            "y":get_y_center(bbox),"x":get_x_center(bbox),
            "col":classify_col(get_x_center(bbox)),"text":text,"conf":conf
        })
    row_items.sort(key=lambda r:r["y"])
    clusters=[]; cur=[]
    for it in row_items:
        if cur and abs(it["y"]-cur[-1]["y"])>20:
            clusters.append(cur); cur=[it]
        else: cur.append(it)
    if cur: clusters.append(cur)

    result={}
    pending=""
    for idx,cluster in enumerate(clusters):
        labels=[it for it in cluster if it["col"]=="label"]
        col1_items=[it for it in cluster if it["col"]=="col1" and is_number_text(it["text"])]
        col2_items=[it for it in cluster if it["col"]=="col2" and is_number_text(it["text"])]
        row_label = " ".join([l["text"] for l in sorted(labels,key=lambda x:x["x"])]).strip().lower() if labels else ""
        has_numbers = bool(col1_items or col2_items)
        if row_label and not has_numbers:
            pending = (pending+" "+row_label).strip()
            continue
        combined = (pending+" "+row_label).strip()
        pending=""
        if not combined: continue
        matched=None
        for pat,fld in label_map.items():
            if pat in combined:
                matched=fld; break
        if matched is None: continue
        c1v=c2v=None
        for it in col1_items:
            v=parse_number(it["text"])
            if v is not None: c1v=v
        for it in col2_items:
            v=parse_number(it["text"])
            if v is not None: c2v=v
        if c1v is not None or c2v is not None:
            entry={}
            if c1v is not None: entry[CURRENT_YEAR]=c1v
            if c2v is not None: entry[PRIOR_YEAR]=c2v
            if matched not in result:
                result[matched]=entry
    return result

balance = parse_financial_page(balance_page, BALANCE_LABEL_MAP) if balance_page else {}
notes = {}
for np_ in notes_pages:
    for k,v in parse_financial_page(np_, NOTES_LABEL_MAP).items():
        notes.setdefault(k,v)

cover = page_text(1)
reg_match = re.search(r"(\d{8})", cover)
reg_number = reg_match.group(1) if reg_match else "Unknown"
year_match = re.search(r"YEAR ENDED (\d{1,2} \w+ \d{4})", cover, re.IGNORECASE)
year_ended = year_match.group(1) if year_match else "Unknown"

output = {
    "company": "Heights (Management) No 2 Limited",
    "registered_number": reg_number,
    "year_ended": year_ended,
    "currency":"GBP",
    "income_statement": None,
    "balance_sheet": {
        "fixed_assets": {k:v for k,v in balance.items() if k in ("tangible_assets","intangible_assets","fixed_asset_investments")},
        "current_assets": {k:v for k,v in balance.items() if k in ("debtors","cash_at_bank","stock")},
        "creditors_within_one_year": balance.get("creditors_within_one_year",{}),
        "net_current_assets": balance.get("net_current_assets",{}),
        "total_assets_less_current_liabilities": balance.get("total_assets_less_current_liabilities",{}),
        "net_assets": balance.get("net_assets",{}),
        "capital_and_reserves": {k:v for k,v in balance.items() if k in ("called_up_share_capital","share_premium","retained_earnings","shareholders_funds")},
    },
    "notes": notes,
    "filing_mode":"filleted_s444(4)",
    "filing_size":"small",
}

warnings=[]
bs = output["balance_sheet"]
for year in (CURRENT_YEAR,PRIOR_YEAR):
    tal = bs["total_assets_less_current_liabilities"].get(year)
    shf = bs["capital_and_reserves"].get("shareholders_funds",{}).get(year)
    if tal is not None and shf is not None and tal!=shf:
        warnings.append(f"{year}: TALCL {tal} != SHF {shf}")
if warnings: output["warnings"]=warnings

print("\n" + "="*60)
print("HEIGHTS MANAGEMENT TEST 2 — PART A OUTPUT")
print("="*60)
print(json.dumps(output, indent=2))
