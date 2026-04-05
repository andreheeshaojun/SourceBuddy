"""Probe the smaller V Installations 2024 filing (Total exemption full accounts)
to verify the iXBRL qualitative architecture degrades gracefully on small filings."""
import sys, os, re
from collections import Counter
sys.path.insert(0, r"C:\Users\Andre Hee\Desktop\SourceBuddy\Merge Data")
from pipeline import _check_env, _init_ch_session, get_accounts_filings, get_document_metadata, determine_filing_format, download_document
from bs4 import BeautifulSoup

_check_env()
_init_ch_session()

CN = "04372047"
filings = get_accounts_filings(CN, count=5)
# Pick the 2024-12-03 one (small, Total exemption full accounts)
target = None
for f in filings:
    if f.get("date") == "2024-12-03":
        target = f
        break
if not target:
    print("Target filing not found")
    sys.exit(1)

print(f"Target: {target.get('date')}  {target.get('description')}")
metadata = get_document_metadata(target)
fmt = determine_filing_format(metadata, target)
print(f"Format: {fmt}")
content, ctype = download_document(metadata, "ixbrl")
print(f"Downloaded: {len(content)} bytes")

out = r"C:\Users\Andre Hee\Desktop\SourceBuddy\__pycache__\vinstall_2024small.xhtml"
with open(out, "wb") as fh: fh.write(content)

soup = BeautifulSoup(content, "html.parser")

# Taxonomy version
print("\n=== Taxonomy ===")
for sr in soup.find_all(re.compile("schemaref", re.I)):
    print(f"  {sr.get('xlink:href') or sr.get('href','')}")

# Boolean flags — which sections are declared present?
print("\n=== Stage 0 signals (boolean flags) ===")
flag_tags = [
    "bus:EntityDormantTruefalse",
    "bus:AccountsStatusAuditedOrUnaudited",
    "bus:AccountsType",
    "bus:ReportIncludesStrategicReportTruefalse",
    "bus:ReportIncludesDetailedProfitLossStatementTruefalse",
    "direp:EntityHasTakenExemptionUnderCompaniesActInNotPublishingItsOwnProfitLossAccountTruefalse",
    "core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse",
    "bus:ApplicableLegislation",
    "bus:AccountingStandardsApplied",
]
for t in flag_tags:
    el = soup.find("ix:nonnumeric", {"name": t})
    if el:
        print(f"  {t} = {el.get_text(' ', strip=True)[:80]!r}")
    else:
        print(f"  {t} = (missing)")

# Narrative tags
nn = soup.find_all("ix:nonnumeric")
print(f"\n=== ix:nonNumeric: {len(nn)} elements, {len(set(e.get('name','') for e in nn))} unique ===")

agg = {}
for el in nn:
    name = el.get("name","")
    txt = el.get_text(" ", strip=True)
    if name not in agg:
        agg[name] = {"count":0, "max_len":0, "sample":""}
    agg[name]["count"] += 1
    if len(txt) > agg[name]["max_len"]:
        agg[name]["max_len"] = len(txt); agg[name]["sample"] = txt

long_tags = [(k,v) for k,v in agg.items() if v["max_len"] >= 100]
short_tags = [(k,v) for k,v in agg.items() if v["max_len"] < 100]
print(f"  Long narrative tags (>=100 chars): {len(long_tags)}")
print(f"  Short metadata/flag tags (<100 chars): {len(short_tags)}")

print("\n--- Long tags, descending by length ---")
for name, data in sorted(long_tags, key=lambda x: -x[1]["max_len"]):
    print(f"  {data['max_len']:5d}  {name}")

# Strip ix:header, get text, run locators
print("\n=== Stage 2+3: body text + PDF Part B locators ===")
soup2 = BeautifulSoup(content, "html.parser")
for h in soup2.find_all("ix:header"):
    h.decompose()
body = soup2.find("body") or soup2
text = body.get_text("\n", strip=True)
print(f"  Body text length: {len(text):,} chars")

ANCHORS = {
    "strategic_report":    r"\bSTRATEGIC REPORT\b|\bStrategic [Rr]eport\b",
    "directors_report":    r"\bDIRECTORS['\u2019]\s*REPORT\b|\bDirectors['\u2019]\s*[Rr]eport\b",
    "section_172":         r"Section 172",
    "principal_risks":     r"\bPrincipal [Rr]isks\b",
    "going_concern":       r"\bGoing [Cc]oncern\b",
    "dir_responsibilities":r"Directors['\u2019]? responsibilit",
    "auditor_report":      r"Independent [Aa]uditor['\u2019]?s? [Rr]eport",
    "principal_activity":  r"Principal activit",
    "accounting_policies": r"Accounting policies|Basis of preparation",
    "critical_estimates":  r"Critical (?:accounting )?(?:judgements|estimates)",
}

def is_heading_like(m, line):
    line = line.strip()
    stripped = re.sub(r"^\s*[IVXLCDM0-9]+(?:\.[IVXLCDM0-9]+)*\.?\s+","", line)
    if stripped.lower().startswith(m.lower()):
        if len(line.split()) <= 12: return True
        if line == line.upper() and any(c.isalpha() for c in line): return True
    return False

for sec, pat in ANCHORS.items():
    rx = re.compile(pat)
    heads = []
    for m in rx.finditer(text):
        s = text.rfind("\n",0,m.start())+1
        e = text.find("\n",m.end())
        if e==-1: e=len(text)
        line = text[s:e].strip()
        if is_heading_like(m.group(0), line):
            heads.append((m.start(), line))
    mark = "OK" if heads else "--"
    print(f"  [{mark}] {sec:22s} heading-like: {len(heads)}")
    for off, l in heads[:3]:
        print(f"       @{off:5d}  '{l[:80]}'")

SIGN = re.compile(r"approved by the (?:Board of )?Directors on|signed on (?:its )?behalf (?:of the Board)?|on behalf of the Board", re.I)
print("\n  Sign-off phrases:")
for m in SIGN.finditer(text):
    s = text.rfind("\n",0,m.start())+1
    e = text.find("\n",m.end())
    if e==-1: e=len(text)
    print(f"    @{m.start():5d}  '{text[s:e].strip()[:80]}'")
