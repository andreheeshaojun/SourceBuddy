"""Download the most recent iXBRL filing for company 04372047 and inspect
every ix:nonNumeric element (narrative tags) + taxonomy version."""
import sys, os, re
from collections import Counter
sys.path.insert(0, r"C:\Users\Andre Hee\Desktop\SourceBuddy\Merge Data")
from pipeline import _check_env, _init_ch_session, ch_get, get_accounts_filings, get_document_metadata, determine_filing_format, download_document
from bs4 import BeautifulSoup

_check_env()
_init_ch_session()

CN = "04372047"
filings = get_accounts_filings(CN, count=3)
print(f"Found {len(filings)} accounts filings")
for f in filings:
    print(f"  {f.get('date')}  {f.get('description')}  paper_filed={f.get('paper_filed')}")

# Pull the most recent one that's iXBRL
target = None
for f in filings:
    md = get_document_metadata(f)
    if not md:
        continue
    fmt = determine_filing_format(md, f)
    print(f"  -> {f.get('date')} format={fmt}")
    if fmt == "ixbrl" and target is None:
        target = (f, md)

if target is None:
    print("No iXBRL filing found")
    sys.exit(1)

filing, metadata = target
print(f"\nDownloading {filing.get('date')} ...")
content, ctype = download_document(metadata, "ixbrl")
print(f"  {len(content)} bytes  {ctype}")

# Save locally for reuse
out = r"C:\Users\Andre Hee\Desktop\SourceBuddy\__pycache__\vinstall_latest.xhtml"
with open(out, "wb") as fh:
    fh.write(content)
print(f"  saved to {out}")

# Parse
soup = BeautifulSoup(content, "html.parser")

# 1) Taxonomy signals
print("\n=== Taxonomy signals ===")
for sr in soup.find_all(re.compile("schemaref", re.I)):
    href = sr.get("xlink:href") or sr.get("href", "")
    print(f"  schemaRef: {href}")
root = soup.find("html") or soup
for k, v in list(root.attrs.items())[:25]:
    if k.startswith("xmlns"):
        print(f"  {k} = {v}")

# 2) ix:nonNumeric tags — the narrative universe
print("\n=== ix:nonNumeric tags (narrative) ===")
nn = soup.find_all("ix:nonnumeric")
print(f"Total ix:nonNumeric elements: {len(nn)}")

tag_counter = Counter()
by_name = {}
for el in nn:
    name = el.get("name", "(no-name)")
    tag_counter[name] += 1
    # Store first occurrence text sample
    if name not in by_name:
        txt = el.get_text(" ", strip=True)
        by_name[name] = txt

print(f"Unique tag names: {len(tag_counter)}\n")
print("Top 40 by occurrence:")
for name, n in tag_counter.most_common(40):
    sample = by_name.get(name, "")
    sample_short = (sample[:120] + "...") if len(sample) > 120 else sample
    print(f"  {n:3d}x  {name}")
    if sample_short:
        print(f"         -> {sample_short!r}")

# 3) Numeric tag count for context
nf = soup.find_all("ix:nonfraction")
print(f"\n=== ix:nonFraction (numeric) count: {len(nf)} ===")
nf_counter = Counter(t.get("name", "") for t in nf)
print(f"Unique numeric tag names: {len(nf_counter)}")
print("Top 10:")
for name, n in nf_counter.most_common(10):
    print(f"  {n:3d}x  {name}")
