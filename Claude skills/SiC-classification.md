# UK SIC 2007 Classification Skill

## Task
Classify UK Companies House SIC codes into their hierarchical sector groupings. Input is always a 5-digit numeric code (sometimes prefixed in a string with its description, e.g. `"24520 Casting of steel"`). Output is the full hierarchy: section, division, group, class.

## Core Rule
The section letter is **not** in the code. It is derived from the first 2 digits (the division). Extract digits positionally:
- Division = digits 1–2
- Group = digits 1–3
- Class = digits 1–4
- Subclass = all 5 digits (this is what Companies House stores)

## Division → Section Mapping (complete)

```
A  Agriculture, Forestry and Fishing           01-03
B  Mining and Quarrying                        05-09
C  Manufacturing                               10-33
D  Electricity, Gas, Steam and Air Con Supply  35
E  Water Supply, Sewerage, Waste Mgmt          36-39
F  Construction                                41-43
G  Wholesale/Retail Trade, Motor Vehicle Repair 45-47
H  Transportation and Storage                  49-53
I  Accommodation and Food Service              55-56
J  Information and Communication               58-63
K  Financial and Insurance Activities          64-66
L  Real Estate Activities                      68
M  Professional, Scientific and Technical      69-75
N  Administrative and Support Services         77-82
O  Public Administration and Defence           84
P  Education                                   85
Q  Human Health and Social Work                86-88
R  Arts, Entertainment and Recreation          90-93
S  Other Service Activities                    94-96
T  Household Activities as Employers           97-98
U  Extraterritorial Organisations              99
```

Gaps in numbering (04, 34, 40, 44, 48, 54, 57, 67, 76, 83, 89) are intentional — reserved by NACE Rev. 2. No valid SIC code starts with these.

## Division Names (2-digit)

```
01 Crop/animal production, hunting    33 Repair/installation of machinery
02 Forestry and logging               35 Electricity, gas, steam, air con
03 Fishing and aquaculture            36 Water collection/treatment/supply
05 Mining of coal and lignite         37 Sewerage
06 Extraction of crude petroleum/gas  38 Waste collection/treatment/disposal
07 Mining of metal ores               39 Remediation activities
08 Other mining and quarrying         41 Construction of buildings
09 Mining support services            42 Civil engineering
10 Manufacture of food products       43 Specialised construction
11 Manufacture of beverages           45 Wholesale/retail/repair motor vehicles
12 Manufacture of tobacco products    46 Wholesale trade (excl. motor vehicles)
13 Manufacture of textiles            47 Retail trade (excl. motor vehicles)
14 Manufacture of wearing apparel     49 Land transport, transport via pipelines
15 Manufacture of leather products    50 Water transport
16 Manufacture of wood products       51 Air transport
17 Manufacture of paper products      52 Warehousing, transport support
18 Printing, reproduction of media    53 Postal and courier activities
19 Manufacture of coke/petroleum      55 Accommodation
20 Manufacture of chemicals           56 Food and beverage services
21 Manufacture of pharmaceuticals     58 Publishing activities
22 Manufacture of rubber/plastic      59 Film, video, TV, sound, music
23 Manufacture of non-metallic mineral 60 Programming and broadcasting
24 Manufacture of basic metals        61 Telecommunications
25 Fabricated metal products          62 Computer programming/consultancy
26 Computer, electronic, optical      63 Information service activities
27 Manufacture of electrical equip    64 Financial services (excl. insurance)
28 Manufacture of machinery n.e.c.    65 Insurance, reinsurance, pension funding
29 Manufacture of motor vehicles      66 Auxiliary to financial/insurance
30 Manufacture of other transport     68 Real estate activities
31 Manufacture of furniture           69 Legal and accounting
32 Other manufacturing                70 Head offices, management consultancy
                                      71 Architecture, engineering, testing
                                      72 Scientific R&D
                                      73 Advertising and market research
                                      74 Other professional/scientific/technical
                                      75 Veterinary activities
                                      77 Rental and leasing
                                      78 Employment activities
                                      79 Travel agency, tour operator
                                      80 Security and investigation
                                      81 Services to buildings/landscape
                                      82 Office admin/business support
                                      84 Public admin, defence, social security
                                      85 Education
                                      86 Human health activities
                                      87 Residential care activities
                                      88 Social work (without accommodation)
                                      90 Creative, arts, entertainment
                                      91 Libraries, archives, museums
                                      92 Gambling and betting
                                      93 Sports, amusement, recreation
                                      94 Membership organisations
                                      95 Repair of computers/household goods
                                      96 Other personal services
                                      97 Households as employers of domestic staff
                                      98 Households producing goods/services for own use
                                      99 Extraterritorial organisations
```

## Classification Rules

1. **Primary SIC code** (`siccode_sictext_1`) determines the company's sector. Companies House lists the principal activity first. Use this for all sector assignments unless explicitly told otherwise.

2. **Multi-sector companies**: If a company has codes in `siccode_sictext_2`, `siccode_sictext_3`, etc. that map to *different sections*, flag it as multi-sector but still classify by primary code. Do not double-count.

3. **Dormant companies**: Code `99999` is a Companies House special code meaning "Dormant Company". It is not Section U. Filter these out or handle as a separate category before classification.

4. **Non-trading**: Code `74990` means "Non-trading company". This sits in Section M technically, but should be flagged separately in any sector analysis — it's a placeholder, not a real activity.

5. **Empty fields**: Secondary/tertiary SIC columns will often be `EMPTY` or null. Ignore them. Only process populated fields.

6. **Parsing**: The CSV column format is `"NNNNN Description text"`. Extract the code as the first whitespace-delimited token. The description is informational — classification comes from the numeric prefix, not from text matching.

7. **No LLM needed for classification**: This is a deterministic lookup. First 2 digits → section. No fuzzy matching, no NLP, no embeddings. A dictionary join is the correct implementation.

---

## Primary vs Secondary SIC Codes

### Primary SIC code — Classification
`siccode_sictext_1` is the principal activity as listed by Companies House. This is the **only** code used for macro-sector and sub-sector classification. All rules below apply exclusively to the primary code.

### Secondary SIC codes — Metadata storage only
`siccode_sictext_2`, `siccode_sictext_3`, `siccode_sictext_4` are stored as-is into the company's `metadata` JSONB column. They are **not** classified and do not affect sector assignment. They are preserved for reference and future filtering.

Store them under `metadata.sic_codes`:

```python
def build_sic_metadata(row):
    """
    Parse all SIC columns from a Companies House row and return a dict
    for storage in the metadata JSONB column.

    Primary code drives classification. Secondary codes are stored raw.
    """
    primary_raw = row.get("siccode_sictext_1", "")
    primary_code = parse_sic(primary_raw)
    classification = classify(primary_code) if primary_code else None

    secondary_codes = []
    for col in ("siccode_sictext_2", "siccode_sictext_3", "siccode_sictext_4"):
        val = row.get(col, "")
        if val and val not in ("EMPTY", ""):
            code = parse_sic(val)
            if code:
                secondary_codes.append({
                    "code": code,
                    "description": val[len(code):].strip(),
                })

    return {
        "sic_codes": {
            "primary": {
                "code": primary_code,
                "description": primary_raw[len(primary_code):].strip() if primary_code else None,
                "sector": classification[0] if classification else None,
                "sub_sector": classification[1] if classification else None,
                "excluded": is_excluded(primary_code) if primary_code else None,
            },
            "secondary": secondary_codes,  # list of {code, description} — not classified
        }
    }
```

**Example output for a company with 3 SIC codes:**
```json
{
  "sic_codes": {
    "primary": {
      "code": "62012",
      "description": "Business and domestic software development",
      "sector": "Technology",
      "sub_sector": "Software",
      "excluded": false
    },
    "secondary": [
      { "code": "62020", "description": "Information technology consultancy activities" },
      { "code": "63990", "description": "Other information service activities n.e.c." }
    ]
  }
}
```

**Rules:**
- Never classify secondary codes — store them raw
- Never use secondary codes to override or change the primary sector assignment
- If the primary code is excluded (dormant, non-trading), set `sector` and `sub_sector` to `null` and `excluded` to `true` — still store secondary codes as normal
- If a secondary code maps to a *different* section than the primary, flag the company as multi-sector in metadata: `"multi_sector": true` — but do not change the sector assignment

---

## Macro-Sector Classification

Companies are classified into 8 macro-sectors. The primary SIC code determines sector. Classification is a two-pass lookup:

1. **Check if division is 26 or 32** — if yes, use the 4-digit class to route (these divisions span multiple sectors).
2. **All other divisions** — 2-digit lookup is sufficient.

The 4-digit class is free from the 5-digit code: `int(code[:4])`.

---

### Excluded / Filtered (check before classification)

| Code | Reason |
|---|---|
| `99999` | Dormant company — Companies House special code, not a real sector |
| `74990` | Non-trading company — placeholder, exclude from sector analysis |
| `97xxx`, `98xxx` | Household activities — non-commercial |
| `99xxx` | Extraterritorial organisations |

---

### TECHNOLOGY

**Sub-sectors:** Software · Hardware & Semiconductors · Telecommunications · Media & Digital Content

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 62 | Computer programming, consultancy | Software |
| 63 | Information service activities | Software |
| 61 | Telecommunications | Telecommunications |
| 58 | Publishing activities | Media & Digital Content |
| 59 | Film, video, TV production, sound recording | Media & Digital Content |
| 60 | Programming and broadcasting | Media & Digital Content |
| 2610 | Manufacture of electronic components | Hardware & Semiconductors |
| 2620 | Manufacture of computers & peripheral equipment | Hardware & Semiconductors |
| 2630 | Manufacture of communication equipment | Hardware & Semiconductors |
| 2640 | Manufacture of consumer electronics | Hardware & Semiconductors |
| 2650/2651 | Measuring, testing & navigation instruments | Hardware & Semiconductors |
| 2670 | Optical instruments & photographic equipment | Hardware & Semiconductors |
| 2680 | Magnetic and optical media | Hardware & Semiconductors |

---

### FINANCIALS

**Sub-sectors:** Banking & Lending · Asset Management · Insurance & Pensions · Capital Markets & Brokerages

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 64 *(excl. 4-digit overrides below)* | Financial service activities excl. insurance | Banking & Lending |
| 6420 | Activities of holding companies | Asset Management |
| 6430 | Trusts, funds and similar financial entities | Asset Management |
| 6499 | Other financial service activities n.e.c. | Asset Management |
| 65 | Insurance, reinsurance and pension funding | Insurance & Pensions |
| 66 | Auxiliary financial and insurance activities | Capital Markets & Brokerages |

---

### HEALTHCARE

**Sub-sectors:** Pharma & Biotech · Healthcare Services · Medical Devices & Equipment · Care & Social Services

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 21 | Manufacture of pharmaceuticals | Pharma & Biotech |
| 72 | Scientific research and development | Pharma & Biotech |
| 86 | Human health activities | Healthcare Services |
| 87 | Residential care activities | Care & Social Services |
| 88 | Social work without accommodation | Care & Social Services |
| 2660 | Irradiation, electromedical & electrotherapeutic equipment | Medical Devices & Equipment |
| 3250 | Manufacture of medical and dental instruments & supplies | Medical Devices & Equipment |

---

### CONSUMER

**Sub-sectors:** Food & Beverage · Retail & E-commerce · Leisure & Hospitality · Consumer Products · Personal Services · Education

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 10 | Manufacture of food products | Food & Beverage |
| 11 | Manufacture of beverages | Food & Beverage |
| 12 | Manufacture of tobacco products | Food & Beverage |
| 56 | Food and beverage service activities | Food & Beverage |
| 45 | Wholesale/retail/repair of motor vehicles | Retail & E-commerce |
| 46 | Wholesale trade excl. motor vehicles | Retail & E-commerce |
| 47 | Retail trade excl. motor vehicles | Retail & E-commerce |
| 55 | Accommodation | Leisure & Hospitality |
| 90 | Creative, arts and entertainment | Leisure & Hospitality |
| 91 | Libraries, archives and museums | Leisure & Hospitality |
| 92 | Gambling and betting | Leisure & Hospitality |
| 93 | Sports activities and recreation | Leisure & Hospitality |
| 13 | Manufacture of textiles | Consumer Products |
| 14 | Manufacture of wearing apparel | Consumer Products |
| 15 | Manufacture of leather and related products | Consumer Products |
| 31 | Manufacture of furniture | Consumer Products |
| 2652 | Manufacture of watches and clocks | Consumer Products |
| 3211/3212/3213 | Manufacture of jewellery and imitation jewellery | Consumer Products |
| 3220 | Manufacture of musical instruments | Consumer Products |
| 3240 | Manufacture of games and toys | Consumer Products |
| 3230 | Manufacture of sports goods | Leisure & Hospitality |
| 75 | Veterinary activities | Personal Services |
| 95 | Repair of computers and household goods | Personal Services |
| 96 | Other personal service activities | Personal Services |
| 85 | Education | Education |

---

### BUSINESS SERVICES

**Sub-sectors:** Professional Services · Staffing & HR · Marketing & Communications · Facilities & Support Services

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 69 | Legal and accounting activities | Professional Services |
| 70 | Head offices and management consultancy | Professional Services |
| 71 | Architecture, engineering and technical testing | Professional Services |
| 74 | Other professional, scientific and technical | Professional Services |
| 78 | Employment activities | Staffing & HR |
| 73 | Advertising and market research | Marketing & Communications |
| 77 | Rental and leasing activities | Facilities & Support Services |
| 79 | Travel agency and tour operator activities | Facilities & Support Services |
| 80 | Security and investigation activities | Facilities & Support Services |
| 81 | Services to buildings and landscape | Facilities & Support Services |
| 82 | Office admin and business support | Facilities & Support Services |

---

### INDUSTRIAL

**Sub-sectors:** Engineering & Manufacturing · Chemicals & Materials · Aerospace & Defence · Automotive & Transport Equipment · Construction

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 16 | Manufacture of wood and wood products | Engineering & Manufacturing |
| 17 | Manufacture of paper and paper products | Engineering & Manufacturing |
| 18 | Printing and reproduction of recorded media | Engineering & Manufacturing |
| 22 | Manufacture of rubber and plastic products | Engineering & Manufacturing |
| 23 | Manufacture of other non-metallic mineral products | Engineering & Manufacturing |
| 24 | Manufacture of basic metals | Engineering & Manufacturing |
| 25 | Manufacture of fabricated metal products | Engineering & Manufacturing |
| 27 | Manufacture of electrical equipment | Engineering & Manufacturing |
| 28 | Manufacture of machinery and equipment n.e.c. | Engineering & Manufacturing |
| 32 *(excl. 4-digit overrides above)* | Other manufacturing n.e.c. | Engineering & Manufacturing |
| 3291 | Manufacture of brooms and brushes | Engineering & Manufacturing |
| 3299 | Other manufacturing n.e.c. | Engineering & Manufacturing |
| 33 | Repair and installation of machinery | Engineering & Manufacturing |
| 20 | Manufacture of chemicals and chemical products | Chemicals & Materials |
| 30 | Manufacture of other transport equipment | Aerospace & Defence |
| 29 | Manufacture of motor vehicles and trailers | Automotive & Transport Equipment |
| 41 | Construction of buildings | Construction |
| 42 | Civil engineering | Construction |
| 43 | Specialised construction activities | Construction |

---

### REAL ASSETS

**Sub-sectors:** Real Estate · Energy · Infrastructure & Utilities · Agriculture & Natural Resources

| Division / Class | Description | Sub-Sector |
|---|---|---|
| 68 | Real estate activities | Real Estate |
| 05 | Mining of coal and lignite | Energy |
| 06 | Extraction of crude petroleum and natural gas | Energy |
| 07 | Mining of metal ores | Energy |
| 08 | Other mining and quarrying | Energy |
| 09 | Mining support service activities | Energy |
| 19 | Manufacture of coke and refined petroleum products | Energy |
| 35 | Electricity, gas, steam and air conditioning supply | Energy |
| 36 | Water collection, treatment and supply | Infrastructure & Utilities |
| 37 | Sewerage | Infrastructure & Utilities |
| 38 | Waste collection, treatment and disposal | Infrastructure & Utilities |
| 39 | Remediation and other waste management | Infrastructure & Utilities |
| 49 | Land transport and transport via pipelines | Infrastructure & Utilities |
| 50 | Water transport | Infrastructure & Utilities |
| 51 | Air transport | Infrastructure & Utilities |
| 52 | Warehousing and transport support | Infrastructure & Utilities |
| 53 | Postal and courier activities | Infrastructure & Utilities |
| 01 | Crop and animal production, hunting | Agriculture & Natural Resources |
| 02 | Forestry and logging | Agriculture & Natural Resources |
| 03 | Fishing and aquaculture | Agriculture & Natural Resources |

---

### PUBLIC SERVICES

**Sub-sectors:** Public Administration & Defence

| Division | Description | Sub-Sector |
|---|---|---|
| 84 | Public administration, defence and social security | Public Administration & Defence |

*Note: Public Services is typically excluded from private market deal flow analysis. Flag these companies separately rather than including them in sector counts.*

---

## Implementation

```python
# Step 1 — strip and parse the SIC code from Companies House format
def parse_sic(raw):
    if not raw or raw in ("EMPTY", ""):
        return None
    code = str(raw).strip().split()[0]  # first token is always the numeric code
    if not code.isdigit() or len(code) != 5:
        return None
    return code

# Step 2 — pre-classification filters
EXCLUDED = {"99999", "74990"}

def is_excluded(code):
    if code in EXCLUDED:
        return True
    div = int(code[:2])
    if div in (97, 98, 99):
        return True
    return False

# Step 3 — classify using two-pass lookup
FOUR_DIGIT_OVERRIDES = {
    # Technology — Hardware
    2610: ("Technology", "Hardware & Semiconductors"),
    2620: ("Technology", "Hardware & Semiconductors"),
    2630: ("Technology", "Hardware & Semiconductors"),
    2640: ("Technology", "Hardware & Semiconductors"),
    2650: ("Technology", "Hardware & Semiconductors"),
    2651: ("Technology", "Hardware & Semiconductors"),
    2670: ("Technology", "Hardware & Semiconductors"),
    2680: ("Technology", "Hardware & Semiconductors"),
    # Healthcare — Medical Devices (overrides Hardware for div 26)
    2660: ("Healthcare", "Medical Devices & Equipment"),
    # Consumer — from div 26
    2652: ("Consumer", "Consumer Products"),
    # Consumer — from div 32
    3211: ("Consumer", "Consumer Products"),
    3212: ("Consumer", "Consumer Products"),
    3213: ("Consumer", "Consumer Products"),
    3220: ("Consumer", "Consumer Products"),
    3230: ("Consumer", "Leisure & Hospitality"),
    3240: ("Consumer", "Consumer Products"),
    # Healthcare — Medical Devices from div 32
    3250: ("Healthcare", "Medical Devices & Equipment"),
    # Industrial — remainder of div 32
    3291: ("Industrial", "Engineering & Manufacturing"),
    3299: ("Industrial", "Engineering & Manufacturing"),
    # Financials — Asset Management overrides within div 64
    6420: ("Financials", "Asset Management"),
    6430: ("Financials", "Asset Management"),
    6499: ("Financials", "Asset Management"),
}

DIVISION_MAP = {
    # Technology
    62: ("Technology", "Software"),
    63: ("Technology", "Software"),
    61: ("Technology", "Telecommunications"),
    58: ("Technology", "Media & Digital Content"),
    59: ("Technology", "Media & Digital Content"),
    60: ("Technology", "Media & Digital Content"),
    # Financials
    64: ("Financials", "Banking & Lending"),
    65: ("Financials", "Insurance & Pensions"),
    66: ("Financials", "Capital Markets & Brokerages"),
    # Healthcare
    21: ("Healthcare", "Pharma & Biotech"),
    72: ("Healthcare", "Pharma & Biotech"),
    86: ("Healthcare", "Healthcare Services"),
    87: ("Healthcare", "Care & Social Services"),
    88: ("Healthcare", "Care & Social Services"),
    # Consumer
    10: ("Consumer", "Food & Beverage"),
    11: ("Consumer", "Food & Beverage"),
    12: ("Consumer", "Food & Beverage"),
    56: ("Consumer", "Food & Beverage"),
    45: ("Consumer", "Retail & E-commerce"),
    46: ("Consumer", "Retail & E-commerce"),
    47: ("Consumer", "Retail & E-commerce"),
    55: ("Consumer", "Leisure & Hospitality"),
    90: ("Consumer", "Leisure & Hospitality"),
    91: ("Consumer", "Leisure & Hospitality"),
    92: ("Consumer", "Leisure & Hospitality"),
    93: ("Consumer", "Leisure & Hospitality"),
    13: ("Consumer", "Consumer Products"),
    14: ("Consumer", "Consumer Products"),
    15: ("Consumer", "Consumer Products"),
    31: ("Consumer", "Consumer Products"),
    75: ("Consumer", "Personal Services"),
    95: ("Consumer", "Personal Services"),
    96: ("Consumer", "Personal Services"),
    85: ("Consumer", "Education"),
    # Business Services
    69: ("Business Services", "Professional Services"),
    70: ("Business Services", "Professional Services"),
    71: ("Business Services", "Professional Services"),
    74: ("Business Services", "Professional Services"),
    78: ("Business Services", "Staffing & HR"),
    73: ("Business Services", "Marketing & Communications"),
    77: ("Business Services", "Facilities & Support Services"),
    79: ("Business Services", "Facilities & Support Services"),
    80: ("Business Services", "Facilities & Support Services"),
    81: ("Business Services", "Facilities & Support Services"),
    82: ("Business Services", "Facilities & Support Services"),
    # Industrial
    16: ("Industrial", "Engineering & Manufacturing"),
    17: ("Industrial", "Engineering & Manufacturing"),
    18: ("Industrial", "Engineering & Manufacturing"),
    22: ("Industrial", "Engineering & Manufacturing"),
    23: ("Industrial", "Engineering & Manufacturing"),
    24: ("Industrial", "Engineering & Manufacturing"),
    25: ("Industrial", "Engineering & Manufacturing"),
    27: ("Industrial", "Engineering & Manufacturing"),
    28: ("Industrial", "Engineering & Manufacturing"),
    33: ("Industrial", "Engineering & Manufacturing"),
    20: ("Industrial", "Chemicals & Materials"),
    30: ("Industrial", "Aerospace & Defence"),
    29: ("Industrial", "Automotive & Transport Equipment"),
    41: ("Industrial", "Construction"),
    42: ("Industrial", "Construction"),
    43: ("Industrial", "Construction"),
    # Real Assets
    68: ("Real Assets", "Real Estate"),
    5:  ("Real Assets", "Energy"),
    6:  ("Real Assets", "Energy"),
    7:  ("Real Assets", "Energy"),
    8:  ("Real Assets", "Energy"),
    9:  ("Real Assets", "Energy"),
    19: ("Real Assets", "Energy"),
    35: ("Real Assets", "Energy"),
    36: ("Real Assets", "Infrastructure & Utilities"),
    37: ("Real Assets", "Infrastructure & Utilities"),
    38: ("Real Assets", "Infrastructure & Utilities"),
    39: ("Real Assets", "Infrastructure & Utilities"),
    49: ("Real Assets", "Infrastructure & Utilities"),
    50: ("Real Assets", "Infrastructure & Utilities"),
    51: ("Real Assets", "Infrastructure & Utilities"),
    52: ("Real Assets", "Infrastructure & Utilities"),
    53: ("Real Assets", "Infrastructure & Utilities"),
    1:  ("Real Assets", "Agriculture & Natural Resources"),
    2:  ("Real Assets", "Agriculture & Natural Resources"),
    3:  ("Real Assets", "Agriculture & Natural Resources"),
    # Public Services
    84: ("Public Services", "Public Administration & Defence"),
}

def classify(code):
    """Return (sector, sub_sector) for a 5-digit SIC code string, or None if excluded."""
    if is_excluded(code):
        return None

    div = int(code[:2])

    # Pass 1 — check 4-digit overrides (only needed for divs 26, 32, 64)
    if div in (26, 32, 64):
        cls = int(code[:4])
        if cls in FOUR_DIGIT_OVERRIDES:
            return FOUR_DIGIT_OVERRIDES[cls]

    # Pass 2 — 2-digit division lookup
    return DIVISION_MAP.get(div)
```
