#!/usr/bin/env python
"""
Scrape Sangamon County Board written‑minutes PDFs that sit on
https://sangamonil.gov/departments/a-c/county-clerk/vital-records/county-board/written-minutes
and load them into county_board_raw.
"""
import re, datetime, calendar, logging, psycopg2, requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s")

BASE   = "https://sangamonil.gov"
LANDING= (BASE +
  "/departments/a-c/county-clerk/vital-records/county-board/written-minutes")

# ── regex that matches: 2024‑05‑14  |  05.14.2024  |  May 14, 2024  ──────────
DATE_RE = re.compile(r"""
    (?:                       #  yyyy‑mm‑dd or yyyy_mm_dd
        (?P<Y1>\d{4})[-_.](?P<M1>\d{1,2})[-_.](?P<D1>\d{1,2})
    ) |
    (?:                       #  mm‑dd‑yyyy / mm‑dd‑yy / mm.dd.yyyy
        (?P<M2>\d{1,2})[-_.](?P<D2>\d{1,2})[-_.](?P<Y2>\d{2,4})
    ) |
    (?:                       #  Month dd, yyyy  (March 11, 2025)
        (?P<MON>[A-Za-z]{3,9})[\s_-]*(?P<D3>\d{1,2}),?\s*(?P<Y3>\d{4})
    )
""", re.I | re.VERBOSE)

MONTH_MAP = {}
for i, name in enumerate(calendar.month_name):      # January … December
    if i == 0:          # month_name[0] is ''
        continue
    MONTH_MAP[name.lower()]     = i   # "october" → 10
    MONTH_MAP[name[:3].lower()] = i   # "oct"     → 10

def extract_date(text:str):
    m = DATE_RE.search(text.replace("%20"," "))
    if not m: return None
    g = m.groupdict()
    if g["Y1"]:  # yyyy‑mm‑dd
        return datetime.date(int(g["Y1"]), int(g["M1"]), int(g["D1"]))
    if g["M2"]:  # mm‑dd‑yyyy/yy
        y = int(g["Y2"]);  y += 2000 if y < 100 else 0
        return datetime.date(y, int(g["M2"]), int(g["D2"]))
    if g["MON"]: # Month dd, yyyy
        mon = MONTH_MAP[g["MON"].lower()[:3]]
        return datetime.date(int(g["Y3"]), mon, int(g["D3"]))
    return None

# ── scrape landing page ───────────────────────────────────────────────────────
html  = requests.get(LANDING, timeout=30).text
soup  = BeautifulSoup(html, "html.parser")

records=[]
for a in soup.select('a[href$=".pdf"]'):
    url  = a["href"]
    if not url.startswith("http"):
        url = BASE + url
    text = (a.get_text(" ", strip=True) + " " + url)

    date = extract_date(text)
    if not date:                       # skip weird files with no date
        continue

    records.append({
        "meeting_date": date,
        "committee"   : "Full Board",
        "doc_type"    : "Minutes",
        "url"         : url
    })

logging.info("Parsed %s docs", len(records))
if not records:
    quit()

# ── load into Postgres ────────────────────────────────────────────────────────
dsn = "dbname=civisort user=civisort host=db password=civisort"
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    cur.executemany("""
        INSERT INTO county_board_raw (meeting_date, committee, doc_type, url)
        VALUES (%(meeting_date)s,%(committee)s,%(doc_type)s,%(url)s)
        ON CONFLICT DO NOTHING
    """, records)
    logging.info("Inserted %s new rows", cur.rowcount)
