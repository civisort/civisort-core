import re, datetime, logging, tempfile, requests, psycopg2
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

ROOT = "https://sangamonil.gov/departments/a-c/building-and-zoning/building/permits"
resp = requests.get(ROOT, timeout=30)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")
links = [
    (a["href"], a.get_text(strip=True))
    for a in soup.select('a[href$=".pdf"]')
]

records = []
for url, text in links:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", url)
    if not m:
        continue
    file_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()

    pdf = requests.get(url, timeout=30).content
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(pdf)
        first_page = extract_text(f.name, maxpages=1)

    pm = re.search(r"(BP-\d+)", first_page)
    am = re.search(r"\d{3,5}\s+[\w\s\.]+", first_page)
    dm = re.search(r"(?:New|Alteration|Remodel|Demo)[\w\s]+", first_page)

    records.append(dict(
        file_date=file_date,
        permit_no=pm.group(1) if pm else None,
        address=am.group(0) if am else None,
        description=dm.group(0) if dm else "—",
        pdf_url=url
    ))

logging.info("Parsed %s permits", len(records))
if not records:
    exit()

dsn="dbname=civisort user=civisort host=db password=civisort"
with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
    cur.executemany(
        """INSERT INTO county_permits_raw
           (file_date, permit_no, address, description, pdf_url)
           VALUES (%(file_date)s,%(permit_no)s,%(address)s,%(description)s,%(pdf_url)s)
           ON CONFLICT DO NOTHING""",
        records)
    logging.info("Inserted %s new rows", cur.rowcount)
