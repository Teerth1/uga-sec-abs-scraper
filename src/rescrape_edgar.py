"""
rescrape_edgar.py
=================
Phase 3: Re-scrape total collections directly from SEC EDGAR HTML filings
for Toyota, BMW, Nissan, Hyundai, Mercedes — issuers whose structured table
CSVs contain zero parseable dollar values due to non-standard HTML formats.

Strategy:
  1. For each target accession, build the EDGAR filing index URL
  2. Download the filing index to find the 10-D document URL  
  3. Fetch the HTML document
  4. Parse all tables with BeautifulSoup, find "Total Collections" row
  5. Extract the dollar value and store with accession + month_key

Output: output/analysis_v7/cache_edgar_rescrape.csv

Rate limit: SEC allows 10 req/sec. We add 0.15s delay between requests.
"""

import pandas as pd
import numpy as np
import requests
import time
import re
import os
from bs4 import BeautifulSoup

OUT  = 'output/analysis_v7'
OUTF = f'{OUT}/cache_edgar_rescrape.csv'
RATE_DELAY = 0.15   # seconds between requests (< 10/sec per SEC policy)
HEADERS = {'User-Agent': 'UGA Finance Research teerth@uga.edu'}

TARGET_BRANDS = ['TOYOTA','BMW','NISSAN','HYUNDAI','MERCEDES','VOLKSWAGEN',
                 'CARMAX','HARLEY']

# Patterns to find the total collections row
COLLECT_PATTERNS = re.compile(
    r'Total\s+Collections|Total\s+Available\s+Funds|Total\s+Available\s+Amount|'
    r'Total\s+Cash\s+Available|Total\s+Available\s+for\s+Distribution|'
    r'Amount\s+Available\s+for\s+Distribution|Aggregate\s+Available\s+Funds|'
    r'Total\s+Distributable\s+Amount|Total\s+Available\s+Collections|'
    r'Distributions\s+from\s+SUBI\s+Collection|'
    r'Total\s+Available\s+Distributions|'
    r'Available\s+Collections|Available\s+Funds|'
    r'Total\s+Available|'
    r'SUBI\s+Collection\s+Account|'
    r'Collection\s+Account\s+Activity',
    re.IGNORECASE
)

def get_brand(name):
    s = str(name).upper()
    for kw,lbl in [('CARMAX','CARMAX'),('FORD','FORD'),('BMW','BMW'),('TOYOTA','TOYOTA'),
                    ('HONDA','HONDA'),('NISSAN','NISSAN'),('HYUNDAI','HYUNDAI'),
                    ('MERCEDES','MERCEDES'),('VOLKSWAGEN','VW'),('HARLEY','HARLEY')]:
        if kw in s: return lbl
    return 'OTHER'

def clean_dollar(s):
    """Extract numeric value from a cell string."""
    s = re.sub(r'[$,()\s]','',str(s))
    s = s.replace('–','').replace('—','').strip()
    if not s or s == '-': return np.nan
    try:
        return abs(float(s))
    except:
        return np.nan

def format_accession(acc):
    """'0000950170-20-012345' → '000095017020012345'"""
    return acc.replace('-','')

def get_cik_from_acc(acc):
    """Extract CIK from accession number (first 10 digits)."""
    digits = re.sub(r'\D','',acc)
    return digits[:10].lstrip('0') if len(digits) >= 10 else None

def fetch_filing_index(acc):
    """Fetch the filing index page and return the list of documents."""
    cik    = get_cik_from_acc(acc)
    acc_nd = format_accession(acc)
    url    = (f'https://www.sec.gov/Archives/edgar/data/{cik}/'
              f'{acc_nd}/{acc}-index.htm')
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        time.sleep(RATE_DELAY)
        if r.status_code != 200:
            return None, url
        return r.text, url
    except Exception as e:
        return None, url

def find_data_document_url(index_html):
    """Robustly find the data document (Exhibit 99.1 or 10-D) in the index."""
    soup = BeautifulSoup(index_html, 'html.parser')
    
    # Collect all links and their surrounding text
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if not href.endswith('.htm'): continue
        if 'index' in href.lower(): continue
        
        # Get text from the link itself and its parent row if possible
        link_text = a.get_text(' ', strip=True).lower()
        parent_row = a.find_parent('tr')
        row_text = parent_row.get_text(' ', strip=True).lower() if parent_row else ""
        
        links.append({
            'url': f"https://www.sec.gov{href}",
            'text': link_text + " " + row_text
        })
    
    # Priority 1: Exhibit 99.1
    for l in links:
        if '99.1' in l['text']:
            return l['url']
            
    # Priority 2: Statement
    for l in links:
        if 'statement' in l['text']:
            return l['url']
            
    # Priority 3: Distribution (excluding the report title)
    for l in links:
        if 'distribution' in l['text'] and 'report' not in l['text']:
            return l['url']
            
    # Priority 4: Primary 10-D
    for l in links:
        if '10-d' in l['text'] or '10d' in l['text']:
            return l['url']
            
    # Priority 3: Any .htm
    if links:
        return links[0]['url']
            
    return None

def extract_collections_from_html(html):
    """
    Parse HTML tables from a 10-D filing and find the total collections value.
    """
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    best_val = np.nan

    for tbl in tables:
        rows = tbl.find_all('tr')
        for row in rows:
            cells = [c.get_text(' ', strip=True) for c in row.find_all(['td','th'])]
            if not cells: continue
            
            row_text = " ".join(cells)
            if COLLECT_PATTERNS.search(row_text):
                # We found a relevant row, now find the numeric value
                for cell in cells:
                    v = clean_dollar(cell)
                    if not np.isnan(v) and v > 1000:
                        # Heuristic: usually total is the largest value in the row 
                        # or specifically for these brands, we want the most plausible 'total'
                        if np.isnan(best_val) or v > best_val:
                            best_val = v
    return best_val

# ── Load metadata and identify target accessions ──────────────────────────────
print("Loading metadata...")
meta = pd.read_csv('output/metadata.csv')
meta['acc']       = meta['accession_number'].astype(str)
meta['dt']        = pd.to_datetime(meta['report_period'].astype(str),
                                    format='%Y%m%d', errors='coerce')
meta['month_key'] = meta['dt'].dt.strftime('%Y-%m')
meta['brand']     = meta['company_name'].apply(get_brand)

# Only target brands with zero coverage AND within the relevant date range (2017+)
target_accs = meta[
    (meta['brand'].isin(TARGET_BRANDS)) & 
    (meta['report_period'] >= 20170101)
].drop_duplicates('acc')
print(f"Target accessions (2017+): {len(target_accs):,}")
print(target_accs['brand'].value_counts())

# Skip already cached accessions
if os.path.exists(OUTF):
    done = set(pd.read_csv(OUTF)['acc'].astype(str).unique())
    target_accs = target_accs[~target_accs['acc'].isin(done)]
    print(f"  ({len(done)} already cached, {len(target_accs)} remaining)")

# ── Main scraping loop ────────────────────────────────────────────────────────
results = []
total   = len(target_accs)

for i, (_, row) in enumerate(target_accs.iterrows()):
    acc   = row['acc']
    brand = row['brand']
    mkey  = row['month_key']
    cname = row['company_name']

    print(f"  [{i+1}/{total}] Processing {brand} | {acc} | {mkey}...", end=' ', flush=True)

    # Step 1: Fetch filing index
    index_html, idx_url = fetch_filing_index(acc)
    if index_html is None:
        print("Failed (index not found)")
        continue

    # Step 2: Find data document URL (Exhibit 99.1 or 10-D)
    doc_url = find_data_document_url(index_html)
    if doc_url is None:
        print("Failed (No relevant document found)")
        continue
    
    print(f"URL: {doc_url[-20:]}...", end=' ', flush=True)

    # Step 3: Fetch the 10-D document
    try:
        r2 = requests.get(doc_url, headers=HEADERS, timeout=20)
        time.sleep(RATE_DELAY)
        if r2.status_code != 200:
            print(f"Failed (HTTP {r2.status_code})")
            continue
    except Exception as e:
        print(f"Failed (Error: {e})")
        continue

    # Step 4: Extract total collections
    val = extract_collections_from_html(r2.text)
    if not np.isnan(val):
        print(f"SUCCESS: ${val:,.2f}")
        results.append({
            'acc':          acc,
            'company_name': cname,
            'brand':        brand,
            'month_key':    mkey,
            'scraped_total': val,
        })
    else:
        print("Failed (no value found)")

    # Save incrementally every 10 records (more frequent)
    if len(results) > 0 and len(results) % 10 == 0:
        pd.DataFrame(results).to_csv(OUTF, index=False)

# Final save
if results:
    out = pd.DataFrame(results)
    # Append to existing if present
    if os.path.exists(OUTF):
        existing = pd.read_csv(OUTF)
        out = pd.concat([existing, out]).drop_duplicates('acc')
    out.to_csv(OUTF, index=False)
    print(f"\nDone! Saved {len(out):,} records to {OUTF}")
    print(out['brand'].value_counts())
else:
    print("No values extracted.")
