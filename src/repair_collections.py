import pandas as pd
import requests
import time
import re
import os
from bs4 import BeautifulSoup
import csv

# UA and Config
UA = "Teerth Patel (tmp00726@uga.edu)"
OUT_PATH = 'output/repaired_collections_full.csv'

# Load data
meta = pd.read_csv('output/metadata.csv')
dirty = pd.read_csv('collections_dirty.csv')
unified = pd.read_csv('output/unified_monthly_summary.csv')

found_pools = set(unified['company_name'].str.strip().str.upper().dropna())
all_pools = set(dirty['poolname'].str.strip().str.upper().dropna())
missing_pools = all_pools - found_pools
print(f"Missing Pools: {len(missing_pools)}")

meta['norm_name'] = meta['company_name'].str.strip().str.upper()

def get_base(name):
    return re.sub(r'\s+\d{4}\S*$', '', name).strip()

meta_by_base = {}
for _, row in meta.iterrows():
    base = get_base(str(row['norm_name']))
    if base not in meta_by_base: meta_by_base[base] = []
    meta_by_base[base].append(row.to_dict())

missing_accs_list = []
for p in missing_pools:
    base_p = get_base(p)
    if base_p in meta_by_base: missing_accs_list.extend(meta_by_base[base_p])

unique_missing = []
seen = set()
for r in missing_accs_list:
    if r['accession_number'] not in seen:
        unique_missing.append(r)
        seen.add(r['accession_number'])

TARGET_ISSUERS = ['TOYOTA','HONDA','HYUNDAI','NISSAN','MERCEDES','EXETER']
def sort_key(row):
    name = str(row['company_name']).upper()
    for i, iss in enumerate(TARGET_ISSUERS):
        if iss in name: return i
    return 99

unique_missing.sort(key=sort_key)
print(f"Total filings to repair: {len(unique_missing)}")

# Resume logic
processed_accs = set()
if os.path.exists(OUT_PATH):
    try:
        existing = pd.read_csv(OUT_PATH)
        # ONLY skip if we actually found a non-zero value
        done = existing[existing['scraped_total_collections'] > 0]
        processed_accs = set(done['accession_number'].astype(str))
        print(f"Resuming: {len(processed_accs)} already successfully recovered.")
    except: pass

unique_missing = [r for r in unique_missing if str(r['accession_number']) not in processed_accs]
print(f"Remaining to process: {len(unique_missing)}")

session = requests.Session()
session.headers.update({"User-Agent": UA})

def clean_num(s):
    if not s: return 0.0
    s_clean = re.sub(r'[$,\s()\-]', '', str(s))
    try: return float(s_clean)
    except: return 0.0

P1_REGEX = re.compile(r'Collections|Available Funds|Total Available|Amount Available|Yield Supplement', re.I)

def parse_robust(txt, issuer_name):
    low_txt = txt.lower()
    
    # Mercedes absolute path
    if "MERCEDES" in issuer_name.upper():
        soup = BeautifulSoup(txt, "lxml")
        divs = soup.find_all("div", style=re.compile(r"position\s*:\s*absolute"))
        target_top = None
        for d in divs:
            if "Available Funds" in d.get_text():
                m = re.search(r"top\s*:\s*(\d+)", d.get("style", ""))
                if m: target_top = int(m.group(1)); break
        if target_top:
            vals = [clean_num(d.get_text(strip=True)) for d in divs if (m := re.search(r"top\s*:\s*(\d+)", d.get("style", ""))) and abs(int(m.group(1)) - target_top) < 10]
            return max(vals) if vals else 0.0

    # Toyota/Standard
    # We use a broader snippet search
    best_val = 0.0
    soup = BeautifulSoup(txt, "lxml")
    tables = soup.find_all('table')
    for t in tables:
        t_txt = t.get_text(' ')
        if P1_REGEX.search(t_txt):
            for r in t.find_all('tr'):
                r_txt = r.get_text(' ')
                if P1_REGEX.search(r_txt):
                    cells = [c.get_text(strip=True) for c in r.find_all(['td', 'th'])]
                    for c in cells:
                        v = clean_num(c)
                        if v > best_val: best_val = v
    return best_val

print(f"Starting resilient repair scrape. Estimated speed: 10/min. Total time: {len(unique_missing)/10:.1f} mins.")

fields = ['accession_number', 'scraped_total_collections', 'company_name', 'report_period', 'status']

with open(OUT_PATH, 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    if not os.path.exists(OUT_PATH) or os.stat(OUT_PATH).st_size == 0: writer.writeheader()
    
    for i, row in enumerate(unique_missing):
        acc = row['accession_number']
        cik = str(int(row['cik']))
        acc_clean = acc.replace('-', '')
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{acc}.txt"
        
        try:
            time.sleep(0.5) # Consistent rate limiting
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                val = parse_robust(resp.text, row['company_name'])
                writer.writerow({'accession_number': acc, 'scraped_total_collections': val, 'company_name': row['company_name'], 'report_period': row['report_period'], 'status': 'OK' if val > 0 else 'MISSING'})
            else:
                writer.writerow({'accession_number': acc, 'scraped_total_collections': 0.0, 'status': f'HTTP_{resp.status_code}'})
        except Exception as e:
            writer.writerow({'accession_number': acc, 'scraped_total_collections': 0.0, 'status': f'ERR:{str(e)[:10]}'})
        
        if (i + 1) % 10 == 0:
            f.flush()
            print(f"Progress: {i+1}/{len(unique_missing)} done.", flush=True)

print("Repair job complete.")
