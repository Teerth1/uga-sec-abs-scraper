# SEC 10-D ABS Scraper
# Optimized for Multi-threading (Parallel Downloads)
# Extracts Tables 2-5 from 10-D filings.
# Filters to AUTO LOANS and AUTO LEASES only.
# Saves incrementally and handles resume safely via ThreadPoolExecutor.

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "output"
CONCURRENCY = 10  # Optimal for 10 RPS limit
SAVE_EVERY = 100  # Number of new filings attempted before checkpointing

# Global session for TLS/SSL connection pooling (Thread-safe)
_global_session = None
_session_lock = threading.Lock()

def get_session():
    global _global_session
    if _global_session is None:
        with _session_lock:
            if _global_session is None:
                s = requests.Session()
                s.headers.update({"User-Agent": "Teerth Patel (tmp00726@uga.edu)"})
                adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENCY*2, pool_maxsize=CONCURRENCY*2, max_retries=3)
                s.mount('https://', adapter)
                _global_session = s
    return _global_session

TABLES = [
    ("table_2_available_funds",  ["available funds", "reserve account", "cash flows", "collections", "funds available for distribution"]),
    ("table_3_distributions",    ["distributions", "determination date", "payment date", "collection period", "additional information"]),
    ("table_4_noteholder",       ["noteholder", "class a-1 notes", "interest distributable"]),
    ("table_5_note_balance",     ["note factor", "note balance", "principal balance"]),
]

AUTO_ABS_CLASSES = {"auto loans", "auto leases"}

COLUMN_ALIASES = {
    "label":         ["label", "description", "item", "item description"],
    "dollar_amount": ["dollar amount", "amount", "value", "available funds", "available collections"],
}

_ALIAS_LOOKUP = {}
for canonical, aliases in COLUMN_ALIASES.items():
    for a in aliases:
        _ALIAS_LOOKUP[a.lower().replace(" ", "")] = canonical
    _ALIAS_LOOKUP[canonical.lower().replace(" ", "")] = canonical

def _norm(s):
    return str(s).lower().strip().replace(" ", "").replace("_", "").replace("-", "").replace(":", "").replace(".", "")

# ---------------------------------------------------------------------------
# Core parsing logic
# ---------------------------------------------------------------------------

def cell_text(cell):
    t = cell.get_text(separator=" ", strip=True)
    t = t.replace("\xa0", " ").replace("\n", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def parse_html_table(table_html):
    # Use lxml for much faster parsing
    soup = BeautifulSoup(table_html, "lxml")
    rows = soup.find_all("tr")
    if not rows:
        # FALLBACK: Text-based table (e.g. BMW / older filings)
        text = soup.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        records = []
        for line in lines:
            # Look for 2+ spaces, 2+ &nbsp;, or tabs
            parts = re.split(r'\s{2,}|\t|\xa0{2,}', line)
            parts = [p.strip() for p in parts if p.strip()]
            if len(parts) >= 2:
                records.append(parts)
        
        if not records: return None
        
        # Max columns
        max_c = max(len(r) for r in records)
        emit_names = ["label"] + [f"col_{i}" for i in range(1, max_c)]
        # Pad records
        for i in range(len(records)):
            while len(records[i]) < max_c: records[i].append("")
        
        df = pd.DataFrame(records, columns=emit_names[:max_c])
        # Add dollar_amount from first numeric col
        for c in df.columns[1:]:
            vals = df[c].astype(str).str.replace(r'[$,\s()\-]', '', regex=True)
            if not vals[vals != ""].empty and pd.to_numeric(vals[vals != ""], errors='coerce').notna().sum() >= 1:
                df['dollar_amount'] = df[c]
                break
        return df

    max_cols = 0
    for row in rows:
        cells = row.find_all(["td", "th"])
        w = sum(int(c.get("colspan", 1)) for c in cells)
        if w > max_cols:
            max_cols = w
    if max_cols == 0: return None

    # Find a layout row: the first row with at least 2 cells, or just the first row
    layout_row = rows[0]
    for row in rows:
        if len(row.find_all(["td", "th"])) >= 2:
            layout_row = row
            break
            
    layout_cells = layout_row.find_all(["td", "th"])
    slot_widths = []
    for cell in layout_cells:
        span = int(cell.get("colspan", 1))
        style = cell.get("style", "")
        match = re.search(r"width:([\d.]+)%", style)
        w = float(match.group(1)) if match else 0.0
        slot_widths.extend([w] * span)
    while len(slot_widths) < max_cols: slot_widths.append(0.0)

    SPACER_MAX = 0.5
    DOLLAR_MAX = 5.0 # Increased to catch Ally's wider columns
    LABEL_MIN  = 15.0 # Lowered to avoid missing short labels

    slot_type = []
    has_label = False
    for i, w in enumerate(slot_widths):
        # Peak at the content of this slot across all rows to see if it's empty
        is_empty = True
        for row_idx in range(1, min(10, len(rows))):
            r_cells = rows[row_idx].find_all(["td", "th"])
            if i < len(r_cells):
                if r_cells[i].get_text(strip=True):
                    is_empty = False
                    break
                    
        if w <= SPACER_MAX and is_empty:
            slot_type.append("spacer")
        elif w <= DOLLAR_MAX:
            slot_type.append("dollar")
        elif w >= LABEL_MIN:
            slot_type.append("label")
            has_label = True
        else:
            slot_type.append("dollar")

    if not has_label and slot_widths:
        biggest = max(range(max_cols), key=lambda i: slot_widths[i])
        slot_type[biggest] = "label"

    emit_slots = [i for i, t in enumerate(slot_type) if t != "spacer"]
    emit_names = []
    label_count = dollar_count = 0
    for i in emit_slots:
        t = slot_type[i]
        if t == "label":
            emit_names.append("label" if label_count == 0 else f"label_{label_count}")
            label_count += 1
        else:
            emit_names.append(f"col_{i}")
            dollar_count += 1

    records = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        slot_data = [""] * max_cols
        slot = 0
        for cell in cells:
            span = int(cell.get("colspan", 1))
            text = cell_text(cell)
            for s in range(slot, min(slot + span, max_cols)):
                slot_data[s] = text
            slot += span
        record = [slot_data[s] for s in emit_slots]
        if any(v.strip() and v != "$" for v in record):
            records.append(record)

    if not records: return None

    df = pd.DataFrame(records, columns=emit_names)
    df = _harmonize_columns(df)

    if 'label' not in df.columns or 'dollar_amount' not in df.columns:
        text_cols = []
        numeric_cols = []
        for c in df.columns:
            if c == 'accession_number': continue
            # Basic cleanup: remove common money symbols, commas, and parentheses for negative values
            # Also handle dashes which are common for zero
            vals = df[c].dropna().astype(str).str.replace(r'[$,\s()\-]', '', regex=True)
            try:
                # If at least 2 non-empty values can be turned into numbers, it's numeric
                clean_vals = vals[vals != '']
                if not clean_vals.empty:
                    nums = pd.to_numeric(clean_vals, errors='coerce')
                    if nums.notna().sum() >= 1: # At least one valid number
                        numeric_cols.append(c)
                    else:
                        text_cols.append(c)
                else:
                    text_cols.append(c)
            except:
                text_cols.append(c)

        if 'label' not in df.columns and text_cols:
            df = df.rename(columns={text_cols[0]: 'label'})
        
        # COALESCE: Build 'dollar_amount' from all numeric columns
        # (World Omni/Santander use multi-column layouts where totals shift right)
        if numeric_cols:
            def pick_value(row):
                for col in numeric_cols:
                    v = str(row[col]).strip()
                    if v and v != "$" and v != "0.00" and v != "0":
                        return row[col]
                # If all are empty/zero, just pick the last one (often the total)
                return row[numeric_cols[-1]]
            
            df['dollar_amount'] = df.apply(pick_value, axis=1)

    return df

def _harmonize_columns(df):
    rename_map = {}
    for col in df.columns:
        key = _norm(col)
        if key in _ALIAS_LOOKUP: rename_map[col] = _ALIAS_LOOKUP[key]
    return df.rename(columns=rename_map)

def extract_metadata(raw_content):
    patterns = {
        "accession":  r"ACCESSION NUMBER:\s+(.+)",
        "cik":        r"CENTRAL INDEX KEY:\s+(.+)",
        "company":    r"COMPANY CONFORMED NAME:\s+(.+)",
        "period":     r"CONFORMED PERIOD OF REPORT:\s+(\d+)",
        "filed_date": r"FILED AS OF DATE:\s+(\d+)",
        "abs_class":  r"ABS ASSET CLASS:\s+(.+)",
    }
    results = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_content, re.IGNORECASE)
        results[key] = match.group(1).strip() if match else ""
    return (results["accession"], results["company"], results["period"],
            results["filed_date"], results["cik"], results["abs_class"])

def extract_exhibit_99(raw_content):
    # Avoid catastrophic backtracking on massive SEC txt files
    # by using fast string searching instead of regex.
    start_idx = raw_content.find("<TYPE>EX-99")
    if start_idx == -1: return raw_content
    
    text_start = raw_content.find("<TEXT>", start_idx)
    if text_start == -1: return raw_content
    text_start += 6 # len("<TEXT>")
    
    text_end = raw_content.find("</TEXT>", text_start)
    if text_end == -1: return raw_content
    
    return raw_content[text_start:text_end]

def load_filing_urls(filepath):
    urls = []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        # Check if file has header
        first_line = f.readline()
        if '|' not in first_line: # Header-less?
            pass
        else:
            f.seek(0)
            f.readline() # skip real header
        
        for line in f:
            parts = line.strip().split("|")
            if len(parts) >= 3:
                full_url = "https://www.sec.gov/Archives/" + parts[2].strip()
                if "Archives/Archives" in full_url: full_url = full_url.replace("Archives/Archives", "Archives")
                urls.append(full_url)
    print(f"Loaded {len(urls)} URLs. Sample: {urls[0] if urls else 'None'}")
    return urls

def extract_table(soup, anchor_texts, table_id=""):
    """
    Finds the best matching table in the content using a scoring system.
    Parses all <table> tags and scores them based on keyword density and layout.
    """
    tables = soup.find_all("table")
    
    best_df = None
    max_score = -1
    
    # Target-specific bonus keywords
    target_bonuses = {
        "table_2_available_funds": ["total collections", "available collections", "total available", "total funds", "collections on receivables", "total available amount", "total available collections", "funds available for distribution"],
        "table_5_note_balance":      ["ending note balance", "ending pool factor", "ending principal balance", "aggregate note balance"]
    }
    # Penalties are removed to avoid skipping Single-Table exhibits (e.g. World Omni 2006)
    penalties = {} 

    for t_idx, t in enumerate(tables):
        t_html = str(t)
        t_text = t.get_text(" ").lower()
        
        score = 0
        # 1. Base keyword matches
        for anchor in anchor_texts:
            if anchor.lower() in t_text:
                score += 10
        
        # 2. Target specific bonuses/penalties
        if table_id in target_bonuses:
            for kw in target_bonuses[table_id]:
                if kw in t_text: score += 20
        if table_id in penalties:
            for kw in penalties[table_id]:
                if kw in t_text: score -= 30

        if score <= 0: continue

        # 3. Structural score: rows + numeric cell density
        df_candidate = parse_html_table(t_html)
        if df_candidate is not None and not df_candidate.empty:
            if len(df_candidate) >= 3:
                score += 5

            # 4. Numeric cell density: count cells with dollar-like values.
            # TOC tables have zero numeric cells; data tables have many.
            # This is the key fix for Ally/BMW/CarMax multi-table exhibits.
            numeric_cell_count = 0
            _dollar_re = re.compile(r"[\d,]+\.\d{2}")
            for cell in t.find_all(["td", "th"]):
                txt = cell.get_text(strip=True)
                if _dollar_re.search(txt):
                    numeric_cell_count += 1
            if numeric_cell_count > 0:
                score += 15 + numeric_cell_count * 3  # Big boost for having actual numbers
            
            if score > max_score:
                max_score = score
                best_df = df_candidate

    return best_df

def scrape_one_url(url):
    """Worker function for ThreadPool using persistent TLS sessions."""
    try:
        # print(f"DEBUG: Starting {url}...")
        time.sleep(1.05)  # 10 threads * 1.05s = ~9.5 RPS (Under SEC 10 limit)
        session = get_session()
        # print(f"Fetching: {url}") # Too noisy for 40k, but good for debug
        response = session.get(url, timeout=15)
        if response.status_code == 429:
            time.sleep(5)
            response = session.get(url, timeout=15)
        response.raise_for_status()
        raw_content = response.text
        accession, company, period, filed_date, cik, abs_class = extract_metadata(raw_content)

        # Evaluate ABS ASSET CLASS strictly first
        abs_class_lower = abs_class.lower().strip()
        is_auto = abs_class_lower in AUTO_ABS_CLASSES
        
        # Phase 2 Fallback: If ABS class is missing or weirdly spelled, check company name and ABS class for safe keywords
        meta = {
            "accession_number": accession,
            "company_name":     company,
            "report_period":    period,
            "filed_date":       filed_date,
            "cik":              cik,
            "abs_class":        abs_class,
        }

        # 2. Extract Asset Class (Filter for Auto)
        asset_class = re.search(r'ABS ASSET CLASS:\s*(.*)', raw_content, re.IGNORECASE)
        ac_val = asset_class.group(1).strip().lower() if asset_class else "unknown"
        
        # 3. Decision: Keep if Asset Class is 'Auto' OR Company Name contains Auto-keywords
        co_name = meta['company_name'].lower()
        
        # Explicitly skip credit cards (common false positives for 'car')
        if 'credit card' in co_name and 'master trust' in co_name:
             return {'url': url, 'scraped': False, 'reason': 'Explicitly credit card', 'meta': meta}

        auto_keywords = [
            'auto', 'moter', 'ford', 'toyota', 'honda', 'nissan', 'hyundai', 
            'volkswagen', 'santander', 'gm financial', 'americredit', 'carvana', 
            'carmax', 'exeter', 'flagship', 'harley', 'mercedes', 'bmw', 'tesla'
        ]
        
        is_auto_ac = 'auto' in ac_val or 'lease' in ac_val
        is_auto_co = any(kw in co_name for kw in auto_keywords)
        
        if not (is_auto_ac or is_auto_co):
            return {'url': url, 'scraped': False, 'reason': f'Not auto: {ac_val} | {meta["company_name"]}', 'meta': meta}
        
        # Update meta with the found asset class
        meta['asset_class'] = ac_val

        frames = {}
        exhibit_content = extract_exhibit_99(raw_content)
        # Parse the entire exhibit OR document only ONCE.
        soup = BeautifulSoup(exhibit_content, "lxml")
        
        for name, anchors in TABLES:
            df = extract_table(soup, anchors, table_id=name)
            if df is not None:
                df.insert(0, "accession_number", accession)
                frames[name] = df
        
        return {"url": url, "scraped": True, "meta": meta, "tables": frames}
    except Exception as e:
        return {"url": url, "scraped": False, "reason": f"error:{str(e)[:50]}"}

def save_batch(batch_data, append=True):
    """Saves a batch of results collected by threads."""
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    metadata_rows = [d["meta"] for d in batch_data if d["scraped"]]
    processed_urls = [d["url"] for d in batch_data]
    
    # Save Metadata
    if metadata_rows:
        meta_path = os.path.join(OUTPUT_DIR, "metadata.csv")
        df_meta = pd.DataFrame(metadata_rows)
        hdr = not os.path.exists(meta_path)
        df_meta.to_csv(meta_path, mode='a', index=False, header=hdr)

    # Save Tables
    for name, _ in TABLES:
        table_frames = [d["tables"][name] for d in batch_data if d["scraped"] and name in d["tables"]]
        if table_frames:
            out_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df_table = pd.concat(table_frames, ignore_index=True)
            hdr = not os.path.exists(out_path)
            df_table.to_csv(out_path, mode='a', index=False, header=hdr)

    # Save Processed URLs
    proc_path = os.path.join(OUTPUT_DIR, "processed_urls.txt")
    with open(proc_path, "a") as f:
        for url in processed_urls:
            f.write(url + "\n")

if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "all_10D_ABS.txt"
    if len(sys.argv) > 2:
        OUTPUT_DIR = sys.argv[2]
        print(f"Set Output Directory: {OUTPUT_DIR}")
        
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
        
    all_urls = load_filing_urls(filepath)
    print(f"Loaded {len(all_urls)} URLs.")

    # Load Resume State
    processed_set = set()
    proc_file = os.path.join(OUTPUT_DIR, "processed_urls.txt")
    if os.path.exists(proc_file):
        with open(proc_file, "r") as f:
            processed_set = set(line.strip() for line in f if line.strip())
    
    to_scrape = [u for u in all_urls if u not in processed_set]
    print(f"Resuming: {len(processed_set)} already done. {len(to_scrape)} remaining.")

    batch = []
    scraped_cnt = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {executor.submit(scrape_one_url, url): url for url in to_scrape}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            batch.append(result)
            
            if result["scraped"]:
                scraped_cnt += 1
                print(f"[{i+1}/{len(to_scrape)}] Scraped: {result['meta']['company_name'][:30]}")
            elif "error" in result["reason"]:
                print(f"[{i+1}/{len(to_scrape)}] ERROR: {result['reason']}")
            
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"--- Throughput: {rate:.2f} urls/sec. Checked {i+1} total. Scraped {scraped_cnt} ---")

            if len(batch) >= SAVE_EVERY:
                save_batch(batch, append=True)
                batch = []

    if batch:
        save_batch(batch, append=True)
    
    print(f"Finished. Total Scraped: {scraped_cnt}. Total Checked: {len(to_scrape)}")